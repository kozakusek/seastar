/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2019 ScyllaDB
 */

#include "fs/cluster.hh"
#include "fs/cluster_allocator.hh"
#include "fs/metadata_log.hh"
#include "seastar/core/aligned_buffer.hh"
#include "seastar/core/do_with.hh"
#include "seastar/core/future-util.hh"
#include "seastar/core/future.hh"
#include "seastar/fs/overloaded.hh"

#include <boost/crc.hpp>
#include <boost/range/irange.hpp>
#include <cstddef>
#include <cstring>
#include <limits>
#include <seastar/core/units.hh>
#include <stdexcept>
#include <unordered_set>
#include <variant>

namespace seastar::fs {

metadata_log::metadata_log(block_device device, uint32_t cluster_size, uint32_t alignment)
: _device(std::move(device))
, _cluster_size(cluster_size)
, _alignment(alignment)
, _curr_cluster_buff(cluster_size, alignment, 0)
, _cluster_allocator({}, {}) {
    assert(is_power_of_2(alignment));
    assert(cluster_size > 0 and cluster_size % alignment == 0);
}

static unix_metadata ondisk_metadata_to_metadata(const ondisk_unix_metadata& ondisk_metadata) noexcept {
    unix_metadata res;
    static_assert(sizeof(ondisk_metadata) == 28, "metadata size changed: check if below assignments needs update");
    res.mode = ondisk_metadata.mode;
    res.uid = ondisk_metadata.uid;
    res.gid = ondisk_metadata.gid;
    res.mtime_ns = ondisk_metadata.mtime_ns;
    res.ctime_ns = ondisk_metadata.ctime_ns;
    return res;
}

future<> metadata_log::bootstrap(cluster_id_t first_metadata_cluster_id, cluster_range available_clusters) {
    // Clear the metadata log
    _inodes.clear();
    // Bootstrap
    return do_with(std::optional(first_metadata_cluster_id), temporary_buffer<uint8_t>::aligned(_alignment, _cluster_size), std::unordered_set<cluster_id_t>{}, [this, available_clusters](auto& cluster_id, auto& buff, auto& taken_clusters) {
        return repeat([this, &cluster_id, &taken_clusters, &buff] {
            if (not cluster_id) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }

            disk_offset_t curr_cluster_offset = cluster_id_to_offset(*cluster_id, _cluster_size);
            return _device.read(curr_cluster_offset, buff.get_write(), _cluster_size).then([this](size_t bytes_read) {
                if (bytes_read != _cluster_size) {
                    return make_exception_future(std::runtime_error("Failed to read whole metadata log cluster"));
                }
                return now();

            }).then([this, &cluster_id, &taken_clusters, &buff, curr_cluster_offset] {
                taken_clusters.emplace(*cluster_id);
                size_t pos = 0;
                auto load_entry = [this, &buff, &pos](auto& entry) {
                    if (pos + sizeof(entry) > _cluster_size) {
                        return false;
                    }

                    memcpy(&entry, buff.get() + pos, sizeof(entry));
                    pos += sizeof(entry);
                    return true;
                };

                auto invalid_entry_exception = [] {
                    return make_exception_future<stop_iteration>(std::runtime_error("Invalid metadata log entry"));
                };

                // Process cluster: the data layout format is:
                // | checkpoint1 | data1... | checkpoint2 | data2... |
                bool log_ended = false;
                cluster_id = std::nullopt;
                for (;;) {
                    if (cluster_id) {
                        break; // Committed block with next cluster_id marks the end of the current metadata log cluster
                    }

                    ondisk_type entry_type;
                    if (not load_entry(entry_type) or entry_type != CHECKPOINT) {
                        log_ended = true; // Invalid entry
                        break;
                    }

                    ondisk_checkpoint checkpoint;
                    if (not load_entry(checkpoint) or pos + checkpoint.checkpointed_data_length > _cluster_size) {
                        log_ended = true; // Invalid checkpoint
                        break;
                    }

                    boost::crc_32_type crc;
                    crc.process_bytes(buff.get() + pos, checkpoint.checkpointed_data_length);
                    crc.process_bytes(&checkpoint.checkpointed_data_length, sizeof(checkpoint.checkpointed_data_length));
                    if (crc.checksum() != checkpoint.crc32_code) {
                        log_ended = true; // Invalid CRC code
                        break;
                    }

                    auto checkpointed_data_end = pos + checkpoint.checkpointed_data_length;
                    while (pos < checkpointed_data_end) {
                        switch (entry_type) {
                        case INVALID: {
                            return invalid_entry_exception();
                        }

                        case NEXT_METADATA_CLUSTER: {
                            ondisk_next_metadata_cluster entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            cluster_id = (cluster_id_t)entry.cluster_id;
                            continue;
                        }

                        case CHECKPOINT: {
                            // CHECKPOINT is handled before entering the switch
                            return invalid_entry_exception();
                        }

                        case CREATE_INODE: {
                            ondisk_create_inode entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto [it, was_inserted] = _inodes.emplace((inode_t)entry.inode, inode_info {
                                0,
                                0, // TODO: maybe creating dangling inode (not attached to any directory is invalid), if not then we have to drop them after finishing bootstraping, because at that point dangling inodes are invaild (that's how I see it now)
                                ondisk_metadata_to_metadata(entry.metadata),
                                [&]() -> decltype(inode_info::contents) {
                                    if (entry.is_directory) {
                                        return inode_info::directory {};
                                    } else {
                                        return inode_info::file {};
                                    }
                                }()
                            });

                            if (not was_inserted) {
                                return invalid_entry_exception(); // inode already exists
                            }
                            continue;
                        }

                        case UPDATE_METADATA: {
                            ondisk_update_metadata entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            it->second.metadata = ondisk_metadata_to_metadata(entry.metadata);
                            continue;
                        }

                        case DELETE_INODE: {
                            // TODO: for compaction: update used inode_data_vec
                            ondisk_delete_inode entry;
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            inode_info& inode_info = it->second;
                            if (inode_info.directories_containing_file > 0) {
                                return invalid_entry_exception();
                            }

                            bool failed = std::visit(overloaded {
                                [](const inode_info::directory& dir) {
                                    return (not dir.entries.empty());
                                },
                                [](const inode_info::file&) {
                                    return false;
                                }
                            }, inode_info.contents);
                            if (failed) {
                                return invalid_entry_exception();
                            }

                            _inodes.erase(it);
                            continue;
                        }

                        case SMALL_WRITE: {
                            // TODO: for compaction: update used inode_data_vec
                            ondisk_small_write_header entry;
                            if (not load_entry(entry) or pos + entry.length > checkpointed_data_end) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            temporary_buffer<uint8_t> data(buff.get() + pos, entry.length);
                            pos += entry.length;

                            inode_data_vec data_vec = {
                                {entry.offset, entry.offset + entry.length},
                                inode_data_vec::in_mem_data {std::move(data)}
                            };

                            inode_info& inode_info = it->second;
                            if (not std::holds_alternative<inode_info::file>(inode_info.contents)) {
                                return invalid_entry_exception();
                            }

                            auto& file = std::get<inode_info::file>(inode_info.contents);
                            write_update(file, std::move(data_vec));
                            inode_info.metadata.mtime_ns = entry.mtime_ns;
                            continue;
                        }

                        case MTIME_UPDATE: {
                            ondisk_mtime_update entry;
                            if (not load_entry(entry) or _inodes.count(entry.inode) != 1) {
                                return invalid_entry_exception();
                            }

                            _inodes[entry.inode].metadata.mtime_ns = entry.mtime_ns;
                            continue;
                        }

                        case TRUNCATE: {
                            ondisk_truncate entry;
                            // TODO: add checks for being directory
                            if (not load_entry(entry)) {
                                return invalid_entry_exception();
                            }

                            auto it = _inodes.find(entry.inode);
                            if (it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            inode_info& inode_info = it->second;
                            if (not std::holds_alternative<inode_info::file>(inode_info.contents)) {
                                return invalid_entry_exception();
                            }

                            auto& file = std::get<inode_info::file>(inode_info.contents);
                            auto fsize = file.size();
                            if (entry.size > fsize) {
                                file.data.emplace(fsize, inode_data_vec {
                                    {fsize, entry.size},
                                    inode_data_vec::hole_data {}
                                });
                            } else {
                                cut_out_data_range(file, {
                                    entry.size,
                                    std::numeric_limits<decltype(file_range::end)>::max()
                                });
                            }

                            inode_info.metadata.mtime_ns = entry.mtime_ns;
                            continue;
                        }

                        case ADD_DIR_ENTRY: {
                            ondisk_add_dir_entry_header entry;
                            if (not load_entry(entry) or pos + entry.entry_name_length > checkpointed_data_end or _inodes.count(entry.dir_inode) != 1 or _inodes.count(entry.entry_inode) != 1) {
                                return invalid_entry_exception();
                            }

                            sstring dir_entry_name((const char*)buff.get() + pos, entry.entry_name_length);
                            pos += entry.entry_name_length;

                            inode_info& dir_inode_info = _inodes[entry.dir_inode];
                            inode_info& dir_entry_inode_info = _inodes[entry.entry_inode];

                            if (not std::holds_alternative<inode_info::directory>(dir_inode_info.contents)) {
                                return invalid_entry_exception();
                            }
                            auto& dir = std::get<inode_info::directory>(dir_inode_info.contents);

                            if (std::holds_alternative<inode_info::directory>(dir_entry_inode_info.contents) and dir_entry_inode_info.directories_containing_file > 0) {
                                return invalid_entry_exception(); // Directory may only be linked once
                            }

                            dir.entries.emplace(std::move(dir_entry_name), (inode_t)entry.entry_inode);
                            ++dir_entry_inode_info.directories_containing_file;
                            continue;
                        }

                        case DELETE_DIR_ENTRY: {
                            ondisk_delete_dir_entry_header entry;
                            if (not load_entry(entry) or pos + entry.entry_name_length > checkpointed_data_end or _inodes.count(entry.dir_inode) != 1) {
                                return invalid_entry_exception();
                            }

                            sstring dir_entry_name((const char*)buff.get() + pos, entry.entry_name_length);
                            pos += entry.entry_name_length;

                            inode_info& dir_inode_info = _inodes[entry.dir_inode];
                            if (not std::holds_alternative<inode_info::directory>(dir_inode_info.contents)) {
                                return invalid_entry_exception();
                            }
                            auto& dir = std::get<inode_info::directory>(dir_inode_info.contents);

                            auto it = dir.entries.find(dir_entry_name);
                            if (it == dir.entries.end()) {
                                return invalid_entry_exception();
                            }

                            inode_t inode = it->second;
                            auto inode_it = _inodes.find(inode);
                            if (inode_it == _inodes.end()) {
                                return invalid_entry_exception();
                            }

                            inode_info& inode_info = inode_it->second;
                            assert(inode_info.directories_containing_file > 0);
                            --inode_info.directories_containing_file;

                            dir.entries.erase(it);
                            continue;
                        }

                        case RENAME_DIR_ENTRY: {
                            // TODO: implement it
                            continue;
                        }

                        case MEDIUM_WRITE: // TODO: will be very similar to SMALL_WRITE
                        case LARGE_WRITE: // TODO: will be very similar to SMALL_WRITE
                        case LARGE_WRITE_WITHOUT_MTIME: // TODO: will be very similar to SMALL_WRITE
                            // TODO: implement it
                            throw std::runtime_error("Not implemented");

                        // default: is omitted to make compiler warn if there is an unhandled type
                        }

                        // unknown type => metadata log inconsistency
                        return invalid_entry_exception();
                    }

                    if (pos != checkpointed_data_end) {
                        return invalid_entry_exception();
                    }

                    if (log_ended) {
                        // Bootstrap _curr_cluster_buff
                        _curr_cluster_buff.reset_from_bootstraped_cluster(curr_cluster_offset, buff.get(), pos);
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                }

                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        }).then([this, &available_clusters, &taken_clusters] {
            // Initialize _cluser_allocator
            std::deque<cluster_id_t> free_clusters;
            for (auto cid : boost::irange(available_clusters.beg, available_clusters.end)) {
                if (taken_clusters.count(cid) == 0) {
                    free_clusters.emplace_back(cid);
                }
            }
            _cluster_allocator = cluster_allocator(std::move(taken_clusters), std::move(free_clusters));
        });
    });
}

void metadata_log::write_update(inode_info::file& file, inode_data_vec data_vec) {
    // TODO: for compaction: update used inode_data_vec
    cut_out_data_range(file, data_vec.data_range);
    file.data.emplace(data_vec.data_range.beg, std::move(data_vec));
}

void metadata_log::cut_out_data_range(inode_info::file& file, file_range range) {
    // TODO: for compaction: update used inode_data_vec
    // Cut all vectors intersecting with range
    auto it = file.data.lower_bound(range.beg);
    if (it != file.data.begin() and are_intersecting(range, prev(it)->second.data_range)) {
        --it;
    }

    while (it != file.data.end() and are_intersecting(range, it->second.data_range)) {
        auto data_vec = std::move(it->second);
        file.data.erase(it++);
        const auto cap = intersection(range, data_vec.data_range);
        if (cap == data_vec.data_range) {
            continue; // Fully intersects => remove it
        }

        // Overlaps => cut it, possibly into two parts:
        // |       data_vec      |
        //         | cap |
        // | left  |     | right |
        inode_data_vec left, right;
        left.data_range = {data_vec.data_range.beg, cap.beg};
        right.data_range = {cap.end, data_vec.data_range.end};
        auto right_beg_shift = right.data_range.beg - data_vec.data_range.beg;
        std::visit(overloaded {
            [&](inode_data_vec::in_mem_data& mem) {
                left.data_location = inode_data_vec::in_mem_data {mem.data.share(0, left.data_range.size())};
                right.data_location = inode_data_vec::in_mem_data {mem.data.share(right_beg_shift, right.data_range.size())};
            },
            [&](inode_data_vec::on_disk_data& data) {
                left.data_location = data;
                right.data_location = inode_data_vec::on_disk_data {data.device_offset + right_beg_shift};
            },
            [&](inode_data_vec::hole_data&) {
                left.data_location = inode_data_vec::hole_data {};
                right.data_location = inode_data_vec::hole_data {};
            },
        }, data_vec.data_location);

        // Save new data vectors
        if (not left.data_range.is_empty()) {
            file.data.emplace(left.data_range.beg, std::move(left));
        }
        if (not right.data_range.is_empty()) {
            file.data.emplace(right.data_range.beg, std::move(right));
        }
    }
}

file_offset_t metadata_log::file_size(inode_t inode) const {
    auto it = _inodes.find(inode);
    if (it == _inodes.end()) {
        throw invalid_inode_exception();
    }

    return std::visit(overloaded {
        [](const inode_info::file& file) {
            return file.size();
        },
        [](const inode_info::directory&) -> file_offset_t {
            throw invalid_inode_exception();
        }
    }, it->second.contents);
}

} // namespace seastar::fs
