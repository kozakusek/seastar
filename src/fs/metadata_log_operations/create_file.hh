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
 * Copyright (C) 2020 ScyllaDB
 */

#pragma once

#include "fs/metadata_log.hh"
#include "fs/path.hh"

namespace seastar::fs {

class create_file_operation {
    metadata_log& _metadata_log;
    bool _is_directory;
    std::string _entry_name;
    file_permissions _perms;
    inode_t _dir_inode;
    inode_info::directory* _dir_info;
    ondisk_create_inode_as_dir_entry_header _ondisk_entry;

    create_file_operation(metadata_log& metadata_log) : _metadata_log(metadata_log) {}

    future<inode_t> create_file(std::string path, file_permissions perms, bool is_directory) {
        _is_directory = is_directory;
        if (is_directory) {
            while (not path.empty() and path.back() == '/') {
                path.pop_back();
            }
        }

        _entry_name = extract_last_component(path);
        if (_entry_name.empty()) {
            if (is_directory) {
                return make_exception_future<inode_t>(std::runtime_error("Invalid path"));
            } else {
                return make_exception_future<inode_t>(std::runtime_error("Path has to end with character different than '/'"));
            }
        }
        assert(path.empty() or path.back() == '/'); // Hence fast-checking for "is directory" is done in path_lookup

        _perms = perms;
        return _metadata_log.path_lookup(path).then([this](inode_t dir_inode) {
            _dir_inode = dir_inode;
            auto dir_it = _metadata_log._inodes.find(_dir_inode);
            if (dir_it == _metadata_log._inodes.end()) {
                return make_exception_future<inode_t>(operation_became_invalid_exception());
            }
            assert(dir_it->second.is_directory() and "Directory cannot become file or there is a BUG in path_lookup");
            _dir_info = &dir_it->second.get_directory();
            return _metadata_log._locks.with_locks(metadata_log::locks::shared {dir_inode},
                    metadata_log::locks::unique {dir_inode, _entry_name}, [this] {
                return create_file_in_directory();
            });
        });
    }

    future<inode_t> create_file_in_directory() {
        if (not _metadata_log.inode_exists(_dir_inode)) {
            return make_exception_future<inode_t>(operation_became_invalid_exception());
        }

        if (_dir_info->entries.count(_entry_name) != 0) {
            return make_exception_future<inode_t>(file_already_exists_exception());
        }

        decltype(_ondisk_entry.entry_name_length) entry_name_length;
        if (_entry_name.size() > std::numeric_limits<decltype(entry_name_length)>::max()) {
            // TODO: add an assert that the culster_size is not too small as it would cause to allocate all clusters
            //       and then return error ENOSPACE
            return make_exception_future<inode_t>(filename_too_long_exception());
        }
        entry_name_length = _entry_name.size();

        using namespace std::chrono;
        uint64_t now_ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
        unix_metadata unx_mtdt = {
            _perms,
            0, // TODO: Eventually, we'll want a user to be able to pass his credentials when bootstrapping the
            0, //       file system -- that will allow us to authorize users on startup (e.g. via LDAP or whatnot).
            now_ns,
            now_ns
        };

        _ondisk_entry = {
            {
                _metadata_log._inode_allocator.alloc(),
                _is_directory,
                metadata_to_ondisk_metadata(unx_mtdt)
            },
            _dir_inode,
            entry_name_length,
        };

        return _metadata_log.append_ondisk_entry(_ondisk_entry, _entry_name.data()).then([this, unx_mtdt] {
            _metadata_log.memory_only_create_inode(_ondisk_entry.entry_inode.inode, _is_directory, unx_mtdt);
            _metadata_log.memory_only_add_dir_entry(*_dir_info, _ondisk_entry.entry_inode.inode, std::move(_entry_name));
        }).then([this] {
            return make_ready_future<inode_t>((inode_t)_ondisk_entry.entry_inode.inode);
        });
    }

public:
    static future<inode_t> perform(metadata_log& metadata_log, std::string path, file_permissions perms,
            bool is_directory) {
        return do_with(create_file_operation(metadata_log),
                [path = std::move(path), perms = std::move(perms), is_directory](auto& cfo) {
            return cfo.create_file(std::move(path), std::move(perms), is_directory);
        });
    }
};

} // namespace seastar::fs
