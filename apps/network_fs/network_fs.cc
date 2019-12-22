#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/gate.hh>
#include <vector>

using namespace seastar;

using std::vector;
using std::cerr;
using std::cout;
using std::endl;

namespace {

constexpr uint32_t alignment = 4096;

template<typename T>
future<std::optional<T>> read_object(input_stream<char>& input) {
	return input.read_exactly(sizeof(T)).then([] (temporary_buffer<char> buff) {
		if (buff.size() != sizeof(T))
			return make_ready_future<std::optional<T>>(std::nullopt);
		T value;
		std::memcpy(&value, buff.get(), sizeof(value));
		return make_ready_future<std::optional<T>>(std::move(value)); // TODO: add htons etc?
	});
}

// string format - length:size_t string:char[]
template <>
future<std::optional<sstring>> read_object(input_stream<char>& input) {
	return read_object<size_t>(input).then([&input] (std::optional<size_t> size) {
		if (size.has_value()) {
			return input.read_exactly(size.value()).then([] (temporary_buffer<char> buff) {
				return make_ready_future<std::optional<sstring>>(
					std::optional<sstring>(sstring(buff.begin(), buff.end())));
			});
		}
		return make_ready_future<std::optional<sstring>>(std::nullopt);
	});
}

future<> read_objects(input_stream<char>&) {
	return make_ready_future<>();
}

template<typename T1, typename... T>
future<> read_objects(input_stream<char>& input, T1 &head, T&... ts){
    return read_object<T1>(input).then([&input, &head, &ts...] (std::optional<T1> ret) {
		if (!ret.has_value())
			return make_exception_future<>(std::runtime_error("Couldn't read all expected objects"));
		head = ret.value();
		return read_objects(input, ts...);
	});
}

template<typename T>
future<> write_object(output_stream<char>& output, T&& obj) {
	char buff[sizeof(obj)];
	std::memcpy(buff, &obj, sizeof(obj));
	return output.write(buff, sizeof(obj));
}

template<>
future<> write_object(output_stream<char>& output, sstring&& str) {
	return output.write(str.size()).then([&output, str = std::move(str)] () {
		return output.write(std::move(str));
	});
}

template<>
future<> write_object(output_stream<char>& output, temporary_buffer<char>&& buff) {
	return output.write(buff.size()).then([&output, buff = std::move(buff)] () {
		return output.write(buff.get(), buff.size());
	});
}

template<>
future<> write_object(output_stream<char>& output, vector<sstring>&& vec) { // TODO: partial specialization?
	return output.write(vec.size()).then([&output, vec = std::move(vec)] () {
		return seastar::do_for_each(vec, [&output] (sstring obj) { // TODO: solve copying
            return write_object<sstring>(output, std::move(obj));
        });
	});
}

future<> write_objects(output_stream<char>&) {
	return make_ready_future<>();
}

template<typename T1, typename... T>
future<> write_objects(output_stream<char>& output, T1&& head, T&&... ts){
    return write_object<T1>(output, std::forward<T1>(head)).then([&output, &ts...] () {
		return write_objects(output, std::forward<T>(ts)...);
	});
}

struct Files {
	std::unordered_map<int, file> fd_map;
	int curr_fd = 0;
} files;

// input format  - path:str flags:int
// output format - retopen err:int [fd:int]
future<> handle_open(input_stream<char>& input, output_stream<char>& output) {
	return do_with(sstring(), 0, 0, [&input, &output] (sstring& path, int& flags, int& fd) {
		return read_objects(input, path, flags).then([&path/*, &flags*/, &fd] { // TODO: use flags
			return open_file_dma(path, open_flags::rw).then([&fd] (auto file) {
				fd = files.curr_fd++;
				files.fd_map[fd] = std::move(file);
			});
		}).then([&output, &fd] {
			// TODO: send sstring not char[]
			return write_objects(output, "retopen", " 0 ", to_sstring(fd)); // TODO: change sstring to int
		}).then([&output] {
			return output.flush();
		});
	}).handle_exception([&output] (std::exception_ptr e) {
		cerr << "An error occurred in handle_open" << endl;
		return write_objects(output, "retopen", " -1").then([&output] {
			return output.flush();
		}).then([e] {
			return make_exception_future(e);
		});
	});
}

// input format  - fd:int
// output format - retclose err:int
future<> handle_close(input_stream<char>& input, output_stream<char>& output) {
	return do_with(0, [&input, &output] (int& fd) {
		return read_objects(input, fd).then([&fd] () {
			auto it = files.fd_map.find(fd);
			if (it == files.fd_map.end())
				return make_exception_future<>(std::runtime_error("Couldn't find given fd"));
			file file = it->second;
			return file.close().then([it = std::move(it)] {
				files.fd_map.erase(it);
			});
		}).then([&output] {
			return write_objects(output, "retclose", " 0"); // TODO: change sstring to int
		}).then([&output] {
			return output.flush();
		});
	}).handle_exception([&output] (std::exception_ptr e) {
		cerr << "An error occurred in handle_close" << endl;
		return write_objects(output, "retclose", " -1").then([&output] {
			return output.flush();
		}).then([e] {
			return make_exception_future(e);
		});
	});
}

// input format  - fd:int count:size_t offset:off_t
// output format - retpread err:int [buff:str]
future<> handle_pread(input_stream<char>& input, output_stream<char>& output) {
	return do_with(0, (size_t)0, (off_t)0, [&input, &output] (int& fd, size_t& count, off_t& offset) {
		return read_objects(input, fd, count, offset).then([&fd, &count, &offset] () {
			auto it = files.fd_map.find(fd);
			if (it == files.fd_map.end())
				return make_exception_future<temporary_buffer<char>>
				       (std::runtime_error("File is not open"));
			file file = it->second;
			return file.dma_read<char>(offset, count);
		}).then([&output] (temporary_buffer<char> read_buf) {
			return write_objects(output, "retpread", " 0 ", std::move(read_buf));
		}).then([&output] {
			return output.flush();
		});
	}).handle_exception([&output] (std::exception_ptr e) {
		cerr << "An error occurred in handle_pread" << endl;
		return write_objects(output, "retpread", " -1").then([&output] {
			return output.flush();
		}).then([e] {
			return make_exception_future(e);
		});
	});
}

// input format  - fd:int buff:str count:size_t
// output format - retpwrite err:int [size:size_t]
// TODO: solve alignment problem
future<> handle_pwrite(input_stream<char>& input, output_stream<char>& output) {
	return do_with(0, sstring(), (size_t)0, (off_t)0,
	                        [&input, &output]
	                        (int& fd, sstring& buff, size_t& count, off_t& offset) {
		return read_objects(input, fd, buff, count, offset).then([&fd, &buff, &count, &offset] () {
			if (count % alignment || offset % alignment)
				return make_exception_future<size_t>(std::runtime_error("count and offset not aligned"));
			auto it = files.fd_map.find(fd);
			if (it == files.fd_map.end())
				return make_exception_future<size_t>(std::runtime_error("Couldn't find given fd"));
			file file = it->second;
			auto temp_buf = temporary_buffer<char>::aligned(alignment, buff.size());
			std::memcpy(temp_buf.get_write(), buff.data(), buff.size());
			return file.dma_write<char>(offset, temp_buf.get(), count)
					.then([temp_buf = std::move(temp_buf)] (size_t write_size) {
				return write_size;
			});
		}).then([&output] (size_t write_size) {
			return write_objects(output, "retpwrite", " 0 ", write_size);
		}).then([&output] {
			return output.flush();
		});
	}).handle_exception([&output] (std::exception_ptr e) {
		cerr << "An error occurred in handle_pwrite" << endl;
		return write_objects(output, "retpwrite", " -1").then([&output] {
			return output.flush();
		}).then([e] {
			return make_exception_future(e);
		});
	});
}

// input format  - path:str
// output format - retreaddir err:int size:int [name:str]{size}
future<> handle_readdir(input_stream<char>& input, output_stream<char>& output) {
	return do_with(sstring(), [&input, &output] (sstring& path) {
		return read_objects(input, path).then([&path] () {
			return open_directory(path);
		}).then([] (file dir) {
			auto vec = make_lw_shared(vector<sstring>());
			auto listing_lmb = dir.list_directory([vec] (directory_entry de) {
					cerr << de.name << endl;
					vec->emplace_back(std::move(de.name));
					return make_ready_future<>();
				});
			auto listing = make_lw_shared(std::move(listing_lmb));
			return listing->done().then([vec, listing, dir] {
				return std::move(*vec);
			});
		}).then([&output] (vector<sstring> vec) {
			return write_objects(output, "retreaddir", " 0 ", std::move(vec));
		}).then([&output] {
			return output.flush();
		});
	}).handle_exception([&output] (std::exception_ptr e) {
		cerr << "An error occurred in handle_readdir" << endl;
		return write_objects(output, "retreaddir", " -1").then([&output] {
			return output.flush();
		}).then([e] {
			return make_exception_future(e);
		});
	});
}

// input format  - path:str
// output format - retgetattr err:int [stat:stat]
future<> handle_getattr(input_stream<char>& input, output_stream<char>& output) {
	return do_with(sstring(), [&input, &output] (sstring& path) {
		return read_objects(input, path).then([&path] () {
			return open_file_dma(path, open_flags::ro);
		}).then([] (file file) {
			return file.stat();
		}).then([&output] (struct stat stat) {
			return write_objects(output, "retgetattr", " 0 ", stat); // TODO: should we pack struct stat before sending?
		}).then([&output] {
			return output.flush();
		});
	}).handle_exception([&output] (std::exception_ptr e) {
		cerr << "An error occurred in handle_getattr" << endl;
		return write_objects(output, "retgetattr", " -1").then([&output] {
			return output.flush();
		}).then([e] {
			return make_exception_future(e);
		});
	});
}

future<> handle_single_operation(input_stream<char>& input, output_stream<char>& output) {
	// read operation name and decide which operation handler to start
	return read_object<sstring>(input).then([&input, &output] (std::optional<sstring> option) {
		if (!option.has_value())
			return make_exception_future<>(std::runtime_error("Error while reading operation name"));
		cerr << "operation: " << option.value() << endl;

		future<> operation = make_ready_future<>();
		if (option.value() == "open")
			operation = handle_open(input, output);
		else if (option.value() == "close")
			operation = handle_close(input, output);
		else if (option.value() == "pread")
			operation = handle_pread(input, output);
		else if (option.value() == "pwrite")
			operation = handle_pwrite(input, output);
		else if (option.value() == "readdir")
			operation = handle_readdir(input, output);
		else if (option.value() == "getattr")
			operation = handle_getattr(input, output);
		else
			operation = make_exception_future<>(std::runtime_error("Operation name invalid"));

		return operation;
	});
}

future<> handle_connection(connected_socket connection, socket_address remote_address) {
	cerr << "New connection from " << remote_address << endl;
	return do_with(connection.input(), connection.output(),
			[] (input_stream<char>& input, output_stream<char>& output) {
		return repeat([&input, &output] {
			return handle_single_operation(input, output).then([] () {
				return stop_iteration::no;
			});
		}).finally([&output] {
			return output.close();
		});
	}).finally([connection = std::move(connection), remote_address = std::move(remote_address)] {
		cerr << "Closing connection with " << remote_address << endl;
	});
}

future<> start_server(uint16_t port) {
	return do_with(engine().listen(make_ipv4_address({port})),
			gate(), [] (server_socket& listener, gate& gate) {
		return keep_doing([&listener, &gate] () {
			return listener.accept().then([&gate] (accept_result connection) {
				auto connection_handler = with_gate(gate, [connection = std::move(connection)] () mutable {
					return handle_connection(std::move(connection.connection), std::move(connection.remote_address))
							.handle_exception([] (std::exception_ptr e) {
						cerr << "An error occurred: " << e << endl;
					});
				});
			});
		}).finally([&gate] {
			return gate.close();
		});
	});

}

}

int main(int argc, char** argv) {
	app_template app;
	namespace bpo = boost::program_options;
	app.add_options()
		("port,p", bpo::value<uint16_t>()->default_value(6969), "port to listen on");

	try {
		app.run(argc, argv, [&app] {
			auto& args = app.configuration();
			return start_server(args["port"].as<uint16_t>())
					.handle_exception([] (std::exception_ptr e) {
				cerr << "An error occurred: " << e << endl;
			});
		});
	} catch(...) {
		cerr << "Couldn't start application: "
		          << std::current_exception() << "\n";
		return 1;
	}
	return 0;
}
