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

#define BOOST_TEST_MODULE kafka

#include <cstdint>

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/test/included/unit_test.hpp>
#include "../../src/kafka/protocol/metadata_request.hh"
#include "../../src/kafka/protocol/metadata_response.hh"
#include "../../src/kafka/protocol/kafka_primitives.hh"
#include "../../src/kafka/protocol/api_versions_response.hh"
#include "../../src/kafka/protocol/kafka_records.hh"

using namespace seastar;

template <typename KafkaType>
void test_deserialize_serialize(std::vector<unsigned char> data,
        KafkaType &kafka_value, int16_t api_version) {
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(reinterpret_cast<char *>(data.data()),
            data.size());

    kafka_value.deserialize(input_stream, api_version);

    std::vector<unsigned char> output(data.size());
    boost::iostreams::stream<boost::iostreams::array_sink> output_stream(reinterpret_cast<char *>(output.data()),
                                                                          output.size());
    kafka_value.serialize(output_stream, api_version);

    BOOST_REQUIRE(!output_stream.bad());

    BOOST_TEST(output == data, boost::test_tools::per_element());
}

template <typename KafkaType>
void test_deserialize_throw(std::vector<unsigned char> data,
                                KafkaType &kafka_value, int16_t api_version) {
    boost::iostreams::stream<boost::iostreams::array_source> input_stream(reinterpret_cast<char *>(data.data()),
                                                                          data.size());

    BOOST_REQUIRE_THROW(kafka_value.deserialize(input_stream, api_version), kafka::parsing_exception);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_number_test) {
    kafka::kafka_number_t<uint32_t> number(15);
    BOOST_REQUIRE_EQUAL(*number, 15);

    test_deserialize_serialize({0x12, 0x34, 0x56, 0x78}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 0x12345678);

    test_deserialize_throw({0x17, 0x27}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 0x12345678);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_varint_test) {
    kafka::kafka_varint_t number(155);

    test_deserialize_serialize({0x00}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 0);

    test_deserialize_serialize({0x08}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 4);

    test_deserialize_serialize({0x07}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -4);

    test_deserialize_serialize({0xAC, 0x02}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, 150);

    test_deserialize_serialize({0xAB, 0x02}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -150);

    test_deserialize_throw({0xAC}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -150);

    test_deserialize_serialize({0xFF, 0xFF, 0xFF, 0xFF, 0xF}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -2147483648);

    test_deserialize_throw({0xFF, 0xFF, 0xFF, 0xFF, 0x1F}, number, 0);
    BOOST_REQUIRE_EQUAL(*number, -2147483648);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_string_test) {
    kafka::kafka_string_t string("321");
    BOOST_REQUIRE_EQUAL(*string, "321");

    test_deserialize_serialize({0, 5, 'a', 'b', 'c', 'd', 'e'}, string, 0);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    test_deserialize_throw({0, 4, 'a', 'b', 'c'}, string, 0);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    test_deserialize_throw({0}, string, 0);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_nullable_string_test) {
    kafka::kafka_nullable_string_t string;
    BOOST_REQUIRE(string.is_null());
    BOOST_REQUIRE_THROW((void) *string, std::exception);

    test_deserialize_serialize({0, 5, 'a', 'b', 'c', 'd', 'e'}, string, 0);
    BOOST_REQUIRE_EQUAL(*string, "abcde");
    BOOST_REQUIRE_EQUAL(string->size(), 5);

    test_deserialize_serialize({0xFF, 0xFF}, string, 0);
    BOOST_REQUIRE(string.is_null());
}

BOOST_AUTO_TEST_CASE(kafka_primitives_bytes_test) {
    kafka::kafka_bytes_t bytes;

    test_deserialize_serialize({0, 0, 0, 5, 'a', 'b', 'c', 'd', 'e'}, bytes, 0);
    BOOST_REQUIRE_EQUAL(*bytes, "abcde");
    BOOST_REQUIRE_EQUAL(bytes->size(), 5);
}

BOOST_AUTO_TEST_CASE(kafka_primitives_array_test) {
    kafka::kafka_array_t<kafka::kafka_string_t> strings;

    test_deserialize_serialize({0, 0, 0, 2, 0, 5, 'a', 'b', 'c', 'd', 'e', 0, 2, 'f', 'g'}, strings, 0);

    BOOST_REQUIRE_EQUAL(strings->size(), 2);
    BOOST_REQUIRE_EQUAL(*strings[0], "abcde");
    BOOST_REQUIRE_EQUAL(*strings[1], "fg");

    test_deserialize_throw({0, 0, 0, 2, 0, 5, 'A', 'B', 'C', 'D', 'E', 0, 2, 'F'}, strings, 0);
    BOOST_REQUIRE_EQUAL(strings->size(), 2);
    BOOST_REQUIRE_EQUAL(*strings[0], "abcde");
    BOOST_REQUIRE_EQUAL(*strings[1], "fg");
}

BOOST_AUTO_TEST_CASE(kafka_api_versions_response_parsing_test) {
    kafka::api_versions_response response;
    test_deserialize_serialize({
        0x00, 0x00, 0x00, 0x00, 0x00, 0x2d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x0b, 0x00, 0x02, 0x00, 0x00, 0x00, 0x05, 0x00, 0x03, 0x00, 0x00, 0x00, 0x08, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x00, 0x00, 0x00, 0x05,
        0x00, 0x07, 0x00, 0x00, 0x00, 0x02, 0x00, 0x08, 0x00, 0x00, 0x00, 0x07, 0x00, 0x09, 0x00, 0x00,
        0x00, 0x05, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x02, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x05, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0x03, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x02, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03,
        0x00, 0x0f, 0x00, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x11, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x12, 0x00, 0x00, 0x00, 0x02, 0x00, 0x13, 0x00, 0x00, 0x00, 0x03, 0x00, 0x14,
        0x00, 0x00, 0x00, 0x03, 0x00, 0x15, 0x00, 0x00, 0x00, 0x01, 0x00, 0x16, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x17, 0x00, 0x00, 0x00, 0x03, 0x00, 0x18, 0x00, 0x00, 0x00, 0x01, 0x00, 0x19, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1c,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x1d, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x1f, 0x00, 0x00, 0x00, 0x01, 0x00, 0x20, 0x00, 0x00, 0x00, 0x02, 0x00, 0x21, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x22, 0x00, 0x00, 0x00, 0x01, 0x00, 0x23, 0x00, 0x00, 0x00, 0x01, 0x00, 0x24,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x25, 0x00, 0x00, 0x00, 0x01, 0x00, 0x26, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x27, 0x00, 0x00, 0x00, 0x01, 0x00, 0x28, 0x00, 0x00, 0x00, 0x01, 0x00, 0x29, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2c,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    }, response, 2);

    BOOST_REQUIRE_EQUAL(*response._throttle_time_ms, 0);
    BOOST_REQUIRE_EQUAL(*response._error_code, 0);
    BOOST_REQUIRE_EQUAL(response._api_keys->size(), 45);
    BOOST_REQUIRE_EQUAL(*response._api_keys[0]._api_key, 0);
    BOOST_REQUIRE_EQUAL(*response._api_keys[0]._min_version, 0);
    BOOST_REQUIRE_EQUAL(*response._api_keys[0]._max_version, 7);
    BOOST_REQUIRE_EQUAL(*response._api_keys[1]._api_key, 1);
    BOOST_REQUIRE_EQUAL(*response._api_keys[1]._min_version, 0);
    BOOST_REQUIRE_EQUAL(*response._api_keys[1]._max_version, 11);
}

BOOST_AUTO_TEST_CASE(kafka_metadata_request_parsing_test) {
    kafka::metadata_request request;
    test_deserialize_serialize({
        0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x74, 0x65, 0x73, 0x74, 0x35, 0x01, 0x00, 0x00
    }, request, 8);

    BOOST_REQUIRE_EQUAL(request._topics->size(), 1);
    BOOST_REQUIRE_EQUAL(*request._topics[0]._name, "test5");
    BOOST_REQUIRE(*request._allow_auto_topic_creation);
    BOOST_REQUIRE(!*request._include_cluster_authorized_operations);
    BOOST_REQUIRE(!*request._include_topic_authorized_operations);
}

BOOST_AUTO_TEST_CASE(kafka_metadata_response_parsing_test) {
    kafka::metadata_response response;
    test_deserialize_serialize({
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x0a, 0x31, 0x37,
        0x32, 0x2e, 0x31, 0x33, 0x2e, 0x30, 0x2e, 0x31, 0x00, 0x00, 0x23, 0x84, 0xff, 0xff, 0x00, 0x16,
        0x6b, 0x4c, 0x5a, 0x35, 0x6a, 0x50, 0x76, 0x44, 0x52, 0x30, 0x43, 0x77, 0x31, 0x79, 0x34, 0x31,
        0x41, 0x66, 0x35, 0x48, 0x55, 0x67, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x05, 0x74, 0x65, 0x73, 0x74, 0x35, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x03, 0xe9, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x03, 0xe9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    }, response, 8);

    BOOST_REQUIRE_EQUAL(*response._throttle_time_ms, 0);
    BOOST_REQUIRE_EQUAL(response._brokers->size(), 1);
    BOOST_REQUIRE_EQUAL(*response._brokers[0]._node_id, 0x3e9);
    BOOST_REQUIRE_EQUAL(*response._brokers[0]._host, "172.13.0.1");
    BOOST_REQUIRE_EQUAL(*response._brokers[0]._port, 0x2384);
    BOOST_REQUIRE(response._brokers[0]._rack.is_null());
    BOOST_REQUIRE_EQUAL(*response._cluster_id, "kLZ5jPvDR0Cw1y41Af5HUg");
    BOOST_REQUIRE_EQUAL(*response._controller_id, 0x3e9);
    BOOST_REQUIRE_EQUAL(response._topics->size(), 1);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._error_code, 0);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._name, "test5");
    BOOST_REQUIRE(!*response._topics[0]._is_internal);
    BOOST_REQUIRE_EQUAL(response._topics[0]._partitions->size(), 1);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._partitions[0]._error_code, 0);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._partitions[0]._partition_index, 0);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._partitions[0]._leader_id, 0x3e9);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._partitions[0]._leader_epoch, 0);
    BOOST_REQUIRE_EQUAL(response._topics[0]._partitions[0]._replica_nodes->size(), 1);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._partitions[0]._replica_nodes[0], 0x3e9);
    BOOST_REQUIRE_EQUAL(response._topics[0]._partitions[0]._isr_nodes->size(), 1);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._partitions[0]._isr_nodes[0], 0x3e9);
    BOOST_REQUIRE_EQUAL(*response._topics[0]._topic_authorized_operations, 0);
    BOOST_REQUIRE_EQUAL(*response._cluster_authorized_operations, 0);
}

BOOST_AUTO_TEST_CASE(kafka_record_parsing_test) {
    kafka::kafka_record record;
    test_deserialize_serialize({
        0x20, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00
    }, record, 0);
    BOOST_REQUIRE_EQUAL(*record._timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*record._offset_delta, 0);
    std::string expected_key{"\x00\x00\x00\x01", 4};
    BOOST_REQUIRE_EQUAL(record._key, expected_key);
    std::string expected_value{"\x00\x00\x00\x00\x00\x00", 6};
    BOOST_REQUIRE_EQUAL(record._value, expected_value);
    BOOST_REQUIRE_EQUAL(record._headers.size(), 0);

    kafka::kafka_record record2;
    test_deserialize_serialize({
        0x10, 0x00, 0x00, 0x00, 0x02, 0x34, 0x02, 0x36, 0x00
    }, record2, 0);
    BOOST_REQUIRE_EQUAL(*record2._timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*record2._offset_delta, 0);
    BOOST_REQUIRE_EQUAL(record2._key, "4");
    BOOST_REQUIRE_EQUAL(record2._value, "6");
    BOOST_REQUIRE_EQUAL(record2._headers.size(), 0);
}

BOOST_AUTO_TEST_CASE(kafka_record_batch_parsing_test) {
    kafka::kafka_record_batch batch;
    test_deserialize_serialize({
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x6f, 0x51, 0x95, 0x17, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x6e, 0xb3,
        0x2b, 0x03, 0x41, 0x00, 0x00, 0x01, 0x6e, 0xb3, 0x2b, 0x03, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00,
        0x00, 0x02, 0x34, 0x02, 0x34, 0x00
    }, batch, 0);

    BOOST_REQUIRE_EQUAL(*batch._base_offset, 4);
    BOOST_REQUIRE_EQUAL(*batch._partition_leader_epoch, 0);
    BOOST_REQUIRE_EQUAL(*batch._magic, 2);
    BOOST_REQUIRE(batch._compression_type == kafka::kafka_record_compression_type::NO_COMPRESSION);
    BOOST_REQUIRE(batch._timestamp_type == kafka::kafka_record_timestamp_type::CREATE_TIME);
    BOOST_REQUIRE(batch._is_transactional);
    BOOST_REQUIRE(!batch._is_control_batch);
    BOOST_REQUIRE_EQUAL(*batch._first_timestamp, 0x16eb32b0341);
    BOOST_REQUIRE_EQUAL(*batch._producer_id, 0);
    BOOST_REQUIRE_EQUAL(*batch._producer_epoch, 0);
    BOOST_REQUIRE_EQUAL(*batch._base_sequence, 3);
    BOOST_REQUIRE_EQUAL(batch._records.size(), 1);
    BOOST_REQUIRE_EQUAL(*batch._records[0]._timestamp_delta, 0);
    BOOST_REQUIRE_EQUAL(*batch._records[0]._offset_delta, 0);
    BOOST_REQUIRE_EQUAL(batch._records[0]._key, "4");
    BOOST_REQUIRE_EQUAL(batch._records[0]._value, "4");
    BOOST_REQUIRE_EQUAL(batch._records[0]._headers.size(), 0);
}
