// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Tests for PostgreSQL JSON/JSONB operators and functions
//!
//! Reference: <https://www.postgresql.org/docs/current/functions-json.html>

use crate::postgres_compat::common::*;

#[test]
fn test_json_arrow_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-OP-TABLE
    // -> operator extracts JSON object field by key
    pg_roundtrip_only!("SELECT data -> 'name' FROM users");
    // TODO: Validate -> operator for JSON field access
}

#[test]
fn test_json_arrow_operator_integer() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-OP-TABLE
    // -> operator extracts JSON array element by index
    pg_roundtrip_only!("SELECT data -> 0 FROM users");
    // TODO: Validate -> operator with integer index
}

#[test]
fn test_json_double_arrow_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-OP-TABLE
    // ->> operator extracts JSON object field as text
    pg_roundtrip_only!("SELECT data ->> 'name' FROM users");
    // TODO: Validate ->> operator
}

#[test]
fn test_json_double_arrow_integer() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-OP-TABLE
    // ->> operator extracts JSON array element as text
    pg_roundtrip_only!("SELECT data ->> 0 FROM users");
    // TODO: Validate ->> operator with integer
}

#[test]
fn test_json_path_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-OP-TABLE
    // #> operator extracts JSON object at path
    pg_roundtrip_only!("SELECT data #> '{address,city}' FROM users");
    // TODO: Validate #> operator
}

#[test]
fn test_json_path_text_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-OP-TABLE
    // #>> operator extracts JSON object at path as text
    pg_roundtrip_only!("SELECT data #>> '{address,city}' FROM users");
    // TODO: Validate #>> operator
}

#[test]
fn test_jsonb_contains_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // @> operator tests whether left JSONB contains right JSONB
    pg_roundtrip_only!("SELECT data @> '{\"name\":\"John\"}' FROM users");
    // TODO: Validate @> operator
}

#[test]
fn test_jsonb_contained_by_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // <@ operator tests whether left JSONB is contained in right JSONB
    pg_roundtrip_only!("SELECT '{\"name\":\"John\"}' <@ data FROM users");
    // TODO: Validate <@ operator
}

#[test]
fn test_jsonb_key_exists_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // ? operator tests whether key exists in JSONB
    pg_roundtrip_only!("SELECT data ? 'name' FROM users");
    // TODO: Validate ? operator
}

#[test]
fn test_jsonb_any_key_exists_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // ?| operator tests whether any key exists in JSONB
    pg_roundtrip_only!("SELECT data ?| ARRAY['name', 'email'] FROM users");
    // TODO: Validate ?| operator
}

#[test]
fn test_jsonb_all_keys_exist_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // ?& operator tests whether all keys exist in JSONB
    pg_roundtrip_only!("SELECT data ?& ARRAY['name', 'email'] FROM users");
    // TODO: Validate ?& operator
}

#[test]
fn test_jsonb_concat_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // || operator concatenates two JSONB values
    pg_roundtrip_only!("SELECT '{\"a\":1}'::JSONB || '{\"b\":2}'::JSONB");
    // TODO: Validate || operator for JSONB
}

#[test]
fn test_jsonb_delete_key_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // - operator deletes key from JSONB
    pg_roundtrip_only!("SELECT data - 'name' FROM users");
    // TODO: Validate - operator for JSONB key deletion
}

#[test]
fn test_jsonb_delete_index_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // - operator deletes array element from JSONB
    pg_roundtrip_only!("SELECT data - 0 FROM users");
    // TODO: Validate - operator with integer
}

#[test]
fn test_jsonb_delete_path_operator() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSONB-OP-TABLE
    // #- operator deletes field at path from JSONB
    pg_roundtrip_only!("SELECT data #- '{address,city}' FROM users");
    // TODO: Validate #- operator
}

#[test]
fn test_json_extract_path_function() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-PROCESSING
    pg_roundtrip_only!("SELECT json_extract_path(data, 'address', 'city') FROM users");
    // TODO: Validate json_extract_path function
}

#[test]
fn test_jsonb_set_function() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-PROCESSING
    pg_roundtrip_only!("SELECT jsonb_set(data, '{name}', '\"Jane\"') FROM users");
    // TODO: Validate jsonb_set function
}

#[test]
fn test_jsonb_insert_function() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-PROCESSING
    pg_roundtrip_only!("SELECT jsonb_insert(data, '{address,zip}', '\"12345\"') FROM users");
    // TODO: Validate jsonb_insert function
}

#[test]
fn test_jsonb_path_query_function() {
    // https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH
    pg_roundtrip_only!("SELECT jsonb_path_query(data, '$.address.city') FROM users");
    // TODO: Validate jsonb_path_query function
}
