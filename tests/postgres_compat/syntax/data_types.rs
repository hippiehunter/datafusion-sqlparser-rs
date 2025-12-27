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

//! Tests for PostgreSQL-specific data types
//!
//! Reference: <https://www.postgresql.org/docs/current/datatype.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{DataType, Statement};

#[test]
fn test_serial_type() {
    // https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL
    pg_roundtrip_only!("CREATE TABLE t (id SERIAL)");
    // TODO: Validate that SERIAL is parsed as a specific DataType variant
}

#[test]
fn test_bigserial_type() {
    // https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL
    pg_roundtrip_only!("CREATE TABLE t (id BIGSERIAL)");
    // TODO: Validate DataType
}

#[test]
fn test_smallserial_type() {
    // https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL
    pg_roundtrip_only!("CREATE TABLE t (id SMALLSERIAL)");
    // TODO: Validate DataType
}

#[test]
fn test_text_type() {
    // https://www.postgresql.org/docs/current/datatype-character.html
    pg_roundtrip_only!("CREATE TABLE t (name TEXT)");
    // TODO: Validate that TEXT is parsed as DataType::Text or similar
}

#[test]
fn test_bytea_type() {
    // https://www.postgresql.org/docs/current/datatype-binary.html
    pg_roundtrip_only!("CREATE TABLE t (data BYTEA)");
    // TODO: Validate DataType
}

#[test]
fn test_inet_type() {
    // https://www.postgresql.org/docs/current/datatype-net-types.html
    pg_roundtrip_only!("CREATE TABLE t (ip INET)");
    // TODO: Validate DataType::Custom or specific variant
}

#[test]
fn test_cidr_type() {
    // https://www.postgresql.org/docs/current/datatype-net-types.html
    pg_roundtrip_only!("CREATE TABLE t (network CIDR)");
    // TODO: Validate DataType
}

#[test]
fn test_macaddr_type() {
    // https://www.postgresql.org/docs/current/datatype-net-types.html
    pg_roundtrip_only!("CREATE TABLE t (mac MACADDR)");
    // TODO: Validate DataType
}

#[test]
fn test_macaddr8_type() {
    // https://www.postgresql.org/docs/current/datatype-net-types.html
    pg_roundtrip_only!("CREATE TABLE t (mac MACADDR8)");
    // TODO: Validate DataType
}

#[test]
fn test_uuid_type() {
    // https://www.postgresql.org/docs/current/datatype-uuid.html
    pg_roundtrip_only!("CREATE TABLE t (id UUID)");
    // TODO: Validate DataType
}

#[test]
fn test_json_type() {
    // https://www.postgresql.org/docs/current/datatype-json.html
    pg_roundtrip_only!("CREATE TABLE t (data JSON)");
    // TODO: Validate DataType::JSON
}

#[test]
fn test_jsonb_type() {
    // https://www.postgresql.org/docs/current/datatype-json.html
    pg_roundtrip_only!("CREATE TABLE t (data JSONB)");
    // TODO: Validate DataType::JSONB or similar
}

#[test]
fn test_money_type() {
    // https://www.postgresql.org/docs/current/datatype-money.html
    pg_roundtrip_only!("CREATE TABLE t (amount MONEY)");
    // TODO: Validate DataType
}

#[test]
fn test_int4range_type() {
    // https://www.postgresql.org/docs/current/rangetypes.html
    pg_roundtrip_only!("CREATE TABLE t (r INT4RANGE)");
    // TODO: Validate DataType
}

#[test]
fn test_int8range_type() {
    // https://www.postgresql.org/docs/current/rangetypes.html
    pg_roundtrip_only!("CREATE TABLE t (r INT8RANGE)");
    // TODO: Validate DataType
}

#[test]
fn test_numrange_type() {
    // https://www.postgresql.org/docs/current/rangetypes.html
    pg_roundtrip_only!("CREATE TABLE t (r NUMRANGE)");
    // TODO: Validate DataType
}

#[test]
fn test_tsrange_type() {
    // https://www.postgresql.org/docs/current/rangetypes.html
    pg_roundtrip_only!("CREATE TABLE t (r TSRANGE)");
    // TODO: Validate DataType
}

#[test]
fn test_tstzrange_type() {
    // https://www.postgresql.org/docs/current/rangetypes.html
    pg_roundtrip_only!("CREATE TABLE t (r TSTZRANGE)");
    // TODO: Validate DataType
}

#[test]
fn test_daterange_type() {
    // https://www.postgresql.org/docs/current/rangetypes.html
    pg_roundtrip_only!("CREATE TABLE t (r DATERANGE)");
    // TODO: Validate DataType
}

#[test]
fn test_array_type_syntax() {
    // https://www.postgresql.org/docs/current/arrays.html
    pg_roundtrip_only!("CREATE TABLE t (arr INTEGER[])");
    // TODO: Validate DataType::Array
}

#[test]
fn test_array_type_with_dimension() {
    // https://www.postgresql.org/docs/current/arrays.html
    pg_roundtrip_only!("CREATE TABLE t (arr INTEGER[10])");
    // TODO: Validate DataType::Array with dimension
}

#[test]
fn test_multidimensional_array_type() {
    // https://www.postgresql.org/docs/current/arrays.html
    pg_roundtrip_only!("CREATE TABLE t (arr INTEGER[][])");
    // TODO: Validate DataType::Array with multiple dimensions
}

#[test]
fn test_varchar_without_length() {
    // PostgreSQL allows VARCHAR without length (unlike standard SQL)
    // https://www.postgresql.org/docs/current/datatype-character.html
    pg_roundtrip_only!("CREATE TABLE t (name VARCHAR)");
    // TODO: Validate DataType::Varchar with None length
}
