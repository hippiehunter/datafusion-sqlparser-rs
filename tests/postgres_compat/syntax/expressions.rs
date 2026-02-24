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

//! Tests for PostgreSQL-specific expression syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-expressions.html>

use crate::postgres_compat::common::*;

#[test]
fn test_double_colon_cast() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS
    pg_roundtrip_only!("SELECT '42'::INTEGER");
    // TODO: Validate that ::cast is parsed as Cast expression
}

#[test]
fn test_double_colon_cast_with_complex_type() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS
    pg_roundtrip_only!("SELECT '2023-01-01'::TIMESTAMP WITH TIME ZONE");
    // TODO: Validate Cast with complex type
}

#[test]
fn test_double_colon_cast_chained() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS
    pg_roundtrip_only!("SELECT '42'::TEXT::INTEGER");
    // TODO: Validate chained casts
}

#[test]
fn test_array_subscript() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1] FROM t");
    // TODO: Validate array subscript expression
}

#[test]
fn test_array_subscript_nested() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1][2] FROM t");
    // TODO: Validate nested array subscripts
}

#[test]
fn test_array_slice() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1:3] FROM t");
    // TODO: Validate array slice expression
}

#[test]
fn test_array_slice_multidimensional() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1:2][3:4] FROM t");
    // TODO: Validate multidimensional array slice
}

#[test]
fn test_regex_match_operator() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email ~ '^[a-z]+@'");
    // TODO: Validate regex match operator (~)
}

#[test]
fn test_regex_match_case_insensitive() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email ~* '^[a-z]+@'");
    // TODO: Validate case-insensitive regex match operator (~*)
}

#[test]
fn test_regex_not_match_operator() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email !~ '^[a-z]+@'");
    // TODO: Validate regex not match operator (!~)
}

#[test]
fn test_regex_not_match_case_insensitive() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email !~* '^[a-z]+@'");
    // TODO: Validate case-insensitive regex not match operator (!~*)
}

#[test]
fn test_string_concatenation_operator() {
    // https://www.postgresql.org/docs/current/functions-string.html
    pg_roundtrip_only!("SELECT 'Hello' || ' ' || 'World'");
    // TODO: Validate || operator for string concatenation
}

#[test]
fn test_factorial_operator() {
    // https://www.postgresql.org/docs/current/functions-math.html
    pg_roundtrip_only!("SELECT 5!");
    // TODO: Validate postfix factorial operator
}

#[test]
fn test_exponentiation_operator() {
    // https://www.postgresql.org/docs/current/functions-math.html
    pg_roundtrip_only!("SELECT 2 ^ 10");
    // TODO: Validate exponentiation operator (^)
}

#[test]
fn test_square_root_operator() {
    // https://www.postgresql.org/docs/current/functions-math.html
    one_statement_parses_to_pg("SELECT |/ 25", "SELECT |/25");
    // TODO: Validate square root operator (|/)
}

#[test]
fn test_cube_root_operator() {
    // https://www.postgresql.org/docs/current/functions-math.html
    one_statement_parses_to_pg("SELECT ||/ 27", "SELECT ||/27");
    // TODO: Validate cube root operator (||/)
}

#[test]
fn test_at_time_zone_operator() {
    // https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-ZONECONVERT
    one_statement_parses_to_pg(
        "SELECT timestamp '2023-01-01 00:00:00' AT TIME ZONE 'UTC'",
        "SELECT TIMESTAMP '2023-01-01 00:00:00' AT TIME ZONE 'UTC'",
    );
    // TODO: Validate AT TIME ZONE operator
}

#[test]
fn test_is_distinct_from() {
    // https://www.postgresql.org/docs/current/functions-comparison.html
    pg_roundtrip_only!("SELECT a IS DISTINCT FROM b FROM t");
    // TODO: Validate IS DISTINCT FROM operator
}

#[test]
fn test_is_not_distinct_from() {
    // https://www.postgresql.org/docs/current/functions-comparison.html
    pg_roundtrip_only!("SELECT a IS NOT DISTINCT FROM b FROM t");
    // TODO: Validate IS NOT DISTINCT FROM operator
}
