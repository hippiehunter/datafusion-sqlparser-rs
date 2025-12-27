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

//! Tests for PostgreSQL array syntax and operators
//!
//! Reference: <https://www.postgresql.org/docs/current/arrays.html>

use crate::postgres_compat::common::*;

#[test]
fn test_array_constructor() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
    pg_roundtrip_only!("SELECT ARRAY[1, 2, 3]");
    // TODO: Validate ARRAY constructor expression
}

#[test]
fn test_array_constructor_empty() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
    pg_roundtrip_only!("SELECT ARRAY[]::INTEGER[]");
    // TODO: Validate empty ARRAY constructor
}

#[test]
fn test_array_constructor_nested() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
    pg_roundtrip_only!("SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]]");
    // TODO: Validate nested ARRAY constructor
}

#[test]
fn test_array_constructor_with_subquery() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
    pg_roundtrip_only!("SELECT ARRAY(SELECT id FROM users)");
    // TODO: Validate ARRAY with subquery
}

#[test]
fn test_array_literal_syntax() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
    pg_roundtrip_only!("SELECT '{1,2,3}'::INTEGER[]");
    // TODO: Validate array literal with cast
}

#[test]
fn test_multidimensional_array_literal() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
    pg_roundtrip_only!("SELECT '{{1,2},{3,4}}'::INTEGER[][]");
    // TODO: Validate multidimensional array literal
}

#[test]
fn test_array_concatenation() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT ARRAY[1, 2] || ARRAY[3, 4]");
    // TODO: Validate array concatenation with || operator
}

#[test]
fn test_array_append() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT ARRAY[1, 2] || 3");
    // TODO: Validate array append
}

#[test]
fn test_array_prepend() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT 0 || ARRAY[1, 2]");
    // TODO: Validate array prepend
}

#[test]
fn test_array_contains_operator() {
    // https://www.postgresql.org/docs/current/functions-array.html
    // @> operator tests if left array contains right array
    pg_roundtrip_only!("SELECT ARRAY[1, 2, 3] @> ARRAY[2, 3]");
    // TODO: Validate @> operator for arrays
}

#[test]
fn test_array_contained_by_operator() {
    // https://www.postgresql.org/docs/current/functions-array.html
    // <@ operator tests if left array is contained by right array
    pg_roundtrip_only!("SELECT ARRAY[2, 3] <@ ARRAY[1, 2, 3]");
    // TODO: Validate <@ operator for arrays
}

#[test]
fn test_array_overlap_operator() {
    // https://www.postgresql.org/docs/current/functions-array.html
    // && operator tests if arrays have any elements in common
    pg_roundtrip_only!("SELECT ARRAY[1, 2] && ARRAY[2, 3]");
    // TODO: Validate && operator for arrays
}

#[test]
fn test_array_equality() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT ARRAY[1, 2] = ARRAY[1, 2]");
    // TODO: Validate array equality
}

#[test]
fn test_array_comparison() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT ARRAY[1, 2] < ARRAY[1, 3]");
    // TODO: Validate array comparison operators
}

#[test]
fn test_unnest_function() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT unnest(ARRAY[1, 2, 3])");
    // TODO: Validate unnest function
}

#[test]
fn test_array_agg_function() {
    // https://www.postgresql.org/docs/current/functions-aggregate.html
    pg_roundtrip_only!("SELECT array_agg(id) FROM users");
    // TODO: Validate array_agg aggregate function
}

#[test]
fn test_array_length_function() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT array_length(ARRAY[1, 2, 3], 1)");
    // TODO: Validate array_length function
}

#[test]
fn test_array_position_function() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT array_position(ARRAY[1, 2, 3], 2)");
    // TODO: Validate array_position function
}

#[test]
fn test_cardinality_function() {
    // https://www.postgresql.org/docs/current/functions-array.html
    pg_roundtrip_only!("SELECT cardinality(ARRAY[1, 2, 3])");
    // TODO: Validate cardinality function
}

#[test]
fn test_any_operator_with_array() {
    // https://www.postgresql.org/docs/current/functions-comparisons.html#FUNCTIONS-COMPARISONS-ANY-SOME
    pg_roundtrip_only!("SELECT 1 = ANY(ARRAY[1, 2, 3])");
    // TODO: Validate ANY with array
}

#[test]
fn test_all_operator_with_array() {
    // https://www.postgresql.org/docs/current/functions-comparisons.html#FUNCTIONS-COMPARISONS-ALL
    pg_roundtrip_only!("SELECT 5 > ALL(ARRAY[1, 2, 3])");
    // TODO: Validate ALL with array
}
