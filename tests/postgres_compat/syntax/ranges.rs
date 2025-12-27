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

//! Tests for PostgreSQL range types and operators
//!
//! Reference: <https://www.postgresql.org/docs/current/rangetypes.html>

use crate::postgres_compat::common::*;

#[test]
fn test_int4range_constructor() {
    // https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRUCT
    pg_roundtrip_only!("SELECT int4range(10, 20)");
    // TODO: Validate range constructor function
}

#[test]
fn test_int8range_constructor() {
    // https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRUCT
    pg_roundtrip_only!("SELECT int8range(1000000, 2000000)");
    // TODO: Validate int8range constructor
}

#[test]
fn test_numrange_constructor() {
    // https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRUCT
    pg_roundtrip_only!("SELECT numrange(1.5, 2.5)");
    // TODO: Validate numrange constructor
}

#[test]
fn test_tsrange_constructor() {
    // https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRUCT
    pg_roundtrip_only!("SELECT tsrange('2023-01-01'::TIMESTAMP, '2023-12-31'::TIMESTAMP)");
    // TODO: Validate tsrange constructor
}

#[test]
fn test_daterange_constructor() {
    // https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRUCT
    pg_roundtrip_only!("SELECT daterange('2023-01-01', '2023-12-31')");
    // TODO: Validate daterange constructor
}

#[test]
fn test_range_constructor_with_bounds() {
    // https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRUCT
    // Third parameter specifies inclusion: '[)' is default (inclusive-exclusive)
    pg_roundtrip_only!("SELECT int4range(10, 20, '[]')");
    // TODO: Validate range bounds parameter
}

#[test]
fn test_range_contains_element_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // @> operator tests if range contains element
    pg_roundtrip_only!("SELECT int4range(10, 20) @> 15");
    // TODO: Validate @> operator for range contains element
}

#[test]
fn test_range_contains_range_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // @> operator tests if range contains another range
    pg_roundtrip_only!("SELECT int4range(10, 30) @> int4range(15, 20)");
    // TODO: Validate @> operator for range contains range
}

#[test]
fn test_element_contained_by_range_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // <@ operator tests if element is contained by range
    pg_roundtrip_only!("SELECT 15 <@ int4range(10, 20)");
    // TODO: Validate <@ operator for element in range
}

#[test]
fn test_range_contained_by_range_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // <@ operator tests if range is contained by another range
    pg_roundtrip_only!("SELECT int4range(15, 20) <@ int4range(10, 30)");
    // TODO: Validate <@ operator for range in range
}

#[test]
fn test_range_overlap_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // && operator tests if ranges overlap
    pg_roundtrip_only!("SELECT int4range(10, 20) && int4range(15, 25)");
    // TODO: Validate && operator for range overlap
}

#[test]
fn test_range_strictly_left_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // << operator tests if range is strictly left of another
    pg_roundtrip_only!("SELECT int4range(1, 10) << int4range(20, 30)");
    // TODO: Validate << operator
}

#[test]
fn test_range_strictly_right_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // >> operator tests if range is strictly right of another
    pg_roundtrip_only!("SELECT int4range(20, 30) >> int4range(1, 10)");
    // TODO: Validate >> operator
}

#[test]
fn test_range_not_extend_right_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // &< operator tests if range does not extend to the right of another
    pg_roundtrip_only!("SELECT int4range(1, 20) &< int4range(10, 30)");
    // TODO: Validate &< operator
}

#[test]
fn test_range_not_extend_left_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // &> operator tests if range does not extend to the left of another
    pg_roundtrip_only!("SELECT int4range(10, 30) &> int4range(1, 20)");
    // TODO: Validate &> operator
}

#[test]
fn test_range_adjacent_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // -|- operator tests if ranges are adjacent
    pg_roundtrip_only!("SELECT int4range(1, 10) -|- int4range(10, 20)");
    // TODO: Validate -|- operator
}

#[test]
fn test_range_union_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // + operator computes union of ranges
    pg_roundtrip_only!("SELECT int4range(1, 10) + int4range(10, 20)");
    // TODO: Validate + operator for range union
}

#[test]
fn test_range_difference_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // - operator computes difference of ranges
    pg_roundtrip_only!("SELECT int4range(1, 20) - int4range(10, 15)");
    // TODO: Validate - operator for range difference
}

#[test]
fn test_range_intersection_operator() {
    // https://www.postgresql.org/docs/current/functions-range.html
    // * operator computes intersection of ranges
    pg_roundtrip_only!("SELECT int4range(1, 20) * int4range(10, 30)");
    // TODO: Validate * operator for range intersection
}

#[test]
fn test_lower_function() {
    // https://www.postgresql.org/docs/current/functions-range.html
    pg_roundtrip_only!("SELECT lower(int4range(10, 20))");
    // TODO: Validate lower() function
}

#[test]
fn test_upper_function() {
    // https://www.postgresql.org/docs/current/functions-range.html
    pg_roundtrip_only!("SELECT upper(int4range(10, 20))");
    // TODO: Validate upper() function
}

#[test]
fn test_isempty_function() {
    // https://www.postgresql.org/docs/current/functions-range.html
    pg_roundtrip_only!("SELECT isempty(int4range(10, 10))");
    // TODO: Validate isempty() function
}

#[test]
fn test_lower_inc_function() {
    // https://www.postgresql.org/docs/current/functions-range.html
    pg_roundtrip_only!("SELECT lower_inc(int4range(10, 20))");
    // TODO: Validate lower_inc() function
}

#[test]
fn test_upper_inc_function() {
    // https://www.postgresql.org/docs/current/functions-range.html
    pg_roundtrip_only!("SELECT upper_inc(int4range(10, 20))");
    // TODO: Validate upper_inc() function
}

#[test]
fn test_lower_inf_function() {
    // https://www.postgresql.org/docs/current/functions-range.html
    pg_roundtrip_only!("SELECT lower_inf(int4range(NULL, 20))");
    // TODO: Validate lower_inf() function
}

#[test]
fn test_upper_inf_function() {
    // https://www.postgresql.org/docs/current/functions-range.html
    pg_roundtrip_only!("SELECT upper_inf(int4range(10, NULL))");
    // TODO: Validate upper_inf() function
}
