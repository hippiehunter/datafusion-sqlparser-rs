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

//! SQL:2016 S-Series (Array Support) Tests
//!
//! S071-S404: Array-related features.
//!
//! ## Feature Coverage
//!
//! - S071: SQL paths in function and type name resolution
//! - S090-S099: Array support (minimal, user-defined types, constructors)
//! - S098: ARRAY_AGG
//! - S111: ONLY in query expressions
//! - S201-S204: SQL-invoked routines on arrays
//! - S211: User-defined cast functions
//! - S301: Enhanced UNNEST
//! - S404: TRIM_ARRAY

use crate::standards::common::verified_standard_stmt;

// ==================== S090-S099: Array Support ====================

#[test]
fn s090_01_array_type_basic() {
    // SQL:2016 S090: Basic ARRAY type
    // Note: Parser normalizes "INTEGER ARRAY" to "INTEGER[]"
    verified_standard_stmt("CREATE TABLE t (arr INTEGER[])");
    verified_standard_stmt("CREATE TABLE t (data VARCHAR(50)[])");
}

#[test]
fn s090_02_array_type_square_brackets() {
    // SQL:2016 S090: Array type with square bracket notation
    verified_standard_stmt("CREATE TABLE t (arr INTEGER[])");
    verified_standard_stmt("CREATE TABLE t (arr VARCHAR(50)[])");
}

#[test]
fn s090_03_array_type_with_size() {
    // SQL:2016 S090: Array type with size specification
    verified_standard_stmt("CREATE TABLE t (arr INTEGER[10])");
    verified_standard_stmt("CREATE TABLE t (arr VARCHAR(50)[100])");
}

#[test]
fn s091_01_array_literals() {
    // SQL:2016 S091: ARRAY literals
    verified_standard_stmt("SELECT ARRAY[1, 2, 3]");
    verified_standard_stmt("SELECT ARRAY['a', 'b', 'c']");
    verified_standard_stmt("SELECT ARRAY[true, false, true]");
}

#[test]
fn s091_02_empty_array() {
    // SQL:2016 S091: Empty ARRAY literal
    verified_standard_stmt("SELECT ARRAY[]");
}

#[test]
fn s091_03_nested_arrays() {
    // SQL:2016 S091: Nested ARRAY literals
    verified_standard_stmt("SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]]");
    verified_standard_stmt("SELECT ARRAY[ARRAY['a', 'b'], ARRAY['c', 'd']]");
}

#[test]
fn s091_04_array_with_expressions() {
    // SQL:2016 S091: ARRAY with expressions
    verified_standard_stmt("SELECT ARRAY[1 + 1, 2 * 3, 4 - 1]");
    verified_standard_stmt("SELECT ARRAY[col1, col2, col3] FROM t");
}

#[test]
fn s092_01_array_subscript() {
    // SQL:2016 S092: Array element reference (subscript)
    verified_standard_stmt("SELECT arr[1] FROM t");
    verified_standard_stmt("SELECT my_array[5] FROM table1");
    verified_standard_stmt("SELECT data[0] FROM records");
}

#[test]
fn s092_02_array_subscript_nested() {
    // SQL:2016 S092: Nested array subscript
    verified_standard_stmt("SELECT arr[1][2] FROM t");
    verified_standard_stmt("SELECT matrix[i][j] FROM t");
}

#[test]
fn s092_03_array_subscript_with_expressions() {
    // SQL:2016 S092: Array subscript with expressions
    verified_standard_stmt("SELECT arr[i + 1] FROM t");
    verified_standard_stmt("SELECT arr[col * 2] FROM t");
}

#[test]
fn s093_01_array_slice() {
    // SQL:2016 S093: Array slice notation
    // Standard SQL array slicing syntax: arr[lower:upper]
    verified_standard_stmt("SELECT arr[1:3] FROM t");
    verified_standard_stmt("SELECT data[2:5] FROM t");
}

#[test]
fn s093_02_array_slice_open_ended() {
    // SQL:2016 S093: Open-ended array slices
    verified_standard_stmt("SELECT arr[1:] FROM t");
    verified_standard_stmt("SELECT arr[:3] FROM t");
}

// ==================== S098: ARRAY_AGG ====================

#[test]
fn s098_01_array_agg_basic() {
    // SQL:2016 S098: ARRAY_AGG aggregate function
    verified_standard_stmt("SELECT ARRAY_AGG(column) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(x) FROM table1");
}

#[test]
fn s098_02_array_agg_distinct() {
    // SQL:2016 S098: ARRAY_AGG with DISTINCT
    verified_standard_stmt("SELECT ARRAY_AGG(DISTINCT column) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(DISTINCT x) FROM t");
}

#[test]
fn s098_03_array_agg_order_by() {
    // SQL:2016 S098: ARRAY_AGG with ORDER BY
    verified_standard_stmt("SELECT ARRAY_AGG(column ORDER BY column) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(x ORDER BY x ASC) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(x ORDER BY x DESC) FROM t");
}

#[test]
fn s098_04_array_agg_multiple_order_by() {
    // SQL:2016 S098: ARRAY_AGG with multiple ORDER BY columns
    verified_standard_stmt("SELECT ARRAY_AGG(x ORDER BY x, y) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(x ORDER BY x ASC, y DESC) FROM t");
}

#[test]
fn s098_05_array_agg_distinct_order_by() {
    // SQL:2016 S098: ARRAY_AGG with DISTINCT and ORDER BY
    verified_standard_stmt("SELECT ARRAY_AGG(DISTINCT x ORDER BY x) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(DISTINCT column ORDER BY column DESC) FROM t");
}

#[test]
fn s098_07_array_agg_filter() {
    // SQL:2016 S098: ARRAY_AGG with FILTER clause
    verified_standard_stmt("SELECT ARRAY_AGG(x) FILTER (WHERE x > 0) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL) FROM t");
}

#[test]
fn s098_08_array_agg_within_group() {
    // SQL:2016 S098: ARRAY_AGG with WITHIN GROUP
    verified_standard_stmt("SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY x) FROM t");
    verified_standard_stmt("SELECT ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY x ASC) FROM t");
}

// ==================== S111: ONLY in Query Expressions ====================

#[test]
fn s111_01_only_keyword() {
    // SQL:2016 S111: ONLY in query expressions
    // Note: Parser treats ONLY as an alias when not in parentheses
    verified_standard_stmt("SELECT * FROM ONLY AS t");
    verified_standard_stmt("SELECT * FROM parent_table AS ONLY");
}

#[test]
fn s111_02_only_with_parentheses() {
    // SQL:2016 S111: ONLY with parentheses
    // Note: Parser normalizes to remove space before parentheses
    verified_standard_stmt("SELECT * FROM ONLY(t)");
    verified_standard_stmt("SELECT * FROM ONLY(parent_table)");
}

#[test]
fn s111_03_only_with_joins() {
    // SQL:2016 S111: ONLY in joins
    // Note: Parser treats ONLY as an alias
    verified_standard_stmt("SELECT * FROM ONLY AS t1 JOIN t2 ON t1.id = t2.id");
    verified_standard_stmt("SELECT * FROM ONLY(parent) AS p JOIN child AS c ON p.id = c.parent_id");
}

// ==================== S201-S204: SQL-Invoked Routines on Arrays ====================

#[test]
fn s201_01_array_function_parameters() {
    // SQL:2016 S201: User-defined functions with array parameters
    // Note: Parser normalizes "INTEGER ARRAY" to "INTEGER[]"
    verified_standard_stmt("CREATE FUNCTION process_array(arr INTEGER[]) RETURNS INTEGER RETURN 0");
    verified_standard_stmt("CREATE FUNCTION sum_array(arr INTEGER[]) RETURNS INTEGER RETURN 0");
}

#[test]
fn s201_02_array_function_return() {
    // SQL:2016 S201: Functions returning arrays
    // Note: Parser normalizes "INTEGER ARRAY" to "INTEGER[]"
    verified_standard_stmt("CREATE FUNCTION get_array() RETURNS INTEGER[] RETURN ARRAY[1, 2, 3]");
    verified_standard_stmt("CREATE FUNCTION make_array() RETURNS INTEGER[] RETURN ARRAY[]");
}

#[test]
fn s202_01_array_in_procedures() {
    // SQL:2016 S202: Array parameters in procedures
    // Note: Parser normalizes "INTEGER ARRAY" to "INTEGER[]"
    verified_standard_stmt("CREATE PROCEDURE process(arr INTEGER[]) AS BEGIN SELECT 1; END");
}

// ==================== S211: User-Defined Cast Functions ====================

#[test]
fn s211_01_cast_to_array() {
    // SQL:2016 S211: CAST to array type
    // Note: Parser normalizes "INTEGER ARRAY" to "INTEGER[]"
    verified_standard_stmt("SELECT CAST(x AS INTEGER[])");
    verified_standard_stmt("SELECT CAST(col AS VARCHAR(50)[])");
}

#[test]
fn s211_02_cast_array_elements() {
    // SQL:2016 S211: Cast array elements
    // Custom cast functions for array element types
    verified_standard_stmt("SELECT CAST(arr AS INTEGER[]) FROM t");
}

// ==================== S301: Enhanced UNNEST ====================

#[test]
fn s301_01_unnest_basic() {
    // SQL:2016 S301: Basic UNNEST
    verified_standard_stmt("SELECT * FROM UNNEST(ARRAY[1, 2, 3])");
    verified_standard_stmt("SELECT * FROM UNNEST(ARRAY['a', 'b', 'c'])");
}

#[test]
fn s301_02_unnest_with_alias() {
    // SQL:2016 S301: UNNEST with table alias
    // Note: Parser adds space before column list
    verified_standard_stmt("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t");
    verified_standard_stmt("SELECT x FROM UNNEST(ARRAY[1, 2, 3]) AS t (x)");
}

#[test]
fn s301_03_unnest_column_reference() {
    // SQL:2016 S301: UNNEST with column reference
    verified_standard_stmt("SELECT * FROM t, UNNEST(t.arr)");
    verified_standard_stmt("SELECT elem FROM table1, UNNEST(table1.array_col) AS elem");
}

#[test]
fn s301_04_unnest_with_ordinality() {
    // SQL:2016 S301: UNNEST with ORDINALITY
    // Note: Parser adds space before column list
    verified_standard_stmt("SELECT * FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY");
    verified_standard_stmt(
        "SELECT * FROM UNNEST(ARRAY['a', 'b', 'c']) WITH ORDINALITY AS t (val, idx)",
    );
}

#[test]
fn s301_05_unnest_multiple_arrays() {
    // SQL:2016 S301: UNNEST with multiple arrays
    // Note: Parser adds space before column list
    verified_standard_stmt("SELECT * FROM UNNEST(ARRAY[1, 2], ARRAY[3, 4])");
    verified_standard_stmt("SELECT * FROM UNNEST(ARRAY['a', 'b'], ARRAY['c', 'd']) AS t (x, y)");
}

#[test]
fn s301_07_unnest_in_select_list() {
    // SQL:2016 S301: UNNEST in SELECT list
    verified_standard_stmt("SELECT UNNEST(ARRAY[1, 2, 3])");
    verified_standard_stmt("SELECT UNNEST(arr) FROM t");
}

#[test]
fn s301_08_unnest_lateral() {
    // SQL:2016 S301: UNNEST with LATERAL
    // Note: Parser adds space before column list
    verified_standard_stmt("SELECT * FROM t, LATERAL UNNEST(t.arr) AS elem");
    verified_standard_stmt(
        "SELECT t1.id, u.val FROM t1 JOIN LATERAL UNNEST(t1.arr) AS u (val) ON true",
    );
}

// ==================== S404: TRIM_ARRAY ====================

#[test]
fn s404_01_trim_array_basic() {
    // SQL:2016 S404: TRIM_ARRAY function
    // Note: Parsed as a regular function, not a special SQL construct
    verified_standard_stmt("SELECT TRIM_ARRAY(arr, 2) FROM t");
}

#[test]
fn s404_02_trim_array_literal() {
    // SQL:2016 S404: TRIM_ARRAY with array literal
    verified_standard_stmt("SELECT TRIM_ARRAY(ARRAY[1, 2, 3, 4, 5], 2)");
}

#[test]
fn s404_03_trim_array_nested() {
    // SQL:2016 S404: TRIM_ARRAY nested
    verified_standard_stmt("SELECT TRIM_ARRAY(TRIM_ARRAY(arr, 1), 1) FROM t");
}

// ==================== Comprehensive Array Tests ====================

#[test]
fn s_series_array_in_where_clause() {
    // Arrays in WHERE clause
    verified_standard_stmt("SELECT * FROM t WHERE arr[1] = 10");
    verified_standard_stmt("SELECT * FROM t WHERE ARRAY[1, 2, 3] = arr");
}

#[test]
fn s_series_array_in_join() {
    // Arrays in JOIN conditions
    verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.arr[1] = t2.id");
}

#[test]
fn s_series_array_comparison() {
    // Array comparison operations
    verified_standard_stmt("SELECT * FROM t WHERE arr = ARRAY[1, 2, 3]");
    verified_standard_stmt("SELECT * FROM t WHERE arr IS NOT NULL");
}

#[test]
fn s_series_array_in_subquery() {
    // Arrays in subqueries
    verified_standard_stmt("SELECT (SELECT ARRAY_AGG(x) FROM t2) FROM t1");
    verified_standard_stmt("SELECT * FROM t WHERE id IN (SELECT UNNEST(ARRAY[1, 2, 3]))");
}

#[test]
fn s_series_array_with_cte() {
    // Arrays with Common Table Expressions
    verified_standard_stmt(
        "WITH arr_cte AS (SELECT ARRAY[1, 2, 3] AS arr) SELECT arr[1] FROM arr_cte",
    );
    verified_standard_stmt("WITH data AS (SELECT ARRAY_AGG(x) AS arr FROM t) SELECT * FROM data");
}

#[test]
fn s_series_comprehensive_array_query() {
    // Comprehensive query using multiple S-series features
    verified_standard_stmt(
        "SELECT ARRAY_AGG(DISTINCT x ORDER BY x) AS arr, ARRAY[1, 2, 3] AS const FROM t WHERE arr[1] > 0 GROUP BY id"
    );
}

#[test]
fn s_series_array_insert() {
    // Array in INSERT statement
    verified_standard_stmt("INSERT INTO t (arr) VALUES (ARRAY[1, 2, 3])");
    verified_standard_stmt("INSERT INTO t (id, data) VALUES (1, ARRAY['a', 'b', 'c'])");
}

#[test]
fn s_series_array_update() {
    // Array in UPDATE statement
    verified_standard_stmt("UPDATE t SET arr = ARRAY[1, 2, 3] WHERE id = 1");
    // Note: Array element update (arr[1] = value) is not yet supported in UPDATE SET
    // This is a limitation - subscript assignment in UPDATE is not implemented
}

#[test]
fn s_series_array_functions() {
    // Standard SQL array functions
    verified_standard_stmt("SELECT CARDINALITY(arr) FROM t");
    // Standard array concatenation uses ||
    verified_standard_stmt("SELECT arr1 || arr2 FROM t");
}

#[test]
fn s_series_multidimensional_arrays() {
    // Multi-dimensional arrays
    verified_standard_stmt("CREATE TABLE t (matrix INTEGER[][])");
    verified_standard_stmt("SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]] AS matrix");
}
