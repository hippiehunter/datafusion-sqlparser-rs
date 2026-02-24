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

//! Tests for CREATE FUNCTION options and attributes
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createfunction.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{FunctionBehavior, FunctionCalledOnNull, FunctionParallel, Statement};

// =============================================================================
// Behavior Attributes (SHOULD PASS - in AST)
// =============================================================================

#[test]
fn test_create_function_immutable() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // IMMUTABLE means function always returns same result for same inputs
    // Note: Serialization order is RETURNS -> LANGUAGE -> attributes -> AS
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER IMMUTABLE LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.behavior, Some(FunctionBehavior::Immutable));
        }
    );
}

#[test]
fn test_create_function_stable() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // STABLE means function results don't change within a single table scan
    pg_parses_to_with_ast!(
        "CREATE FUNCTION current_user_id() RETURNS INTEGER STABLE LANGUAGE SQL AS $$ SELECT user_id FROM current_user $$",
        "CREATE FUNCTION current_user_id() RETURNS INTEGER LANGUAGE SQL STABLE AS $$ SELECT user_id FROM current_user $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.behavior, Some(FunctionBehavior::Stable));
        }
    );
}

#[test]
fn test_create_function_volatile() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // VOLATILE (default) means function can return different results on successive calls
    pg_parses_to_with_ast!(
        "CREATE FUNCTION random_int() RETURNS INTEGER VOLATILE LANGUAGE SQL AS $$ SELECT (RANDOM() * 100)::INTEGER $$",
        "CREATE FUNCTION random_int() RETURNS INTEGER LANGUAGE SQL VOLATILE AS $$ SELECT (RANDOM() * 100)::INTEGER $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.behavior, Some(FunctionBehavior::Volatile));
        }
    );
}

#[test]
fn test_create_function_called_on_null_input() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // CALLED ON NULL INPUT (default) means function is called even with NULL arguments
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add_nullable(INTEGER, INTEGER) RETURNS INTEGER CALLED ON NULL INPUT LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        "CREATE FUNCTION add_nullable(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL CALLED ON NULL INPUT AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(
                cf.called_on_null,
                Some(FunctionCalledOnNull::CalledOnNullInput)
            );
        }
    );
}

#[test]
fn test_create_function_returns_null_on_null_input() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS NULL ON NULL INPUT means function returns NULL if any argument is NULL
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add_strict(INTEGER, INTEGER) RETURNS INTEGER RETURNS NULL ON NULL INPUT LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        "CREATE FUNCTION add_strict(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL RETURNS NULL ON NULL INPUT AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(
                cf.called_on_null,
                Some(FunctionCalledOnNull::ReturnsNullOnNullInput)
            );
        }
    );
}

#[test]
fn test_create_function_strict() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // STRICT is equivalent to RETURNS NULL ON NULL INPUT
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add_strict(INTEGER, INTEGER) RETURNS INTEGER STRICT LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        "CREATE FUNCTION add_strict(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL STRICT AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.called_on_null, Some(FunctionCalledOnNull::Strict));
        }
    );
}

#[test]
fn test_create_function_parallel_safe() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PARALLEL SAFE means function is safe to run in parallel mode
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER PARALLEL SAFE LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL PARALLEL SAFE AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.parallel, Some(FunctionParallel::Safe));
        }
    );
}

#[test]
fn test_create_function_parallel_unsafe() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PARALLEL UNSAFE (default) means function cannot be run in parallel
    pg_expect_parse_error!(
        "CREATE FUNCTION write_to_file() RETURNS VOID PARALLEL UNSAFE LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_write_file('/tmp/log', 'data'); END $$"
    );
}

#[test]
fn test_create_function_parallel_restricted() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PARALLEL RESTRICTED can run in parallel but only in parallel group leader
    pg_parses_to_with_ast!(
        "CREATE FUNCTION create_temp_table() RETURNS VOID PARALLEL RESTRICTED LANGUAGE SQL AS $$ CREATE TEMP TABLE tmp (a INT) $$",
        "CREATE FUNCTION create_temp_table() RETURNS VOID LANGUAGE SQL PARALLEL RESTRICTED AS $$ CREATE TEMP TABLE tmp (a INT) $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.parallel, Some(FunctionParallel::Restricted));
        }
    );
}

#[test]
fn test_create_function_combined_attributes() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Multiple attributes can be combined
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.behavior, Some(FunctionBehavior::Immutable));
            assert_eq!(cf.called_on_null, Some(FunctionCalledOnNull::Strict));
            assert_eq!(cf.parallel, Some(FunctionParallel::Safe));
        }
    );
}

// =============================================================================
// Cost and Rows Estimates (EXPECTED TO FAIL - not in AST)
// =============================================================================

#[test]
fn test_create_function_cost() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // COST hint for query planner (estimated execution cost)
    // EXPECTED TO FAIL: COST not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION expensive_function() RETURNS INTEGER COST 1000 LANGUAGE SQL AS $$ SELECT COUNT(*) FROM large_table $$"
    );
}

#[test]
fn test_create_function_rows() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // ROWS hint for set-returning functions
    // EXPECTED TO FAIL: ROWS not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION get_many_rows() RETURNS SETOF INTEGER ROWS 1000 LANGUAGE SQL AS $$ SELECT generate_series(1, 1000) $$"
    );
}

#[test]
fn test_create_function_cost_and_rows() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Both COST and ROWS can be specified
    // EXPECTED TO FAIL: COST and ROWS not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION complex_query() RETURNS SETOF RECORD COST 500 ROWS 100 LANGUAGE SQL AS $$ SELECT * FROM complex_view $$"
    );
}

// =============================================================================
// Security and Permissions (EXPECTED TO FAIL - not in AST)
// =============================================================================

#[test]
fn test_create_function_security_definer() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // SECURITY DEFINER executes with privileges of function owner
    // EXPECTED TO FAIL: SECURITY DEFINER not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION secure_delete(table_name TEXT) RETURNS VOID SECURITY DEFINER LANGUAGE SQL AS $$ DELETE FROM table_name $$"
    );
}

#[test]
fn test_create_function_security_invoker() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // SECURITY INVOKER (default) executes with privileges of calling user
    // EXPECTED TO FAIL: SECURITY INVOKER not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION get_my_data() RETURNS SETOF records SECURITY INVOKER LANGUAGE SQL AS $$ SELECT * FROM my_table $$"
    );
}

// =============================================================================
// Leakproof (EXPECTED TO FAIL - not in AST)
// =============================================================================

#[test]
fn test_create_function_leakproof() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // LEAKPROOF means function has no side effects and reveals nothing about arguments
    // EXPECTED TO FAIL: LEAKPROOF not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION safe_compare(a INTEGER, b INTEGER) RETURNS BOOLEAN LEAKPROOF LANGUAGE SQL AS $$ SELECT a = b $$"
    );
}

#[test]
fn test_create_function_not_leakproof() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // NOT LEAKPROOF is the default
    // EXPECTED TO FAIL: NOT LEAKPROOF not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION logging_compare(a INTEGER, b INTEGER) RETURNS BOOLEAN NOT LEAKPROOF LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'Comparing % and %', a, b; RETURN a = b; END $$"
    );
}

// =============================================================================
// Configuration Parameters (EXPECTED TO FAIL - not in AST)
// =============================================================================

#[test]
fn test_create_function_set_search_path() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // SET configuration_parameter allows setting GUC parameters
    // EXPECTED TO FAIL: SET not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION secure_function() RETURNS INTEGER SET search_path = public LANGUAGE SQL AS $$ SELECT 1 $$"
    );
}

#[test]
fn test_create_function_set_work_mem() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // SET can configure memory settings
    // EXPECTED TO FAIL: SET not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION memory_intensive() RETURNS INTEGER SET work_mem = '256MB' LANGUAGE SQL AS $$ SELECT COUNT(*) FROM large_table $$"
    );
}

#[test]
fn test_create_function_set_multiple_params() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Multiple SET clauses can be specified
    // EXPECTED TO FAIL: SET not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION configured_func() RETURNS INTEGER SET search_path = public SET work_mem = '256MB' LANGUAGE SQL AS $$ SELECT 1 $$"
    );
}

#[test]
fn test_create_function_set_from_current() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // SET FROM CURRENT captures current session value
    // EXPECTED TO FAIL: SET FROM CURRENT not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION inherit_config() RETURNS INTEGER SET search_path FROM CURRENT LANGUAGE SQL AS $$ SELECT 1 $$"
    );
}

// =============================================================================
// Window Functions (EXPECTED TO FAIL - not in AST)
// =============================================================================

#[test]
fn test_create_function_window() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // WINDOW indicates this is a window function
    // EXPECTED TO FAIL: WINDOW attribute not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION custom_rank() RETURNS INTEGER WINDOW LANGUAGE internal AS 'window_rank'"
    );
}

// =============================================================================
// Support Functions (EXPECTED TO FAIL - not in AST for support)
// =============================================================================

#[test]
fn test_create_function_support() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // SUPPORT function for planner support
    // EXPECTED TO FAIL: SUPPORT not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION my_function(INTEGER) RETURNS INTEGER SUPPORT my_support_func LANGUAGE SQL AS $$ SELECT $1 $$"
    );
}
