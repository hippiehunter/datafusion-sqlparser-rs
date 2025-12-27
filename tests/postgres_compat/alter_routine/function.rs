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

//! Tests for ALTER FUNCTION compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-alterfunction.html>
//!
//! ## Feature Status
//!
//! ALTER FUNCTION is NOT implemented in the AST. All tests use `pg_expect_parse_error!`
//! and are expected to fail until the feature is implemented.

use crate::postgres_compat::common::*;

// =============================================================================
// ALTER FUNCTION ... RENAME TO
// =============================================================================

#[test]
fn test_alter_function_rename_basic() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Rename function without arguments (only valid if function name is unique)
    pg_expect_parse_error!("ALTER FUNCTION my_func RENAME TO new_func");
}

#[test]
fn test_alter_function_rename_with_args() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Rename function with argument signature
    pg_expect_parse_error!("ALTER FUNCTION add(INTEGER, INTEGER) RENAME TO sum_integers");
}

#[test]
fn test_alter_function_rename_qualified() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Rename function with schema-qualified name
    pg_expect_parse_error!("ALTER FUNCTION myschema.myfunc(TEXT) RENAME TO newfunc");
}

#[test]
fn test_alter_function_rename_no_args() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Rename function with empty parameter list
    pg_expect_parse_error!("ALTER FUNCTION get_time() RENAME TO current_time");
}

// =============================================================================
// ALTER FUNCTION ... OWNER TO
// =============================================================================

#[test]
fn test_alter_function_owner_basic() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Change function owner
    pg_expect_parse_error!("ALTER FUNCTION my_func OWNER TO new_owner");
}

#[test]
fn test_alter_function_owner_with_args() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Change owner with argument signature
    pg_expect_parse_error!("ALTER FUNCTION calculate(NUMERIC, NUMERIC) OWNER TO admin");
}

#[test]
fn test_alter_function_owner_current_user() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Set owner to CURRENT_USER
    pg_expect_parse_error!("ALTER FUNCTION my_func() OWNER TO CURRENT_USER");
}

#[test]
fn test_alter_function_owner_current_role() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Set owner to CURRENT_ROLE
    pg_expect_parse_error!("ALTER FUNCTION my_func() OWNER TO CURRENT_ROLE");
}

#[test]
fn test_alter_function_owner_session_user() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Set owner to SESSION_USER
    pg_expect_parse_error!("ALTER FUNCTION my_func() OWNER TO SESSION_USER");
}

// =============================================================================
// ALTER FUNCTION ... SET SCHEMA
// =============================================================================

#[test]
fn test_alter_function_set_schema() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Move function to different schema
    pg_expect_parse_error!("ALTER FUNCTION my_func SET SCHEMA new_schema");
}

#[test]
fn test_alter_function_set_schema_with_args() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Move function with argument signature to different schema
    pg_expect_parse_error!("ALTER FUNCTION process(TEXT, INTEGER) SET SCHEMA archive");
}

#[test]
fn test_alter_function_set_schema_qualified() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Move schema-qualified function to different schema
    pg_expect_parse_error!("ALTER FUNCTION old_schema.my_func() SET SCHEMA new_schema");
}

// =============================================================================
// ALTER FUNCTION ... VOLATILITY
// =============================================================================

#[test]
fn test_alter_function_immutable() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // IMMUTABLE: function cannot modify database and always returns same result for same inputs
    pg_expect_parse_error!("ALTER FUNCTION calculate(INTEGER) IMMUTABLE");
}

#[test]
fn test_alter_function_stable() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // STABLE: function cannot modify database, may return different results for same inputs within a statement
    pg_expect_parse_error!("ALTER FUNCTION get_current_value() STABLE");
}

#[test]
fn test_alter_function_volatile() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // VOLATILE: function can modify database and/or return different results for same inputs
    pg_expect_parse_error!("ALTER FUNCTION random_number() VOLATILE");
}

// =============================================================================
// ALTER FUNCTION ... NULL HANDLING
// =============================================================================

#[test]
fn test_alter_function_strict() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // STRICT: function returns NULL if any argument is NULL (same as RETURNS NULL ON NULL INPUT)
    pg_expect_parse_error!("ALTER FUNCTION divide(NUMERIC, NUMERIC) STRICT");
}

#[test]
fn test_alter_function_returns_null_on_null_input() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Explicit form of STRICT
    pg_expect_parse_error!("ALTER FUNCTION divide(NUMERIC, NUMERIC) RETURNS NULL ON NULL INPUT");
}

#[test]
fn test_alter_function_called_on_null_input() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Function is called even if arguments are NULL
    pg_expect_parse_error!("ALTER FUNCTION coalesce_custom(TEXT, TEXT) CALLED ON NULL INPUT");
}

// =============================================================================
// ALTER FUNCTION ... PARALLEL SAFETY
// =============================================================================

#[test]
fn test_alter_function_parallel_safe() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // PARALLEL SAFE: function is safe to run in parallel mode
    pg_expect_parse_error!("ALTER FUNCTION calculate(INTEGER) PARALLEL SAFE");
}

#[test]
fn test_alter_function_parallel_unsafe() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // PARALLEL UNSAFE: function cannot be run in parallel mode
    pg_expect_parse_error!("ALTER FUNCTION write_log(TEXT) PARALLEL UNSAFE");
}

#[test]
fn test_alter_function_parallel_restricted() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // PARALLEL RESTRICTED: function can run in parallel mode but only in leader process
    pg_expect_parse_error!("ALTER FUNCTION get_sequence_value() PARALLEL RESTRICTED");
}

// =============================================================================
// ALTER FUNCTION ... COST/ROWS
// =============================================================================

#[test]
fn test_alter_function_cost() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // COST: estimated execution cost in units of cpu_operator_cost
    pg_expect_parse_error!("ALTER FUNCTION expensive_calc(INTEGER) COST 1000");
}

#[test]
fn test_alter_function_cost_fractional() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // COST can be a fractional value
    pg_expect_parse_error!("ALTER FUNCTION simple_calc(INTEGER) COST 0.5");
}

#[test]
fn test_alter_function_rows() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // ROWS: estimated number of rows for set-returning function
    pg_expect_parse_error!("ALTER FUNCTION generate_series_custom(INTEGER, INTEGER) ROWS 100");
}

#[test]
fn test_alter_function_cost_and_rows() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // COST and ROWS can be combined
    pg_expect_parse_error!("ALTER FUNCTION get_records() COST 500 ROWS 1000");
}

// =============================================================================
// ALTER FUNCTION ... SECURITY
// =============================================================================

#[test]
fn test_alter_function_security_definer() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // SECURITY DEFINER: function executes with privileges of owner
    pg_expect_parse_error!("ALTER FUNCTION privileged_operation() SECURITY DEFINER");
}

#[test]
fn test_alter_function_security_invoker() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // SECURITY INVOKER: function executes with privileges of caller (default)
    pg_expect_parse_error!("ALTER FUNCTION public_operation() SECURITY INVOKER");
}

// =============================================================================
// ALTER FUNCTION ... LEAKPROOF
// =============================================================================

#[test]
fn test_alter_function_leakproof() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // LEAKPROOF: function has no side effects and reveals no information about arguments
    pg_expect_parse_error!("ALTER FUNCTION compare_hash(TEXT, TEXT) LEAKPROOF");
}

#[test]
fn test_alter_function_not_leakproof() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // NOT LEAKPROOF: explicitly mark function as not leakproof
    pg_expect_parse_error!("ALTER FUNCTION debug_info(TEXT) NOT LEAKPROOF");
}

// =============================================================================
// ALTER FUNCTION ... SET/RESET
// =============================================================================

#[test]
fn test_alter_function_set_config() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // SET configuration parameter for function execution
    pg_expect_parse_error!("ALTER FUNCTION my_func() SET search_path TO myschema, public");
}

#[test]
fn test_alter_function_set_config_equals() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // SET with = syntax
    pg_expect_parse_error!("ALTER FUNCTION my_func() SET work_mem = '256MB'");
}

#[test]
fn test_alter_function_set_config_from_current() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // SET FROM CURRENT: set parameter to current session value
    pg_expect_parse_error!("ALTER FUNCTION my_func() SET timezone FROM CURRENT");
}

#[test]
fn test_alter_function_reset_config() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // RESET configuration parameter
    pg_expect_parse_error!("ALTER FUNCTION my_func() RESET search_path");
}

#[test]
fn test_alter_function_reset_all() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // RESET ALL: reset all function-specific configuration
    pg_expect_parse_error!("ALTER FUNCTION my_func() RESET ALL");
}

// =============================================================================
// ALTER FUNCTION ... DEPENDS ON EXTENSION
// =============================================================================

#[test]
fn test_alter_function_depends_on_extension() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Mark function as dependent on extension
    pg_expect_parse_error!("ALTER FUNCTION my_func() DEPENDS ON EXTENSION my_extension");
}

#[test]
fn test_alter_function_no_depends_on_extension() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Remove extension dependency
    pg_expect_parse_error!("ALTER FUNCTION my_func() NO DEPENDS ON EXTENSION my_extension");
}

// =============================================================================
// Complex combinations
// =============================================================================

#[test]
fn test_alter_function_multiple_options() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Multiple options can be specified in one statement
    pg_expect_parse_error!("ALTER FUNCTION calculate(INTEGER) IMMUTABLE PARALLEL SAFE COST 10");
}

#[test]
fn test_alter_function_all_common_options() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    // Comprehensive test with many common options
    pg_expect_parse_error!(
        "ALTER FUNCTION process_data(TEXT) STABLE STRICT SECURITY DEFINER PARALLEL RESTRICTED COST 500"
    );
}
