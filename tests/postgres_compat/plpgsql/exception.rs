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

//! Tests for PL/pgSQL exception handling
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING>

use crate::postgres_compat::common::*;

// =============================================================================
// Basic Exception Handling
// =============================================================================

#[test]
fn test_exception_when_others() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Basic EXCEPTION WHEN OTHERS handler (catches all errors)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Caught an error';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_specific_condition() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Catching a specific error condition by name
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    PERFORM 1 / 0;
EXCEPTION
    WHEN division_by_zero THEN
        RAISE NOTICE 'Cannot divide by zero';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_multiple_conditions() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Multiple WHEN clauses for different error types
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    -- Some operation
    PERFORM operation();
EXCEPTION
    WHEN division_by_zero THEN
        RAISE NOTICE 'Division by zero';
    WHEN numeric_value_out_of_range THEN
        RAISE NOTICE 'Numeric overflow';
    WHEN OTHERS THEN
        RAISE NOTICE 'Unknown error';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_or_conditions() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Multiple conditions in a single WHEN clause (OR)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    -- Some operation
    PERFORM operation();
EXCEPTION
    WHEN division_by_zero OR numeric_value_out_of_range THEN
        RAISE NOTICE 'Math error occurred';
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Exception Information Variables
// =============================================================================

#[test]
fn test_exception_sqlerrm() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Using SQLERRM to get error message
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error message: %', SQLERRM;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_sqlstate() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Using SQLSTATE to get error code
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error code: %', SQLSTATE;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_both_sqlerrm_and_sqlstate() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Using both SQLERRM and SQLSTATE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error [%]: %', SQLSTATE, SQLERRM;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// GET DIAGNOSTICS
// =============================================================================

#[test]
fn test_get_diagnostics_returned_sqlstate() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS for detailed error information
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    error_code TEXT;
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        GET DIAGNOSTICS error_code = RETURNED_SQLSTATE;
        RAISE NOTICE 'Error code: %', error_code;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_message_text() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS MESSAGE_TEXT
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    error_msg TEXT;
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        GET DIAGNOSTICS error_msg = MESSAGE_TEXT;
        RAISE NOTICE 'Error: %', error_msg;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_multiple_items() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS with multiple items
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    error_code TEXT;
    error_msg TEXT;
    error_detail TEXT;
    error_hint TEXT;
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        GET DIAGNOSTICS
            error_code = RETURNED_SQLSTATE,
            error_msg = MESSAGE_TEXT,
            error_detail = PG_EXCEPTION_DETAIL,
            error_hint = PG_EXCEPTION_HINT;
        RAISE NOTICE 'Code: %, Message: %', error_code, error_msg;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_pg_exception_context() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS PG_EXCEPTION_CONTEXT (stack trace)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    error_context TEXT;
BEGIN
    RAISE EXCEPTION 'test error';
EXCEPTION
    WHEN OTHERS THEN
        GET DIAGNOSTICS error_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'Context: %', error_context;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_column_name() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS COLUMN_NAME (for constraint violations)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    col_name TEXT;
BEGIN
    INSERT INTO users (id) VALUES (NULL);
EXCEPTION
    WHEN not_null_violation THEN
        GET DIAGNOSTICS col_name = COLUMN_NAME;
        RAISE NOTICE 'Column % cannot be null', col_name;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_constraint_name() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS CONSTRAINT_NAME
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    constraint_name TEXT;
BEGIN
    INSERT INTO users (email) VALUES ('duplicate@example.com');
EXCEPTION
    WHEN unique_violation THEN
        GET DIAGNOSTICS constraint_name = CONSTRAINT_NAME;
        RAISE NOTICE 'Constraint violated: %', constraint_name;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_table_name() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS TABLE_NAME
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    tbl_name TEXT;
BEGIN
    INSERT INTO nonexistent_table VALUES (1);
EXCEPTION
    WHEN undefined_table THEN
        GET DIAGNOSTICS tbl_name = TABLE_NAME;
        RAISE NOTICE 'Table not found: %', tbl_name;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_schema_name() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS SCHEMA_NAME
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    schema_name TEXT;
BEGIN
    SELECT * FROM nonexistent_schema.table1;
EXCEPTION
    WHEN undefined_table THEN
        GET DIAGNOSTICS schema_name = SCHEMA_NAME;
        RAISE NOTICE 'Schema: %', schema_name;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_get_diagnostics_datatype_name() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS DATATYPE_NAME
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    dtype_name TEXT;
BEGIN
    PERFORM 'text'::invalid_type;
EXCEPTION
    WHEN undefined_object THEN
        GET DIAGNOSTICS dtype_name = DATATYPE_NAME;
        RAISE NOTICE 'Datatype: %', dtype_name;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Nested Exception Handling
// =============================================================================

#[test]
fn test_nested_exception_blocks() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Nested BEGIN/EXCEPTION blocks
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    BEGIN
        RAISE EXCEPTION 'inner error';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Caught inner exception';
            RAISE EXCEPTION 'outer error';
    END;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Caught outer exception';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_re_raising_exception() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    // Re-raising an exception with RAISE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'original error';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Logging error: %', SQLERRM;
        RAISE;  -- Re-raise the same exception
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Common Exception Conditions
// =============================================================================

#[test]
fn test_exception_unique_violation() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // unique_violation (23505)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    INSERT INTO users (id, email) VALUES (1, 'test@example.com');
EXCEPTION
    WHEN unique_violation THEN
        RAISE NOTICE 'Duplicate key detected';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_foreign_key_violation() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // foreign_key_violation (23503)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    INSERT INTO orders (user_id) VALUES (99999);
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'Referenced user does not exist';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_not_null_violation() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // not_null_violation (23502)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    INSERT INTO users (name) VALUES (NULL);
EXCEPTION
    WHEN not_null_violation THEN
        RAISE NOTICE 'Name cannot be null';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_check_violation() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // check_violation (23514)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    INSERT INTO users (age) VALUES (-5);
EXCEPTION
    WHEN check_violation THEN
        RAISE NOTICE 'Check constraint failed';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_no_data_found() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // no_data_found (P0002) - raised by SELECT INTO with no rows
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    result INTEGER;
BEGIN
    SELECT id INTO STRICT result FROM users WHERE id = 99999;
EXCEPTION
    WHEN no_data_found THEN
        RAISE NOTICE 'No user found';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_too_many_rows() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // too_many_rows (P0003) - raised by SELECT INTO STRICT with multiple rows
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    result INTEGER;
BEGIN
    SELECT id INTO STRICT result FROM users;
EXCEPTION
    WHEN too_many_rows THEN
        RAISE NOTICE 'Multiple rows found, expected one';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_undefined_table() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // undefined_table (42P01)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT * FROM nonexistent_table';
EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'Table does not exist';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exception_undefined_column() {
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // undefined_column (42703)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT nonexistent_column FROM users';
EXCEPTION
    WHEN undefined_column THEN
        RAISE NOTICE 'Column does not exist';
END $$ LANGUAGE plpgsql"#
    );
}
