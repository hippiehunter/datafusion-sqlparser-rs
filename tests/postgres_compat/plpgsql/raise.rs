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

//! Tests for PL/pgSQL RAISE statements
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html>

use crate::postgres_compat::common::*;

// =============================================================================
// RAISE Levels
// =============================================================================

#[test]
fn test_raise_debug() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE DEBUG - lowest priority, only logged if log_min_messages is DEBUG
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE DEBUG 'Debug information';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_log() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE LOG - written to server log only
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE LOG 'Log message';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_info() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE INFO - informational message sent to client
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE INFO 'Informational message';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_notice() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE NOTICE - notice sent to client (default)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'This is a notice';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_warning() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE WARNING - warning message sent to client
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE WARNING 'This is a warning';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_exception() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE EXCEPTION - error that aborts the transaction
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'This is an error';
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Message Formatting
// =============================================================================

#[test]
fn test_raise_with_format_string() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with % format placeholders
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'User % has % items', 'Alice', 42;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_multiple_placeholders() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // Multiple % placeholders
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    username TEXT := 'Bob';
    age INTEGER := 30;
    city TEXT := 'NYC';
BEGIN
    RAISE NOTICE 'User: %, Age: %, City: %', username, age, city;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_literal_percent() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // Use %% to include a literal %
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Progress: 50%%';
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// RAISE with Options
// =============================================================================

#[test]
fn test_raise_with_using_message() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING MESSAGE option
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'error_code' USING MESSAGE = 'Custom error message';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_detail() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING DETAIL option
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Something went wrong'
        USING DETAIL = 'Additional details about the error';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_hint() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING HINT option
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Invalid input'
        USING HINT = 'Try using a positive number';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_errcode() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING ERRCODE option (SQLSTATE code)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Custom error'
        USING ERRCODE = '22012';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_errcode_name() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING ERRCODE symbolic name
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Division by zero detected'
        USING ERRCODE = 'division_by_zero';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_multiple_using_options() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with multiple USING options
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Operation failed'
        USING
            DETAIL = 'The operation could not complete',
            HINT = 'Check the input parameters',
            ERRCODE = 'P0001';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_column() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING COLUMN option (for constraint violations)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Invalid column value'
        USING COLUMN = 'email';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_constraint() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING CONSTRAINT option
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Constraint violation'
        USING CONSTRAINT = 'users_email_key';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_table() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING TABLE option
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Table error'
        USING TABLE = 'users';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_schema() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING SCHEMA option
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Schema error'
        USING SCHEMA = 'public';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_using_datatype() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE with USING DATATYPE option
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE EXCEPTION 'Datatype error'
        USING DATATYPE = 'integer';
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// RAISE without a message (re-raising)
// =============================================================================

#[test]
fn test_raise_without_message() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE without message re-raises the current exception
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    BEGIN
        RAISE EXCEPTION 'Original error';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE;  -- Re-raise the caught exception
    END;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_with_sqlstate() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE SQLSTATE for custom error codes
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE SQLSTATE '12345';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_raise_sqlstate_with_using() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE SQLSTATE with USING options
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE SQLSTATE '12345'
        USING MESSAGE = 'Custom SQLSTATE error',
              DETAIL = 'Additional information';
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Conditional RAISE
// =============================================================================

#[test]
fn test_raise_in_if_statement() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
    // RAISE in conditional logic
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(value INTEGER) RETURNS void AS $$
BEGIN
    IF value < 0 THEN
        RAISE EXCEPTION 'Value cannot be negative: %', value;
    ELSIF value = 0 THEN
        RAISE WARNING 'Value is zero';
    ELSE
        RAISE NOTICE 'Value is positive: %', value;
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Assert Statement
// =============================================================================

#[test]
fn test_assert_basic() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-ASSERT
    // ASSERT statement (raises error if condition is false)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS void AS $$
BEGIN
    ASSERT x > 0;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_assert_with_message() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-ASSERT
    // ASSERT with custom error message
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS void AS $$
BEGIN
    ASSERT x > 0, 'x must be positive';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_assert_with_formatted_message() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-ASSERT
    // ASSERT with formatted message
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS void AS $$
BEGIN
    ASSERT x BETWEEN 1 AND 100, FORMAT('Value %s is out of range', x);
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_assert_disabled_by_config() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-ASSERT
    // ASSERT can be disabled with plpgsql.check_asserts = off
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS void AS $$
BEGIN
    -- This assert is skipped if plpgsql.check_asserts is off
    ASSERT x IS NOT NULL, 'x cannot be null';
    RAISE NOTICE 'Value: %', x;
END $$ LANGUAGE plpgsql"#
    );
}
