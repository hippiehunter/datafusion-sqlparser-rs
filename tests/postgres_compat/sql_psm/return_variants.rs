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

//! Tests for PL/pgSQL RETURN statement variants
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING>

use crate::postgres_compat::common::*;

// =============================================================================
// RETURN (single value)
// =============================================================================

#[test]
fn test_return_simple_value() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Basic RETURN with scalar value
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
BEGIN
    RETURN 42;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_expression() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN with expression
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER, y INTEGER) RETURNS INTEGER AS $$
BEGIN
    RETURN x + y;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_null() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NULL
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
BEGIN
    RETURN NULL;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_from_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN value from variable
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
DECLARE
    result INTEGER := 100;
BEGIN
    RETURN result;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_from_subquery() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN value from subquery
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM users);
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// RETURN NEXT (for set-returning functions)
// =============================================================================

#[test]
fn test_return_next_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NEXT to build result set one row at a time
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF INTEGER AS $$
BEGIN
    RETURN NEXT 1;
    RETURN NEXT 2;
    RETURN NEXT 3;
    RETURN;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_next_in_loop() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NEXT in a loop
    pg_expect_parse_error!(
        r#"CREATE FUNCTION generate_series_plpgsql(start INTEGER, stop INTEGER) RETURNS SETOF INTEGER AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN start..stop LOOP
        RETURN NEXT i;
    END LOOP;
    RETURN;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_next_record() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NEXT with record type
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
DECLARE
    r users%ROWTYPE;
BEGIN
    FOR r IN SELECT * FROM users LOOP
        RETURN NEXT r;
    END LOOP;
    RETURN;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_next_with_modification() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NEXT with modified row
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
DECLARE
    r users%ROWTYPE;
BEGIN
    FOR r IN SELECT * FROM users LOOP
        r.name := UPPER(r.name);
        RETURN NEXT r;
    END LOOP;
    RETURN;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// RETURN QUERY (for set-returning functions)
// =============================================================================

#[test]
fn test_return_query_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY to return entire query result
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users WHERE active = true;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_query_with_parameters() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY with function parameters
    pg_expect_parse_error!(
        r#"CREATE FUNCTION get_active_users(min_age INTEGER) RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users WHERE active = true AND age >= min_age;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_query_multiple_times() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Multiple RETURN QUERY statements (results are concatenated)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users WHERE role = 'admin';
    RETURN QUERY SELECT * FROM users WHERE role = 'moderator';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_query_with_ordering() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY with ORDER BY
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users ORDER BY created_at DESC LIMIT 10;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// RETURN QUERY EXECUTE (dynamic SQL)
// =============================================================================

#[test]
fn test_return_query_execute() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY EXECUTE for dynamic SQL
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT) RETURNS SETOF RECORD AS $$
BEGIN
    RETURN QUERY EXECUTE 'SELECT * FROM ' || quote_ident(table_name);
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_query_execute_with_parameters() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY EXECUTE with USING parameters
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(min_age INTEGER) RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY EXECUTE 'SELECT * FROM users WHERE age >= $1' USING min_age;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_query_execute_dynamic_condition() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY EXECUTE with dynamically built query
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, column_name TEXT, threshold INTEGER) RETURNS SETOF RECORD AS $$
DECLARE
    query TEXT;
BEGIN
    query := FORMAT('SELECT * FROM %I WHERE %I > $1', table_name, column_name);
    RETURN QUERY EXECUTE query USING threshold;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Mixing RETURN variants
// =============================================================================

#[test]
fn test_return_next_and_return_query() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Mixing RETURN NEXT and RETURN QUERY
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF INTEGER AS $$
BEGIN
    RETURN NEXT 0;
    RETURN QUERY SELECT generate_series(1, 10);
    RETURN NEXT 11;
    RETURN;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_in_conditional() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN in conditional branches
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS TEXT AS $$
BEGIN
    IF x < 0 THEN
        RETURN 'negative';
    ELSIF x > 0 THEN
        RETURN 'positive';
    ELSE
        RETURN 'zero';
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// RETURN TABLE functions
// =============================================================================

#[test]
fn test_return_table_function() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Function with RETURNS TABLE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS TABLE(id INTEGER, name TEXT) AS $$
BEGIN
    RETURN QUERY SELECT user_id, user_name FROM users;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_table_with_computation() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURNS TABLE with computed columns
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS TABLE(name TEXT, age_years INTEGER, age_days INTEGER) AS $$
BEGIN
    RETURN QUERY SELECT user_name, user_age, user_age * 365 FROM users;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Early RETURN (function exit)
// =============================================================================

#[test]
fn test_return_early_exit() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Early RETURN to exit function
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS INTEGER AS $$
BEGIN
    IF x = 0 THEN
        RETURN 0;  -- Early exit
    END IF;

    RETURN 100 / x;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_from_exception_handler() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN from exception handler
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER, y INTEGER) RETURNS INTEGER AS $$
BEGIN
    RETURN x / y;
EXCEPTION
    WHEN division_by_zero THEN
        RETURN 0;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// RETURN without value (procedures)
// =============================================================================

#[test]
fn test_return_void() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN in void function (just exits)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Starting';
    RETURN;
    RAISE NOTICE 'Never reached';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_setof_final() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Final RETURN in SETOF function (ends iteration)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS SETOF INTEGER AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..10 LOOP
        RETURN NEXT i * i;
    END LOOP;
    RETURN;  -- End of result set
END $$ LANGUAGE plpgsql"#
    );
}
