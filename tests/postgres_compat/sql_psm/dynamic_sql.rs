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

//! Tests for PL/pgSQL dynamic SQL execution
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN>

use crate::postgres_compat::common::*;

// =============================================================================
// Basic EXECUTE
// =============================================================================

#[test]
fn test_execute_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // Basic EXECUTE with string literal
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT 1';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_with_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE with SQL from variable
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    query TEXT := 'SELECT * FROM users';
BEGIN
    EXECUTE query;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_concatenated_query() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE with dynamically built query
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT) RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(table_name);
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// EXECUTE with INTO
// =============================================================================

#[test]
fn test_execute_into_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE with INTO to capture results
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM users' INTO result;
    RETURN result;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_into_record() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE INTO record type
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    rec RECORD;
BEGIN
    EXECUTE 'SELECT * FROM users WHERE id = 1' INTO rec;
    RAISE NOTICE 'User: %', rec.name;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_into_multiple_variables() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE INTO multiple variables
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    user_id INTEGER;
    user_name TEXT;
BEGIN
    EXECUTE 'SELECT id, name FROM users WHERE id = 1' INTO user_id, user_name;
    RAISE NOTICE 'User % has name %', user_id, user_name;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_into_strict() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE INTO STRICT (raises error if not exactly one row)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM users' INTO STRICT result;
    RETURN result;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// EXECUTE with USING (parameter substitution)
// =============================================================================

#[test]
fn test_execute_using_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE with USING for parameters
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(user_id INTEGER) RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT * FROM users WHERE id = $1' USING user_id;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_using_multiple_params() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE with multiple parameters
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(min_age INTEGER, max_age INTEGER) RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT * FROM users WHERE age BETWEEN $1 AND $2' USING min_age, max_age;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_into_using() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE with both INTO and USING
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(user_id INTEGER) RETURNS TEXT AS $$
DECLARE
    user_name TEXT;
BEGIN
    EXECUTE 'SELECT name FROM users WHERE id = $1' INTO user_name USING user_id;
    RETURN user_name;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Dynamic DDL
// =============================================================================

#[test]
fn test_execute_create_table() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE for CREATE TABLE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT) RETURNS void AS $$
BEGIN
    EXECUTE 'CREATE TABLE ' || quote_ident(table_name) || ' (id INTEGER, name TEXT)';
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_drop_table() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE for DROP TABLE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT) RETURNS void AS $$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(table_name);
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_alter_table() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE for ALTER TABLE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, column_name TEXT) RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('ALTER TABLE %I ADD COLUMN %I INTEGER', table_name, column_name);
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Dynamic DML
// =============================================================================

#[test]
fn test_execute_insert() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE for INSERT
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, user_name TEXT) RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('INSERT INTO %I (name) VALUES ($1)', table_name) USING user_name;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_update() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE for UPDATE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, user_id INTEGER) RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('UPDATE %I SET active = true WHERE id = $1', table_name) USING user_id;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_delete() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE for DELETE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, user_id INTEGER) RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('DELETE FROM %I WHERE id = $1', table_name) USING user_id;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Using FORMAT for query building
// =============================================================================

#[test]
fn test_execute_with_format() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE with FORMAT function for safer query building
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, column_name TEXT) RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('SELECT %I FROM %I', column_name, table_name);
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_format_with_literal() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // FORMAT with %L for literal values
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(value TEXT) RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('SELECT * FROM users WHERE name = %L', value);
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_format_mixed() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // FORMAT with mixed %I (identifier) and %L (literal)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, status TEXT) RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('UPDATE %I SET status = %L', table_name, status);
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// EXECUTE in loops
// =============================================================================

#[test]
fn test_execute_in_loop() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE in a loop for batch operations
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    LOOP
        EXECUTE FORMAT('GRANT SELECT ON %I TO readonly', table_name);
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// GET DIAGNOSTICS after EXECUTE
// =============================================================================

#[test]
fn test_execute_get_diagnostics_row_count() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // GET DIAGNOSTICS ROW_COUNT after EXECUTE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
DECLARE
    rows_affected INTEGER;
BEGIN
    EXECUTE 'UPDATE users SET active = true WHERE age >= 18';
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_found_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // FOUND variable after EXECUTE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    EXECUTE 'UPDATE users SET active = true WHERE id = 999';
    IF FOUND THEN
        RAISE NOTICE 'User updated';
    ELSE
        RAISE NOTICE 'User not found';
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// EXECUTE with RETURNING
// =============================================================================

#[test]
fn test_execute_insert_returning() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE INSERT with RETURNING clause
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(user_name TEXT) RETURNS INTEGER AS $$
DECLARE
    new_id INTEGER;
BEGIN
    EXECUTE 'INSERT INTO users (name) VALUES ($1) RETURNING id' INTO new_id USING user_name;
    RETURN new_id;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_update_returning() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // EXECUTE UPDATE with RETURNING clause
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(user_id INTEGER) RETURNS TEXT AS $$
DECLARE
    old_name TEXT;
BEGIN
    EXECUTE 'UPDATE users SET active = false WHERE id = $1 RETURNING name' 
        INTO old_name USING user_id;
    RETURN old_name;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Security considerations
// =============================================================================

#[test]
fn test_execute_with_quote_ident() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // Using quote_ident to prevent SQL injection
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT, column_name TEXT) RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT ' || quote_ident(column_name) || ' FROM ' || quote_ident(table_name);
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_with_quote_literal() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // Using quote_literal to safely embed literals
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(search_value TEXT) RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT * FROM users WHERE name = ' || quote_literal(search_value);
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_execute_with_quote_nullable() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    // Using quote_nullable (handles NULL values)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(search_value TEXT) RETURNS void AS $$
BEGIN
    EXECUTE 'SELECT * FROM users WHERE name = ' || quote_nullable(search_value);
END $$ LANGUAGE plpgsql"#
    );
}
