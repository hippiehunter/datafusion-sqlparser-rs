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

//! Tests for basic DO block syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-do.html>

use crate::postgres_compat::common::*;

// =============================================================================
// Basic DO Block Syntax
// =============================================================================

#[test]
fn test_do_block_minimal() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Simplest possible DO block with NULL statement
    pg_expect_parse_error!("DO $$ BEGIN NULL; END $$", "Expected");
}

#[test]
fn test_do_block_empty_begin_end() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // DO block with empty BEGIN/END block
    pg_expect_parse_error!("DO $$ BEGIN END $$", "Expected");
}

#[test]
fn test_do_block_simple_statement() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // DO block with a simple SQL statement
    pg_expect_parse_error!("DO $$ BEGIN SELECT 1; END $$", "Expected");
}

#[test]
fn test_do_block_with_declare() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // DO block with variable declaration
    pg_expect_parse_error!("DO $$ DECLARE x INTEGER; BEGIN x := 1; END $$", "Expected");
}

#[test]
fn test_do_block_multiple_variables() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // DO block with multiple variable declarations
    pg_expect_parse_error!(
        "DO $$ DECLARE x INTEGER; y TEXT; BEGIN x := 1; y := 'hello'; END $$",
        "Expected"
    );
}

// =============================================================================
// Language Specification
// =============================================================================

#[test]
fn test_do_language_plpgsql_before() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // LANGUAGE clause before the code block
    pg_expect_parse_error!(
        "DO LANGUAGE plpgsql $$ BEGIN RAISE NOTICE 'test'; END $$",
        "Expected"
    );
}

#[test]
fn test_do_language_plpgsql_after() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // LANGUAGE clause after the code block (less common but valid)
    pg_expect_parse_error!(
        "DO $$ BEGIN RAISE NOTICE 'test'; END $$ LANGUAGE plpgsql",
        "Expected"
    );
}

#[test]
fn test_do_language_sql() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // DO block with SQL language (rarely used, plpgsql is default)
    pg_expect_parse_error!("DO LANGUAGE sql $$ SELECT 1 $$", "Expected");
}

#[test]
fn test_do_language_case_insensitive() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Language names are case-insensitive
    pg_expect_parse_error!("DO LANGUAGE PLPGSQL $$ BEGIN NULL; END $$", "Expected");
}

// =============================================================================
// Dollar Quoting Variants
// =============================================================================

#[test]
fn test_do_block_tagged_dollar_quotes() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Dollar quotes with custom tags to avoid conflicts
    pg_expect_parse_error!(
        "DO $body$ BEGIN RAISE NOTICE 'test'; END $body$",
        "Expected"
    );
}

#[test]
fn test_do_block_nested_dollar_quotes() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Nested dollar quotes with different tags
    pg_expect_parse_error!(
        "DO $outer$ BEGIN EXECUTE $inner$ SELECT 1 $inner$; END $outer$",
        "Expected"
    );
}

#[test]
fn test_do_block_single_quoted_body() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Single-quoted body (requires escaping, less common)
    pg_expect_parse_error!("DO 'BEGIN NULL; END'", "Expected");
}

// =============================================================================
// RAISE Statements
// =============================================================================

#[test]
fn test_do_block_raise_notice() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // RAISE NOTICE for informational messages
    pg_expect_parse_error!(
        "DO $$ BEGIN RAISE NOTICE 'Hello, World!'; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_raise_notice_with_variables() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // RAISE NOTICE with variable interpolation
    pg_expect_parse_error!(
        "DO $$ DECLARE msg TEXT := 'test'; BEGIN RAISE NOTICE 'Message: %', msg; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_raise_exception() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // RAISE EXCEPTION to abort with error
    pg_expect_parse_error!(
        "DO $$ BEGIN RAISE EXCEPTION 'An error occurred'; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_raise_warning() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // RAISE WARNING for warnings
    pg_expect_parse_error!(
        "DO $$ BEGIN RAISE WARNING 'This is a warning'; END $$",
        "Expected"
    );
}

// =============================================================================
// Control Flow
// =============================================================================

#[test]
fn test_do_block_if_statement() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // IF statement for conditional execution
    pg_expect_parse_error!(
        "DO $$ BEGIN IF 1 = 1 THEN RAISE NOTICE 'true'; END IF; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_if_else_statement() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // IF-ELSE statement
    pg_expect_parse_error!(
        "DO $$ BEGIN IF 1 = 2 THEN RAISE NOTICE 'false'; ELSE RAISE NOTICE 'true'; END IF; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_loop() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Basic LOOP with EXIT
    pg_expect_parse_error!(
        "DO $$ DECLARE i INTEGER := 0; BEGIN LOOP i := i + 1; EXIT WHEN i > 5; END LOOP; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_for_loop() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // FOR loop over integer range
    pg_expect_parse_error!(
        "DO $$ DECLARE i INTEGER; BEGIN FOR i IN 1..10 LOOP RAISE NOTICE '%', i; END LOOP; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_for_query_loop() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // FOR loop over query results
    pg_expect_parse_error!(
        "DO $$ DECLARE r RECORD; BEGIN FOR r IN SELECT * FROM pg_tables LOOP RAISE NOTICE '%', r.tablename; END LOOP; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_while_loop() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // WHILE loop with condition
    pg_expect_parse_error!(
        "DO $$ DECLARE i INTEGER := 0; BEGIN WHILE i < 5 LOOP i := i + 1; END LOOP; END $$",
        "Expected"
    );
}

// =============================================================================
// Exception Handling
// =============================================================================

#[test]
fn test_do_block_exception_handler() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Exception handling with EXCEPTION block
    pg_expect_parse_error!(
        "DO $$ BEGIN RAISE EXCEPTION 'error'; EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'caught'; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_specific_exception() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Catching specific exception types
    pg_expect_parse_error!(
        "DO $$ BEGIN INSERT INTO nonexistent VALUES (1); EXCEPTION WHEN undefined_table THEN RAISE NOTICE 'table not found'; END $$",
        "Expected"
    );
}

// =============================================================================
// DML Operations
// =============================================================================

#[test]
fn test_do_block_insert() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // INSERT statement in DO block
    pg_expect_parse_error!(
        "DO $$ BEGIN INSERT INTO users (name) VALUES ('Alice'); END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_update() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // UPDATE statement in DO block
    pg_expect_parse_error!(
        "DO $$ BEGIN UPDATE users SET active = true WHERE id = 1; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_delete() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // DELETE statement in DO block
    pg_expect_parse_error!(
        "DO $$ BEGIN DELETE FROM users WHERE inactive = true; END $$",
        "Expected"
    );
}

// =============================================================================
// Dynamic SQL
// =============================================================================

#[test]
fn test_do_block_execute() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // EXECUTE for dynamic SQL
    pg_expect_parse_error!("DO $$ BEGIN EXECUTE 'SELECT 1'; END $$", "Expected");
}

#[test]
fn test_do_block_execute_with_into() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // EXECUTE with INTO for capturing results
    pg_expect_parse_error!(
        "DO $$ DECLARE result INTEGER; BEGIN EXECUTE 'SELECT 42' INTO result; END $$",
        "Expected"
    );
}

#[test]
fn test_do_block_execute_with_parameters() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // EXECUTE with parameter substitution
    pg_expect_parse_error!(
        "DO $$ BEGIN EXECUTE 'SELECT * FROM users WHERE id = $1' USING 123; END $$",
        "Expected"
    );
}

// =============================================================================
// PERFORM (PL/pgSQL specific)
// =============================================================================

#[test]
fn test_do_block_perform() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // PERFORM executes query and discards results (PL/pgSQL only)
    pg_expect_parse_error!(
        "DO $$ BEGIN PERFORM * FROM users WHERE active = true; END $$",
        "Expected"
    );
}

// =============================================================================
// Complex Real-World Examples
// =============================================================================

#[test]
fn test_do_block_administrative_task() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Real-world example: Grant permissions to all tables
    pg_expect_parse_error!(
        r#"DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    LOOP
        EXECUTE 'GRANT SELECT ON ' || quote_ident(r.tablename) || ' TO readonly';
    END LOOP;
END $$"#,
        "Expected"
    );
}

#[test]
fn test_do_block_migration_script() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // Real-world example: Conditional schema migration
    pg_expect_parse_error!(
        r#"DO LANGUAGE plpgsql $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'users' AND column_name = 'email'
    ) THEN
        ALTER TABLE users ADD COLUMN email VARCHAR(255);
        RAISE NOTICE 'Added email column to users table';
    ELSE
        RAISE NOTICE 'Email column already exists';
    END IF;
END $$"#,
        "Expected"
    );
}

#[test]
fn test_do_block_with_transaction_control() {
    // https://www.postgresql.org/docs/current/sql-do.html
    // DO block with COMMIT (only in procedures, not functions)
    // Note: COMMIT/ROLLBACK only work in DO blocks when called as procedures
    pg_expect_parse_error!(
        r#"DO $$
BEGIN
    INSERT INTO log (message) VALUES ('Starting batch');
    COMMIT;
    INSERT INTO log (message) VALUES ('Completed batch');
    COMMIT;
END $$"#,
        "Expected"
    );
}
