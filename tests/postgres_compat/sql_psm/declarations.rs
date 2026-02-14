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

//! Tests for PL/pgSQL DECLARE section and variable declarations
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql-declarations.html>

use crate::postgres_compat::common::*;

// =============================================================================
// Basic Variable Declarations
// =============================================================================

#[test]
fn test_declare_single_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Single variable declaration with type
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE x INTEGER; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_multiple_variables() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Multiple variable declarations
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    x INTEGER;
    y TEXT;
    z BOOLEAN;
BEGIN
    NULL;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_with_default_value() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Variable with default value using DEFAULT
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE x INTEGER DEFAULT 42; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_with_assignment() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Variable with default value using := assignment
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE x INTEGER := 100; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_constant() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Constant variable (immutable)
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE PI CONSTANT NUMERIC := 3.14159; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_not_null() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // NOT NULL constraint on variable
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE x INTEGER NOT NULL := 0; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_constant_not_null() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Constant with NOT NULL
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE MAX_SIZE CONSTANT INTEGER NOT NULL := 1000; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

// =============================================================================
// Type Specifications
// =============================================================================

#[test]
fn test_declare_type_from_table_column() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Variable type from table column using %TYPE
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE user_name users.name%TYPE; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_type_from_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Variable type from another variable using %TYPE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    x INTEGER;
    y x%TYPE;
BEGIN
    NULL;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_row_type() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Row type variable using %ROWTYPE
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE user_row users%ROWTYPE; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_record_type() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Generic RECORD type
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE rec RECORD; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

// =============================================================================
// Array and Composite Types
// =============================================================================

#[test]
fn test_declare_array_type() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Array type declaration
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE numbers INTEGER[]; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_array_with_initial_value() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Array with initial values
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE numbers INTEGER[] := ARRAY[1, 2, 3]; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_composite_type() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Custom composite type
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    person person_type;
BEGIN
    NULL;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Special Variable Types
// =============================================================================

#[test]
fn test_declare_found_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    // FOUND is a special boolean variable (implicitly declared)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    SELECT * FROM users WHERE id = 1;
    IF FOUND THEN
        RAISE NOTICE 'User found';
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_with_collation() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Variable with explicit collation
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE name TEXT COLLATE \"en_US\"; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

// =============================================================================
// Default Value Expressions
// =============================================================================

#[test]
fn test_declare_default_from_function() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Default value from function call
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE created_at TIMESTAMP := NOW(); BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_default_from_subquery() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Default value from subquery
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE user_count INTEGER := (SELECT COUNT(*) FROM users); BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

#[test]
fn test_declare_default_from_expression() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Default value from expression
    pg_expect_parse_error!(
        "CREATE FUNCTION test() RETURNS void AS $$ DECLARE doubled INTEGER := 2 * 21; BEGIN NULL; END $$ LANGUAGE plpgsql"
    );
}

// =============================================================================
// Variable Scope and Shadowing
// =============================================================================

#[test]
fn test_declare_block_scope() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Nested blocks with variable scope
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    x INTEGER := 1;
BEGIN
    DECLARE
        x INTEGER := 2;
    BEGIN
        RAISE NOTICE 'Inner x: %', x;
    END;
    RAISE NOTICE 'Outer x: %', x;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_label_qualification() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // Using block labels to qualify variable names
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
<<outer>>
DECLARE
    x INTEGER := 1;
BEGIN
    <<inner>>
    DECLARE
        x INTEGER := 2;
    BEGIN
        RAISE NOTICE 'Outer x: %', outer.x;
        RAISE NOTICE 'Inner x: %', inner.x;
    END;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Parameter Aliases
// =============================================================================

#[test]
fn test_declare_alias_for_parameter() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // ALIAS for function parameters (legacy syntax)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(user_id INTEGER) RETURNS void AS $$
DECLARE
    uid ALIAS FOR user_id;
BEGIN
    RAISE NOTICE 'User ID: %', uid;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_alias_for_dollar_parameter() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    // ALIAS for $n parameter notation
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(INTEGER) RETURNS void AS $$
DECLARE
    user_id ALIAS FOR $1;
BEGIN
    RAISE NOTICE 'User ID: %', user_id;
END $$ LANGUAGE plpgsql"#
    );
}
