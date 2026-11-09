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

//! Tests for CREATE FUNCTION body variants and quoting
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createfunction.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{CreateFunctionBody, DoBody, ProcedureBody, Statement};

/// A PL/pgSQL body with comments, mixed case, `PERFORM` and a reversed
/// stepped integer loop.
const PL_SOURCE: &str = "\nDECLARE\n  Total Integer := 0; -- c\nBEGIN\n  /* b */ PERFORM 1;\n  FOR i IN REVERSE n..1 BY 2 LOOP\n    total := total + i;\n  END LOOP;\n  Return total;\nEND\n";

#[test]
fn string_body_source_is_verbatim() {
    // https://www.postgresql.org/docs/current/catalog-pg-proc.html
    // PostgreSQL stores a routine's string body as written (prosrc), and
    // pg_dump writes that text back unchanged.
    let sql = format!(
        "CREATE FUNCTION f(n INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $${PL_SOURCE}$$"
    );
    let statement = verified_pg_stmt(&sql);
    assert_eq!(statement.to_string(), sql);
    let Some(CreateFunctionBody::PlBlock(body)) = &extract_create_function(&statement).function_body
    else {
        panic!("expected a PL/pgSQL body");
    };
    assert_eq!(body.source, PL_SOURCE);
    assert_eq!(body.block.declarations.len(), 1);
    assert_eq!(body.block.statements.len(), 3);

    // A single-quoted body keeps the string's value; a value containing `$`
    // is written back with a delimiter that does not occur in it, as pg_dump
    // chooses one.
    let quoted =
        verified_pg_stmt("CREATE FUNCTION g() RETURNS TEXT LANGUAGE plpgsql AS 'BEGIN RETURN ''$$''; END'");
    let Some(CreateFunctionBody::PlBlock(body)) = &extract_create_function(&quoted).function_body
    else {
        panic!("expected a PL/pgSQL body");
    };
    assert_eq!(body.source, "BEGIN RETURN '$$'; END");
    assert_eq!(
        quoted.to_string(),
        "CREATE FUNCTION g() RETURNS TEXT LANGUAGE plpgsql AS $_$BEGIN RETURN '$$'; END$_$"
    );
    assert_eq!(quoted, verified_pg_stmt(&quoted.to_string()));

    // CREATE PROCEDURE and DO keep their source the same way.
    let procedure = format!("CREATE PROCEDURE p(n INTEGER) LANGUAGE plpgsql AS $${PL_SOURCE}$$");
    let statement = verified_pg_stmt(&procedure);
    assert_eq!(statement.to_string(), procedure);
    let Statement::CreateProcedure {
        body: ProcedureBody::PlBlock(body),
        ..
    } = &statement
    else {
        panic!("expected a PL/pgSQL procedure body");
    };
    assert_eq!(body.source, PL_SOURCE);

    let sql_procedure = "CREATE PROCEDURE q() LANGUAGE sql AS $$ insert into t values (1); -- c\n$$";
    let statement = verified_pg_stmt(sql_procedure);
    assert_eq!(statement.to_string(), sql_procedure);
    let Statement::CreateProcedure {
        body: ProcedureBody::SqlStatements { source, statements },
        ..
    } = &statement
    else {
        panic!("expected a SQL procedure body");
    };
    assert_eq!(source, " insert into t values (1); -- c\n");
    assert_eq!(statements.len(), 1);

    let do_block = "DO $$\nBEGIN\n  /* b */ PERFORM 1; -- c\nEND\n$$";
    let statement = verified_pg_stmt(do_block);
    assert_eq!(statement.to_string(), do_block);
    let Statement::Do(statement) = &statement else {
        panic!("expected DO");
    };
    let DoBody::Block(body) = &statement.body else {
        panic!("expected a PL/pgSQL DO body");
    };
    assert_eq!(body.source, "\nBEGIN\n  /* b */ PERFORM 1; -- c\nEND\n");
}

#[test]
fn test_create_function_body_single_quotes() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Simple body with single quotes
    pg_test!(
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL AS 'SELECT $1 + $2'",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_dollar_quotes() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Dollar quoting is the standard way to avoid escaping
    pg_test!(
        "CREATE FUNCTION hello() RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'Hello, World!' $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_custom_dollar_tag() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Dollar quotes can have custom tags
    pg_test!(
        "CREATE FUNCTION body_test() RETURNS TEXT LANGUAGE SQL AS $body$ SELECT 'test' $body$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_nested_quotes() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Dollar quotes allow nested single quotes without escaping
    pg_test!(
        r#"CREATE FUNCTION quote_test() RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'It''s a test' $$"#,
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_multiline() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function body can span multiple lines
    pg_test!(
        "CREATE FUNCTION multi() RETURNS INTEGER LANGUAGE SQL AS $$
    SELECT 1 + 2 + 3
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_plpgsql_begin_end() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PL/pgSQL functions use BEGIN...END blocks
    pg_test!(
        "CREATE FUNCTION increment(i INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$
BEGIN
    RETURN i + 1;
END
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "plpgsql");
        }
    );
}

#[test]
fn test_create_function_body_plpgsql_declare() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PL/pgSQL with DECLARE section
    pg_test!(
        "CREATE FUNCTION calc() RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    result INTEGER;
BEGIN
    result := 42;
    RETURN result;
END
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_sql_multiple_statements() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // SQL language functions can have multiple statements
    pg_test!(
        "CREATE FUNCTION insert_and_return() RETURNS INTEGER LANGUAGE SQL AS $$
    INSERT INTO logs (message) VALUES ('test');
    SELECT 1;
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_return_keyword() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURN keyword for simple expressions (SQL:2016 syntax)
    pg_test!(
        "CREATE FUNCTION get_pi() RETURNS NUMERIC LANGUAGE SQL RETURN 3.14159",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_return_expression() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURN with expression
    pg_test!(
        "CREATE FUNCTION square(x INTEGER) RETURNS INTEGER LANGUAGE SQL RETURN x * x",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_return_query() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURN with query
    pg_test!(
        "CREATE FUNCTION get_count() RETURNS BIGINT LANGUAGE SQL RETURN (SELECT COUNT(*) FROM users)",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_c_language_object_file() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // C language functions reference object file and symbol
    // EXPECTED TO FAIL: AS 'file', 'symbol' syntax not yet supported
    pg_expect_parse_error!(
        "CREATE FUNCTION add_one(INTEGER) RETURNS INTEGER LANGUAGE C AS 'my_module', 'add_one_func'"
    );
}

#[test]
fn test_create_function_c_language_dollar_lib() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // $libdir macro for C functions
    pg_test!(
        "CREATE FUNCTION my_c_func(INTEGER) RETURNS INTEGER LANGUAGE C AS '$libdir/my_extension'",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "c");
        }
    );
}

#[test]
fn test_create_function_internal_language() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Internal functions reference internal symbol name
    pg_test!(
        "CREATE FUNCTION pg_sleep(DOUBLE PRECISION) RETURNS VOID LANGUAGE internal AS 'pg_sleep'",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "internal");
        }
    );
}

#[test]
fn test_create_function_body_with_comments() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function body can contain SQL comments
    pg_test!(
        "CREATE FUNCTION commented() RETURNS INTEGER LANGUAGE SQL AS $$
    -- This is a comment
    SELECT 1 -- inline comment
    /* block comment */
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_complex_plpgsql() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Complex PL/pgSQL with control structures
    pg_test!(
        "CREATE FUNCTION factorial(n INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    result INTEGER := 1;
    i INTEGER;
BEGIN
    IF n < 0 THEN
        RAISE EXCEPTION 'Negative input not allowed';
    END IF;

    FOR i IN 1..n LOOP
        result := result * i;
    END LOOP;

    RETURN result;
END
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "plpgsql");
        }
    );
}

#[test]
fn test_create_function_body_exception_handling() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PL/pgSQL with exception handling
    pg_test!(
        "CREATE FUNCTION safe_divide(a INTEGER, b INTEGER) RETURNS NUMERIC LANGUAGE plpgsql AS $$
BEGIN
    RETURN a / b;
EXCEPTION
    WHEN division_by_zero THEN
        RETURN NULL;
END
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_body_return_next() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PL/pgSQL RETURN NEXT for set-returning functions
    // EXPECTED TO FAIL: SETOF not yet supported in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION generate_numbers(n INTEGER) RETURNS SETOF INTEGER LANGUAGE plpgsql AS $$
BEGIN
    FOR i IN 1..n LOOP
        RETURN NEXT i;
    END LOOP;
    RETURN;
END
$$"
    );
}

#[test]
fn test_create_function_body_return_query() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PL/pgSQL RETURN QUERY
    // EXPECTED TO FAIL: SETOF not yet supported in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION get_employees() RETURNS SETOF employee LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY SELECT * FROM employee WHERE active = true;
END
$$"
    );
}

#[test]
fn test_create_function_body_dynamic_sql() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PL/pgSQL EXECUTE for dynamic SQL
    pg_test!(
        "CREATE FUNCTION execute_dynamic(table_name TEXT) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
    result BIGINT;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || quote_ident(table_name) INTO result;
    RETURN result;
END
$$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.function_body.is_some());
        }
    );
}
