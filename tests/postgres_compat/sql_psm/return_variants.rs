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

use crate::postgres_compat::common::verified_pg_stmt;
use sqlparser::ast::{CreateFunctionBody, ReturnStatementValue, Statement};

// These are supported forms, not permissive expected-error probes. Assert both
// structured-body parsing and a real Display/reparse round trip for every case.
fn return_round_trip(sql: &str) -> Statement {
    let statement = verified_pg_stmt(sql);
    let Statement::CreateFunction(function) = &statement else {
        panic!("expected CREATE FUNCTION");
    };
    assert!(matches!(
        function.function_body,
        Some(CreateFunctionBody::AsBeginEnd(_))
    ));
    assert_eq!(statement, verified_pg_stmt(&statement.to_string()));
    statement
}

#[test]
fn test_return_implicit_select_clauses() {
    for expression in [
        "value FROM items WHERE id = wanted",
        "CASE WHEN enabled THEN value ELSE 0 END FROM items WHERE id = wanted",
        "count(*) FROM items HAVING count(*) > 0",
        "value FROM items ORDER BY id DESC LIMIT 1 OFFSET 1",
        "value FROM items ORDER BY id FETCH FIRST 1 ROW ONLY",
        "42 WHERE FALSE",
        "';FROM' FROM (SELECT 1) AS t",
        "value /* comment */ FROM items WHERE id = wanted",
    ] {
        let sql = format!("CREATE FUNCTION f(wanted INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN {expression}; END $$");
        let Statement::CreateFunction(function) = return_round_trip(&sql) else {
            unreachable!()
        };
        let Some(CreateFunctionBody::AsBeginEnd(block)) = function.function_body else {
            unreachable!()
        };
        let [Statement::Return(ret)] = block.statements.as_slice() else {
            panic!("missing RETURN")
        };
        let Some(ReturnStatementValue::ExprQuery(query)) = &ret.value else {
            panic!("expected a scalar query, got {ret:?}")
        };
        let canonical = verified_pg_stmt(&format!("SELECT {expression}"));
        assert_eq!(Statement::Query(query.clone()), canonical);
    }
}

#[test]
fn test_return_next_implicit_select_is_not_return_query() {
    let statement = return_round_trip("CREATE FUNCTION f() RETURNS SETOF INT LANGUAGE plpgsql AS $$ BEGIN RETURN NEXT value FROM items ORDER BY value LIMIT 1; RETURN QUERY SELECT value FROM items; RETURN; END $$");
    let Statement::CreateFunction(function) = statement else {
        unreachable!()
    };
    let Some(CreateFunctionBody::AsBeginEnd(block)) = function.function_body else {
        unreachable!()
    };
    assert!(
        matches!(&block.statements[0], Statement::Return(ret) if matches!(&ret.value, Some(ReturnStatementValue::NextExprQuery(_))))
    );
    assert!(
        matches!(&block.statements[1], Statement::Return(ret) if matches!(&ret.value, Some(ReturnStatementValue::Query(_))))
    );
    assert!(matches!(&block.statements[2], Statement::Return(ret) if ret.value.is_none()));
}

#[test]
fn test_pgtap_prokind_body() {
    return_round_trip(
        r#"CREATE OR REPLACE FUNCTION _prokind(p_oid oid)
RETURNS "char" AS $$
BEGIN
    IF pg_version_num() >= 110000 THEN
        RETURN prokind FROM pg_catalog.pg_proc WHERE oid = p_oid;
    ELSE
        RETURN CASE WHEN proisagg THEN 'a' WHEN proiswindow THEN 'w' ELSE 'f' END
            FROM pg_catalog.pg_proc WHERE oid = p_oid;
    END IF;
END;
$$ LANGUAGE plpgsql STABLE"#,
    );
}

#[test]
fn test_return_implicit_select_rejects_invalid_or_unconsumed_tokens() {
    use sqlparser::{
        dialect::{MsSqlDialect, OracleDialect, PostgreSqlDialect},
        parser::Parser,
    };
    for expression in [
        "1 FROM",
        "1 FROM t WHERE",
        "1 FROM t WHERE TRUE unexpected",
        "1 UNION SELECT 2",
        "1 INTERSECT SELECT 2",
        "1 EXCEPT SELECT 2",
        "1 INTO target",
    ] {
        let sql = format!("CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN {expression}; END $$");
        assert!(
            Parser::parse_sql(&PostgreSqlDialect {}, &sql).is_err(),
            "accepted: {sql}"
        );
    }
    assert!(Parser::parse_sql(&PostgreSqlDialect {}, "RETURN value FROM items").is_err());
    assert!(Parser::parse_sql(&PostgreSqlDialect {}, "CREATE FUNCTION f() RETURNS INT LANGUAGE SQL BEGIN ATOMIC RETURN value FROM items; END").is_err());
    assert!(Parser::parse_sql(&MsSqlDialect {}, "BEGIN RETURN value FROM items; END").is_err());
    assert!(Parser::parse_sql(&OracleDialect {}, "BEGIN RETURN value FROM items; END").is_err());
    return_round_trip("CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT 1 UNION SELECT 2) FROM items LIMIT 1; END $$");
}

// =============================================================================
// RETURN (single value)
// =============================================================================

#[test]
fn test_return_simple_value() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Basic RETURN with scalar value
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
BEGIN
    RETURN 42;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_expression() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN with expression
    return_round_trip(
        r#"CREATE FUNCTION test(x INTEGER, y INTEGER) RETURNS INTEGER AS $$
BEGIN
    RETURN x + y;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_null() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NULL
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
BEGIN
    RETURN NULL;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_from_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN value from variable
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
DECLARE
    result INTEGER := 100;
BEGIN
    RETURN result;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_from_subquery() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN value from subquery
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM users);
END $$ LANGUAGE plpgsql"#,
    );
}

// =============================================================================
// RETURN NEXT (for set-returning functions)
// =============================================================================

#[test]
fn test_return_next_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NEXT to build result set one row at a time
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS SETOF INTEGER AS $$
BEGIN
    RETURN NEXT 1;
    RETURN NEXT 2;
    RETURN NEXT 3;
    RETURN;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_next_in_loop() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN NEXT in a loop
    return_round_trip(
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
    return_round_trip(
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
    return_round_trip(
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
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users WHERE active = true;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_query_with_parameters() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY with function parameters
    return_round_trip(
        r#"CREATE FUNCTION get_active_users(min_age INTEGER) RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users WHERE active = true AND age >= min_age;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_query_multiple_times() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Multiple RETURN QUERY statements (results are concatenated)
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users WHERE role = 'admin';
    RETURN QUERY SELECT * FROM users WHERE role = 'moderator';
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_query_with_ordering() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY with ORDER BY
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY SELECT * FROM users ORDER BY created_at DESC LIMIT 10;
END $$ LANGUAGE plpgsql"#,
    );
}

// =============================================================================
// RETURN QUERY EXECUTE (dynamic SQL)
// =============================================================================

#[test]
fn test_return_query_execute() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY EXECUTE for dynamic SQL
    return_round_trip(
        r#"CREATE FUNCTION test(table_name TEXT) RETURNS SETOF RECORD AS $$
BEGIN
    RETURN QUERY EXECUTE 'SELECT * FROM ' || quote_ident(table_name);
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_query_execute_with_parameters() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY EXECUTE with USING parameters
    return_round_trip(
        r#"CREATE FUNCTION test(min_age INTEGER) RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY EXECUTE 'SELECT * FROM users WHERE age >= $1' USING min_age;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_query_execute_dynamic_condition() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN QUERY EXECUTE with dynamically built query
    return_round_trip(
        r#"CREATE FUNCTION test(table_name TEXT, column_name TEXT, threshold INTEGER) RETURNS SETOF RECORD AS $$
DECLARE
    query TEXT;
BEGIN
    query := FORMAT('SELECT * FROM %I WHERE %I > $1', table_name, column_name);
    RETURN QUERY EXECUTE query USING threshold;
END $$ LANGUAGE plpgsql"#,
    );
}

// =============================================================================
// Mixing RETURN variants
// =============================================================================

#[test]
fn test_return_next_and_return_query() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Mixing RETURN NEXT and RETURN QUERY
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS SETOF INTEGER AS $$
BEGIN
    RETURN NEXT 0;
    RETURN QUERY SELECT generate_series(1, 10);
    RETURN NEXT 11;
    RETURN;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_in_conditional() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN in conditional branches
    return_round_trip(
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
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS TABLE(id INTEGER, name TEXT) AS $$
BEGIN
    RETURN QUERY SELECT user_id, user_name FROM users;
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_table_with_computation() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURNS TABLE with computed columns
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS TABLE(name TEXT, age_years INTEGER, age_days INTEGER) AS $$
BEGIN
    RETURN QUERY SELECT user_name, user_age, user_age * 365 FROM users;
END $$ LANGUAGE plpgsql"#,
    );
}

// =============================================================================
// Early RETURN (function exit)
// =============================================================================

#[test]
fn test_return_early_exit() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Early RETURN to exit function
    return_round_trip(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS INTEGER AS $$
BEGIN
    IF x = 0 THEN
        RETURN 0;
    END IF;

    RETURN 100 / x;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_return_from_exception_handler() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN from exception handler
    return_round_trip(
        r#"CREATE FUNCTION test(x INTEGER, y INTEGER) RETURNS INTEGER AS $$
BEGIN
    RETURN x / y;
EXCEPTION
    WHEN division_by_zero THEN
        RETURN 0;
END $$ LANGUAGE plpgsql"#,
    );
}

// =============================================================================
// RETURN without value (procedures)
// =============================================================================

#[test]
fn test_return_void() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // RETURN in void function (just exits)
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Starting';
    RETURN;
    RAISE NOTICE 'Never reached';
END $$ LANGUAGE plpgsql"#,
    );
}

#[test]
fn test_return_setof_final() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-RETURNING
    // Final RETURN in SETOF function (ends iteration)
    return_round_trip(
        r#"CREATE FUNCTION test() RETURNS SETOF INTEGER AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..10 LOOP
        RETURN NEXT i * i;
    END LOOP;
    RETURN;
END $$ LANGUAGE plpgsql"#
    );
}
