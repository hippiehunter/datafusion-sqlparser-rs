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

//! Tests for PostgreSQL-specific expression syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-expressions.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{
    AstBox as Box, BinaryOperator, CastKind, DataType, Expr, Ident, ObjectName, UnaryOperator,
};
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;

#[test]
fn test_double_colon_cast() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS
    pg_roundtrip_only!("SELECT '42'::INTEGER");
    // TODO: Validate that ::cast is parsed as Cast expression
}

#[test]
fn test_double_colon_cast_with_complex_type() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS
    pg_roundtrip_only!("SELECT '2023-01-01'::TIMESTAMP WITH TIME ZONE");
    // TODO: Validate Cast with complex type
}

#[test]
fn test_double_colon_cast_chained() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS
    pg_roundtrip_only!("SELECT '42'::TEXT::INTEGER");
    // TODO: Validate chained casts
}

#[test]
fn test_array_subscript() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1] FROM t");
    // TODO: Validate array subscript expression
}

#[test]
fn test_array_subscript_nested() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1][2] FROM t");
    // TODO: Validate nested array subscripts
}

#[test]
fn test_array_slice() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1:3] FROM t");
    // TODO: Validate array slice expression
}

#[test]
fn test_array_slice_multidimensional() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
    pg_roundtrip_only!("SELECT arr[1:2][3:4] FROM t");
    // TODO: Validate multidimensional array slice
}

#[test]
fn test_regex_match_operator() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email ~ '^[a-z]+@'");
    // TODO: Validate regex match operator (~)
}

#[test]
fn test_regex_match_case_insensitive() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email ~* '^[a-z]+@'");
    // TODO: Validate case-insensitive regex match operator (~*)
}

#[test]
fn test_regex_not_match_operator() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email !~ '^[a-z]+@'");
    // TODO: Validate regex not match operator (!~)
}

#[test]
fn test_regex_not_match_case_insensitive() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    pg_roundtrip_only!("SELECT name FROM users WHERE email !~* '^[a-z]+@'");
    // TODO: Validate case-insensitive regex not match operator (!~*)
}

#[test]
fn test_string_concatenation_operator() {
    // https://www.postgresql.org/docs/current/functions-string.html
    pg_roundtrip_only!("SELECT 'Hello' || ' ' || 'World'");
    // TODO: Validate || operator for string concatenation
}

#[test]
fn test_factorial_operator() {
    // PostgreSQL 14 removed the postfix operators: `5!` reads `!` as an
    // infix operator name with no right operand. `factorial` is the function.
    let error = try_parse_pg("SELECT 5!")
        .expect_err("PostgreSQL has no postfix operators");
    assert!(
        error
            .to_string()
            .contains("Expected: an expression, found: EOF"),
        "{error}"
    );
    pg_roundtrip_only!("SELECT factorial(5)");
}

#[test]
fn test_exponentiation_operator() {
    // https://www.postgresql.org/docs/current/functions-math.html
    pg_roundtrip_only!("SELECT 2 ^ 10");
    // TODO: Validate exponentiation operator (^)
}

#[test]
fn test_square_root_operator() {
    // https://www.postgresql.org/docs/current/functions-math.html
    one_statement_parses_to_pg("SELECT |/ 25", "SELECT |/25");
    // TODO: Validate square root operator (|/)
}

#[test]
fn test_cube_root_operator() {
    // https://www.postgresql.org/docs/current/functions-math.html
    one_statement_parses_to_pg("SELECT ||/ 27", "SELECT ||/27");
    // TODO: Validate cube root operator (||/)
}

#[test]
fn test_at_time_zone_operator() {
    // https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-ZONECONVERT
    one_statement_parses_to_pg(
        "SELECT timestamp '2023-01-01 00:00:00' AT TIME ZONE 'UTC'",
        "SELECT TIMESTAMP '2023-01-01 00:00:00' AT TIME ZONE 'UTC'",
    );
    // TODO: Validate AT TIME ZONE operator
}

#[test]
fn test_is_distinct_from() {
    // https://www.postgresql.org/docs/current/functions-comparison.html
    pg_roundtrip_only!("SELECT a IS DISTINCT FROM b FROM t");
    // TODO: Validate IS DISTINCT FROM operator
}

#[test]
fn test_is_not_distinct_from() {
    // https://www.postgresql.org/docs/current/functions-comparison.html
    pg_roundtrip_only!("SELECT a IS NOT DISTINCT FROM b FROM t");
    // TODO: Validate IS NOT DISTINCT FROM operator
}

#[test]
fn test_postfix_isnull_after_infix() {
    // https://www.postgresql.org/docs/current/functions-comparison.html
    one_statement_parses_to_pg("SELECT (a::INT ISNULL)", "SELECT (a::INT IS NULL)");
    one_statement_parses_to_pg(
        "SELECT ((a + 1) ISNULL) IS NOT UNKNOWN",
        "SELECT ((a + 1) IS NULL) IS NOT UNKNOWN",
    );
}

#[test]
fn test_collate_after_cast() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SQL-SYNTAX-COLLATE-EXPRS
    pg_roundtrip_only!("SELECT 'x'::TEXT COLLATE \"C\"");
    pg_roundtrip_only!("SELECT (('x'::TEXT COLLATE \"C\") COLLATE \"C\")");
    pg_roundtrip_only!("SELECT (1::INTEGER)::CHARACTER VARYING(795) COLLATE \"C\"");
}

fn pg_expr(sql: &str) -> Expr {
    let expr = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|parser| parser.parse_expr())
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let reparsed = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&expr.to_string())
        .and_then(|parser| parser.parse_expr())
        .unwrap_or_else(|e| panic!("{expr}: {e}"));
    assert_eq!(expr, reparsed, "{sql} rendered as {expr}");
    expr
}

fn collate(expr: Expr) -> Expr {
    Expr::Collate {
        expr: Box::new(expr),
        collation: ObjectName::from(vec![Ident::with_quote('"', "C")]),
    }
}

fn identifier(name: &str) -> Expr {
    Expr::Identifier(Ident::new(name))
}

#[test]
fn test_collate_is_an_infix_operator() {
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-PRECEDENCE
    let column = Expr::CompoundIdentifier(vec![Ident::new("t"), Ident::new("c0")]);
    assert_eq!(pg_expr("t.c0 COLLATE \"C\""), collate(column.clone()));
    assert_eq!(
        pg_expr("(t.c0 COLLATE \"C\") COLLATE \"C\""),
        collate(Expr::Nested(Box::new(collate(column))))
    );

    match pg_expr("s.f(x) COLLATE \"C\"") {
        Expr::Collate { expr, .. } => match &*expr {
            Expr::Function(function) => assert_eq!(function.name.to_string(), "s.f"),
            other => panic!("expected a function call under COLLATE, got {other:?}"),
        },
        other => panic!("expected COLLATE, got {other:?}"),
    }

    match pg_expr("x::text COLLATE \"C\" || 'a'") {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::StringConcat,
            ..
        } => match &*left {
            Expr::Collate { expr, .. } => assert!(matches!(
                **expr,
                Expr::Cast {
                    kind: CastKind::DoubleColon,
                    data_type: DataType::Text,
                    ..
                }
            )),
            other => panic!("expected COLLATE on the left of ||, got {other:?}"),
        },
        other => panic!("expected ||, got {other:?}"),
    }

    match pg_expr("-x COLLATE \"C\"") {
        Expr::Collate { expr, .. } => assert!(matches!(
            *expr,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                ..
            }
        )),
        other => panic!("expected COLLATE over unary minus, got {other:?}"),
    }

    pg_roundtrip_only!("SELECT t.c0 COLLATE \"C\" FROM t");
    pg_roundtrip_only!("SELECT s.f(x) COLLATE \"C\" < 'b'");
    pg_roundtrip_only!("SELECT x COLLATE \"C\" = y COLLATE \"POSIX\"");
}

#[test]
fn test_unary_minus_binds_tighter_than_caret() {
    match pg_expr("-2 ^ 2") {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::PGExp,
            ..
        } => assert!(matches!(
            *left,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                ..
            }
        )),
        other => panic!("expected (-2) ^ 2, got {other:?}"),
    }
    match pg_expr("-a[1]::int") {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
            ..
        } => assert!(matches!(*expr, Expr::Cast { .. })),
        other => panic!("expected -(a[1]::int), got {other:?}"),
    }
    assert!(matches!(
        pg_expr("-a * b"),
        Expr::BinaryOp {
            op: BinaryOperator::Multiply,
            ..
        }
    ));
}

#[test]
fn test_prefix_collate_unchanged_for_other_dialects() {
    let expr = Parser::new(&MySqlDialect {})
        .try_with_sql("-x COLLATE \"C\"")
        .and_then(|parser| parser.parse_expr())
        .expect("MySQL COLLATE");
    match expr {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
            ..
        } => assert_eq!(*expr, collate(identifier("x"))),
        other => panic!("expected -(x COLLATE \"C\"), got {other:?}"),
    }
}

#[test]
fn test_question_operators() {
    // https://www.postgresql.org/docs/current/functions-json.html
    match pg_expr("'{\"a\":1}'::jsonb ?'a'") {
        Expr::BinaryOp {
            op: BinaryOperator::Question,
            right,
            ..
        } => assert_eq!(right.to_string(), "'a'"),
        other => panic!("expected ?, got {other:?}"),
    }
    for (sql, op) in [
        ("a ? 'k'", BinaryOperator::Question),
        ("a ?| b", BinaryOperator::QuestionPipe),
        ("a ?& b", BinaryOperator::QuestionAnd),
        ("a ?- b", BinaryOperator::QuestionDash),
        ("a ?-| b", BinaryOperator::QuestionDashPipe),
        ("a ?|| b", BinaryOperator::QuestionDoublePipe),
        ("a ?# b", BinaryOperator::QuestionHash),
    ] {
        match pg_expr(sql) {
            Expr::BinaryOp { op: parsed, .. } => assert_eq!(parsed, op, "{sql}"),
            other => panic!("expected a binary operator for {sql}, got {other:?}"),
        }
    }
    pg_roundtrip_only!("SELECT (a ? 'k')");
    pg_roundtrip_only!("SELECT f(a ?| b)");
}
