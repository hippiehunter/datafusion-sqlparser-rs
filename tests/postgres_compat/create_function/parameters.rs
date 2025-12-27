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

//! Tests for CREATE FUNCTION parameter modes and defaults
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createfunction.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{ArgMode, DataType, Statement};

#[test]
fn test_create_function_in_parameter() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // IN is the default mode, but can be explicit
    pg_test!(
        "CREATE FUNCTION add(IN a INTEGER, IN b INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT a + b $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
            assert_eq!(args[0].mode, Some(ArgMode::In));
            assert_eq!(args[1].mode, Some(ArgMode::In));
        }
    );
}

#[test]
fn test_create_function_out_parameter() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // OUT parameters are returned as part of a composite type
    pg_test!(
        "CREATE FUNCTION sum_and_product(a INTEGER, b INTEGER, OUT sum INTEGER, OUT product INTEGER) LANGUAGE SQL AS $$ SELECT a + b, a * b $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 4);
            // First two are IN (default)
            assert!(args[0].mode.is_none() || args[0].mode == Some(ArgMode::In));
            assert!(args[1].mode.is_none() || args[1].mode == Some(ArgMode::In));
            // Last two are OUT
            assert_eq!(args[2].mode, Some(ArgMode::Out));
            assert_eq!(args[3].mode, Some(ArgMode::Out));
        }
    );
}

#[test]
fn test_create_function_out_parameters_no_returns() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Functions with OUT parameters don't need RETURNS clause
    pg_test!(
        "CREATE FUNCTION dup(IN x INTEGER, OUT x2 INTEGER, OUT x3 INTEGER) LANGUAGE SQL AS $$ SELECT x * 2, x * 3 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
            assert_eq!(args[0].mode, Some(ArgMode::In));
            assert_eq!(args[1].mode, Some(ArgMode::Out));
            assert_eq!(args[2].mode, Some(ArgMode::Out));
            // RETURNS clause should be optional with OUT parameters
        }
    );
}

#[test]
fn test_create_function_inout_parameter() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // INOUT parameters serve as both input and output
    pg_test!(
        "CREATE FUNCTION increment_inout(INOUT x INTEGER) LANGUAGE SQL AS $$ SELECT x + 1 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].mode, Some(ArgMode::InOut));
        }
    );
}

#[test]
fn test_create_function_mixed_parameter_modes() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Mix of IN, OUT, and INOUT parameters
    pg_test!(
        "CREATE FUNCTION complex(IN a INTEGER, INOUT b INTEGER, OUT c INTEGER) LANGUAGE SQL AS $$ SELECT b + a, a * 2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
            assert_eq!(args[0].mode, Some(ArgMode::In));
            assert_eq!(args[1].mode, Some(ArgMode::InOut));
            assert_eq!(args[2].mode, Some(ArgMode::Out));
        }
    );
}

#[test]
fn test_create_function_default_value_constant() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Parameters can have default values
    // Note: PostgreSQL serializes DEFAULT as = in round-trip
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add(a INTEGER, b INTEGER DEFAULT 10) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT a + b $$",
        "CREATE FUNCTION add(a INTEGER, b INTEGER = 10) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT a + b $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
            assert_eq!(args[0].name.as_ref().unwrap().to_string(), "a");
            assert!(args[0].default_expr.is_none());
            assert_eq!(args[1].name.as_ref().unwrap().to_string(), "b");
            assert!(args[1].default_expr.is_some());
        }
    );
}

#[test]
fn test_create_function_default_value_string() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Default values can be strings
    // Note: PostgreSQL serializes DEFAULT as = in round-trip
    pg_parses_to_with_ast!(
        "CREATE FUNCTION greet(name TEXT DEFAULT 'World') RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'Hello, ' || name $$",
        "CREATE FUNCTION greet(name TEXT = 'World') RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'Hello, ' || name $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert!(args[0].default_expr.is_some());
        }
    );
}

#[test]
fn test_create_function_default_value_null() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Default value can be NULL
    // Note: PostgreSQL serializes DEFAULT as = in round-trip
    pg_parses_to_with_ast!(
        "CREATE FUNCTION process(data TEXT DEFAULT NULL) RETURNS TEXT LANGUAGE SQL AS $$ SELECT COALESCE(data, 'empty') $$",
        "CREATE FUNCTION process(data TEXT = NULL) RETURNS TEXT LANGUAGE SQL AS $$ SELECT COALESCE(data, 'empty') $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert!(args[0].default_expr.is_some());
        }
    );
}

#[test]
fn test_create_function_default_value_expression() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Default values can be expressions
    // Note: PostgreSQL serializes DEFAULT as = in round-trip
    pg_parses_to_with_ast!(
        "CREATE FUNCTION add_to_now(hours INTEGER DEFAULT 24) RETURNS TIMESTAMP LANGUAGE SQL AS $$ SELECT NOW() + (hours || ' hours')::INTERVAL $$",
        "CREATE FUNCTION add_to_now(hours INTEGER = 24) RETURNS TIMESTAMP LANGUAGE SQL AS $$ SELECT NOW() + (hours || ' hours')::INTERVAL $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert!(args[0].default_expr.is_some());
        }
    );
}

#[test]
fn test_create_function_multiple_defaults() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Multiple parameters with defaults
    // Note: PostgreSQL serializes DEFAULT as = in round-trip
    pg_parses_to_with_ast!(
        "CREATE FUNCTION range_check(val INTEGER, min_val INTEGER DEFAULT 0, max_val INTEGER DEFAULT 100) RETURNS BOOLEAN LANGUAGE SQL AS $$ SELECT val >= min_val AND val <= max_val $$",
        "CREATE FUNCTION range_check(val INTEGER, min_val INTEGER = 0, max_val INTEGER = 100) RETURNS BOOLEAN LANGUAGE SQL AS $$ SELECT val >= min_val AND val <= max_val $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
            assert!(args[0].default_expr.is_none());
            assert!(args[1].default_expr.is_some());
            assert!(args[2].default_expr.is_some());
        }
    );
}

#[test]
fn test_create_function_equals_default_syntax() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PostgreSQL allows both DEFAULT and = for default values
    pg_test!(
        "CREATE FUNCTION add(a INTEGER, b INTEGER = 10) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT a + b $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
            assert!(args[1].default_expr.is_some());
        }
    );
}

#[test]
fn test_create_function_variadic_parameter() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // VARIADIC allows variable number of arguments
    // EXPECTED TO FAIL: VARIADIC not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION sum_all(VARIADIC numbers INTEGER[]) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT SUM(n) FROM unnest(numbers) AS n $$"
    );
}

#[test]
fn test_create_function_variadic_any() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // VARIADIC "any" accepts variable arguments of any type
    // NOTE: Parser currently accepts VARIADIC as a parameter name, not a keyword
    // This test documents that VARIADIC is parsed but not as the intended keyword
    pg_roundtrip_only!(
        r#"CREATE FUNCTION concat_all(VARIADIC "any") RETURNS TEXT LANGUAGE SQL AS $$ SELECT array_to_string(ARRAY[$1], ',') $$"#
    );
    // TODO: Verify that VARIADIC is parsed as a mode/keyword, not just a parameter name
}

#[test]
fn test_create_function_variadic_with_other_params() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // VARIADIC must be the last parameter
    // EXPECTED TO FAIL: VARIADIC not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION make_array(prefix TEXT, VARIADIC elements INTEGER[]) RETURNS INTEGER[] LANGUAGE SQL AS $$ SELECT elements $$"
    );
}

#[test]
fn test_create_function_variadic_with_default() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // VARIADIC parameter can have default value
    // EXPECTED TO FAIL: VARIADIC not yet in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION sum_values(VARIADIC vals INTEGER[] DEFAULT ARRAY[]::INTEGER[]) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT COALESCE(SUM(v), 0) FROM unnest(vals) AS v $$"
    );
}

#[test]
fn test_create_function_parameter_type_modifiers() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Parameters can have type modifiers like VARCHAR(50)
    pg_test!(
        "CREATE FUNCTION trim_string(s VARCHAR(100)) RETURNS VARCHAR LANGUAGE SQL AS $$ SELECT TRIM(s) $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].name.as_ref().unwrap().to_string(), "s");
        }
    );
}

#[test]
fn test_create_function_numeric_type_with_precision() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // NUMERIC type with precision and scale
    // Note: NUMERIC(10, 2) serializes without space as NUMERIC(10,2)
    pg_parses_to_with_ast!(
        "CREATE FUNCTION round_money(amount NUMERIC(10, 2)) RETURNS NUMERIC LANGUAGE SQL AS $$ SELECT ROUND(amount, 2) $$",
        "CREATE FUNCTION round_money(amount NUMERIC(10,2)) RETURNS NUMERIC LANGUAGE SQL AS $$ SELECT ROUND(amount, 2) $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn test_create_function_array_parameter() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Array types as parameters
    pg_test!(
        "CREATE FUNCTION first_element(arr INTEGER[]) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT arr[1] $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert!(matches!(args[0].data_type, DataType::Array(_)));
        }
    );
}

#[test]
fn test_create_function_multidimensional_array() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Multi-dimensional array parameter
    pg_test!(
        "CREATE FUNCTION matrix_sum(matrix INTEGER[][]) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT SUM(elem) FROM unnest(matrix) AS elem $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn test_create_function_composite_type_parameter() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Custom composite type as parameter
    pg_test!(
        "CREATE FUNCTION get_name(person_record person) RETURNS TEXT LANGUAGE SQL AS $$ SELECT (person_record).name $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].name.as_ref().unwrap().to_string(), "person_record");
        }
    );
}
