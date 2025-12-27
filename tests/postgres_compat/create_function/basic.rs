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

//! Tests for basic CREATE FUNCTION syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createfunction.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{DataType, Statement};

#[test]
fn test_create_function_minimal() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Minimal function with SQL language
    pg_test!(
        "CREATE FUNCTION one() RETURNS INTEGER LANGUAGE SQL AS $$ SELECT 1 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "one");
            assert!(!cf.or_replace);
            assert!(!cf.or_alter);
            assert!(!cf.temporary);
            assert!(!cf.if_not_exists);
            assert!(cf.args.as_ref().map(|a| a.is_empty()).unwrap_or(false));
            assert!(cf.return_type.is_some());
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "SQL");
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_no_args() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function with empty parentheses
    pg_test!(
        "CREATE FUNCTION get_current_time() RETURNS TIMESTAMP LANGUAGE SQL AS $$ SELECT NOW() $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "get_current_time");
            assert!(cf.args.as_ref().map(|a| a.is_empty()).unwrap_or(false));
            assert!(cf.return_type.is_some());
        }
    );
}

#[test]
fn test_create_or_replace_function() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // OR REPLACE allows redefining existing functions
    pg_test!(
        "CREATE OR REPLACE FUNCTION add(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT a + b $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "add");
            assert!(cf.or_replace, "Expected or_replace=true");
            assert!(!cf.or_alter);
            assert!(!cf.temporary);
        }
    );
}

#[test]
fn test_create_function_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function with schema-qualified name
    pg_test!(
        "CREATE FUNCTION myschema.myfunc() RETURNS INTEGER LANGUAGE SQL AS $$ SELECT 42 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "myschema.myfunc");
            assert_eq!(cf.name.0.len(), 2);
        }
    );
}

#[test]
fn test_create_function_double_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function with database.schema.function name
    pg_test!(
        "CREATE FUNCTION mydb.myschema.myfunc() RETURNS INTEGER LANGUAGE SQL AS $$ SELECT 42 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "mydb.myschema.myfunc");
            assert_eq!(cf.name.0.len(), 3);
        }
    );
}

#[test]
fn test_create_function_quoted_identifier() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function with quoted identifier (case-sensitive)
    pg_test!(
        r#"CREATE FUNCTION "MyFunction"() RETURNS INTEGER LANGUAGE SQL AS $$ SELECT 1 $$"#,
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), r#""MyFunction""#);
        }
    );
}

#[test]
fn test_create_function_language_sql() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // LANGUAGE SQL is the most common for simple functions
    pg_test!(
        "CREATE FUNCTION square(x INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT x * x $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "square");
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "SQL");
        }
    );
}

#[test]
fn test_create_function_language_plpgsql() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // LANGUAGE plpgsql for procedural functions
    pg_test!(
        "CREATE FUNCTION increment(i INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$ BEGIN RETURN i + 1; END $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "increment");
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "plpgsql");
        }
    );
}

#[test]
fn test_create_function_language_c() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // LANGUAGE C for external C functions with file and symbol name
    // EXPECTED TO FAIL: AS 'file', 'symbol' syntax not yet supported
    pg_expect_parse_error!(
        "CREATE FUNCTION add_one(INTEGER) RETURNS INTEGER LANGUAGE C STRICT AS 'filename', 'add_one_func'"
    );
}

#[test]
fn test_create_function_language_internal() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // LANGUAGE internal for PostgreSQL internal functions
    pg_test!(
        "CREATE FUNCTION pg_sleep(DOUBLE PRECISION) RETURNS VOID LANGUAGE internal STRICT AS 'pg_sleep'",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.language.as_ref().unwrap().to_string(), "internal");
        }
    );
}

#[test]
fn test_create_function_single_arg() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function with single argument
    pg_test!(
        "CREATE FUNCTION double(x INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT x * 2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "double");
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].name.as_ref().unwrap().to_string(), "x");
            assert!(matches!(args[0].data_type, DataType::Integer(_)));
        }
    );
}

#[test]
fn test_create_function_multiple_args() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Function with multiple arguments
    pg_test!(
        "CREATE FUNCTION add_three(a INTEGER, b INTEGER, c INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT a + b + c $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "add_three");
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
            assert_eq!(args[0].name.as_ref().unwrap().to_string(), "a");
            assert_eq!(args[1].name.as_ref().unwrap().to_string(), "b");
            assert_eq!(args[2].name.as_ref().unwrap().to_string(), "c");
        }
    );
}

#[test]
fn test_create_function_unnamed_args() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Arguments don't need names (can use $1, $2, etc in function body)
    pg_test!(
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "add");
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
            assert!(args[0].name.is_none());
            assert!(args[1].name.is_none());
        }
    );
}

#[test]
fn test_create_function_mixed_named_unnamed_args() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Mix of named and unnamed arguments
    pg_test!(
        "CREATE FUNCTION func(INTEGER, name TEXT) RETURNS TEXT LANGUAGE SQL AS $$ SELECT name $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
            assert!(args[0].name.is_none());
            assert_eq!(args[1].name.as_ref().unwrap().to_string(), "name");
        }
    );
}

#[test]
fn test_create_function_various_return_types() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Test various return type declarations

    // INTEGER return type
    pg_test!(
        "CREATE FUNCTION f1() RETURNS INTEGER LANGUAGE SQL AS $$ SELECT 1 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(matches!(
                cf.return_type.as_ref().unwrap(),
                DataType::Integer(_)
            ));
        }
    );

    // TEXT return type
    pg_test!(
        "CREATE FUNCTION f2() RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'hello' $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(matches!(cf.return_type.as_ref().unwrap(), DataType::Text));
        }
    );

    // VOID return type
    pg_test!(
        "CREATE FUNCTION f3() RETURNS VOID LANGUAGE SQL AS $$ SELECT NULL $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            // VOID is represented as Custom type in AST
        }
    );
}

#[test]
fn test_create_function_overloading() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // PostgreSQL allows function overloading (same name, different argument types)

    // First function: add(INTEGER, INTEGER)
    pg_test!(
        "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "add");
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );

    // Second function: add(DOUBLE PRECISION, DOUBLE PRECISION)
    pg_test!(
        "CREATE FUNCTION add(DOUBLE PRECISION, DOUBLE PRECISION) RETURNS DOUBLE PRECISION LANGUAGE SQL AS $$ SELECT $1 + $2 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "add");
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

#[test]
fn test_create_function_complex_types() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Functions with complex type arguments

    // Array type
    pg_test!(
        "CREATE FUNCTION array_length_custom(arr INTEGER[]) RETURNS INTEGER LANGUAGE SQL AS $$ SELECT array_length(arr, 1) $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
            assert!(matches!(args[0].data_type, DataType::Array(_)));
        }
    );
}

#[test]
fn test_create_function_as_expression() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Simple expression without dollar quotes
    pg_test!(
        "CREATE FUNCTION one() RETURNS INTEGER LANGUAGE SQL AS 'SELECT 1'",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "one");
            assert!(cf.function_body.is_some());
        }
    );
}

#[test]
fn test_create_function_return_expression() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURN keyword for simple expressions
    pg_test!(
        "CREATE FUNCTION forty_two() RETURNS INTEGER LANGUAGE SQL RETURN 42",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert_eq!(cf.name.to_string(), "forty_two");
            assert!(cf.function_body.is_some());
        }
    );
}
