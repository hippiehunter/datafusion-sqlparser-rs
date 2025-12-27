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

//! Tests for CREATE FUNCTION RETURNS clause variations
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createfunction.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{DataType, Statement};

#[test]
fn test_create_function_returns_integer() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Basic RETURNS with scalar type
    pg_test!(
        "CREATE FUNCTION get_one() RETURNS INTEGER LANGUAGE SQL AS $$ SELECT 1 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            assert!(matches!(
                cf.return_type.as_ref().unwrap(),
                DataType::Integer(_)
            ));
        }
    );
}

#[test]
fn test_create_function_returns_text() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS TEXT
    pg_test!(
        "CREATE FUNCTION hello() RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'Hello' $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(matches!(cf.return_type.as_ref().unwrap(), DataType::Text));
        }
    );
}

#[test]
fn test_create_function_returns_void() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Functions that don't return a value
    pg_test!(
        "CREATE FUNCTION do_nothing() RETURNS VOID LANGUAGE SQL AS $$ SELECT NULL $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            // VOID is represented as Custom type in AST
        }
    );
}

#[test]
fn test_create_function_returns_timestamp() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS TIMESTAMP
    pg_test!(
        "CREATE FUNCTION current_time_func() RETURNS TIMESTAMP LANGUAGE SQL AS $$ SELECT NOW() $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
        }
    );
}

#[test]
fn test_create_function_returns_numeric() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS NUMERIC with precision
    // Note: NUMERIC(10, 2) serializes without space as NUMERIC(10,2)
    pg_parses_to_with_ast!(
        "CREATE FUNCTION get_price() RETURNS NUMERIC(10, 2) LANGUAGE SQL AS $$ SELECT 99.99 $$",
        "CREATE FUNCTION get_price() RETURNS NUMERIC(10,2) LANGUAGE SQL AS $$ SELECT 99.99 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
        }
    );
}

#[test]
fn test_create_function_returns_array() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS array type
    pg_test!(
        "CREATE FUNCTION get_array() RETURNS INTEGER[] LANGUAGE SQL AS $$ SELECT ARRAY[1, 2, 3] $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            assert!(matches!(
                cf.return_type.as_ref().unwrap(),
                DataType::Array(_)
            ));
        }
    );
}

#[test]
fn test_create_function_returns_composite_type() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS a composite type
    pg_test!(
        "CREATE FUNCTION get_employee() RETURNS employee LANGUAGE SQL AS $$ SELECT * FROM employee LIMIT 1 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
        }
    );
}

#[test]
fn test_create_function_returns_table_simple() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS TABLE defines output columns
    pg_test!(
        "CREATE FUNCTION get_users() RETURNS TABLE(id INTEGER, name TEXT) LANGUAGE SQL AS $$ SELECT id, name FROM users $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            // Should parse as RETURNS TABLE with column definitions
        }
    );
}

#[test]
fn test_create_function_returns_table_multiple_columns() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS TABLE with multiple columns
    pg_test!(
        "CREATE FUNCTION get_products() RETURNS TABLE(product_id INTEGER, product_name TEXT, price NUMERIC, in_stock BOOLEAN) LANGUAGE SQL AS $$ SELECT product_id, product_name, price, in_stock FROM products $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
        }
    );
}

#[test]
fn test_create_function_returns_setof_scalar() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS SETOF returns multiple rows of a type
    // EXPECTED TO FAIL: SETOF not yet fully supported in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION get_numbers() RETURNS SETOF INTEGER LANGUAGE SQL AS $$ SELECT generate_series(1, 10) $$"
    );
}

#[test]
fn test_create_function_returns_setof_composite() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS SETOF composite type
    // EXPECTED TO FAIL: SETOF not yet fully supported in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION get_all_employees() RETURNS SETOF employee LANGUAGE SQL AS $$ SELECT * FROM employee $$"
    );
}

#[test]
fn test_create_function_returns_setof_record() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS SETOF RECORD for dynamic columns
    // EXPECTED TO FAIL: SETOF not yet fully supported in AST
    pg_expect_parse_error!(
        "CREATE FUNCTION get_records() RETURNS SETOF RECORD LANGUAGE SQL AS $$ SELECT * FROM some_table $$"
    );
}

#[test]
fn test_create_function_returns_trigger() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Trigger functions return TRIGGER
    // NOTE: Parser accepts TRIGGER as a custom type
    pg_test!(
        "CREATE FUNCTION check_update() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            // TRIGGER is parsed as Custom type - could add specific handling in future
        }
    );
}

#[test]
fn test_create_function_returns_event_trigger() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // Event trigger functions return event_trigger
    // NOTE: Parser accepts event_trigger as a custom type
    pg_test!(
        "CREATE FUNCTION log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'DDL executed'; END $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            // event_trigger is parsed as Custom type - could add specific handling in future
        }
    );
}

#[test]
fn test_create_function_out_parameters_implicit_record() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // OUT parameters create an implicit RECORD return type
    pg_test!(
        "CREATE FUNCTION dup(IN x INTEGER, OUT x2 INTEGER, OUT x3 INTEGER) LANGUAGE SQL AS $$ SELECT x * 2, x * 3 $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            // When OUT parameters are present, RETURNS clause is optional
            // The function implicitly returns a record type
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

#[test]
fn test_create_function_out_parameters_with_returns() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // OUT parameters can be combined with explicit RETURNS
    pg_test!(
        "CREATE FUNCTION sum_product(a INTEGER, b INTEGER, OUT sum INTEGER, OUT product INTEGER) RETURNS RECORD LANGUAGE SQL AS $$ SELECT a + b, a * b $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
            let args = cf.args.as_ref().unwrap();
            assert_eq!(args.len(), 4);
        }
    );
}

#[test]
fn test_create_function_returns_domain_type() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS a domain type (custom constrained type)
    pg_test!(
        "CREATE FUNCTION get_email() RETURNS email_address LANGUAGE SQL AS $$ SELECT 'user@example.com'::email_address $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
        }
    );
}

#[test]
fn test_create_function_returns_enum_type() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    // RETURNS an enum type
    pg_test!(
        "CREATE FUNCTION get_status() RETURNS order_status LANGUAGE SQL AS $$ SELECT 'pending'::order_status $$",
        |stmt: Statement| {
            let cf = extract_create_function(&stmt);
            assert!(cf.return_type.is_some());
        }
    );
}
