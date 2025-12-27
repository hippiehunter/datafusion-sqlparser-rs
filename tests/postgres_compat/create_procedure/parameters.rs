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

//! Tests for CREATE PROCEDURE parameter modes and defaults
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createprocedure.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{ArgMode, DataType, Statement};

// ============================================================================
// Single Parameter Tests
// ============================================================================

#[test]
fn test_create_procedure_single_in_parameter() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // IN is the default mode for parameters
    pg_test!(
        "CREATE PROCEDURE proc_in(IN x INTEGER) AS BEGIN INSERT INTO tbl VALUES (x); END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name.to_string(), "x");
            assert_eq!(params[0].mode, Some(ArgMode::In));
        }
    );
}

#[test]
fn test_create_procedure_single_out_parameter() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // OUT parameters receive values from the procedure
    pg_test!(
        "CREATE PROCEDURE proc_out(OUT x INTEGER) AS BEGIN x := 42; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name.to_string(), "x");
            assert_eq!(params[0].mode, Some(ArgMode::Out));
        }
    );
}

#[test]
fn test_create_procedure_single_inout_parameter() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // INOUT parameters are both input and output
    pg_test!(
        "CREATE PROCEDURE proc_inout(INOUT x INTEGER) AS BEGIN x := x * 2; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name.to_string(), "x");
            assert_eq!(params[0].mode, Some(ArgMode::InOut));
        }
    );
}

#[test]
fn test_create_procedure_parameter_no_mode() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Parameters without explicit mode default to IN
    pg_test!(
        "CREATE PROCEDURE proc_default(x INTEGER) AS BEGIN SELECT x; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name.to_string(), "x");
            // mode should be None (implicitly IN)
            assert!(params[0].mode.is_none() || params[0].mode == Some(ArgMode::In));
        }
    );
}

// ============================================================================
// Multiple Parameter Tests
// ============================================================================

#[test]
fn test_create_procedure_multiple_in_parameters() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Multiple IN parameters
    pg_test!(
        "CREATE PROCEDURE multi_in(IN a INTEGER, IN b TEXT, IN c BOOLEAN) AS BEGIN SELECT a, b, c; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 3);
            assert_eq!(params[0].name.to_string(), "a");
            assert_eq!(params[1].name.to_string(), "b");
            assert_eq!(params[2].name.to_string(), "c");
            assert_eq!(params[0].mode, Some(ArgMode::In));
            assert_eq!(params[1].mode, Some(ArgMode::In));
            assert_eq!(params[2].mode, Some(ArgMode::In));
        }
    );
}

#[test]
fn test_create_procedure_mixed_parameter_modes() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Mix of IN, OUT, and INOUT parameters
    pg_test!(
        "CREATE PROCEDURE mixed_modes(IN x INTEGER, OUT y INTEGER, INOUT z INTEGER) AS BEGIN y := x; z := z * 2; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 3);
            assert_eq!(params[0].mode, Some(ArgMode::In));
            assert_eq!(params[1].mode, Some(ArgMode::Out));
            assert_eq!(params[2].mode, Some(ArgMode::InOut));
        }
    );
}

#[test]
fn test_create_procedure_multiple_out_parameters() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Multiple OUT parameters
    pg_test!(
        "CREATE PROCEDURE multi_out(OUT a INTEGER, OUT b TEXT) AS BEGIN a := 1; b := 'test'; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 2);
            assert_eq!(params[0].mode, Some(ArgMode::Out));
            assert_eq!(params[1].mode, Some(ArgMode::Out));
        }
    );
}

#[test]
fn test_create_procedure_explicit_and_implicit_in() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Mix of explicit IN and implicit (no mode)
    pg_test!(
        "CREATE PROCEDURE mixed_in(a INTEGER, IN b INTEGER, c TEXT) AS BEGIN SELECT a, b, c; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 3);
            // First and third are implicit IN (mode=None)
            // Second is explicit IN (mode=Some(In))
            assert_eq!(params[1].mode, Some(ArgMode::In));
        }
    );
}

// ============================================================================
// Parameter Types Tests
// ============================================================================

#[test]
fn test_create_procedure_various_data_types() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures support all PostgreSQL data types
    pg_test!(
        "CREATE PROCEDURE type_test(a INTEGER, b TEXT, c TIMESTAMP, d NUMERIC(10,2), e BOOLEAN) AS BEGIN SELECT a, b, c, d, e; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 5);
            assert_eq!(params[0].name.to_string(), "a");
            assert_eq!(params[1].name.to_string(), "b");
            assert_eq!(params[2].name.to_string(), "c");
            assert_eq!(params[3].name.to_string(), "d");
            assert_eq!(params[4].name.to_string(), "e");
        }
    );
}

#[test]
fn test_create_procedure_array_parameter() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Array types are supported
    pg_test!(
        "CREATE PROCEDURE array_param(arr INTEGER[]) AS BEGIN SELECT arr; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name.to_string(), "arr");
            // TODO: Verify data_type is Array(Integer)
        }
    );
}

#[test]
fn test_create_procedure_composite_type_parameter() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Composite types (table row types) are supported
    pg_test!(
        "CREATE PROCEDURE composite_param(rec my_table) AS BEGIN SELECT rec; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].name.to_string(), "rec");
        }
    );
}

// ============================================================================
// Default Values Tests
// ============================================================================

#[test]
fn test_create_procedure_parameter_with_default() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Parameters can have default values
    pg_test!(
        "CREATE PROCEDURE with_default(x INTEGER = 42) AS BEGIN SELECT x; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert!(params[0].default.is_some());
            // TODO: Verify default value is 42
        }
    );
}

#[test]
fn test_create_procedure_parameter_default_text() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Default values can be any constant expression
    pg_test!(
        "CREATE PROCEDURE text_default(name TEXT = 'unknown') AS BEGIN SELECT name; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert!(params[0].default.is_some());
            // TODO: Verify default is 'unknown'
        }
    );
}

#[test]
fn test_create_procedure_multiple_defaults() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Multiple parameters can have defaults
    pg_test!(
        "CREATE PROCEDURE multi_defaults(a INTEGER = 1, b INTEGER = 2, c INTEGER = 3) AS BEGIN SELECT a, b, c; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 3);
            assert!(params[0].default.is_some());
            assert!(params[1].default.is_some());
            assert!(params[2].default.is_some());
        }
    );
}

#[test]
fn test_create_procedure_mixed_default_no_default() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Parameters with defaults must come after those without
    pg_test!(
        "CREATE PROCEDURE mixed_defaults(a INTEGER, b INTEGER = 10, c TEXT = 'test') AS BEGIN SELECT a, b, c; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 3);
            assert!(params[0].default.is_none());
            assert!(params[1].default.is_some());
            assert!(params[2].default.is_some());
        }
    );
}

#[test]
fn test_create_procedure_default_null() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // NULL can be a default value
    pg_test!(
        "CREATE PROCEDURE null_default(x INTEGER = NULL) AS BEGIN SELECT x; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert!(params[0].default.is_some());
            // TODO: Verify default is NULL
        }
    );
}

#[test]
fn test_create_procedure_default_expression() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Default can be an expression (e.g., function call)
    pg_test!(
        "CREATE PROCEDURE expr_default(ts TIMESTAMP = CURRENT_TIMESTAMP) AS BEGIN SELECT ts; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert!(params[0].default.is_some());
            // TODO: Verify default is CURRENT_TIMESTAMP expression
        }
    );
}

// ============================================================================
// VARIADIC Parameters - PostgreSQL Extension (LIKELY FAIL)
// ============================================================================

#[test]
fn test_create_procedure_variadic_not_supported() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // VARIADIC allows procedures to accept variable number of arguments
    // This is likely not yet implemented in the AST
    pg_expect_parse_error!(
        "CREATE PROCEDURE variadic_test(VARIADIC args INTEGER[]) AS BEGIN SELECT args; END"
    );
}

// ============================================================================
// Named Parameter Syntax
// ============================================================================

#[test]
fn test_create_procedure_quoted_parameter_names() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Parameter names can be quoted
    pg_test!(
        r#"CREATE PROCEDURE quoted_params("Param1" INTEGER, "param2" TEXT) AS BEGIN SELECT "Param1", "param2"; END"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 2);
            // Parser should preserve quoted identifiers
        }
    );
}

#[test]
fn test_create_procedure_long_parameter_list() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can have many parameters
    pg_test!(
        "CREATE PROCEDURE many_params(p1 INT, p2 INT, p3 INT, p4 INT, p5 INT, p6 INT, p7 INT, p8 INT) AS BEGIN SELECT p1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 8);
        }
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_create_procedure_out_mode_with_default() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // OUT parameters can have defaults (though unusual)
    pg_test!(
        "CREATE PROCEDURE out_with_default(OUT x INTEGER = 0) AS BEGIN x := 42; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].mode, Some(ArgMode::Out));
            assert!(params[0].default.is_some());
        }
    );
}

#[test]
fn test_create_procedure_inout_mode_with_default() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // INOUT parameters can have defaults
    pg_test!(
        "CREATE PROCEDURE inout_with_default(INOUT x INTEGER = 10) AS BEGIN x := x * 2; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(params[0].mode, Some(ArgMode::InOut));
            assert!(params[0].default.is_some());
        }
    );
}

#[test]
fn test_create_procedure_parameter_all_uppercase() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Parameter modes are case-insensitive
    pg_test!(
        "CREATE PROCEDURE uppercase(IN X INTEGER, OUT Y INTEGER, INOUT Z INTEGER) AS BEGIN Y := X; Z := Z + 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            let params = proc.params.as_ref().unwrap();
            assert_eq!(params.len(), 3);
            assert_eq!(params[0].mode, Some(ArgMode::In));
            assert_eq!(params[1].mode, Some(ArgMode::Out));
            assert_eq!(params[2].mode, Some(ArgMode::InOut));
        }
    );
}
