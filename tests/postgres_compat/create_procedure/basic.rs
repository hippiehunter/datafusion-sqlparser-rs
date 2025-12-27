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

//! Tests for basic CREATE PROCEDURE syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createprocedure.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::Statement;

#[test]
fn test_create_procedure_minimal() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Minimal procedure with no parameters
    pg_test!(
        "CREATE PROCEDURE insert_data() LANGUAGE SQL AS $$ INSERT INTO tbl VALUES (1) $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "insert_data");
            assert!(!proc.or_alter);
            assert!(proc.params.as_ref().map(|p| p.is_empty()).unwrap_or(true));
            assert!(proc.language.is_some());
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "SQL");
            assert!(proc.has_as);
        }
    );
}

#[test]
fn test_create_procedure_no_args() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedure with empty parentheses
    pg_test!(
        "CREATE PROCEDURE proc_no_args() AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "proc_no_args");
            assert!(proc.params.as_ref().map(|p| p.is_empty()).unwrap_or(true));
        }
    );
}

#[test]
fn test_create_procedure_with_language() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // LANGUAGE clause specifies the implementation language
    pg_test!(
        "CREATE PROCEDURE test_proc() LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'test'; END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "test_proc");
            assert!(proc.language.is_some());
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "plpgsql");
        }
    );
}

#[test]
fn test_create_procedure_dollar_quoted_body() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Dollar-quoted strings are the standard way to quote procedure bodies
    pg_test!(
        "CREATE PROCEDURE dollar_quoted() AS $$ BEGIN SELECT 1; END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "dollar_quoted");
            assert!(proc.has_as);
            // TODO: Verify that body is parsed as AST, not a string
        }
    );
}

#[test]
fn test_create_procedure_custom_dollar_tag() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Dollar-quoted strings can have custom tags
    pg_test!(
        "CREATE PROCEDURE custom_tag() AS $body$ BEGIN SELECT 1; END $body$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "custom_tag");
            assert!(proc.has_as);
            // TODO: Verify that body is parsed as AST, not a string
        }
    );
}

#[test]
fn test_create_procedure_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedure with schema-qualified name
    pg_test!(
        "CREATE PROCEDURE myschema.my_proc() AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "myschema.my_proc");
            assert_eq!(proc.name.0.len(), 2);
        }
    );
}

#[test]
fn test_create_procedure_double_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedure with database.schema.procedure name
    pg_test!(
        "CREATE PROCEDURE mydb.myschema.my_proc() AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "mydb.myschema.my_proc");
            assert_eq!(proc.name.0.len(), 3);
        }
    );
}

#[test]
fn test_create_procedure_begin_end_block() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // PL/pgSQL procedure with BEGIN...END block
    pg_test!(
        "CREATE PROCEDURE test_begin_end() LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'hello'; END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "test_begin_end");
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "plpgsql");
            // TODO: Verify body contains BEGIN...END block AST
        }
    );
}

#[test]
fn test_create_procedure_sql_language() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // SQL language procedures execute SQL commands
    pg_test!(
        "CREATE PROCEDURE insert_default() LANGUAGE SQL AS $$ INSERT INTO tbl DEFAULT VALUES $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "insert_default");
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "SQL");
            // TODO: Verify body contains INSERT statement AST
        }
    );
}

#[test]
fn test_create_procedure_language_case_insensitive() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Language names are case-insensitive
    pg_test!(
        "CREATE PROCEDURE case_test() LANGUAGE PlPgSql AS $$ BEGIN END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert!(proc.language.is_some());
            // Note: Parser may normalize case
        }
    );
}

// ============================================================================
// OR REPLACE Tests - PostgreSQL 11+ Feature
// ============================================================================

#[test]
fn test_create_or_replace_procedure_not_supported() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // CREATE OR REPLACE PROCEDURE was added in PostgreSQL 11
    // The AST has or_alter but not or_replace for procedures
    // This is a gap in the current implementation
    pg_expect_parse_error!("CREATE OR REPLACE PROCEDURE replace_test() AS BEGIN SELECT 1; END");
}

#[test]
fn test_create_or_alter_procedure() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // The AST supports or_alter for procedures (SQL Server-style)
    // PostgreSQL uses OR REPLACE, not OR ALTER
    pg_test!(
        "CREATE OR ALTER PROCEDURE alter_test() AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "alter_test");
            assert!(proc.or_alter, "Expected or_alter=true");
        }
    );
}

// ============================================================================
// BEGIN ATOMIC Tests - PostgreSQL 14+ Feature
// ============================================================================

#[test]
fn test_create_procedure_begin_atomic_not_supported() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // BEGIN ATOMIC was added in PostgreSQL 14 for SQL-standard syntax
    // This is likely not yet supported
    pg_expect_parse_error!(
        "CREATE PROCEDURE atomic_test() LANGUAGE SQL BEGIN ATOMIC INSERT INTO t VALUES (1); END"
    );
}

// ============================================================================
// Edge Cases and Syntax Variations
// ============================================================================

#[test]
fn test_create_procedure_without_as_keyword() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Some dialects allow omitting AS before the body
    // PostgreSQL requires AS or the body to be a string literal
    pg_test!(
        "CREATE PROCEDURE no_as() BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "no_as");
            // has_as should be false if AS is omitted
        }
    );
}

#[test]
fn test_create_procedure_with_single_quotes() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Single-quoted strings can also be used (less common)
    pg_test!(
        "CREATE PROCEDURE single_quoted() AS 'BEGIN SELECT 1; END'",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "single_quoted");
            assert!(proc.has_as);
            // TODO: Verify body is parsed correctly
        }
    );
}

#[test]
fn test_create_procedure_multiline_body() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures often have multi-line bodies
    pg_test!(
        r#"CREATE PROCEDURE multiline() AS $$
BEGIN
    INSERT INTO tbl VALUES (1);
    UPDATE tbl SET val = 2;
    DELETE FROM tbl WHERE id = 3;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "multiline");
            // TODO: Verify body contains multiple statements
        }
    );
}

#[test]
fn test_create_procedure_empty_body() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can have empty bodies (though not very useful)
    pg_test!(
        "CREATE PROCEDURE empty_body() AS $$ BEGIN END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "empty_body");
            // TODO: Verify body is empty
        }
    );
}

#[test]
fn test_create_procedure_quoted_identifier() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Quoted identifiers preserve case and allow special characters
    pg_test!(
        r#"CREATE PROCEDURE "MyProc"() AS BEGIN SELECT 1; END"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            // Parser should preserve quoted identifier
            assert!(proc.name.to_string().contains("MyProc"));
        }
    );
}

#[test]
fn test_create_procedure_schema_qualified_quoted() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Schema-qualified name with quoted identifiers
    pg_test!(
        r#"CREATE PROCEDURE "MySchema"."MyProc"() AS BEGIN SELECT 1; END"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.0.len(), 2);
        }
    );
}
