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

//! Common utilities for PostgreSQL compatibility testing.
//!
//! This module provides test helpers, macros, and utilities specifically designed
//! for PostgreSQL compatibility testing with the PostgreSqlDialect.
//!
//! # Reference Documentation
//!
//! All tests reference official PostgreSQL documentation:
//! - Functions: <https://www.postgresql.org/docs/current/sql-createfunction.html>
//! - Procedures: <https://www.postgresql.org/docs/current/sql-createprocedure.html>
//! - Routines: <https://www.postgresql.org/docs/current/sql-alterroutine.html>
//! - DO blocks: <https://www.postgresql.org/docs/current/sql-do.html>
//! - PL/pgSQL: <https://www.postgresql.org/docs/current/plpgsql.html>
//! - Triggers: <https://www.postgresql.org/docs/current/sql-createtrigger.html>
//! - Aggregates: <https://www.postgresql.org/docs/current/sql-createaggregate.html>
//! - Operators: <https://www.postgresql.org/docs/current/sql-createoperator.html>

use sqlparser::ast::{
    ConditionalStatements, CreateFunction, CreateTrigger, Ident, ObjectName, ProcedureParam,
    Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::test_utils::TestedDialects;

// =============================================================================
// Dialect and Parsing Functions
// =============================================================================

/// Returns a TestedDialects configured for PostgreSQL compatibility testing.
///
/// Uses PostgreSqlDialect as the target since we're testing PostgreSQL-specific
/// features and syntax extensions.
pub fn pg_dialect() -> TestedDialects {
    TestedDialects::new(vec![Box::new(PostgreSqlDialect {})])
}

/// Verifies that SQL parses and round-trips correctly using PostgreSqlDialect.
///
/// This is the primary function for testing implemented features.
/// The SQL string must parse successfully and serialize back to an identical string.
///
/// # Example
///
/// ```ignore
/// let stmt = verified_pg_stmt("CREATE FUNCTION add(a INT, b INT) RETURNS INT AS $$ SELECT a + b $$ LANGUAGE SQL");
/// ```
pub fn verified_pg_stmt(sql: &str) -> Statement {
    pg_dialect().verified_stmt(sql)
}

/// Verifies that SQL parses to a specific canonical form.
///
/// Use this when the input SQL has an alternate form that normalizes
/// to a canonical representation.
///
/// # Example
///
/// ```ignore
/// one_statement_parses_to_pg(
///     "create function f() returns int language sql as 'select 1'",
///     "CREATE FUNCTION f() RETURNS INT LANGUAGE sql AS 'select 1'"
/// );
/// ```
pub fn one_statement_parses_to_pg(sql: &str, canonical: &str) -> Statement {
    pg_dialect().one_statement_parses_to(sql, canonical)
}

/// Attempts to parse SQL and returns the result.
///
/// Useful for conditional test behavior or error inspection.
pub fn try_parse_pg(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql)
}

/// Asserts that parsing fails with an error containing the expected substring.
///
/// # Panics
///
/// Panics if parsing succeeds, or if the error message doesn't contain the expected substring.
pub fn assert_parse_error(sql: &str, expected_error_substring: &str) {
    match try_parse_pg(sql) {
        Ok(_) => panic!(
            "Expected parsing to fail for SQL: {}\nBut parsing succeeded",
            sql
        ),
        Err(e) => {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains(expected_error_substring),
                "Error message '{}' did not contain expected substring '{}'\nSQL: {}",
                error_msg,
                expected_error_substring,
                sql
            );
        }
    }
}

// =============================================================================
// AST Extraction Helpers
// =============================================================================

/// Extracts CreateFunction from a Statement, panicking with a clear message if not found.
///
/// # Panics
///
/// Panics if the statement is not a CreateFunction statement.
pub fn extract_create_function(stmt: &Statement) -> &CreateFunction {
    match stmt {
        Statement::CreateFunction(cf) => cf,
        other => panic!(
            "Expected Statement::CreateFunction, got: {:?}",
            statement_variant_name(other)
        ),
    }
}

/// Helper struct to hold extracted CreateProcedure fields.
///
/// Since CreateProcedure is an inline struct variant in Statement (not a separate type),
/// we need this struct to return the fields.
#[derive(Debug)]
pub struct CreateProcedureExtract<'a> {
    pub or_alter: bool,
    pub name: &'a ObjectName,
    pub params: &'a Option<Vec<ProcedureParam>>,
    pub language: &'a Option<Ident>,
    pub has_as: bool,
    pub body: &'a ConditionalStatements,
}

/// Extracts CreateProcedure fields from a Statement, panicking with a clear message if not found.
///
/// # Panics
///
/// Panics if the statement is not a CreateProcedure statement.
pub fn extract_create_procedure(stmt: &Statement) -> CreateProcedureExtract<'_> {
    match stmt {
        Statement::CreateProcedure {
            or_alter,
            name,
            params,
            language,
            has_as,
            body,
            ..
        } => CreateProcedureExtract {
            or_alter: *or_alter,
            name,
            params,
            language,
            has_as: *has_as,
            body,
        },
        other => panic!(
            "Expected Statement::CreateProcedure, got: {:?}",
            statement_variant_name(other)
        ),
    }
}

/// Extracts CreateTrigger from a Statement, panicking with a clear message if not found.
///
/// # Panics
///
/// Panics if the statement is not a CreateTrigger statement.
pub fn extract_create_trigger(stmt: &Statement) -> &CreateTrigger {
    match stmt {
        Statement::CreateTrigger(ct) => ct,
        other => panic!(
            "Expected Statement::CreateTrigger, got: {:?}",
            statement_variant_name(other)
        ),
    }
}

/// Returns a human-readable name for the Statement variant.
fn statement_variant_name(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "Query",
        Statement::Insert(_) => "Insert",
        Statement::Update { .. } => "Update",
        Statement::Delete(_) => "Delete",
        Statement::CreateTable(_) => "CreateTable",
        Statement::CreateFunction(_) => "CreateFunction",
        Statement::CreateProcedure { .. } => "CreateProcedure",
        Statement::CreateTrigger(_) => "CreateTrigger",
        Statement::DropTrigger(_) => "DropTrigger",
        Statement::DropFunction(_) => "DropFunction",
        Statement::DropProcedure { .. } => "DropProcedure",
        Statement::AlterTable { .. } => "AlterTable",
        Statement::AlterIndex { .. } => "AlterIndex",
        Statement::Drop { .. } => "Drop",
        Statement::Declare { .. } => "Declare",
        _ => "Other",
    }
}

// =============================================================================
// Test Macros
// =============================================================================

/// Macro for PostgreSQL tests that parse correctly and validate AST shape.
///
/// This macro is the primary way to write PostgreSQL compatibility tests.
/// It parses the SQL, verifies round-trip serialization, and runs custom
/// AST validation logic.
///
/// # Example
///
/// ```ignore
/// #[test]
/// fn test_create_function_basic() {
///     pg_test!(
///         "CREATE FUNCTION add(a INT, b INT) RETURNS INT AS $$ SELECT a + b $$ LANGUAGE SQL",
///         |stmt: Statement| {
///             let cf = extract_create_function(&stmt);
///             assert_eq!(cf.name.to_string(), "add");
///             assert_eq!(cf.args.as_ref().unwrap().len(), 2);
///         }
///     );
/// }
/// ```
#[macro_export]
macro_rules! pg_test {
    ($sql:expr, $validator:expr) => {{
        let stmt = $crate::postgres_compat::common::verified_pg_stmt($sql);
        let validator: fn(sqlparser::ast::Statement) = $validator;
        validator(stmt);
    }};
}

/// Macro for PostgreSQL tests that verify round-trip only (without AST validation).
///
/// Use this with a TODO comment documenting what validation is needed.
#[macro_export]
macro_rules! pg_roundtrip_only {
    ($sql:expr) => {{
        $crate::postgres_compat::common::verified_pg_stmt($sql)
    }};
}

/// Macro for tests that expect parsing to fail.
///
/// This macro verifies that the SQL fails to parse, which documents
/// features not yet implemented. When the feature is implemented,
/// this test will fail, signaling it should be converted to a passing test.
///
/// # Example
///
/// ```ignore
/// #[test]
/// fn test_do_block_not_yet_implemented() {
///     pg_expect_parse_error!(
///         "DO $$ BEGIN RAISE NOTICE 'hello'; END $$",
///         "Expected" // partial error message
///     );
/// }
/// ```
#[macro_export]
macro_rules! pg_expect_parse_error {
    ($sql:expr) => {{
        let result = $crate::postgres_compat::common::try_parse_pg($sql);
        assert!(
            result.is_err(),
            "Expected parsing to fail, but it succeeded for: {}",
            $sql
        );
        result.unwrap_err()
    }};
    ($sql:expr, $expected_error:expr) => {{
        $crate::postgres_compat::common::assert_parse_error($sql, $expected_error);
    }};
}

/// Macro for tests that parse to a canonical form with AST validation.
///
/// Combines `one_statement_parses_to_pg()` with AST validation.
#[macro_export]
macro_rules! pg_parses_to_with_ast {
    ($sql:expr, $canonical:expr, $validator:expr) => {{
        let stmt = $crate::postgres_compat::common::one_statement_parses_to_pg($sql, $canonical);
        let validator: fn(sqlparser::ast::Statement) = $validator;
        validator(stmt);
    }};
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_dialect_basic_select() {
        let stmt = verified_pg_stmt("SELECT 1");
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_try_parse_pg_returns_result() {
        assert!(try_parse_pg("SELECT 1").is_ok());
        assert!(try_parse_pg("COMPLETELY INVALID SQL SYNTAX !!!").is_err());
    }

    #[test]
    fn test_assert_parse_error() {
        assert_parse_error("COMPLETELY INVALID", "Expected");
    }

    #[test]
    #[should_panic(expected = "Expected parsing to fail")]
    fn test_assert_parse_error_panics_on_success() {
        assert_parse_error("SELECT 1", "any error");
    }

    #[test]
    fn test_extract_create_function() {
        let stmt = verified_pg_stmt(
            "CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$",
        );
        let cf = extract_create_function(&stmt);
        assert_eq!(cf.name.to_string(), "add");
    }

    #[test]
    #[should_panic(expected = "Expected Statement::CreateFunction, got: \"Query\"")]
    fn test_extract_create_function_panics_on_wrong_type() {
        let stmt = verified_pg_stmt("SELECT 1");
        extract_create_function(&stmt);
    }

    #[test]
    fn test_extract_create_procedure() {
        let stmt = verified_pg_stmt("CREATE PROCEDURE test_proc() AS BEGIN SELECT 1; END");
        let proc = extract_create_procedure(&stmt);
        assert_eq!(proc.name.to_string(), "test_proc");
        assert!(!proc.or_alter);
    }

    #[test]
    #[should_panic(expected = "Expected Statement::CreateProcedure, got: \"Query\"")]
    fn test_extract_create_procedure_panics_on_wrong_type() {
        let stmt = verified_pg_stmt("SELECT 1");
        extract_create_procedure(&stmt);
    }

    #[test]
    fn test_extract_create_trigger() {
        let stmt = verified_pg_stmt(
            "CREATE TRIGGER my_trigger BEFORE INSERT ON my_table FOR EACH ROW EXECUTE FUNCTION my_func()",
        );
        let trigger = extract_create_trigger(&stmt);
        assert_eq!(trigger.name.to_string(), "my_trigger");
    }

    #[test]
    #[should_panic(expected = "Expected Statement::CreateTrigger, got: \"Query\"")]
    fn test_extract_create_trigger_panics_on_wrong_type() {
        let stmt = verified_pg_stmt("SELECT 1");
        extract_create_trigger(&stmt);
    }

    #[test]
    fn test_statement_variant_name() {
        assert_eq!(
            statement_variant_name(&verified_pg_stmt("SELECT 1")),
            "Query"
        );
        assert_eq!(
            statement_variant_name(&verified_pg_stmt("CREATE TABLE t (a INT)")),
            "CreateTable"
        );
    }
}
