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

//! Common utilities for SQL standards compliance testing.
//!
//! This module provides test helpers, macros, and utilities specifically designed
//! for standards compliance testing with the GenericDialect.

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::test_utils::TestedDialects;

/// Returns a TestedDialects configured for SQL standards compliance testing.
///
/// Uses GenericDialect as the primary target since standards compliance
/// should work across all dialects without dialect-specific extensions.
pub fn standard_dialect() -> TestedDialects {
    TestedDialects::new(vec![Box::new(GenericDialect {})])
}

/// Verifies that SQL parses and round-trips correctly using GenericDialect.
///
/// This is the primary function for testing implemented features.
/// The SQL string must parse successfully and serialize back to an identical string.
///
/// # Example
///
/// ```ignore
/// let stmt = verified_standard_stmt("SELECT a, b FROM t WHERE c > 1");
/// // stmt is the parsed Statement that round-trips correctly
/// ```
pub fn verified_standard_stmt(sql: &str) -> Statement {
    standard_dialect().verified_stmt(sql)
}

/// Verifies that SQL parses to a specific canonical form.
///
/// Use this when the input SQL has an alternate form that normalizes
/// to a canonical representation.
///
/// # Example
///
/// ```ignore
/// // "select" normalizes to "SELECT"
/// one_statement_parses_to_std("select a from t", "SELECT a FROM t");
/// ```
pub fn one_statement_parses_to_std(sql: &str, canonical: &str) -> Statement {
    standard_dialect().one_statement_parses_to(sql, canonical)
}

/// Attempts to parse SQL and returns the result.
///
/// Useful for conditional test behavior or error inspection.
pub fn try_parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(&GenericDialect {}, sql)
}

/// Macro for features that parse correctly but need AST validation later.
///
/// Use this with a TODO comment documenting what validation is needed.
/// The statement is verified to round-trip correctly.
///
/// # Example
///
/// ```ignore
/// #[test]
/// fn t612_15_window_frame_exclude() {
///     // SQL:2016 T612-15: EXCLUDE clause in window frame
///     // TODO(SQL:2016 T612-15): Validate WindowFrame.exclude field
///     verified_roundtrip_only!(
///         "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM t"
///     );
/// }
/// ```
#[macro_export]
macro_rules! verified_roundtrip_only {
    ($sql:expr) => {{
        $crate::standards::common::verified_standard_stmt($sql)
    }};
}

/// Macro for fully verified tests with AST validation.
///
/// # Example
///
/// ```ignore
/// #[test]
/// fn e011_01_integer_types() {
///     verified_with_ast!(
///         "CREATE TABLE t (a INTEGER, b SMALLINT)",
///         |stmt: Statement| {
///             match stmt {
///                 Statement::CreateTable(ct) => {
///                     assert_eq!(ct.columns.len(), 2);
///                 }
///                 _ => panic!("Expected CreateTable"),
///             }
///         }
///     );
/// }
/// ```
#[macro_export]
macro_rules! verified_with_ast {
    ($sql:expr, $validator:expr) => {{
        let stmt = $crate::standards::common::verified_standard_stmt($sql);
        let validator: fn(sqlparser::ast::Statement) = $validator;
        validator(stmt);
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_dialect_basic_select() {
        let stmt = verified_standard_stmt("SELECT 1");
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_try_parse_returns_result() {
        assert!(try_parse("SELECT 1").is_ok());
        assert!(try_parse("COMPLETELY INVALID SQL SYNTAX !!!").is_err());
    }
}
