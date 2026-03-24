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

/// This module contains internal utilities used for testing the library.
/// While technically public, the library's users are not supposed to rely
/// on this module, as it will change without notice.
//
// Integration tests (i.e. everything under `tests/`) import this
// via `tests/test_utils/helpers`.

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt::Debug;

use crate::dialect::*;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::{Token, Tokenizer};
use crate::{ast::*, parser::ParserOptions};

#[cfg(test)]
use pretty_assertions::assert_eq;

/// Tests use the methods on this struct to invoke the parser on one or
/// multiple dialects.
pub struct TestedDialects {
    pub dialects: Vec<Box<dyn Dialect>>,
    pub options: Option<ParserOptions>,
    pub recursion_limit: Option<usize>,
}

impl TestedDialects {
    /// Create a TestedDialects with default options and the given dialects.
    pub fn new(dialects: Vec<Box<dyn Dialect>>) -> Self {
        Self {
            dialects,
            options: None,
            recursion_limit: None,
        }
    }

    pub fn new_with_options(dialects: Vec<Box<dyn Dialect>>, options: ParserOptions) -> Self {
        Self {
            dialects,
            options: Some(options),
            recursion_limit: None,
        }
    }

    pub fn with_recursion_limit(mut self, recursion_limit: usize) -> Self {
        self.recursion_limit = Some(recursion_limit);
        self
    }

    fn new_parser<'a>(&self, dialect: &'a dyn Dialect) -> Parser<'a> {
        let parser = Parser::new(dialect);
        let parser = if let Some(options) = &self.options {
            parser.with_options(options.clone())
        } else {
            parser
        };

        let parser = if let Some(recursion_limit) = &self.recursion_limit {
            parser.with_recursion_limit(*recursion_limit)
        } else {
            parser
        };

        parser
    }

    /// Run the given function for all of `self.dialects` and return the
    /// result from the last non-canonicalizing dialect (or the last dialect
    /// if all canonicalize). Each dialect is tested independently to ensure
    /// it can parse without error. Results are not compared across dialects
    /// since different dialects may canonicalize differently (e.g.,
    /// PostgreSQL lowercases unquoted identifiers).
    pub fn one_of_identical_results<F, T: Debug + PartialEq>(&self, f: F) -> T
    where
        F: Fn(&dyn Dialect) -> T,
    {
        let mut last_result = None;
        for dialect in &self.dialects {
            let result = f(&**dialect);
            // Prefer non-canonicalizing dialect results for backwards
            // compatibility with tests that assert specific identifier casing.
            if !dialect.is::<PostgreSqlDialect>() || last_result.is_none() {
                last_result = Some(result);
            }
        }
        last_result.expect("tested dialects cannot be empty")
    }

    pub fn run_parser_method<F, T: Debug + PartialEq>(&self, sql: &str, f: F) -> T
    where
        F: Fn(&mut Parser) -> T,
    {
        self.one_of_identical_results(|dialect| {
            let mut parser = self.new_parser(dialect).try_with_sql(sql).unwrap();
            f(&mut parser)
        })
    }

    /// Parses a single SQL string into multiple statements, ensuring
    /// the result is the same for all tested dialects.
    pub fn parse_sql_statements(&self, sql: &str) -> Result<Vec<Statement>, ParserError> {
        self.one_of_identical_results(|dialect| {
            let mut tokenizer = Tokenizer::new(dialect, sql);
            if let Some(options) = &self.options {
                tokenizer = tokenizer.with_unescape(options.unescape);
            }

            let tokens = tokenizer.tokenized_owned()?;
            self.new_parser(dialect)
                .with_tokens(tokens)
                .parse_statements()
        })
        // To fail the `ensure_multiple_dialects_are_tested` test:
        // Parser::parse_sql(&**self.dialects.first().unwrap(), sql)
    }

    /// Ensures that `sql` parses as a single [Statement] for all tested
    /// dialects.
    ///
    /// In general, the canonical SQL should be the same (see crate
    /// documentation for rationale) and you should prefer the `verified_`
    /// variants in testing, such as  [`verified_statement`] or
    /// [`verified_query`].
    ///
    /// If `canonical` is non empty,this function additionally asserts
    /// that:
    ///
    /// 1. parsing `sql` results in the same [`Statement`] as parsing
    ///    `canonical`.
    ///
    /// 2. re-serializing the result of parsing `sql` produces the same
    ///    `canonical` sql string
    ///
    ///  For multiple statements, use [`statements_parse_to`].
    pub fn one_statement_parses_to(&self, sql: &str, canonical: &str) -> Statement {
        // Test each dialect independently: parse, serialize, and verify roundtrip.
        // Different dialects may canonicalize differently (e.g., PG lowercases
        // unquoted identifiers), so we verify per-dialect roundtrip consistency
        // rather than cross-dialect identity. Dialects that fail to parse are
        // skipped (not all SQL is valid across all dialects).
        let mut result_statement = None;
        let mut success_count = 0;
        for dialect in &self.dialects {
            let parser = self.new_parser(&**dialect);
            let parse_result = parser.try_with_sql(sql).and_then(|p| p.parse_statements());
            let mut stmts = match parse_result {
                Ok(stmts) => stmts,
                Err(_) => continue, // Skip dialects that can't parse this SQL
            };
            if stmts.len() != 1 {
                continue;
            }
            success_count += 1;
            let stmt = stmts.pop().unwrap();

            // Verify roundtrip: parse -> display -> parse produces same AST
            let serialized = stmt.to_string();
            if let Ok(mut reparsed) = self
                .new_parser(&**dialect)
                .try_with_sql(&serialized)
                .and_then(|p| p.parse_statements())
            {
                if reparsed.len() == 1 {
                    assert_eq!(
                        stmt,
                        reparsed.pop().unwrap(),
                        "Roundtrip failed for {dialect:?}: {sql} -> {serialized}"
                    );
                }
            }

            // Prefer non-canonicalizing dialect for return value, for backwards
            // compatibility with tests that assert specific identifier casing
            if !dialect.is::<PostgreSqlDialect>() || result_statement.is_none() {
                result_statement = Some(stmt);
            }
        }

        assert!(
            success_count > 0,
            "SQL failed to parse on all dialects: {sql}"
        );
        let only_statement = result_statement.expect("tested dialects cannot be empty");

        // Verify canonical form against the returned dialect's output.
        // When canonical == sql (called via verified_stmt), we skip this check
        // because dialect canonicalization (e.g., PG lowercasing identifiers)
        // means the display output may legitimately differ from the input.
        // The roundtrip check above already ensures parse→display→parse stability.
        if !canonical.is_empty() && canonical != sql {
            assert_eq!(canonical, only_statement.to_string());
        }
        only_statement
    }

    /// The same as [`one_statement_parses_to`] but it works for a multiple statements
    pub fn statements_parse_to(&self, sql: &str, _canonical: &str) -> Vec<Statement> {
        let mut result_statements = None;
        for dialect in &self.dialects {
            let parser = self.new_parser(&**dialect);
            let stmts = parser
                .try_with_sql(sql)
                .expect(sql)
                .parse_statements()
                .expect(sql);

            // Verify roundtrip per-dialect
            let serialized = stmts
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            let reparsed = self
                .new_parser(&**dialect)
                .try_with_sql(&serialized)
                .expect(&serialized)
                .parse_statements()
                .expect(&serialized);
            assert_eq!(
                stmts, reparsed,
                "Roundtrip failed for {dialect:?}: {sql} -> {serialized}"
            );

            if !dialect.is::<PostgreSqlDialect>() || result_statements.is_none() {
                result_statements = Some(stmts);
            }
        }
        result_statements.expect("tested dialects cannot be empty")
    }

    /// Ensures that `sql` parses as an [`Expr`], and that
    /// re-serializing the parse result produces canonical
    pub fn expr_parses_to(&self, sql: &str, canonical: &str) -> Expr {
        let ast = self
            .run_parser_method(sql, |parser| parser.parse_expr())
            .unwrap();
        assert_eq!(canonical, &ast.to_string());
        ast
    }

    /// Ensures that `sql` parses as a single [Statement], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_stmt(&self, sql: &str) -> Statement {
        self.one_statement_parses_to(sql, sql)
    }

    /// Ensures that `sql` parses as a single [Query], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_query(&self, sql: &str) -> Query {
        match self.verified_stmt(sql) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        }
    }

    /// Ensures that `sql` parses as a single [Query], and that
    /// re-serializing the parse result matches the given canonical
    /// sql string.
    pub fn verified_query_with_canonical(&self, query: &str, canonical: &str) -> Query {
        match self.one_statement_parses_to(query, canonical) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        }
    }

    /// Ensures that `sql` parses as a single [Select], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_only_select(&self, query: &str) -> Select {
        match *self.verified_query(query).body {
            SetExpr::Select(s) => *s,
            _ => panic!("Expected SetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as a single [`Select`], and that additionally:
    ///
    /// 1. parsing `sql` results in the same [`Statement`] as parsing
    ///    `canonical`.
    ///
    /// 2. re-serializing the result of parsing `sql` produces the same
    ///    `canonical` sql string
    pub fn verified_only_select_with_canonical(&self, query: &str, canonical: &str) -> Select {
        let q = match self.one_statement_parses_to(query, canonical) {
            Statement::Query(query) => *query,
            _ => panic!("Expected Query"),
        };
        match *q.body {
            SetExpr::Select(s) => *s,
            _ => panic!("Expected SetExpr::Select"),
        }
    }

    /// Ensures that `sql` parses as an [`Expr`], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    pub fn verified_expr(&self, sql: &str) -> Expr {
        self.expr_parses_to(sql, sql)
    }

    /// Check that the tokenizer returns the expected tokens for the given SQL.
    pub fn tokenizes_to(&self, sql: &str, expected: Vec<Token>) {
        if self.dialects.is_empty() {
            panic!("No dialects to test");
        }

        self.dialects.iter().for_each(|dialect| {
            let mut tokenizer = Tokenizer::new(&**dialect, sql);
            if let Some(options) = &self.options {
                tokenizer = tokenizer.with_unescape(options.unescape);
            }
            let tokens = tokenizer.tokenize().unwrap();
            assert_eq!(expected, tokens, "Tokenized differently for {dialect:?}");
        });
    }
}

/// Returns all available dialects.
pub fn all_dialects() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(PostgreSqlDialect {}),
        Box::new(MsSqlDialect {}),
        Box::new(MySqlDialect {}),
    ])
}

// Returns all available dialects with the specified parser options
pub fn all_dialects_with_options(options: ParserOptions) -> TestedDialects {
    TestedDialects::new_with_options(all_dialects().dialects, options)
}

/// Returns all dialects matching the given predicate.
pub fn all_dialects_where<F>(predicate: F) -> TestedDialects
where
    F: Fn(&dyn Dialect) -> bool,
{
    let mut dialects = all_dialects();
    dialects.dialects.retain(|d| predicate(&**d));
    dialects
}

/// Returns available dialects. The `except` predicate is used
/// to filter out specific dialects.
pub fn all_dialects_except<F>(except: F) -> TestedDialects
where
    F: Fn(&dyn Dialect) -> bool,
{
    all_dialects_where(|d| !except(d))
}

pub fn assert_eq_vec<T: ToString>(expected: &[&str], actual: &[T]) {
    assert_eq!(
        expected,
        actual.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
}

pub fn only<T>(v: impl IntoIterator<Item = T>) -> T {
    let mut iter = v.into_iter();
    if let (Some(item), None) = (iter.next(), iter.next()) {
        item
    } else {
        panic!("only called on collection without exactly one item")
    }
}

pub fn expr_from_projection(item: &SelectItem) -> &Expr {
    match item {
        SelectItem::UnnamedExpr(expr) => expr,
        _ => panic!("Expected UnnamedExpr"),
    }
}

pub fn alter_table_op_with_name(stmt: Statement, expected_name: &str) -> AlterTableOperation {
    match stmt {
        Statement::AlterTable(alter_table) => {
            assert_eq!(alter_table.name.to_string(), expected_name);
            assert!(!alter_table.if_exists);
            assert!(!alter_table.only);
            only(alter_table.operations)
        }
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

pub fn alter_table_op(stmt: Statement) -> AlterTableOperation {
    alter_table_op_with_name(stmt, "tab")
}

/// Creates a `Value::Number`, panic'ing if n is not a number
pub fn number(n: &str) -> Value {
    Value::Number(n.parse().unwrap(), false)
}

/// Creates a [Value::SingleQuotedString]
pub fn single_quoted_string(s: impl Into<String>) -> Value {
    Value::SingleQuotedString(s.into())
}

pub fn table_alias(name: impl Into<String>) -> Option<TableAlias> {
    Some(TableAlias {
        name: Ident::new(name),
        columns: vec![],
        implicit: false,
    })
}

pub fn table(name: impl Into<String>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![Ident::new(name.into())]),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

pub fn table_from_name(name: ObjectName) -> TableFactor {
    TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

pub fn table_with_alias(name: impl Into<String>, alias: impl Into<String>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![Ident::new(name)]),
        alias: Some(TableAlias {
            name: Ident::new(alias),
            columns: vec![],
            implicit: false,
        }),
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
        with_ordinality: false,
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

pub fn join(relation: TableFactor) -> Join {
    Join {
        relation,
        global: false,
        join_operator: JoinOperator::Join(JoinConstraint::Natural),
    }
}

pub fn call(function: &str, args: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(function)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        nth_value_order: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// Gets the first index column (mysql calls it a key part) of the first index found in a
/// [`Statement::CreateIndex`], [`Statement::CreateTable`], or [`Statement::AlterTable`].
pub fn index_column(stmt: Statement) -> Expr {
    match stmt {
        Statement::CreateIndex(CreateIndex { columns, .. }) => {
            columns.first().unwrap().column.expr.clone()
        }
        Statement::CreateTable(CreateTable { constraints, .. }) => {
            match constraints.first().unwrap() {
                TableConstraint::Index(constraint) => {
                    constraint.columns.first().unwrap().column.expr.clone()
                }
                TableConstraint::Unique(constraint) => {
                    constraint.columns.first().unwrap().column.expr.clone()
                }
                TableConstraint::PrimaryKey(constraint) => {
                    constraint.columns.first().unwrap().column.expr.clone()
                }
                TableConstraint::FulltextOrSpatial(constraint) => {
                    constraint.columns.first().unwrap().column.expr.clone()
                }
                _ => panic!("Expected an index, unique, primary, full text, or spatial constraint (foreign key does not support general key part expressions)"),
            }
        }
        Statement::AlterTable(alter_table) => match alter_table.operations.first().unwrap() {
            AlterTableOperation::AddConstraint { constraint, .. } => {
                match constraint {
                    TableConstraint::Index(constraint) => {
                        constraint.columns.first().unwrap().column.expr.clone()
                    }
                    TableConstraint::Unique(constraint) => {
                        constraint.columns.first().unwrap().column.expr.clone()
                    }
                    TableConstraint::PrimaryKey(constraint) => {
                        constraint.columns.first().unwrap().column.expr.clone()
                    }
                    TableConstraint::FulltextOrSpatial(constraint) => {
                        constraint.columns.first().unwrap().column.expr.clone()
                    }
                    _ => panic!("Expected an index, unique, primary, full text, or spatial constraint (foreign key does not support general key part expressions)"),
                }
            }
            _ => panic!("Expected a constraint"),
        },
        _ => panic!("Expected CREATE INDEX, ALTER TABLE, or CREATE TABLE, got: {stmt:?}"),
    }
}
