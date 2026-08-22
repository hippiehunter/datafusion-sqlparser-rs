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

//! SQL parser for PL/pgSQL routine bodies and for the parts of
//! `CREATE FUNCTION` / `CREATE PROCEDURE` that PostgreSQL shares between the
//! two commands.
//!
//! References:
//! - <https://www.postgresql.org/docs/current/plpgsql.html>
//! - `src/pl/plpgsql/src/pl_gram.y` in the PostgreSQL sources

#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec, vec::Vec};

use super::{Parser, ParserError};
use crate::{
    ast::{
        AtomicBlock, AttachedToken, Box, ConditionalStatementBlock, ConditionalStatements,
        CreateFunctionBody, DataType, DiagnosticsItem, ExecuteInto, Expr, ForLoopVariant,
        FunctionBehavior, FunctionCalledOnNull, FunctionParallel, Ident, ObjectName, PlpgsqlAssert,
        Query, RoutineAttribute, SqlPsmAssignment, SqlPsmDataType, SqlPsmQueryAssignment,
        Statement,
    },
    dialect::Precedence,
    keywords::Keyword,
    tokenizer::{BorrowedToken, TokenWithSpan},
};

/// The `WHEN` arms of a PL/pgSQL `CASE` statement, paired with the per-arm
/// value lists that [`crate::ast::CaseStatement::when_values`] records.
type CaseWhenArms = (Vec<ConditionalStatementBlock>, Vec<Option<Vec<Expr>>>);

impl Parser<'_> {
    /// Parse a statement introduced by a PL/pgSQL `<<label>>`.
    ///
    /// A label before `DECLARE` or `BEGIN` labels the block itself and is
    /// recorded in [`crate::ast::BeginEndStatements::label`]; a label before a
    /// loop is kept in a [`Statement::PlSqlLabeled`] wrapper around it.
    ///
    /// <https://www.postgresql.org/docs/current/plpgsql-structure.html>
    pub(super) fn parse_plpgsql_labeled_statement(&self) -> Result<Statement, ParserError> {
        let start = self.index.get();
        let label = match self.parse_sql_psm_label()? {
            Some(label) => label,
            None => return self.expected("a <<label>>", self.peek_token()),
        };

        if self.peek_keyword(Keyword::DECLARE) || self.peek_keyword(Keyword::BEGIN) {
            self.index.set(start);
            return self.parse_sql_psm_block().map(Statement::PlSqlBlock);
        }

        let statement = self.parse_statement()?;
        Ok(Statement::PlSqlLabeled {
            label,
            statement: Box::new(statement),
        })
    }

    /// Parse a nested PL/pgSQL block introduced by `DECLARE`.
    ///
    /// PL/pgSQL reads a statement-initial `DECLARE` as the declaration section
    /// of a nested block. A `DECLARE ... CURSOR FOR` statement handed to the
    /// server as plain SQL is not a block, so it falls back to
    /// [`Parser::parse_declare`].
    pub(super) fn parse_plpgsql_declare_statement(&self) -> Result<Statement, ParserError> {
        let after_declare = self.index.get();
        self.prev_token();
        if let Some(block) = self.maybe_parse(|parser| parser.parse_sql_psm_block())? {
            return Ok(Statement::PlSqlBlock(block));
        }
        self.index.set(after_declare);
        self.parse_declare()
    }

    /// Parse a PL/pgSQL `ASSERT condition [, message]`.
    ///
    /// <https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-ASSERT>
    pub(super) fn parse_plpgsql_assert(&self) -> Result<Statement, ParserError> {
        let assert_token = self.attached_token_from_current();
        let condition = self.parse_expr()?;
        let message = if self.consume_token(&BorrowedToken::Comma) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::PlpgsqlAssert(PlpgsqlAssert {
            assert_token,
            condition,
            message,
        }))
    }

    /// Parse a PL/pgSQL `EXECUTE command-string [INTO [STRICT] target] [USING expr, ...]`.
    ///
    /// `INTO` and `USING` may be written in either order, each at most once.
    ///
    /// <https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN>
    pub(super) fn parse_plpgsql_execute(&self) -> Result<Statement, ParserError> {
        self.expect_keyword_is(Keyword::EXECUTE)?;
        let query_expr = self.parse_expr()?;

        let mut into: Option<ExecuteInto> = None;
        let mut using: Option<Vec<Expr>> = None;
        loop {
            if into.is_none() && self.parse_keyword(Keyword::INTO) {
                let strict = self.parse_keyword(Keyword::STRICT);
                let targets = self.parse_comma_separated(Parser::parse_identifier)?;
                into = Some(ExecuteInto { strict, targets });
            } else if using.is_none() && self.parse_keyword(Keyword::USING) {
                using = Some(self.parse_comma_separated(Parser::parse_expr)?);
            } else {
                break;
            }
        }

        Ok(Statement::ExecuteDynamic {
            query_expr: Box::new(query_expr),
            into,
            using,
        })
    }

    /// Parse a PL/pgSQL assignment statement.
    ///
    /// Returns `Ok(None)` when the statement is not an assignment, leaving the
    /// parser where it started.
    ///
    /// The right hand side is whatever PL/pgSQL would run as
    /// `SELECT <rest of line>`: a plain expression becomes a
    /// [`Statement::SqlPsmAssignment`], while a select list with more than one
    /// item or with query clauses after it becomes a
    /// [`Statement::SqlPsmQueryAssignment`].
    ///
    /// <https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-ASSIGNMENT>
    pub(super) fn parse_plpgsql_assignment(&self) -> Result<Option<Statement>, ParserError> {
        let start = self.index.get();
        // A target is an identifier, a record field or an array element, all of
        // which bind tighter than the `=` that can spell the assignment.
        let target = match self
            .maybe_parse(|parser| parser.parse_subexpr(parser.dialect.prec_value(Precedence::Eq)))?
        {
            Some(target) => target,
            None => {
                self.index.set(start);
                return Ok(None);
            }
        };
        if !self.consume_token(&BorrowedToken::Assignment)
            && !self.consume_token(&BorrowedToken::Eq)
        {
            self.index.set(start);
            return Ok(None);
        }

        let value_start = self.index.get();
        if let Some(value) = self.maybe_parse(|parser| parser.parse_expr())? {
            if self.peek_ends_plpgsql_statement() {
                return Ok(Some(Statement::SqlPsmAssignment(SqlPsmAssignment {
                    target,
                    value,
                })));
            }
        }

        self.index.set(value_start);
        let query = self.parse_plpgsql_assignment_query()?;
        Ok(Some(Statement::SqlPsmQueryAssignment(
            SqlPsmQueryAssignment { target, query },
        )))
    }

    /// True when the parser sits at something that can follow a complete
    /// PL/pgSQL statement.
    fn peek_ends_plpgsql_statement(&self) -> bool {
        matches!(
            self.peek_token_ref().token,
            BorrowedToken::SemiColon | BorrowedToken::EOF
        )
    }

    /// Parse the remainder of a PL/pgSQL assignment as the query PL/pgSQL runs
    /// for it, by reading the select list and the clauses after it as
    /// `SELECT <those tokens>`.
    fn parse_plpgsql_assignment_query(&self) -> Result<Box<Query>, ParserError> {
        let span = self.peek_token_ref().span;
        let mut tokens = vec![TokenWithSpan::new(
            BorrowedToken::make_keyword("SELECT"),
            span,
        )];
        let mut depth: usize = 0;
        loop {
            match &self.peek_token_ref().token {
                BorrowedToken::EOF => break,
                BorrowedToken::SemiColon if depth == 0 => break,
                BorrowedToken::LParen | BorrowedToken::LBracket => depth += 1,
                BorrowedToken::RParen | BorrowedToken::RBracket => depth = depth.saturating_sub(1),
                _ => {}
            }
            tokens.push(self.next_token().to_static());
        }
        let parser = Parser::new(self.dialect).with_tokens_with_locations(tokens);
        parser.parse_query()
    }

    /// Parse the second and later scalar targets of `FOR a, b, c IN query LOOP`.
    ///
    /// <https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING>
    pub(super) fn parse_plpgsql_for_targets(&self) -> Result<Vec<Ident>, ParserError> {
        let mut targets = vec![];
        while self.consume_token(&BorrowedToken::Comma) {
            targets.push(self.parse_identifier()?);
        }
        Ok(targets)
    }

    /// Parse the query of `FOR target IN query LOOP`.
    ///
    /// The query is any SQL command that returns rows, so `INSERT`, `UPDATE`,
    /// `DELETE` and `MERGE` with a `RETURNING` clause are all accepted.
    pub(super) fn parse_plpgsql_for_in_query(&self) -> Result<ForLoopVariant, ParserError> {
        if let Some(variant) = self.maybe_parse(|parser| {
            let variant = parser.parse_plpgsql_for_query_source()?;
            parser.expect_at_loop_body()?;
            Ok(variant)
        })? {
            return Ok(variant);
        }

        // The query parser read the body keyword as an alias; hand it only the
        // tokens that precede it.
        let parser = Parser::new(self.dialect)
            .with_tokens_with_locations(self.take_tokens_before_loop_body()?);
        parser.parse_plpgsql_for_query_source()
    }

    /// Parse the query of a `FOR` loop, stopping at the `LOOP` or `DO` that
    /// opens the body.
    pub(super) fn parse_for_loop_query(&self) -> Result<Box<Query>, ParserError> {
        if let Some(query) = self.maybe_parse(|parser| {
            let query = parser.parse_query()?;
            parser.expect_at_loop_body()?;
            Ok(query)
        })? {
            return Ok(query);
        }
        let parser = Parser::new(self.dialect)
            .with_tokens_with_locations(self.take_tokens_before_loop_body()?);
        parser.parse_query()
    }

    fn expect_at_loop_body(&self) -> Result<(), ParserError> {
        if self.peek_keyword(Keyword::LOOP) || self.peek_keyword(Keyword::DO) {
            Ok(())
        } else {
            Err(ParserError::ParserError(
                "query consumed the keyword that opens the loop body".to_string(),
            ))
        }
    }

    fn take_tokens_before_loop_body(&self) -> Result<Vec<TokenWithSpan<'static>>, ParserError> {
        let mut tokens = vec![];
        while !self.peek_keyword(Keyword::LOOP) && !self.peek_keyword(Keyword::DO) {
            if self.peek_token_ref().token == BorrowedToken::EOF {
                return Err(ParserError::ParserError(
                    "Expected LOOP after FOR ... IN query".to_string(),
                ));
            }
            tokens.push(self.next_token().to_static());
        }
        Ok(tokens)
    }

    fn parse_plpgsql_for_query_source(&self) -> Result<ForLoopVariant, ParserError> {
        if self
            .peek_one_of_keywords(&[
                Keyword::INSERT,
                Keyword::UPDATE,
                Keyword::DELETE,
                Keyword::MERGE,
            ])
            .is_some()
        {
            let statement = self.parse_statement()?;
            return Ok(ForLoopVariant::StatementQuery {
                statement: Box::new(statement),
            });
        }
        Ok(ForLoopVariant::InQuery(self.parse_query()?))
    }

    /// Parse the `WHEN` arms of a PL/pgSQL `CASE` statement that has a search
    /// expression. Each arm carries one or more comma-separated values.
    ///
    /// Returns the arms and, when at least one arm has more than one value, the
    /// per-arm value lists for [`crate::ast::CaseStatement::when_values`].
    ///
    /// <https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS-CASE-SIMPLE>
    pub(super) fn parse_plpgsql_case_when_blocks(&self) -> Result<CaseWhenArms, ParserError> {
        let mut when_blocks = vec![];
        let mut when_values = vec![];
        let mut has_value_list = false;

        while self.parse_keyword(Keyword::WHEN) {
            let start_token = AttachedToken::from(self.get_current_token().clone());
            let mut values = self.parse_comma_separated(Parser::parse_expr)?;
            let then_token = self.expect_keyword(Keyword::THEN)?;
            let conditional_statements =
                self.parse_conditional_statements(&[Keyword::WHEN, Keyword::ELSE, Keyword::END])?;

            let condition = if values.len() == 1 {
                when_values.push(None);
                values.pop()
            } else {
                has_value_list = true;
                when_values.push(Some(values));
                None
            };

            when_blocks.push(ConditionalStatementBlock {
                start_token,
                condition,
                then_token: Some(AttachedToken::from(then_token)),
                conditional_statements,
            });
        }

        if !has_value_list {
            when_values.clear();
        }
        Ok((when_blocks, when_values))
    }

    /// Wrap a routine parameter or return type in `%TYPE` when the source spells
    /// it that way: `CREATE FUNCTION f(x tab.col%TYPE)`.
    ///
    /// See `func_type` in PostgreSQL's grammar.
    pub(super) fn parse_percent_type_suffix(
        &self,
        data_type: DataType,
    ) -> Result<DataType, ParserError> {
        if !self.peek_keywords_after_mod(Keyword::TYPE) {
            return Ok(data_type);
        }
        if let DataType::SetOf(element) = data_type {
            let element = self.parse_percent_type_suffix(Box::into_inner(element))?;
            return Ok(DataType::SetOf(Box::new(element)));
        }
        let name = match data_type_as_object_name(&data_type) {
            Some(name) => name,
            None => return Ok(data_type),
        };
        self.expect_token(&BorrowedToken::Mod)?;
        self.expect_keyword_is(Keyword::TYPE)?;
        Ok(DataType::TypeOf(name))
    }

    fn peek_keywords_after_mod(&self, keyword: Keyword) -> bool {
        if self.peek_token_ref().token != BorrowedToken::Mod {
            return false;
        }
        matches!(&self.peek_nth_token_ref(1).token, BorrowedToken::Word(word)
            if word.quote_style.is_none() && word.keyword == keyword)
    }

    /// Parse the `[]` array decorations that may follow a PL/pgSQL declared
    /// type, including `ARRAY` written before them.
    ///
    /// See `read_datatype` in `pl_gram.y`; PostgreSQL ignores the declared
    /// dimensions and sizes.
    pub(super) fn parse_sql_psm_array_suffix(
        &self,
        data_type: SqlPsmDataType,
    ) -> Result<SqlPsmDataType, ParserError> {
        let mut data_type = data_type;
        let mut is_array = self.parse_keyword(Keyword::ARRAY);
        while self.consume_token(&BorrowedToken::LBracket) {
            if matches!(self.peek_token_ref().token, BorrowedToken::Number(_, _)) {
                self.advance_token();
            }
            self.expect_token(&BorrowedToken::RBracket)?;
            data_type = SqlPsmDataType::Array(Box::new(data_type));
            is_array = false;
        }
        if is_array {
            data_type = SqlPsmDataType::Array(Box::new(data_type));
        }
        Ok(data_type)
    }

    /// Parse one routine attribute that has no dedicated field on
    /// `CREATE FUNCTION` / `CREATE PROCEDURE`.
    ///
    /// <https://www.postgresql.org/docs/current/sql-createprocedure.html>
    pub(super) fn parse_routine_attribute(&self) -> Result<Option<RoutineAttribute>, ParserError> {
        if self.parse_keyword(Keyword::IMMUTABLE) {
            return Ok(Some(RoutineAttribute::Behavior(
                FunctionBehavior::Immutable,
            )));
        }
        if self.parse_keyword(Keyword::STABLE) {
            return Ok(Some(RoutineAttribute::Behavior(FunctionBehavior::Stable)));
        }
        if self.parse_keyword(Keyword::VOLATILE) {
            return Ok(Some(RoutineAttribute::Behavior(FunctionBehavior::Volatile)));
        }
        if self.parse_keywords(&[Keyword::CALLED, Keyword::ON, Keyword::NULL, Keyword::INPUT]) {
            return Ok(Some(RoutineAttribute::CalledOnNull(
                FunctionCalledOnNull::CalledOnNullInput,
            )));
        }
        if self.parse_keywords(&[
            Keyword::RETURNS,
            Keyword::NULL,
            Keyword::ON,
            Keyword::NULL,
            Keyword::INPUT,
        ]) {
            return Ok(Some(RoutineAttribute::CalledOnNull(
                FunctionCalledOnNull::ReturnsNullOnNullInput,
            )));
        }
        if self.parse_keyword(Keyword::STRICT) {
            return Ok(Some(RoutineAttribute::CalledOnNull(
                FunctionCalledOnNull::Strict,
            )));
        }
        if self.parse_keyword(Keyword::PARALLEL) {
            let parallel = if self.parse_keyword(Keyword::UNSAFE) {
                FunctionParallel::Unsafe
            } else if self.parse_keyword(Keyword::RESTRICTED) {
                FunctionParallel::Restricted
            } else if self.parse_keyword(Keyword::SAFE) {
                FunctionParallel::Safe
            } else {
                return self.expected("one of UNSAFE | RESTRICTED | SAFE", self.peek_token());
            };
            return Ok(Some(RoutineAttribute::Parallel(parallel)));
        }
        if self.parse_keywords(&[Keyword::NOT, Keyword::LEAKPROOF]) {
            return Ok(Some(RoutineAttribute::Leakproof(false)));
        }
        if self.parse_keyword(Keyword::LEAKPROOF) {
            return Ok(Some(RoutineAttribute::Leakproof(true)));
        }
        if self.parse_keyword(Keyword::WINDOW) {
            return Ok(Some(RoutineAttribute::Window));
        }
        if self.parse_keyword(Keyword::COST) {
            return Ok(Some(RoutineAttribute::Cost(self.parse_expr()?)));
        }
        if self.parse_keyword(Keyword::ROWS) {
            return Ok(Some(RoutineAttribute::Rows(self.parse_expr()?)));
        }
        if self.parse_keyword(Keyword::SUPPORT) {
            return Ok(Some(RoutineAttribute::Support(
                self.parse_object_name(false)?,
            )));
        }
        if self.parse_keyword(Keyword::TRANSFORM) {
            return Ok(Some(RoutineAttribute::Transform(
                self.parse_transform_types()?,
            )));
        }
        Ok(None)
    }

    /// Parse `TRANSFORM { FOR TYPE type_name } [, ...]`.
    pub(super) fn parse_transform_types(&self) -> Result<Vec<DataType>, ParserError> {
        self.parse_comma_separated(|parser| {
            parser.expect_keywords(&[Keyword::FOR, Keyword::TYPE])?;
            parser.parse_data_type()
        })
    }

    /// Parse the `BEGIN ATOMIC statement; ... END` body of a SQL-language
    /// routine. Every statement is terminated by a semicolon, and the body may
    /// be empty.
    ///
    /// <https://www.postgresql.org/docs/current/sql-createfunction.html>
    pub(super) fn parse_atomic_block(&self) -> Result<AtomicBlock, ParserError> {
        self.expect_keywords(&[Keyword::BEGIN, Keyword::ATOMIC])?;
        let mut statements = vec![];
        while !self.parse_keyword(Keyword::END) {
            if self.peek_token_ref().token == BorrowedToken::EOF {
                return self.expected("END after BEGIN ATOMIC", self.peek_token());
            }
            statements.push(self.parse_statement()?);
            self.expect_token(&BorrowedToken::SemiColon)?;
        }
        Ok(AtomicBlock { statements })
    }

    /// Parse the `AS` item list of `CREATE FUNCTION`, which names either one
    /// definition or an object file and a link symbol.
    pub(super) fn parse_create_function_as_body(&self) -> Result<CreateFunctionBody, ParserError> {
        let first = self.parse_create_function_body_string()?;
        if self.consume_token(&BorrowedToken::Comma) {
            let link_symbol = self.parse_create_function_body_string()?;
            return Ok(CreateFunctionBody::AsObjectFileLinkSymbol {
                obj_file: first,
                link_symbol,
            });
        }
        Ok(CreateFunctionBody::AsBeforeOptions(first))
    }

    /// Collapse the bodies written for one routine into a single
    /// [`CreateFunctionBody`]. PostgreSQL accepts more than one and rejects the
    /// duplicate when the routine is created.
    pub(super) fn collapse_create_function_bodies(
        bodies: Vec<CreateFunctionBody>,
    ) -> Option<CreateFunctionBody> {
        let mut bodies = bodies;
        match bodies.len() {
            0 => None,
            1 => bodies.pop(),
            _ => Some(CreateFunctionBody::Multiple(bodies)),
        }
    }

    /// Parse the `BEGIN ATOMIC` body a SQL-language procedure may carry in
    /// place of a quoted one.
    pub(super) fn parse_procedure_atomic_body(&self) -> Result<ConditionalStatements, ParserError> {
        Ok(ConditionalStatements::BeginAtomic(
            self.parse_atomic_block()?,
        ))
    }
}

/// The name a `%TYPE` reference is written against, when the type parsed so far
/// is just a name.
fn data_type_as_object_name(data_type: &DataType) -> Option<ObjectName> {
    match data_type {
        DataType::Custom(name, modifiers) if modifiers.is_empty() => Some(name.clone()),
        _ => None,
    }
}

/// The `GET [CURRENT | STACKED] DIAGNOSTICS` items PostgreSQL's PL/pgSQL
/// scanner matches as unreserved words, so that they stay usable as
/// identifiers everywhere else.
///
/// <https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-DIAGNOSTICS>
pub(super) fn plpgsql_diagnostics_item(name: &str) -> Option<DiagnosticsItem> {
    if name.eq_ignore_ascii_case("pg_routine_oid") {
        Some(DiagnosticsItem::PgRoutineOid)
    } else if name.eq_ignore_ascii_case("pg_datatype_name") {
        Some(DiagnosticsItem::PgDatatypeName)
    } else if name.eq_ignore_ascii_case("column_name") {
        Some(DiagnosticsItem::ColumnName)
    } else if name.eq_ignore_ascii_case("constraint_name") {
        Some(DiagnosticsItem::ConstraintName)
    } else if name.eq_ignore_ascii_case("table_name") {
        Some(DiagnosticsItem::TableName)
    } else if name.eq_ignore_ascii_case("schema_name") {
        Some(DiagnosticsItem::SchemaName)
    } else {
        None
    }
}
