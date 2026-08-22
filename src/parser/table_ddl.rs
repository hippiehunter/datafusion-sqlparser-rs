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

//! Parser support for PostgreSQL table-shaped DDL: `CREATE TABLE` and its
//! relatives, `ALTER TABLE`, foreign tables and their options, domains, typed
//! tables and schema element lists.

#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};

use super::{Parser, ParserError};
use crate::ast::table_ddl::{
    AlterConstraint, AlterTableAllInTablespace, ColumnCompression, ConstraintAttribute,
    ConstraintInheritability, CreateTableAsExecute, CreateTableWithData, DomainConstraint,
    IdentityColumnOption, IndexConstraintDetails, NotNullConstraint, RelationOption, RowsFromItem,
    SetAccessMethod, SetStatisticsValue, TableLikeElement, TableLikeOptionKind, TypedTableColumn,
    TypedTableElement, ViewCheckOption,
};
use crate::ast::{
    AlterColumnOperation, AlterTableOperation, Box, ColumnDef, ColumnOption, ColumnOptionDef,
    CreateTableLike, CreateTableLikeDefaults, CreateTableLikeOption, Expr, GeneratedAs, Ident,
    ObjectName, ReferentialAction, SequenceOptions, SqlMedOptionAction, SqlOption, Statement,
    TableConstraint, TriggerGroup, UnaryOperator, UserDefinedTypeStorage,
};
use crate::dialect::PostgreSqlDialect;
use crate::keywords::Keyword;
use crate::tokenizer::BorrowedToken;

impl Parser<'_> {
    /// Whether the token `n` positions ahead is the given keyword.
    pub(super) fn peek_nth_keyword(&self, n: usize, expected: Keyword) -> bool {
        matches!(&self.peek_nth_token(n).token, BorrowedToken::Word(w) if expected == w.keyword)
    }

    /// Parses the `(<alias>, ...)` column-alias list of
    /// `CREATE TABLE <name> (<alias>, ...) AS <query>`, whose members carry no
    /// data types. Fails (so the caller can rewind) when the parenthesised list
    /// is a column-definition list instead.
    pub(super) fn parse_create_table_column_aliases(&self) -> Result<Vec<Ident>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let aliases = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_token(&BorrowedToken::RParen)?;
        let tail = [
            Keyword::AS,
            Keyword::ON,
            Keyword::WITH,
            Keyword::TABLESPACE,
            Keyword::USING,
        ];
        if tail.iter().any(|kw| self.peek_keyword(*kw)) {
            Ok(aliases)
        } else {
            self.expected("a column alias list", self.peek_token())
        }
    }

    /// Parses the `ALTER TABLE` actions that are not covered by the shared
    /// action list, returning `None` when the next tokens start some other
    /// action.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-altertable.html)
    pub(super) fn parse_pg_alter_table_action(
        &self,
    ) -> Result<Option<AlterTableOperation>, ParserError> {
        if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            return Ok(Some(AlterTableOperation::SetSchema {
                new_schema: self.parse_object_name(false)?,
            }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::LOGGED]) {
            return Ok(Some(AlterTableOperation::SetLogged));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::UNLOGGED]) {
            return Ok(Some(AlterTableOperation::SetUnlogged));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::WITHOUT, Keyword::CLUSTER]) {
            return Ok(Some(AlterTableOperation::SetWithoutCluster));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::WITHOUT, Keyword::OIDS]) {
            return Ok(Some(AlterTableOperation::SetWithoutOids));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::ACCESS, Keyword::METHOD]) {
            let method = if self.parse_keyword(Keyword::DEFAULT) {
                SetAccessMethod::Default
            } else {
                SetAccessMethod::Name(self.parse_identifier()?)
            };
            return Ok(Some(AlterTableOperation::SetAccessMethod { method }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::TABLESPACE]) {
            return Ok(Some(AlterTableOperation::SetTablespace {
                name: self.parse_identifier()?,
            }));
        }
        if self.parse_keywords(&[Keyword::CLUSTER, Keyword::ON]) {
            return Ok(Some(AlterTableOperation::ClusterOn {
                index_name: self.parse_identifier()?,
            }));
        }
        if self.peek_keyword(Keyword::RESET)
            && self.peek_nth_token(1).token == BorrowedToken::LParen
        {
            self.expect_keyword(Keyword::RESET)?;
            return Ok(Some(AlterTableOperation::ResetOptionsParens {
                options: self.parse_parenthesized_relation_options()?,
            }));
        }
        if self.parse_keyword(Keyword::INHERIT) {
            return Ok(Some(AlterTableOperation::Inherit {
                parent: self.parse_object_name(false)?,
            }));
        }
        if self.parse_keywords(&[Keyword::NO, Keyword::INHERIT]) {
            return Ok(Some(AlterTableOperation::NoInherit {
                parent: self.parse_object_name(false)?,
            }));
        }
        if self.parse_keywords(&[Keyword::NOT, Keyword::OF]) {
            return Ok(Some(AlterTableOperation::NotOf));
        }
        if self.parse_keyword(Keyword::OF) {
            return Ok(Some(AlterTableOperation::OfType {
                type_name: self.parse_object_name(false)?,
            }));
        }
        if self.parse_keywords(&[Keyword::ALTER, Keyword::CONSTRAINT]) {
            let name = self.parse_identifier()?;
            let characteristics = self.parse_constraint_characteristics()?;
            let inheritability = if self.parse_keywords(&[Keyword::NO, Keyword::INHERIT]) {
                Some(ConstraintInheritability::NoInherit)
            } else if self.parse_keyword(Keyword::INHERIT) {
                Some(ConstraintInheritability::Inherit)
            } else {
                None
            };
            return Ok(Some(AlterTableOperation::AlterConstraint(
                AlterConstraint {
                    name,
                    characteristics,
                    inheritability,
                },
            )));
        }
        if self.peek_keyword(Keyword::OPTIONS)
            && self.peek_nth_token(1).token == BorrowedToken::LParen
        {
            self.expect_keyword(Keyword::OPTIONS)?;
            return Ok(Some(AlterTableOperation::Options {
                options: self.parse_sql_med_option_action_list()?,
            }));
        }
        Ok(None)
    }

    /// Parses the tail of
    /// `ALTER TABLE ALL IN TABLESPACE <name> [ OWNED BY <role>, ... ]
    ///  SET TABLESPACE <name> [ NOWAIT ]`.
    pub(super) fn parse_alter_table_all_in_tablespace(&self) -> Result<Statement, ParserError> {
        let tablespace = self.parse_identifier()?;
        let owned_by = if self.parse_keywords(&[Keyword::OWNED, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_owner)?
        } else {
            Vec::new()
        };
        self.expect_keywords(&[Keyword::SET, Keyword::TABLESPACE])?;
        let new_tablespace = self.parse_identifier()?;
        let nowait = self.parse_keyword(Keyword::NOWAIT);
        Ok(Statement::AlterTableAllInTablespace(
            AlterTableAllInTablespace {
                tablespace,
                owned_by,
                new_tablespace,
                nowait,
            },
        ))
    }

    /// Parses `{ ENABLE | DISABLE } TRIGGER { ALL | USER }`, whose target is a
    /// keyword rather than a trigger name.
    pub(super) fn parse_trigger_group(&self) -> Option<TriggerGroup> {
        if self.parse_keyword(Keyword::ALL) {
            Some(TriggerGroup::All)
        } else if self.parse_keyword(Keyword::USER) {
            Some(TriggerGroup::User)
        } else {
            None
        }
    }

    /// Parses the `ALTER TABLE ... ALTER [COLUMN] <name>` actions that are not
    /// covered by the shared action list.
    pub(super) fn parse_pg_alter_column_action(
        &self,
    ) -> Result<Option<AlterColumnOperation>, ParserError> {
        if self.parse_keywords(&[Keyword::SET, Keyword::STATISTICS]) {
            let value = if self.parse_keyword(Keyword::DEFAULT) {
                SetStatisticsValue::Default
            } else {
                SetStatisticsValue::Value(self.parse_statistics_target()?)
            };
            return Ok(Some(AlterColumnOperation::SetStatistics { value }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::COMPRESSION]) {
            return Ok(Some(AlterColumnOperation::SetCompression {
                compression: self.parse_column_compression()?,
            }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::EXPRESSION, Keyword::AS]) {
            self.expect_token(&BorrowedToken::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&BorrowedToken::RParen)?;
            return Ok(Some(AlterColumnOperation::SetExpression { expr }));
        }
        if self.parse_keywords(&[Keyword::DROP, Keyword::EXPRESSION]) {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            return Ok(Some(AlterColumnOperation::DropExpression { if_exists }));
        }
        if self.parse_keywords(&[Keyword::DROP, Keyword::IDENTITY]) {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            return Ok(Some(AlterColumnOperation::DropIdentity { if_exists }));
        }
        if self.peek_keyword(Keyword::SET) && self.peek_nth_token(1).token == BorrowedToken::LParen
        {
            self.expect_keyword(Keyword::SET)?;
            return Ok(Some(AlterColumnOperation::SetOptionsParens {
                options: self.parse_parenthesized_relation_options()?,
            }));
        }
        if self.peek_keyword(Keyword::RESET)
            && self.peek_nth_token(1).token == BorrowedToken::LParen
        {
            self.expect_keyword(Keyword::RESET)?;
            return Ok(Some(AlterColumnOperation::ResetOptionsParens {
                options: self.parse_parenthesized_relation_options()?,
            }));
        }
        if self.peek_keyword(Keyword::OPTIONS)
            && self.peek_nth_token(1).token == BorrowedToken::LParen
        {
            self.expect_keyword(Keyword::OPTIONS)?;
            return Ok(Some(AlterColumnOperation::Options {
                options: self.parse_sql_med_option_action_list()?,
            }));
        }

        let mut options = Vec::new();
        loop {
            if self.parse_keyword(Keyword::RESTART) {
                let with = self.parse_keyword(Keyword::WITH);
                let value = self.maybe_parse(|parser| parser.parse_number_expr())?;
                options.push(IdentityColumnOption::Restart { with, value });
            } else if self.parse_keywords(&[Keyword::SET, Keyword::GENERATED]) {
                let generated_as = if self.parse_keywords(&[Keyword::BY, Keyword::DEFAULT]) {
                    GeneratedAs::ByDefault
                } else {
                    self.expect_keyword(Keyword::ALWAYS)?;
                    GeneratedAs::Always
                };
                options.push(IdentityColumnOption::SetGenerated(generated_as));
            } else if self.peek_keyword(Keyword::SET) {
                let Some(option) = self.maybe_parse(|parser| parser.parse_set_sequence_option())?
                else {
                    break;
                };
                options.push(IdentityColumnOption::SetSequenceOption(option));
            } else {
                break;
            }
        }
        if options.is_empty() {
            Ok(None)
        } else {
            Ok(Some(AlterColumnOperation::IdentityOptions { options }))
        }
    }

    fn parse_set_sequence_option(&self) -> Result<SequenceOptions, ParserError> {
        self.expect_keyword(Keyword::SET)?;
        let mut options = self.parse_create_sequence_options()?;
        if options.len() == 1 {
            Ok(options.remove(0))
        } else {
            self.expected("a single sequence option after SET", self.peek_token())
        }
    }

    fn parse_number_expr(&self) -> Result<Expr, ParserError> {
        let negative = self.consume_token(&BorrowedToken::Minus);
        if !negative {
            let _ = self.consume_token(&BorrowedToken::Plus);
        }
        let value = self.parse_number_value()?;
        if negative {
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(value)),
            })
        } else {
            Ok(Expr::Value(value))
        }
    }

    pub(super) fn parse_statistics_target(&self) -> Result<i64, ParserError> {
        let negative = self.consume_token(&BorrowedToken::Minus);
        if !negative {
            let _ = self.consume_token(&BorrowedToken::Plus);
        }
        let value = self.parse_literal_uint()?;
        let value = i64::try_from(value)
            .map_err(|_| ParserError::ParserError("statistics target out of range".to_string()))?;
        Ok(if negative { -value } else { value })
    }

    pub(super) fn parse_column_compression(&self) -> Result<ColumnCompression, ParserError> {
        if self.parse_keyword(Keyword::DEFAULT) {
            Ok(ColumnCompression::Default)
        } else {
            Ok(ColumnCompression::Method(self.parse_identifier()?))
        }
    }

    pub(super) fn parse_column_storage(&self) -> Result<UserDefinedTypeStorage, ParserError> {
        match self.parse_one_of_keywords(&[
            Keyword::PLAIN,
            Keyword::EXTERNAL,
            Keyword::EXTENDED,
            Keyword::MAIN,
            Keyword::DEFAULT,
        ]) {
            Some(Keyword::PLAIN) => Ok(UserDefinedTypeStorage::Plain),
            Some(Keyword::EXTERNAL) => Ok(UserDefinedTypeStorage::External),
            Some(Keyword::EXTENDED) => Ok(UserDefinedTypeStorage::Extended),
            Some(Keyword::MAIN) => Ok(UserDefinedTypeStorage::Main),
            Some(Keyword::DEFAULT) => Ok(UserDefinedTypeStorage::Default),
            _ => self.expected(
                "storage type (PLAIN, EXTERNAL, EXTENDED, MAIN, or DEFAULT)",
                self.peek_token(),
            ),
        }
    }

    /// Parses `( <name>[.<name>] [= <value>], ... )`, PostgreSQL's `reloptions`.
    pub(super) fn parse_parenthesized_relation_options(
        &self,
    ) -> Result<Vec<SqlOption>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_relation_option)?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(options)
    }

    pub(super) fn parse_relation_option(&self) -> Result<SqlOption, ParserError> {
        let name = self.parse_object_name(false)?;
        let value = if self.consume_token(&BorrowedToken::Eq) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        if name.0.len() == 1 {
            if let Some(value) = value {
                return Ok(SqlOption::KeyValue {
                    key: self.object_name_leaf(&name)?,
                    value,
                });
            }
        }
        Ok(SqlOption::Reloption(RelationOption { name, value }))
    }

    fn object_name_leaf(&self, name: &ObjectName) -> Result<Ident, ParserError> {
        match name.0.last().and_then(|part| part.as_ident()) {
            Some(ident) => Ok(ident.clone()),
            None => self.expected("an option name", self.peek_token()),
        }
    }

    /// Parses `( [ ADD | SET | DROP ] <name> ['<value>'], ... )`, PostgreSQL's
    /// `alter_generic_option_list`, where the action keyword may be omitted and
    /// then means `ADD`.
    pub(super) fn parse_sql_med_option_action_list(
        &self,
    ) -> Result<Vec<SqlMedOptionAction>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let options = self.parse_comma_separated(|parser| {
            if parser.parse_keyword(Keyword::SET) {
                let key = parser.parse_identifier()?;
                let value = parser.parse_identifier()?;
                Ok(SqlMedOptionAction::Set { key, value })
            } else if parser.parse_keyword(Keyword::ADD) {
                let key = parser.parse_identifier()?;
                let value = parser.parse_identifier()?;
                Ok(SqlMedOptionAction::Add { key, value })
            } else if parser.parse_keyword(Keyword::DROP) {
                let key = parser.parse_identifier()?;
                Ok(SqlMedOptionAction::Drop { key })
            } else {
                let key = parser.parse_identifier()?;
                let value = parser.parse_identifier()?;
                Ok(SqlMedOptionAction::Implicit { key, value })
            }
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(options)
    }

    /// Parses `OPTIONS ( <name> '<value>', ... )`, PostgreSQL's
    /// `create_generic_options`, used in foreign-table column definitions.
    fn parse_generic_options(&self) -> Result<Vec<SqlMedOptionAction>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let options = self.parse_comma_separated(|parser| {
            let key = parser.parse_identifier()?;
            let value = parser.parse_identifier()?;
            Ok(SqlMedOptionAction::Implicit { key, value })
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(options)
    }

    /// Parses the PostgreSQL-only column qualifiers: the storage and
    /// compression clauses, foreign-table options, and the constraint
    /// attributes PostgreSQL accepts as standalone qualifiers.
    pub(super) fn parse_pg_column_option(&self) -> Result<Option<ColumnOption>, ParserError> {
        if !self.dialect.is::<PostgreSqlDialect>() {
            return Ok(None);
        }
        if self.parse_keyword(Keyword::STORAGE) {
            return Ok(Some(ColumnOption::Storage(self.parse_column_storage()?)));
        }
        if self.parse_keyword(Keyword::COMPRESSION) {
            return Ok(Some(ColumnOption::Compression(
                self.parse_column_compression()?,
            )));
        }
        if self.peek_keyword(Keyword::OPTIONS)
            && self.peek_nth_token(1).token == BorrowedToken::LParen
        {
            self.expect_keyword(Keyword::OPTIONS)?;
            return Ok(Some(ColumnOption::GenericOptions(
                self.parse_generic_options()?,
            )));
        }
        if self.parse_keywords(&[Keyword::NO, Keyword::INHERIT]) {
            return Ok(Some(ColumnOption::NoInherit));
        }
        let attribute = if self.parse_keywords(&[Keyword::NOT, Keyword::DEFERRABLE]) {
            ConstraintAttribute::NotDeferrable
        } else if self.parse_keyword(Keyword::DEFERRABLE) {
            ConstraintAttribute::Deferrable
        } else if self.parse_keywords(&[Keyword::INITIALLY, Keyword::DEFERRED]) {
            ConstraintAttribute::InitiallyDeferred
        } else if self.parse_keywords(&[Keyword::INITIALLY, Keyword::IMMEDIATE]) {
            ConstraintAttribute::InitiallyImmediate
        } else if self.parse_keywords(&[Keyword::NOT, Keyword::ENFORCED]) {
            ConstraintAttribute::NotEnforced
        } else if self.parse_keyword(Keyword::ENFORCED) {
            ConstraintAttribute::Enforced
        } else {
            return Ok(None);
        };
        Ok(Some(ColumnOption::ConstraintAttribute(attribute)))
    }

    /// Parses the `INCLUDE (...)`, `WITH (...)` and `USING INDEX TABLESPACE ...`
    /// tail shared by index-backed table constraints.
    pub(super) fn parse_index_constraint_details(
        &self,
    ) -> Result<IndexConstraintDetails, ParserError> {
        let mut details = IndexConstraintDetails::default();
        if self.parse_keyword(Keyword::INCLUDE) {
            self.expect_token(&BorrowedToken::LParen)?;
            details.include = self.parse_comma_separated(Parser::parse_identifier)?;
            self.expect_token(&BorrowedToken::RParen)?;
        }
        if self.peek_keyword(Keyword::WITH) && self.peek_nth_token(1).token == BorrowedToken::LParen
        {
            self.expect_keyword(Keyword::WITH)?;
            self.expect_token(&BorrowedToken::LParen)?;
            details.with_options = self.parse_comma_separated(|parser| {
                let name = parser.parse_object_name(false)?;
                let value = if parser.consume_token(&BorrowedToken::Eq) {
                    Some(parser.parse_expr()?)
                } else {
                    None
                };
                Ok(RelationOption { name, value })
            })?;
            self.expect_token(&BorrowedToken::RParen)?;
        }
        if self.parse_keywords(&[Keyword::USING, Keyword::INDEX, Keyword::TABLESPACE]) {
            details.index_tablespace = Some(self.parse_identifier()?);
        }
        Ok(details)
    }

    /// Parses `USING INDEX <name>`, the form that adopts an existing index as a
    /// `UNIQUE` or `PRIMARY KEY` constraint's index.
    pub(super) fn parse_constraint_existing_index(&self) -> Result<Option<Ident>, ParserError> {
        if self.peek_keyword(Keyword::USING)
            && self.peek_nth_keyword(1, Keyword::INDEX)
            && !self.peek_nth_keyword(2, Keyword::TABLESPACE)
        {
            self.expect_keywords(&[Keyword::USING, Keyword::INDEX])?;
            Ok(Some(self.parse_identifier()?))
        } else {
            Ok(None)
        }
    }

    /// Parses a table-level `NOT NULL <column> [ NO INHERIT ]` constraint.
    pub(super) fn parse_not_null_table_constraint(
        &self,
        name: Option<Ident>,
    ) -> Result<TableConstraint, ParserError> {
        let column = self.parse_identifier()?;
        let no_inherit = self.parse_keywords(&[Keyword::NO, Keyword::INHERIT]);
        Ok(NotNullConstraint {
            name,
            column,
            no_inherit,
        }
        .into())
    }

    /// Parses the full PostgreSQL `TableLikeOptionList`.
    pub(super) fn parse_pg_table_like_options(&self) -> Vec<CreateTableLikeOption> {
        let mut options = Vec::new();
        loop {
            let including = if self.parse_keyword(Keyword::INCLUDING) {
                true
            } else if self.parse_keyword(Keyword::EXCLUDING) {
                false
            } else {
                break;
            };
            let Some(kind) = self.parse_table_like_option_kind() else {
                self.prev_token();
                break;
            };
            options.push(match (including, kind) {
                (true, TableLikeOptionKind::Defaults) => CreateTableLikeOption::IncludingDefaults,
                (false, TableLikeOptionKind::Defaults) => CreateTableLikeOption::ExcludingDefaults,
                (true, TableLikeOptionKind::Constraints) => {
                    CreateTableLikeOption::IncludingConstraints
                }
                (false, TableLikeOptionKind::Constraints) => {
                    CreateTableLikeOption::ExcludingConstraints
                }
                (true, kind) => CreateTableLikeOption::Including(kind),
                (false, kind) => CreateTableLikeOption::Excluding(kind),
            });
        }
        options
    }

    fn parse_table_like_option_kind(&self) -> Option<TableLikeOptionKind> {
        match self.parse_one_of_keywords(&[
            Keyword::COMMENTS,
            Keyword::COMPRESSION,
            Keyword::CONSTRAINTS,
            Keyword::DEFAULTS,
            Keyword::GENERATED,
            Keyword::IDENTITY,
            Keyword::INDEXES,
            Keyword::STATISTICS,
            Keyword::STORAGE,
            Keyword::ALL,
        ]) {
            Some(Keyword::COMMENTS) => Some(TableLikeOptionKind::Comments),
            Some(Keyword::COMPRESSION) => Some(TableLikeOptionKind::Compression),
            Some(Keyword::CONSTRAINTS) => Some(TableLikeOptionKind::Constraints),
            Some(Keyword::DEFAULTS) => Some(TableLikeOptionKind::Defaults),
            Some(Keyword::GENERATED) => Some(TableLikeOptionKind::Generated),
            Some(Keyword::IDENTITY) => Some(TableLikeOptionKind::Identity),
            Some(Keyword::INDEXES) => Some(TableLikeOptionKind::Indexes),
            Some(Keyword::STATISTICS) => Some(TableLikeOptionKind::Statistics),
            Some(Keyword::STORAGE) => Some(TableLikeOptionKind::Storage),
            Some(Keyword::ALL) => Some(TableLikeOptionKind::All),
            _ => None,
        }
    }

    /// Parses a `CREATE TABLE` element list that may contain `LIKE` clauses
    /// among the column definitions.
    #[allow(clippy::type_complexity)]
    pub(super) fn parse_pg_table_elements(
        &self,
    ) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>, Vec<TableLikeElement>), ParserError> {
        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        let mut likes = Vec::new();
        if !self.consume_token(&BorrowedToken::LParen) || self.consume_token(&BorrowedToken::RParen)
        {
            return Ok((columns, constraints, likes));
        }

        loop {
            if self.parse_keyword(Keyword::LIKE) {
                let name = self.parse_object_name(false)?;
                let options = self.parse_pg_table_like_options();
                let defaults = options.iter().find_map(|option| match option {
                    CreateTableLikeOption::IncludingDefaults => {
                        Some(CreateTableLikeDefaults::Including)
                    }
                    CreateTableLikeOption::ExcludingDefaults => {
                        Some(CreateTableLikeDefaults::Excluding)
                    }
                    _ => None,
                });
                likes.push(TableLikeElement {
                    after_columns: columns.len() as u32,
                    source: CreateTableLike {
                        name,
                        defaults,
                        options,
                    },
                });
            } else if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let BorrowedToken::Word(_) = self.peek_token().token {
                columns.push(self.parse_column_def()?);
            } else {
                return self.expected("column name or constraint definition", self.peek_token());
            }

            let comma = self.consume_token(&BorrowedToken::Comma);
            let rparen = self.peek_token().token == BorrowedToken::RParen;

            if !comma && !rparen {
                return self.expected("',' or ')' after column definition", self.peek_token());
            }

            if rparen
                && (!comma
                    || self.features.supports_column_definition_trailing_commas
                    || self.options.trailing_commas)
            {
                let _ = self.consume_token(&BorrowedToken::RParen);
                break;
            }
        }

        Ok((columns, constraints, likes))
    }

    /// Parses the element list of `CREATE TABLE <name> OF <type> ( ... )`.
    pub(super) fn parse_typed_table_elements(&self) -> Result<Vec<TypedTableElement>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let elements = self.parse_comma_separated(|parser| {
            if let Some(constraint) = parser.parse_optional_table_constraint()? {
                return Ok(TypedTableElement::Constraint(constraint));
            }
            let name = parser.parse_identifier()?;
            let with_options = parser.parse_keywords(&[Keyword::WITH, Keyword::OPTIONS]);
            let mut options = Vec::new();
            loop {
                if parser.parse_keyword(Keyword::CONSTRAINT) {
                    let name = Some(parser.parse_identifier()?);
                    match parser.parse_optional_column_option()? {
                        Some(option) => options.push(ColumnOptionDef { name, option }),
                        None => {
                            return parser.expected(
                                "constraint details after CONSTRAINT <name>",
                                parser.peek_token(),
                            )
                        }
                    }
                } else if let Some(option) = parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name: None, option });
                } else {
                    break;
                }
            }
            Ok(TypedTableElement::Column(TypedTableColumn {
                name,
                with_options,
                options,
            }))
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(elements)
    }

    /// Parses `AS EXECUTE <name> [ (<parameter>, ...) ]`.
    pub(super) fn parse_create_table_as_execute(
        &self,
    ) -> Result<CreateTableAsExecute, ParserError> {
        let name = self.parse_identifier()?;
        let parameters = if self.consume_token(&BorrowedToken::LParen) {
            let parameters = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&BorrowedToken::RParen)?;
            parameters
        } else {
            Vec::new()
        };
        Ok(CreateTableAsExecute { name, parameters })
    }

    /// Parses the trailing `WITH [ NO ] DATA` of `CREATE TABLE ... AS`.
    pub(super) fn parse_create_table_with_data(&self) -> Option<CreateTableWithData> {
        if self.parse_keywords(&[Keyword::WITH, Keyword::NO, Keyword::DATA]) {
            Some(CreateTableWithData::WithNoData)
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::DATA]) {
            Some(CreateTableWithData::WithData)
        } else {
            None
        }
    }

    /// Parses the statements that may follow a `CREATE SCHEMA` name and be
    /// created inside the new schema.
    pub(super) fn parse_schema_elements(&self) -> Result<Vec<Statement>, ParserError> {
        let mut elements = Vec::new();
        loop {
            if self.peek_keyword(Keyword::GRANT) {
                self.expect_keyword(Keyword::GRANT)?;
                elements.push(self.parse_grant()?);
                continue;
            }
            if !self.peek_keyword(Keyword::CREATE) {
                break;
            }
            let allowed = [
                Keyword::TABLE,
                Keyword::VIEW,
                Keyword::INDEX,
                Keyword::SEQUENCE,
                Keyword::TRIGGER,
                Keyword::UNIQUE,
                Keyword::TEMP,
                Keyword::TEMPORARY,
                Keyword::UNLOGGED,
                Keyword::LOCAL,
                Keyword::GLOBAL,
                Keyword::OR,
                Keyword::MATERIALIZED,
                Keyword::RECURSIVE,
                Keyword::CONSTRAINT,
            ];
            if !allowed.iter().any(|kw| self.peek_nth_keyword(1, *kw)) {
                break;
            }
            self.expect_keyword(Keyword::CREATE)?;
            elements.push(self.parse_create()?);
        }
        Ok(elements)
    }

    /// Parses the constraint list of `CREATE DOMAIN`, which PostgreSQL spells
    /// with the column-qualifier grammar.
    pub(super) fn parse_domain_constraints(&self) -> Result<Vec<DomainConstraint>, ParserError> {
        let mut constraints = Vec::new();
        loop {
            let name = if self.parse_keyword(Keyword::CONSTRAINT) {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            let Some(option) = self.parse_optional_column_option()? else {
                if name.is_some() {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.peek_token(),
                    );
                }
                break;
            };
            let no_inherit = self.parse_keywords(&[Keyword::NO, Keyword::INHERIT]);
            constraints.push(DomainConstraint {
                name,
                option,
                no_inherit,
            });
        }
        Ok(constraints)
    }

    /// Parses `WITH [ CASCADED | LOCAL ] CHECK OPTION` after a view's query.
    pub(super) fn parse_view_check_option(&self) -> Option<ViewCheckOption> {
        if self.parse_keywords(&[
            Keyword::WITH,
            Keyword::CASCADED,
            Keyword::CHECK,
            Keyword::OPTION,
        ]) {
            Some(ViewCheckOption::Cascaded)
        } else if self.parse_keywords(&[
            Keyword::WITH,
            Keyword::LOCAL,
            Keyword::CHECK,
            Keyword::OPTION,
        ]) {
            Some(ViewCheckOption::Local)
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::CHECK, Keyword::OPTION]) {
            Some(ViewCheckOption::Unqualified)
        } else {
            None
        }
    }

    /// Parses a PostgreSQL trigger function argument list, whose members are
    /// literal constants rather than typed parameters.
    pub(super) fn parse_trigger_func_args(&self) -> Result<Option<Vec<Expr>>, ParserError> {
        if !self.consume_token(&BorrowedToken::LParen) {
            return Ok(None);
        }
        if self.consume_token(&BorrowedToken::RParen) {
            return Ok(Some(Vec::new()));
        }
        let args = self.parse_comma_separated(|parser| {
            let token = parser.peek_token();
            match token.token {
                BorrowedToken::Word(_) => Ok(Expr::Identifier(parser.parse_identifier()?)),
                BorrowedToken::Minus | BorrowedToken::Plus | BorrowedToken::Number(_, _) => {
                    parser.parse_number_expr()
                }
                _ => Ok(Expr::Value(parser.parse_value()?)),
            }
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(Some(args))
    }

    /// Parses `ROWS FROM ( <function> [ AS (<coldef>, ...) ], ... )`.
    pub(super) fn parse_rows_from_items(&self) -> Result<Vec<RowsFromItem>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let items = self.parse_comma_separated(|parser| {
            let function = parser.parse_expr()?;
            let column_defs = if parser.parse_keyword(Keyword::AS) {
                parser.expect_token(&BorrowedToken::LParen)?;
                let defs = parser.parse_comma_separated(Parser::parse_column_def)?;
                parser.expect_token(&BorrowedToken::RParen)?;
                defs
            } else {
                Vec::new()
            };
            Ok(RowsFromItem {
                function,
                column_defs,
            })
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(items)
    }

    /// The `ON DELETE SET NULL (<column>, ...)` column list, which PostgreSQL
    /// allows only after `SET NULL` and `SET DEFAULT`.
    pub(super) fn parse_referential_action_columns(
        &self,
        action: &ReferentialAction,
    ) -> Result<Vec<Ident>, ParserError> {
        if !matches!(
            action,
            ReferentialAction::SetNull | ReferentialAction::SetDefault
        ) {
            return Ok(Vec::new());
        }
        if self.peek_token().token != BorrowedToken::LParen {
            return Ok(Vec::new());
        }
        self.expect_token(&BorrowedToken::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(columns)
    }
}
