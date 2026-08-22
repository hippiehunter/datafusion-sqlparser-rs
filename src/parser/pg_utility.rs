// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Parser for the PostgreSQL utility and transaction statements:
//! `VACUUM`, `ANALYZE`, `TABLE`, and the two-phase commit commands.

#[cfg(not(feature = "std"))]
use alloc::{format, string::ToString, vec, vec::Vec};

use super::{Parser, ParserError};
use crate::{
    ast::{
        Analyze, AttachedToken, ObjectName, PgRelationExpr, PreparedTransactionAction,
        PreparedTransactionStatement, Statement, Table, VacuumOption, VacuumOptionName,
        VacuumOptionValue, VacuumRelation, VacuumStatement, Value, ValueWithSpan,
    },
    keywords::Keyword,
    tokenizer::BorrowedToken,
};

impl Parser<'_> {
    /// Parse a PostgreSQL `relation_expr`:
    /// `name`, `name *`, `ONLY name` or `ONLY ( name )`.
    pub fn parse_pg_relation_expr(&self) -> Result<PgRelationExpr, ParserError> {
        if self.parse_keyword(Keyword::ONLY) {
            let parenthesized = self.consume_token(&BorrowedToken::LParen);
            let name = self.parse_object_name(false)?;
            if parenthesized {
                self.expect_token(&BorrowedToken::RParen)?;
            }
            return Ok(PgRelationExpr {
                name,
                only: true,
                parenthesized,
                descendants: false,
            });
        }
        let name = self.parse_object_name(false)?;
        let descendants = self.consume_token(&BorrowedToken::Mul);
        Ok(PgRelationExpr {
            name,
            only: false,
            parenthesized: false,
            descendants,
        })
    }

    /// Parse one `table_and_columns` entry of `VACUUM` or `ANALYZE`:
    /// a relation followed by an optional column list.
    fn parse_vacuum_relation(&self) -> Result<VacuumRelation, ParserError> {
        let relation = self.parse_pg_relation_expr()?;
        let columns = if self.consume_token(&BorrowedToken::LParen) {
            let columns = self.parse_comma_separated(Parser::parse_identifier)?;
            self.expect_token(&BorrowedToken::RParen)?;
            columns
        } else {
            vec![]
        };
        Ok(VacuumRelation { relation, columns })
    }

    /// Parse the optional `table_and_columns [, ...]` tail of `VACUUM` or
    /// `ANALYZE`. An empty list means every table the caller is allowed to
    /// process.
    fn parse_vacuum_relation_list(&self) -> Result<Vec<VacuumRelation>, ParserError> {
        if matches!(
            self.peek_token_ref().token,
            BorrowedToken::EOF | BorrowedToken::SemiColon | BorrowedToken::RParen
        ) || self.peek_keyword(Keyword::TO)
        {
            return Ok(vec![]);
        }
        self.parse_comma_separated(Parser::parse_vacuum_relation)
    }

    /// Parse the parenthesized `( option [, ...] )` list of `VACUUM` or
    /// `ANALYZE`.
    fn parse_vacuum_options(&self) -> Result<Vec<VacuumOption>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_vacuum_option)?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(options)
    }

    fn parse_vacuum_option(&self) -> Result<VacuumOption, ParserError> {
        let ident = self.parse_identifier()?;
        let name = match ident.quote_style {
            Some(_) => VacuumOptionName::Other(ident),
            None => match ident.value.to_ascii_uppercase().as_str() {
                "FULL" => VacuumOptionName::Full,
                "FREEZE" => VacuumOptionName::Freeze,
                "VERBOSE" => VacuumOptionName::Verbose,
                "ANALYZE" | "ANALYSE" => VacuumOptionName::Analyze,
                "DISABLE_PAGE_SKIPPING" => VacuumOptionName::DisablePageSkipping,
                "SKIP_LOCKED" => VacuumOptionName::SkipLocked,
                "INDEX_CLEANUP" => VacuumOptionName::IndexCleanup,
                "PROCESS_MAIN" => VacuumOptionName::ProcessMain,
                "PROCESS_TOAST" => VacuumOptionName::ProcessToast,
                "TRUNCATE" => VacuumOptionName::Truncate,
                "PARALLEL" => VacuumOptionName::Parallel,
                "BUFFER_USAGE_LIMIT" => VacuumOptionName::BufferUsageLimit,
                "SKIP_DATABASE_STATS" => VacuumOptionName::SkipDatabaseStats,
                "ONLY_DATABASE_STATS" => VacuumOptionName::OnlyDatabaseStats,
                _ => VacuumOptionName::Other(ident),
            },
        };
        Ok(VacuumOption {
            name,
            value: self.parse_vacuum_option_value()?,
        })
    }

    fn parse_vacuum_option_value(&self) -> Result<Option<VacuumOptionValue>, ParserError> {
        if matches!(
            self.peek_token_ref().token,
            BorrowedToken::Comma | BorrowedToken::RParen
        ) {
            return Ok(None);
        }
        if matches!(
            self.peek_token_ref().token,
            BorrowedToken::Number(_, _) | BorrowedToken::Minus | BorrowedToken::Plus
        ) {
            return Ok(Some(VacuumOptionValue::Number(
                self.parse_signed_number_value()?.value,
            )));
        }
        if matches!(
            self.peek_token_ref().token,
            BorrowedToken::SingleQuotedString(_)
        ) {
            return Ok(Some(VacuumOptionValue::StringLiteral(
                self.parse_value()?.value,
            )));
        }
        let word = self.parse_identifier()?;
        if word.quote_style.is_none() {
            match word.value.to_ascii_uppercase().as_str() {
                "TRUE" => return Ok(Some(VacuumOptionValue::Boolean(true))),
                "FALSE" => return Ok(Some(VacuumOptionValue::Boolean(false))),
                _ => {}
            }
        }
        Ok(Some(VacuumOptionValue::Word(word)))
    }

    /// The value of the named option, read as a boolean; `None` when the
    /// option was not given.
    fn vacuum_option_flag(options: &[VacuumOption], name: &VacuumOptionName) -> bool {
        options
            .iter()
            .filter(|option| &option.name == name)
            .next_back()
            .is_some_and(VacuumOption::is_enabled)
    }

    /// Parse a `VACUUM` statement in either the bare keyword spelling or the
    /// parenthesized option spelling.
    ///
    /// ```sql
    /// VACUUM [ FULL ] [ FREEZE ] [ VERBOSE ] [ ANALYZE ] [ table_and_columns [, ...] ]
    /// VACUUM ( option [, ...] ) [ table_and_columns [, ...] ]
    /// ```
    pub(super) fn parse_vacuum(&self) -> Result<Statement, ParserError> {
        let token = self.attached_token_from_current();
        self.expect_keyword(Keyword::VACUUM)?;

        let mut sort_only = false;
        let mut delete_only = false;
        let mut reindex = false;
        let mut recluster = false;
        let options;
        let (full, freeze, verbose, analyze);
        if matches!(self.peek_token_ref().token, BorrowedToken::LParen) {
            options = self.parse_vacuum_options()?;
            full = Self::vacuum_option_flag(&options, &VacuumOptionName::Full);
            freeze = Self::vacuum_option_flag(&options, &VacuumOptionName::Freeze);
            verbose = Self::vacuum_option_flag(&options, &VacuumOptionName::Verbose);
            analyze = Self::vacuum_option_flag(&options, &VacuumOptionName::Analyze);
        } else {
            options = vec![];
            full = self.parse_keyword(Keyword::FULL);
            sort_only = self.parse_keywords(&[Keyword::SORT, Keyword::ONLY]);
            delete_only = self.parse_keywords(&[Keyword::DELETE, Keyword::ONLY]);
            reindex = self.parse_keyword(Keyword::REINDEX);
            recluster = self.parse_keyword(Keyword::RECLUSTER);
            freeze = self.parse_keyword(Keyword::FREEZE);
            verbose = self.parse_keyword(Keyword::VERBOSE);
            analyze = self.parse_keyword(Keyword::ANALYZE);
        }

        let relations = self.parse_vacuum_relation_list()?;
        let table_name = relations
            .first()
            .map(|relation| relation.relation.name.clone());
        let threshold = if self.parse_keyword(Keyword::TO) {
            let value = self.parse_value()?;
            self.expect_keyword(Keyword::PERCENT)?;
            Some(value.value)
        } else {
            None
        };
        let boost = self.parse_keyword(Keyword::BOOST);

        Ok(Statement::Vacuum(VacuumStatement {
            token,
            full,
            sort_only,
            delete_only,
            reindex,
            recluster,
            analyze,
            table_name,
            threshold,
            boost,
            freeze,
            verbose,
            options,
            relations,
        }))
    }

    /// Parse an `ANALYZE` statement.
    ///
    /// PostgreSQL spells it
    /// `ANALYZE [ ( option [, ...] ) | VERBOSE ] [ table_and_columns [, ...] ]`;
    /// the `ANALYZE TABLE name ...` spelling of Hive and Spark takes the
    /// legacy path.
    pub fn parse_analyze(&self) -> Result<Statement, ParserError> {
        let has_table_keyword = self.parse_keyword(Keyword::TABLE);
        let mut options = vec![];
        let mut verbose = false;
        let mut relations = vec![];
        let table_name = if has_table_keyword {
            self.parse_object_name(false)?
        } else {
            if matches!(self.peek_token_ref().token, BorrowedToken::LParen) {
                options = self.parse_vacuum_options()?;
                verbose = Self::vacuum_option_flag(&options, &VacuumOptionName::Verbose);
            } else {
                verbose = self.parse_keyword(Keyword::VERBOSE);
            }
            relations = self.parse_vacuum_relation_list()?;
            relations
                .first()
                .map(|relation| relation.relation.name.clone())
                .unwrap_or_else(|| ObjectName(vec![]))
        };

        let mut for_columns = false;
        let mut cache_metadata = false;
        let mut noscan = false;
        let mut partitions = None;
        let mut compute_statistics = false;
        let mut columns = vec![];
        loop {
            match self.parse_one_of_keywords(&[
                Keyword::PARTITION,
                Keyword::FOR,
                Keyword::CACHE,
                Keyword::NOSCAN,
                Keyword::COMPUTE,
            ]) {
                Some(Keyword::PARTITION) => {
                    self.expect_token(&BorrowedToken::LParen)?;
                    partitions = Some(self.parse_comma_separated(Parser::parse_expr)?);
                    self.expect_token(&BorrowedToken::RParen)?;
                }
                Some(Keyword::NOSCAN) => noscan = true,
                Some(Keyword::FOR) => {
                    self.expect_keyword_is(Keyword::COLUMNS)?;

                    columns = self
                        .maybe_parse(|parser| {
                            parser.parse_comma_separated(|p| p.parse_identifier())
                        })?
                        .unwrap_or_default();
                    for_columns = true
                }
                Some(Keyword::CACHE) => {
                    self.expect_keyword_is(Keyword::METADATA)?;
                    cache_metadata = true
                }
                Some(Keyword::COMPUTE) => {
                    self.expect_keyword_is(Keyword::STATISTICS)?;
                    compute_statistics = true
                }
                _ => break,
            }
        }

        Ok(Analyze {
            has_table_keyword,
            table_name,
            for_columns,
            columns,
            partitions,
            cache_metadata,
            noscan,
            compute_statistics,
            options,
            relations,
            verbose,
        }
        .into())
    }

    /// Parse `TABLE relation_expr` as a query body, the shorthand for
    /// `SELECT * FROM relation_expr`.
    pub fn parse_as_table(&self) -> Result<Table, ParserError> {
        let relation = self.parse_pg_relation_expr()?;
        let mut parts = relation.name.0.iter().rev().filter_map(|part| {
            part.as_ident()
                .map(|ident| ident.value.clone())
                .or_else(|| Some(part.to_string()))
        });
        let table_name = parts.next();
        let schema_name = parts.next();
        Ok(Table {
            table_name,
            schema_name,
            relation: Some(relation),
        })
    }

    /// Whether the next tokens spell a possibly signed numeric literal.
    pub(super) fn peek_signed_number(&self) -> bool {
        match self.peek_token_ref().token {
            BorrowedToken::Number(_, _) => true,
            BorrowedToken::Minus | BorrowedToken::Plus => {
                matches!(
                    self.peek_nth_token_ref(1).token,
                    BorrowedToken::Number(_, _)
                )
            }
            _ => false,
        }
    }

    /// Parse a possibly signed numeric literal, PostgreSQL's `SignedIconst`.
    pub fn parse_signed_number_value(&self) -> Result<ValueWithSpan, ParserError> {
        let negative = if self.consume_token(&BorrowedToken::Minus) {
            true
        } else {
            let _ = self.consume_token(&BorrowedToken::Plus);
            false
        };
        let value = self.parse_number_value()?;
        if !negative {
            return Ok(value);
        }
        match value.value {
            Value::Number(number, long) => Ok(ValueWithSpan {
                value: Value::Number(Self::parse(format!("-{number}"), value.span.start)?, long),
                span: value.span,
            }),
            _ => {
                self.prev_token();
                self.expected("literal number", self.peek_token())
            }
        }
    }

    /// Parse `ABORT [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]`, PostgreSQL's
    /// spelling of `ROLLBACK`.
    pub(super) fn parse_abort(&self) -> Result<Statement, ParserError> {
        let rollback_token = self.attached_token_from_current();
        Ok(Statement::Rollback {
            rollback_token,
            chain: self.parse_commit_rollback_chain()?,
            savepoint: None,
        })
    }

    /// Parse the tail of a two-phase commit command, the `PREPARE` /
    /// `PREPARED` keyword having been consumed.
    pub(super) fn parse_prepared_transaction(
        &self,
        token: AttachedToken,
        action: PreparedTransactionAction,
    ) -> Result<Statement, ParserError> {
        let gid = self.parse_value()?.value;
        Ok(PreparedTransactionStatement { token, action, gid }.into())
    }
}
