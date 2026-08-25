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

//! Parser for the SQL/JSON grammar: the query functions (`JSON_EXISTS`,
//! `JSON_QUERY`, `JSON_VALUE`), the constructor functions (`JSON`,
//! `JSON_SCALAR`, `JSON_SERIALIZE`, `JSON_OBJECT`, `JSON_ARRAY` and their
//! aggregate forms) and the `JSON_TABLE` table function.
//!
//! Reference: <https://www.postgresql.org/docs/18/functions-json.html>

#[cfg(not(feature = "std"))]
use alloc::{vec, vec::Vec};

use super::{Parser, ParserError};
use crate::{
    ast::{
        helpers::attached_token::AttachedToken, Expr, ExprWithAlias, FunctionArgExpr,
        FunctionArgumentClause, FunctionArgumentList, JsonEncoding, JsonFormatClause,
        JsonFormattedExpr, JsonOnBehavior, JsonQueryWrapper, JsonQuotesBehavior, JsonQuotesClause,
        JsonReturningClause, ObjectName, SqlJsonTable, SqlJsonTableColumn,
        SqlJsonTableExistsColumn, SqlJsonTableNestedColumn, SqlJsonTableRegularColumn, TableFactor,
        Value,
    },
    keywords::Keyword,
    tokenizer::BorrowedToken,
};

impl Parser<'_> {
    /// Parses an optional `FORMAT JSON [ENCODING <name>]` clause.
    ///
    /// `FORMAT` is only taken as the start of this clause when it is directly
    /// followed by `JSON`, mirroring PostgreSQL's `FORMAT_LA` lookahead.
    pub fn maybe_parse_json_format_clause(&self) -> Result<Option<JsonFormatClause>, ParserError> {
        if !self.parse_keywords(&[Keyword::FORMAT, Keyword::JSON]) {
            return Ok(None);
        }
        let encoding = if self.parse_keyword(Keyword::ENCODING) {
            let name = self.parse_identifier()?;
            Some(if name.value.eq_ignore_ascii_case("utf8") {
                JsonEncoding::Utf8
            } else if name.value.eq_ignore_ascii_case("utf16") {
                JsonEncoding::Utf16
            } else if name.value.eq_ignore_ascii_case("utf32") {
                JsonEncoding::Utf32
            } else {
                JsonEncoding::Custom(name)
            })
        } else {
            None
        };
        Ok(Some(JsonFormatClause { encoding }))
    }

    /// Wraps an expression in its `FORMAT JSON` clause when one follows,
    /// producing the SQL/JSON `<JSON value expression>`.
    fn apply_json_format(&self, expr: Expr) -> Result<Expr, ParserError> {
        match self.maybe_parse_json_format_clause()? {
            Some(format) => Ok(Expr::JsonFormatted(JsonFormattedExpr {
                expr: crate::ast::Box::new(expr),
                format,
            })),
            None => Ok(expr),
        }
    }

    /// Parses a SQL/JSON `<JSON value expression>`: an expression with an
    /// optional `FORMAT JSON [ENCODING ...]` clause.
    pub fn parse_json_value_expr(&self) -> Result<Expr, ParserError> {
        let expr = self.parse_expr()?;
        self.apply_json_format(expr)
    }

    /// Parses a function argument, accepting a trailing `FORMAT JSON` clause
    /// on ordinary expressions.
    pub(super) fn parse_json_function_arg_expr(&self) -> Result<FunctionArgExpr, ParserError> {
        let arg: FunctionArgExpr = self.parse_wildcard_expr()?.into();
        match arg {
            FunctionArgExpr::Expr(expr) => Ok(FunctionArgExpr::Expr(self.apply_json_format(expr)?)),
            other => Ok(other),
        }
    }

    /// Whether the call being parsed is `JSON_ARRAY(<query>)`, whose sole
    /// argument is a query written without enclosing parentheses.
    pub(super) fn peek_json_array_query(&self, name: &ObjectName) -> bool {
        if self
            .peek_one_of_keywords(&[Keyword::SELECT, Keyword::WITH, Keyword::VALUES])
            .is_none()
        {
            return false;
        }
        name.0.len() == 1
            && name.0[0]
                .as_ident()
                .is_some_and(|ident| ident.value.eq_ignore_ascii_case("json_array"))
    }

    /// Parses the argument list of `JSON_ARRAY(<query> [FORMAT JSON] [RETURNING ...])`.
    pub(super) fn parse_json_array_query_argument_list(
        &self,
    ) -> Result<FunctionArgumentList, ParserError> {
        let query = self.parse_query()?;
        let mut clauses = vec![];
        self.parse_sql_json_call_clauses(&mut clauses)?;
        let close_paren = self.expect_token(&BorrowedToken::RParen)?;
        Ok(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![crate::ast::FunctionArg::Unnamed(FunctionArgExpr::Query(
                query,
            ))],
            clauses,
            close_paren_token: AttachedToken::from(close_paren),
        })
    }

    /// Parses the SQL/JSON clauses that may trail the arguments of a JSON
    /// function call, in any order, until a token that starts none of them.
    pub(super) fn parse_sql_json_call_clauses(
        &self,
        clauses: &mut Vec<FunctionArgumentClause>,
    ) -> Result<(), ParserError> {
        loop {
            if self.parse_keyword(Keyword::PASSING) {
                clauses.push(FunctionArgumentClause::JsonPassing(
                    self.parse_json_passing_arguments()?,
                ));
            } else if self.parse_keyword(Keyword::RETURNING) {
                let data_type = self.parse_data_type()?;
                let format = self.maybe_parse_json_format_clause()?;
                clauses.push(FunctionArgumentClause::JsonReturningClause(
                    JsonReturningClause { data_type, format },
                ));
            } else if let Some(wrapper) = self.maybe_parse_json_query_wrapper() {
                clauses.push(FunctionArgumentClause::JsonQueryWrapper(wrapper));
            } else if let Some(quotes) = self.maybe_parse_json_quotes_clause() {
                clauses.push(FunctionArgumentClause::JsonQuotes(quotes));
            } else if let Some(unique) = self.parse_json_unique_keys() {
                clauses.push(FunctionArgumentClause::JsonUniqueKeys(unique));
            } else if let Some(null_clause) = self.parse_json_null_clause() {
                clauses.push(FunctionArgumentClause::JsonNullClause(null_clause));
            } else if let Some(on_clause) = self.maybe_parse_json_on_clause()? {
                clauses.push(on_clause);
            } else if let Some(format) = self.maybe_parse_json_format_clause()? {
                clauses.push(FunctionArgumentClause::JsonFormat(format));
            } else {
                return Ok(());
            }
        }
    }

    /// Parses `PASSING <value> AS <varname> [, ...]`.
    fn parse_json_passing_arguments(&self) -> Result<Vec<ExprWithAlias>, ParserError> {
        self.parse_comma_separated(|parser| {
            let expr = parser.parse_json_value_expr()?;
            parser.expect_keyword_is(Keyword::AS)?;
            let alias = parser.parse_identifier()?;
            Ok(ExprWithAlias {
                expr,
                alias: Some(alias),
            })
        })
    }

    /// Parses the array wrapper behavior of `JSON_QUERY` and of a `JSON_TABLE`
    /// column, where `ARRAY` is a noise word.
    pub fn maybe_parse_json_query_wrapper(&self) -> Option<JsonQueryWrapper> {
        if self.parse_keywords(&[
            Keyword::WITH,
            Keyword::UNCONDITIONAL,
            Keyword::ARRAY,
            Keyword::WRAPPER,
        ]) {
            Some(JsonQueryWrapper::WithUnconditionalArray)
        } else if self.parse_keywords(&[
            Keyword::WITH,
            Keyword::CONDITIONAL,
            Keyword::ARRAY,
            Keyword::WRAPPER,
        ]) {
            Some(JsonQueryWrapper::WithConditionalArray)
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::UNCONDITIONAL, Keyword::WRAPPER]) {
            Some(JsonQueryWrapper::WithUnconditional)
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::CONDITIONAL, Keyword::WRAPPER]) {
            Some(JsonQueryWrapper::WithConditional)
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::ARRAY, Keyword::WRAPPER]) {
            Some(JsonQueryWrapper::WithArray)
        } else if self.parse_keywords(&[Keyword::WITHOUT, Keyword::ARRAY, Keyword::WRAPPER]) {
            Some(JsonQueryWrapper::WithoutArray)
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::WRAPPER]) {
            Some(JsonQueryWrapper::With)
        } else if self.parse_keywords(&[Keyword::WITHOUT, Keyword::WRAPPER]) {
            Some(JsonQueryWrapper::Without)
        } else {
            None
        }
    }

    /// Parses `{KEEP | OMIT} QUOTES [ON SCALAR STRING]`.
    pub fn maybe_parse_json_quotes_clause(&self) -> Option<JsonQuotesClause> {
        let behavior = if self.parse_keywords(&[Keyword::KEEP, Keyword::QUOTES]) {
            JsonQuotesBehavior::Keep
        } else if self.parse_keywords(&[Keyword::OMIT, Keyword::QUOTES]) {
            JsonQuotesBehavior::Omit
        } else {
            return None;
        };
        let on_scalar_string =
            self.parse_keywords(&[Keyword::ON, Keyword::SCALAR, Keyword::STRING]);
        Some(JsonQuotesClause {
            behavior,
            on_scalar_string,
        })
    }

    /// Parses a SQL/JSON behavior: what a function yields when its path
    /// matches nothing or raises an error.
    fn maybe_parse_json_behavior(&self) -> Result<Option<JsonOnBehavior>, ParserError> {
        let behavior = if self.parse_keyword(Keyword::NULL) {
            JsonOnBehavior::Null
        } else if self.parse_keyword(Keyword::ERROR) {
            JsonOnBehavior::Error
        } else if self.parse_keyword(Keyword::TRUE) {
            JsonOnBehavior::True
        } else if self.parse_keyword(Keyword::FALSE) {
            JsonOnBehavior::False
        } else if self.parse_keyword(Keyword::UNKNOWN) {
            JsonOnBehavior::Unknown
        } else if self.parse_keyword(Keyword::DEFAULT) {
            JsonOnBehavior::Default(crate::ast::Box::new(self.parse_expr()?))
        } else if self.parse_keyword(Keyword::EMPTY) {
            // A bare `EMPTY` is PostgreSQL's Oracle-compatible spelling of
            // `EMPTY ARRAY`.
            if self.parse_keyword(Keyword::OBJECT) {
                JsonOnBehavior::EmptyObject
            } else {
                let _ = self.parse_keyword(Keyword::ARRAY);
                JsonOnBehavior::EmptyArray
            }
        } else {
            return Ok(None);
        };
        Ok(Some(behavior))
    }

    /// Parses `<behavior> ON {EMPTY | ERROR}`, restoring the parser position
    /// when the behavior is not followed by an `ON` clause.
    fn maybe_parse_json_on_clause(&self) -> Result<Option<FunctionArgumentClause>, ParserError> {
        let start = self.index.get();
        let Some(behavior) = self.maybe_parse_json_behavior()? else {
            return Ok(None);
        };
        if self.parse_keywords(&[Keyword::ON, Keyword::EMPTY]) {
            Ok(Some(FunctionArgumentClause::JsonOnEmpty(behavior)))
        } else if self.parse_keywords(&[Keyword::ON, Keyword::ERROR]) {
            Ok(Some(FunctionArgumentClause::JsonOnError(behavior)))
        } else {
            self.index.set(start);
            Ok(None)
        }
    }

    /// Parses `<behavior> ON ERROR`, restoring the parser position when no
    /// such clause is present.
    fn maybe_parse_json_on_error(&self) -> Result<Option<JsonOnBehavior>, ParserError> {
        let start = self.index.get();
        let Some(behavior) = self.maybe_parse_json_behavior()? else {
            return Ok(None);
        };
        if self.parse_keywords(&[Keyword::ON, Keyword::ERROR]) {
            Ok(Some(behavior))
        } else {
            self.index.set(start);
            Ok(None)
        }
    }

    /// Parses `[<behavior> ON EMPTY] [<behavior> ON ERROR]` for a `JSON_TABLE`
    /// column.
    fn parse_json_behavior_clauses(
        &self,
    ) -> Result<(Option<JsonOnBehavior>, Option<JsonOnBehavior>), ParserError> {
        let mut on_empty = None;
        let mut on_error = None;
        while let Some(clause) = self.maybe_parse_json_on_clause()? {
            match clause {
                FunctionArgumentClause::JsonOnEmpty(behavior) => on_empty = Some(behavior),
                FunctionArgumentClause::JsonOnError(behavior) => on_error = Some(behavior),
                _ => break,
            }
        }
        Ok((on_empty, on_error))
    }

    /// Parses the SQL/JSON `JSON_TABLE` table function, with the opening
    /// parenthesis already consumed.
    pub fn parse_sql_json_table(&self) -> Result<TableFactor, ParserError> {
        let context_item = self.parse_json_value_expr()?;
        self.expect_token(&BorrowedToken::Comma)?;
        let path = self.parse_expr()?;
        let path_name = if self.parse_keyword(Keyword::AS) {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let passing = if self.parse_keyword(Keyword::PASSING) {
            self.parse_json_passing_arguments()?
        } else {
            vec![]
        };
        self.expect_keyword_is(Keyword::COLUMNS)?;
        let columns = self.parse_sql_json_table_columns()?;
        let on_error = self.maybe_parse_json_on_error()?;
        self.expect_token(&BorrowedToken::RParen)?;
        let alias = self.maybe_parse_table_alias()?;
        Ok(TableFactor::SqlJsonTable(SqlJsonTable {
            context_item,
            path,
            path_name,
            passing,
            columns,
            on_error,
            alias,
        }))
    }

    fn parse_sql_json_table_columns(&self) -> Result<Vec<SqlJsonTableColumn>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_sql_json_table_column)?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(columns)
    }

    /// Parses one entry of a `JSON_TABLE` `COLUMNS (...)` list.
    fn parse_sql_json_table_column(&self) -> Result<SqlJsonTableColumn, ParserError> {
        if self.peek_nested_json_table_column() {
            self.expect_keyword_is(Keyword::NESTED)?;
            let _ = self.parse_keyword(Keyword::PATH);
            let path = self.parse_value()?.value;
            let path_name = if self.parse_keyword(Keyword::AS) {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            self.expect_keyword_is(Keyword::COLUMNS)?;
            let columns = self.parse_sql_json_table_columns()?;
            return Ok(SqlJsonTableColumn::Nested(SqlJsonTableNestedColumn {
                path,
                path_name,
                columns,
            }));
        }

        let name = self.parse_identifier()?;
        if self.parse_keyword(Keyword::FOR) {
            self.expect_keyword_is(Keyword::ORDINALITY)?;
            return Ok(SqlJsonTableColumn::ForOrdinality(name));
        }
        let data_type = self.parse_data_type()?;
        if self.parse_keyword(Keyword::EXISTS) {
            let path = self.parse_json_table_column_path()?;
            let on_error = self.maybe_parse_json_on_error()?;
            return Ok(SqlJsonTableColumn::Exists(SqlJsonTableExistsColumn {
                name,
                data_type,
                path,
                on_error,
            }));
        }
        let format = self.maybe_parse_json_format_clause()?;
        let path = self.parse_json_table_column_path()?;
        let wrapper = self.maybe_parse_json_query_wrapper();
        let quotes = self.maybe_parse_json_quotes_clause();
        let (on_empty, on_error) = self.parse_json_behavior_clauses()?;
        Ok(SqlJsonTableColumn::Regular(SqlJsonTableRegularColumn {
            name,
            data_type,
            format,
            path,
            wrapper,
            quotes,
            on_empty,
            on_error,
        }))
    }

    /// A `NESTED` column definition starts with the `NESTED` keyword followed
    /// by `PATH` or by the path literal itself; anywhere else `NESTED` is an
    /// ordinary column name.
    fn peek_nested_json_table_column(&self) -> bool {
        if !self.peek_keyword(Keyword::NESTED) {
            return false;
        }
        match &self.peek_nth_token_ref(1).token {
            BorrowedToken::SingleQuotedString(_) | BorrowedToken::DoubleQuotedString(_) => true,
            BorrowedToken::Word(word) => word.keyword == Keyword::PATH,
            _ => false,
        }
    }

    fn parse_json_table_column_path(&self) -> Result<Option<Value>, ParserError> {
        if self.parse_keyword(Keyword::PATH) {
            Ok(Some(self.parse_value()?.value))
        } else {
            Ok(None)
        }
    }
}
