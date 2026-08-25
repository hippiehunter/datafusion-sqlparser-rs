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

//! SQL Parser for the PostgreSQL query and DML grammar

#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec, vec::Vec};

use super::{Parser, ParserError};
use crate::arena::AstBox as Box;
use crate::{
    ast::{
        helpers::attached_token::AttachedToken, AccessExpr, BinaryOperator, ColumnTarget,
        ConflictIndexElement, ConflictInference, ConflictTarget, DataType, Expr, Function,
        FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, OrderByOptions,
        OverridingKind, ReturningRowAlias, ReturningRowVersion, TableFactor, TableFunctionArgs,
        TableFunctionColumnDef, TableFunctionItem, TrimWhereField, XmlRootStandalone,
        XmlRootVersion,
    },
    dialect::Precedence,
    keywords::{get_keyword, Keyword, POSTGRES_RESERVED_KEYWORDS},
    tokenizer::BorrowedToken,
};

/// Type names the parser resolves to a [`crate::ast::GeometricTypeKind`]
/// literal of its own, which must not be captured as a generic typed string.
const GEOMETRIC_TYPE_KEYWORDS: &[Keyword] = &[
    Keyword::BOX,
    Keyword::CIRCLE,
    Keyword::LINE,
    Keyword::LSEG,
    Keyword::PATH,
    Keyword::POINT,
    Keyword::POLYGON,
];

/// Keywords that can follow an empty select list, i.e. that prove the query
/// used PostgreSQL's empty `opt_target_list`.
const EMPTY_TARGET_LIST_TERMINATORS: &[Keyword] = &[
    Keyword::EXCEPT,
    Keyword::FETCH,
    Keyword::FOR,
    Keyword::FROM,
    Keyword::GROUP,
    Keyword::HAVING,
    Keyword::INTERSECT,
    Keyword::INTO,
    Keyword::LIMIT,
    Keyword::MINUS,
    Keyword::OFFSET,
    Keyword::ORDER,
    Keyword::UNION,
    Keyword::WHERE,
    Keyword::WINDOW,
];

/// Keywords that end a `GROUP BY` clause, and therefore prove that a `GROUP BY
/// ALL` was the whole clause rather than the set quantifier of a grouping
/// element list.
const GROUP_BY_CLAUSE_TERMINATORS: &[Keyword] = &[
    Keyword::EXCEPT,
    Keyword::FETCH,
    Keyword::FOR,
    Keyword::HAVING,
    Keyword::INTERSECT,
    Keyword::INTO,
    Keyword::LIMIT,
    Keyword::MINUS,
    Keyword::OFFSET,
    Keyword::ORDER,
    Keyword::QUALIFY,
    Keyword::RETURNING,
    Keyword::SETTINGS,
    Keyword::UNION,
    Keyword::WHERE,
    Keyword::WINDOW,
    Keyword::WITH,
];

impl Parser<'_> {
    /// Parse the `( <grouping element>, ... )` body of a `GROUPING SETS`
    /// clause, positioned on the opening parenthesis.
    ///
    /// The spelling in which every element is a parenthesized column list keeps
    /// producing [`Expr::GroupingSets`]; everything else PostgreSQL allows
    /// there — bare expressions, `ROLLUP`/`CUBE` and nested `GROUPING SETS` —
    /// produces [`Expr::GroupingSetsElements`].
    pub(super) fn parse_grouping_sets_body(&self) -> Result<Expr, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let elements = self.parse_comma_separated(Parser::parse_group_by_expr)?;
        self.expect_token(&BorrowedToken::RParen)?;
        if elements
            .iter()
            .all(|element| matches!(element, Expr::Tuple(_) | Expr::Nested(_)))
        {
            let sets = elements
                .into_iter()
                .map(|element| match element {
                    Expr::Tuple(exprs) => exprs,
                    Expr::Nested(inner) => vec![Box::into_inner(inner)],
                    other => vec![other],
                })
                .collect();
            return Ok(Expr::GroupingSets(sets));
        }
        Ok(Expr::GroupingSetsElements(elements))
    }

    /// Whether the token after `SELECT` shows that the query has no select
    /// list at all, which PostgreSQL allows: `SELECT UNION SELECT`.
    pub(super) fn peek_empty_target_list(&self) -> bool {
        match &self.peek_token_ref().token {
            BorrowedToken::EOF | BorrowedToken::RParen | BorrowedToken::SemiColon => true,
            BorrowedToken::Word(word) => EMPTY_TARGET_LIST_TERMINATORS.contains(&word.keyword),
            _ => false,
        }
    }

    /// Whether the token that follows a `GROUP BY` set quantifier begins a
    /// grouping element list rather than ending the clause.
    pub(super) fn peek_begins_group_by_list(&self) -> bool {
        match &self.peek_token_ref().token {
            BorrowedToken::EOF
            | BorrowedToken::RParen
            | BorrowedToken::SemiColon
            | BorrowedToken::Comma => false,
            BorrowedToken::Word(word) => !GROUP_BY_CLAUSE_TERMINATORS.contains(&word.keyword),
            _ => true,
        }
    }

    /// Parse the operator named by the `USING` sort specification of a
    /// PostgreSQL `ORDER BY` item, which is PostgreSQL's `qual_all_Op`: any
    /// operator name, or `OPERATOR(schema.name)`.
    pub(super) fn parse_order_by_using_operator(&self) -> Result<BinaryOperator, ParserError> {
        if self.parse_keyword(Keyword::OPERATOR) {
            self.expect_token(&BorrowedToken::LParen)?;
            let mut idents = vec![];
            loop {
                self.advance_token();
                idents.push(self.get_current_token().to_string());
                if !self.consume_token(&BorrowedToken::Period) {
                    break;
                }
            }
            self.expect_token(&BorrowedToken::RParen)?;
            return Ok(BinaryOperator::PGCustomBinaryOperator(idents));
        }
        // PostgreSQL's `all_Op: MathOp | Op`: the arithmetic and comparison
        // operators have their own spellings, every other operator name is
        // whatever sequence of operator characters was written.
        let token = self.next_token();
        let operator = match &token.token {
            BorrowedToken::Plus => BinaryOperator::Plus,
            BorrowedToken::Minus => BinaryOperator::Minus,
            BorrowedToken::Mul => BinaryOperator::Multiply,
            BorrowedToken::Div => BinaryOperator::Divide,
            BorrowedToken::Mod => BinaryOperator::Modulo,
            BorrowedToken::Caret => BinaryOperator::PGExp,
            BorrowedToken::Lt => BinaryOperator::Lt,
            BorrowedToken::Gt => BinaryOperator::Gt,
            BorrowedToken::LtEq => BinaryOperator::LtEq,
            BorrowedToken::GtEq => BinaryOperator::GtEq,
            BorrowedToken::Eq => BinaryOperator::Eq,
            BorrowedToken::Neq => BinaryOperator::NotEq,
            other => {
                let name = other.to_string();
                if name.is_empty()
                    || !name
                        .chars()
                        .all(|ch| self.dialect.is_custom_operator_part(ch))
                {
                    return self.expected("an operator name after USING", token);
                }
                BinaryOperator::Custom(name)
            }
        };
        Ok(operator)
    }

    /// PostgreSQL's `AexprConst: func_name Sconst` production lets any
    /// non-reserved type or function name introduce a literal of that type,
    /// e.g. `xml '<a/>'` or `name 'x'`. Reserved words never reach that
    /// production, which is what keeps `NOT 'a' LIKE 'b'` a negation.
    pub(super) fn custom_type_may_prefix_literal(&self, data_type: &DataType) -> bool {
        if !self.features.supports_generic_typed_string_literals {
            return false;
        }
        let DataType::Custom(name, _) = data_type else {
            return false;
        };
        let Some(ObjectNamePart::Identifier(first)) = name.0.first() else {
            return false;
        };
        if !matches!(
            self.peek_token_ref().token,
            BorrowedToken::SingleQuotedString(_)
                | BorrowedToken::EscapedStringLiteral(_)
                | BorrowedToken::DollarQuotedString(_)
                | BorrowedToken::UnicodeStringLiteral(_)
        ) {
            return false;
        }
        if first.quote_style.is_some() {
            return true;
        }
        match get_keyword(&first.value) {
            Some(keyword) => {
                !POSTGRES_RESERVED_KEYWORDS.contains(&keyword)
                    && !(self.features.supports_geometric_types
                        && GEOMETRIC_TYPE_KEYWORDS.contains(&keyword))
            }
            None => true,
        }
    }

    /// Parse the SQL-standard regular expression form of `SUBSTRING`,
    /// positioned just after the opening parenthesis.
    ///
    /// ```sql
    /// SUBSTRING(<expr> SIMILAR <pattern> ESCAPE <escape>)
    /// ```
    pub(super) fn maybe_parse_substring_similar(&self) -> Result<Option<Expr>, ParserError> {
        self.maybe_parse(|parser| {
            let expr = parser.parse_subexpr(parser.dialect.prec_value(Precedence::Like))?;
            parser.expect_keyword_is(Keyword::SIMILAR)?;
            let pattern = parser.parse_expr()?;
            parser.expect_keyword_is(Keyword::ESCAPE)?;
            let escape = parser.parse_expr()?;
            parser.expect_token(&BorrowedToken::RParen)?;
            Ok(Expr::SubstringSimilar {
                expr: Box::new(expr),
                pattern: Box::new(pattern),
                escape: Box::new(escape),
            })
        })
    }

    /// Parse the `<expr> [, <characters>]` tail of a `TRIM` call, positioned
    /// just after its `FROM`.
    ///
    /// ```sql
    /// TRIM(FROM '  padded  ')
    /// TRIM(BOTH FROM '  padded  ')
    /// TRIM(BOTH 'x' FROM 'xax')
    /// ```
    pub(super) fn parse_trim_from_list(
        &self,
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<Expr>>,
    ) -> Result<Expr, ParserError> {
        let mut exprs = if self.features.supports_trim_character_list {
            self.parse_comma_separated(Parser::parse_expr)?
        } else {
            vec![self.parse_expr()?]
        };
        self.expect_token(&BorrowedToken::RParen)?;
        let expr = exprs.remove(0);
        Ok(Expr::Trim {
            expr: Box::new(expr),
            trim_where,
            trim_what,
            trim_characters: (!exprs.is_empty()).then_some(exprs),
        })
    }

    /// Parse the argument of PostgreSQL's `COLLATION FOR (<expr>)`.
    pub(super) fn parse_collation_for(&self) -> Result<Expr, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(Expr::CollationFor(Box::new(expr)))
    }

    /// Parse a `ColId opt_indirection` write target, used by `INSERT` column
    /// lists and `UPDATE`/`MERGE` assignments.
    ///
    /// ```sql
    /// INSERT INTO t (f2[1], f3.if1, f3.if2[1]) ...
    /// ```
    pub(super) fn parse_column_target(&self) -> Result<ColumnTarget, ParserError> {
        let column = ObjectName::from(vec![self.parse_identifier()?]);
        let indirection = self.parse_column_indirection()?;
        Ok(ColumnTarget {
            column,
            indirection,
        })
    }

    /// Parse the `opt_indirection` that may follow a write target: any mix of
    /// `.field` selections and `[...]` subscripts.
    pub(super) fn parse_column_indirection(&self) -> Result<Vec<AccessExpr>, ParserError> {
        let mut indirection = Vec::new();
        loop {
            if self.consume_token(&BorrowedToken::Period) {
                indirection.push(AccessExpr::Dot(Expr::Identifier(self.parse_identifier()?)));
            } else if self.peek_token_ref().token == BorrowedToken::LBracket {
                self.parse_multi_dim_subscript(&mut indirection)?;
            } else {
                return Ok(indirection);
            }
        }
    }

    /// Parse the parenthesized `INSERT` column target list, returning the base
    /// column names alongside the full targets when any of them writes into a
    /// field or subscript of its column.
    pub(super) fn parse_insert_column_list(
        &self,
        optional: super::IsOptional,
        allow_empty: bool,
    ) -> Result<(Vec<Ident>, Option<Vec<ColumnTarget>>), ParserError> {
        let targets =
            self.parse_parenthesized_column_list_inner(optional, allow_empty, |parser| {
                parser.parse_column_target()
            })?;
        let columns = targets
            .iter()
            .filter_map(|target| {
                target
                    .column
                    .0
                    .first()
                    .and_then(|part| part.as_ident())
                    .cloned()
            })
            .collect();
        let has_indirection = targets.iter().any(|target| !target.indirection.is_empty());
        Ok((columns, has_indirection.then_some(targets)))
    }

    /// Parse an `OVERRIDING { SYSTEM | USER } VALUE` clause.
    pub(super) fn parse_overriding_kind(&self) -> Result<Option<OverridingKind>, ParserError> {
        if !self.parse_keyword(Keyword::OVERRIDING) {
            return Ok(None);
        }
        if self.parse_keywords(&[Keyword::SYSTEM, Keyword::VALUE]) {
            Ok(Some(OverridingKind::SystemValue))
        } else if self.parse_keywords(&[Keyword::USER, Keyword::VALUE]) {
            Ok(Some(OverridingKind::UserValue))
        } else {
            self.expected(
                "SYSTEM VALUE or USER VALUE after OVERRIDING",
                self.peek_token(),
            )
        }
    }

    /// Parse the conflict target of an `ON CONFLICT` clause, positioned on the
    /// opening parenthesis. A plain column list without an index predicate
    /// keeps the [`ConflictTarget::Columns`] shape.
    pub(super) fn parse_conflict_target(&self) -> Result<ConflictTarget, ParserError> {
        let inference = self.parse_conflict_inference()?;
        if inference.predicate.is_none() {
            let columns: Option<Vec<Ident>> = inference
                .elements
                .iter()
                .map(|element| match element {
                    ConflictIndexElement {
                        expr: Expr::Identifier(ident),
                        collation: None,
                        opclass: None,
                        options:
                            OrderByOptions {
                                asc: None,
                                nulls_first: None,
                            },
                    } => Some(ident.clone()),
                    _ => None,
                })
                .collect();
            if let Some(columns) = columns {
                return Ok(ConflictTarget::Columns(columns));
            }
        }
        Ok(ConflictTarget::Inference(inference))
    }

    /// Parse a PostgreSQL `ON CONFLICT` inference clause, positioned on the
    /// opening parenthesis of its index element list.
    ///
    /// ```sql
    /// ON CONFLICT (lower(fruit) COLLATE "C" text_pattern_ops) WHERE fruit IS NOT NULL
    /// ```
    pub(super) fn parse_conflict_inference(&self) -> Result<ConflictInference, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let elements = self.parse_comma_separated(Parser::parse_conflict_index_element)?;
        self.expect_token(&BorrowedToken::RParen)?;
        let predicate = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(ConflictInference {
            elements,
            predicate,
        })
    }

    fn parse_conflict_index_element(&self) -> Result<ConflictIndexElement, ParserError> {
        let expr = self.parse_expr()?;
        let collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_object_name(false)?)
        } else {
            None
        };
        let opclass = match self.peek_token_ref().token {
            BorrowedToken::Word(ref word)
                if !matches!(
                    word.keyword,
                    Keyword::ASC | Keyword::DESC | Keyword::NULLS | Keyword::WHERE
                ) =>
            {
                Some(self.parse_object_name(false)?)
            }
            _ => None,
        };
        let options = self.parse_order_by_options()?;
        Ok(ConflictIndexElement {
            expr,
            collation,
            opclass,
            options,
        })
    }

    /// Parse the `WITH ( OLD AS o, NEW AS n )` prefix of a PostgreSQL 18
    /// `RETURNING` list, positioned on the `WITH` keyword.
    pub(super) fn parse_returning_row_aliases(
        &self,
    ) -> Result<Option<Vec<ReturningRowAlias>>, ParserError> {
        let checkpoint = self.index.get();
        if !self.parse_keyword(Keyword::WITH) {
            return Ok(None);
        }
        if !self.consume_token(&BorrowedToken::LParen) {
            self.index.set(checkpoint);
            return Ok(None);
        }
        let aliases = self.parse_comma_separated(|parser| {
            let version = match parser.expect_one_of_keywords(&[Keyword::OLD, Keyword::NEW])? {
                Keyword::OLD => ReturningRowVersion::Old,
                _ => ReturningRowVersion::New,
            };
            parser.expect_keyword_is(Keyword::AS)?;
            Ok(ReturningRowAlias {
                version,
                alias: parser.parse_identifier()?,
            })
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(Some(aliases))
    }

    /// Parse a `TableFuncElementList`: the column definition list a
    /// record-returning function may be given in the `FROM` clause.
    ///
    /// ```sql
    /// SELECT * FROM dynamic_record(5) AS (a int, b numeric, c text)
    /// ```
    pub(super) fn parse_table_function_column_defs(
        &self,
    ) -> Result<Vec<TableFunctionColumnDef>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let defs = self.parse_comma_separated(|parser| {
            let name = parser.parse_identifier()?;
            let data_type = parser.parse_data_type()?;
            let collation = if parser.parse_keyword(Keyword::COLLATE) {
                Some(parser.parse_object_name(false)?)
            } else {
                None
            };
            Ok(TableFunctionColumnDef {
                name,
                data_type,
                collation,
            })
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(defs)
    }

    /// Parse an `XMLROOT` call, positioned just after the `XMLROOT` keyword.
    ///
    /// ```sql
    /// XMLROOT(<xml>, VERSION {<expr> | NO VALUE} [, STANDALONE {YES | NO | NO VALUE}])
    /// ```
    pub(super) fn parse_xmlroot(&self) -> Result<Expr, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let xml = self.parse_expr()?;
        self.expect_token(&BorrowedToken::Comma)?;
        self.expect_keyword_is(Keyword::VERSION)?;
        let version = if self.parse_keywords(&[Keyword::NO, Keyword::VALUE]) {
            XmlRootVersion::NoValue
        } else {
            XmlRootVersion::Version(self.parse_expr()?)
        };
        let standalone = if self.consume_token(&BorrowedToken::Comma) {
            self.expect_keyword_is(Keyword::STANDALONE)?;
            if self.parse_keyword(Keyword::YES) {
                Some(XmlRootStandalone::Yes)
            } else if self.parse_keywords(&[Keyword::NO, Keyword::VALUE]) {
                Some(XmlRootStandalone::NoValue)
            } else {
                self.expect_keyword_is(Keyword::NO)?;
                Some(XmlRootStandalone::No)
            }
        } else {
            None
        };
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(Expr::XmlRoot {
            xml: Box::new(xml),
            version: Box::new(version),
            standalone,
        })
    }

    /// Whether the next tokens open a `ROWS FROM ( ... )` table reference.
    pub(super) fn peek_rows_from(&self) -> bool {
        matches!(
            (
                &self.peek_nth_token_ref(0).token,
                &self.peek_nth_token_ref(1).token,
                &self.peek_nth_token_ref(2).token,
            ),
            (
                BorrowedToken::Word(rows),
                BorrowedToken::Word(from),
                BorrowedToken::LParen,
            ) if rows.keyword == Keyword::ROWS && from.keyword == Keyword::FROM
        )
    }

    /// Parse `ROWS FROM ( ... ) [WITH ORDINALITY] [alias]`, positioned on the
    /// `ROWS` keyword.
    pub(super) fn parse_rows_from_table_factor(
        &self,
        lateral: bool,
    ) -> Result<TableFactor, ParserError> {
        self.expect_keywords(&[Keyword::ROWS, Keyword::FROM])?;
        let functions = self.parse_rows_from_items()?;
        let with_ordinality = self.parse_keywords(&[Keyword::WITH, Keyword::ORDINALITY]);
        let alias = self.maybe_parse_table_alias()?;
        Ok(TableFactor::RowsFrom {
            lateral,
            rows_from: true,
            functions,
            with_ordinality,
            alias,
        })
    }

    /// Parse the `AS ( a int, b text )` column definition list of a table
    /// function's alias clause when it names no relation.
    pub(super) fn maybe_parse_nameless_column_defs(
        &self,
    ) -> Result<Option<Vec<TableFunctionColumnDef>>, ParserError> {
        if !matches!(
            (
                &self.peek_nth_token_ref(0).token,
                &self.peek_nth_token_ref(1).token,
            ),
            (BorrowedToken::Word(as_word), BorrowedToken::LParen)
                if as_word.keyword == Keyword::AS
        ) {
            return Ok(None);
        }
        self.expect_keyword_is(Keyword::AS)?;
        self.parse_table_function_column_defs().map(Some)
    }

    /// Parse the `( func(...) [AS (coldefs)], ... )` body of a `ROWS FROM`
    /// table reference, positioned on the opening parenthesis.
    pub(super) fn parse_rows_from_items(&self) -> Result<Vec<TableFunctionItem>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let items = self.parse_comma_separated(|parser| {
            let function = parser.parse_expr()?;
            let column_defs = if parser.parse_keyword(Keyword::AS) {
                parser.parse_table_function_column_defs()?
            } else {
                vec![]
            };
            Ok(TableFunctionItem {
                function,
                column_defs,
            })
        })?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(items)
    }
}

/// Rebuild the function call expression of a PostgreSQL table function from the
/// name and argument list the `FROM` clause parser already consumed.
pub(super) fn table_function_call(name: ObjectName, args: Option<TableFunctionArgs>) -> Expr {
    Expr::Function(Function {
        name,
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args.map(|args| args.args).unwrap_or_default(),
            clauses: vec![],
            // The `FROM` clause parser consumed the call, so the closing paren
            // is not available to attach here.
            close_paren_token: AttachedToken::empty(),
        }),
        filter: None,
        nth_value_order: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}
