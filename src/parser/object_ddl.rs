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

//! SQL parser for PostgreSQL object DDL: `COMMENT ON`, the `CREATE` forms of
//! miscellaneous database objects, the typed `DROP` forms, and ownership
//! transfer between roles.

#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use super::{Parser, ParserError};
use crate::ast::{
    AggregateArgs, AggregateSignature, CastSignature, CollationDefinition, CommentObject,
    CommentObjectDetail, CreateCollation, CreateConversion, CreateEventTrigger, CreateLanguage,
    DropAggregate, DropCast, DropOperator, DropOperatorClass, DropOwned, DropRoutine,
    DropTransform, EventTriggerCondition, Expr, Ident, ObjectName, ObjectNamePart,
    OperatorOperandTypes, OperatorSignature, ReassignOwned, RoleGrantOption, RoleGrantOptionValue,
    SqlOption, Statement, TriggerExecBody, TriggerExecBodyType,
};
use crate::keywords::Keyword;
use crate::tokenizer::{BorrowedToken, Token, TokenWithSpan};

/// The characters PostgreSQL allows in an operator name.
///
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-OPERATORS)
const OPERATOR_CHARACTERS: &[char] = &[
    '+', '-', '*', '/', '<', '>', '=', '~', '!', '@', '#', '%', '^', '&', '|', '`', '?',
];

impl Parser<'_> {
    /// Parse the target of a `COMMENT ON` statement: its object type, the name
    /// of the object (empty when the form names none) and whatever else
    /// identifies it.
    pub(super) fn parse_comment_target(
        &self,
    ) -> Result<(CommentObject, ObjectName, Option<CommentObjectDetail>), ParserError> {
        let no_name = ObjectName(vec![]);
        if self.parse_keywords(&[Keyword::ACCESS, Keyword::METHOD]) {
            return Ok((
                CommentObject::AccessMethod,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keyword(Keyword::AGGREGATE) {
            let signature = self.parse_aggregate_signature()?;
            return Ok((
                CommentObject::Aggregate,
                signature.name,
                Some(CommentObjectDetail::AggregateArguments(signature.args)),
            ));
        }
        if self.parse_keyword(Keyword::CAST) {
            let signature = self.parse_cast_signature()?;
            return Ok((
                CommentObject::Cast,
                no_name,
                Some(CommentObjectDetail::Cast(signature)),
            ));
        }
        if self.parse_keyword(Keyword::COLLATION) {
            return Ok((
                CommentObject::Collation,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keyword(Keyword::COLUMN) {
            return Ok((CommentObject::Column, self.parse_object_name(false)?, None));
        }
        if self.parse_keyword(Keyword::CONSTRAINT) {
            let name = self.parse_object_name(false)?;
            self.expect_keyword_is(Keyword::ON)?;
            let detail = if self.parse_keyword(Keyword::DOMAIN) {
                CommentObjectDetail::OnDomain(self.parse_object_name(false)?)
            } else {
                CommentObjectDetail::On(self.parse_object_name(false)?)
            };
            return Ok((CommentObject::Constraint, name, Some(detail)));
        }
        if self.parse_keyword(Keyword::CONVERSION) {
            return Ok((
                CommentObject::Conversion,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keyword(Keyword::DATABASE) {
            return Ok((
                CommentObject::Database,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keyword(Keyword::DOMAIN) {
            return Ok((CommentObject::Domain, self.parse_object_name(false)?, None));
        }
        if self.parse_keywords(&[Keyword::EVENT, Keyword::TRIGGER]) {
            return Ok((
                CommentObject::EventTrigger,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keyword(Keyword::EXTENSION) {
            return Ok((
                CommentObject::Extension,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keywords(&[Keyword::FOREIGN, Keyword::DATA, Keyword::WRAPPER]) {
            return Ok((
                CommentObject::ForeignDataWrapper,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keywords(&[Keyword::FOREIGN, Keyword::TABLE]) {
            return Ok((
                CommentObject::ForeignTable,
                self.parse_object_name(false)?,
                None,
            ));
        }
        for (keyword, object) in [
            (Keyword::FUNCTION, CommentObject::Function),
            (Keyword::PROCEDURE, CommentObject::Procedure),
            (Keyword::ROUTINE, CommentObject::Routine),
        ] {
            if self.parse_keyword(keyword) {
                let desc = self.parse_function_desc()?;
                return Ok((
                    object,
                    desc.name,
                    desc.args.map(CommentObjectDetail::Arguments),
                ));
            }
        }
        if self.parse_keyword(Keyword::INDEX) {
            return Ok((CommentObject::Index, self.parse_object_name(false)?, None));
        }
        if self.parse_keywords(&[Keyword::LARGE, Keyword::OBJECT]) {
            let oid = self.parse_number_value()?;
            return Ok((
                CommentObject::LargeObject,
                no_name,
                Some(CommentObjectDetail::LargeObject(oid)),
            ));
        }
        if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            return Ok((
                CommentObject::MaterializedView,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keyword(Keyword::OPERATOR) {
            if self.parse_keyword(Keyword::CLASS) {
                let name = self.parse_object_name(false)?;
                self.expect_keyword_is(Keyword::USING)?;
                let method = self.parse_identifier()?;
                return Ok((
                    CommentObject::OperatorClass,
                    name,
                    Some(CommentObjectDetail::Using(method)),
                ));
            }
            if self.parse_keyword(Keyword::FAMILY) {
                let name = self.parse_object_name(false)?;
                self.expect_keyword_is(Keyword::USING)?;
                let method = self.parse_identifier()?;
                return Ok((
                    CommentObject::OperatorFamily,
                    name,
                    Some(CommentObjectDetail::Using(method)),
                ));
            }
            let signature = self.parse_operator_signature()?;
            return Ok((
                CommentObject::Operator,
                signature.name,
                Some(CommentObjectDetail::OperatorArguments(signature.args)),
            ));
        }
        for (keyword, object) in [
            (Keyword::POLICY, CommentObject::Policy),
            (Keyword::RULE, CommentObject::Rule),
            (Keyword::TRIGGER, CommentObject::Trigger),
        ] {
            if self.parse_keyword(keyword) {
                let name = self.parse_object_name(false)?;
                self.expect_keyword_is(Keyword::ON)?;
                let table = self.parse_object_name(false)?;
                return Ok((object, name, Some(CommentObjectDetail::On(table))));
            }
        }
        if self.parse_keywords(&[Keyword::PROCEDURAL, Keyword::LANGUAGE])
            || self.parse_keyword(Keyword::LANGUAGE)
        {
            return Ok((
                CommentObject::Language,
                self.parse_object_name(false)?,
                None,
            ));
        }
        if self.parse_keyword(Keyword::TRANSFORM) {
            self.expect_keyword_is(Keyword::FOR)?;
            let data_type = self.parse_data_type()?;
            self.expect_keyword_is(Keyword::LANGUAGE)?;
            let language = self.parse_identifier()?;
            return Ok((
                CommentObject::Transform,
                no_name,
                Some(CommentObjectDetail::Transform {
                    data_type,
                    language,
                }),
            ));
        }
        if self.parse_keywords(&[Keyword::TEXT, Keyword::SEARCH]) {
            let object = match self.expect_one_of_keywords(&[
                Keyword::CONFIGURATION,
                Keyword::DICTIONARY,
                Keyword::PARSER,
                Keyword::TEMPLATE,
            ])? {
                Keyword::CONFIGURATION => CommentObject::TextSearchConfiguration,
                Keyword::DICTIONARY => CommentObject::TextSearchDictionary,
                Keyword::PARSER => CommentObject::TextSearchParser,
                _ => CommentObject::TextSearchTemplate,
            };
            return Ok((object, self.parse_object_name(false)?, None));
        }
        for (keyword, object) in [
            (Keyword::PUBLICATION, CommentObject::Publication),
            (Keyword::ROLE, CommentObject::Role),
            (Keyword::SCHEMA, CommentObject::Schema),
            (Keyword::SEQUENCE, CommentObject::Sequence),
            (Keyword::SERVER, CommentObject::Server),
            (Keyword::STATISTICS, CommentObject::Statistics),
            (Keyword::SUBSCRIPTION, CommentObject::Subscription),
            (Keyword::TABLE, CommentObject::Table),
            (Keyword::TABLESPACE, CommentObject::Tablespace),
            (Keyword::TYPE, CommentObject::Type),
            (Keyword::USER, CommentObject::User),
            (Keyword::VIEW, CommentObject::View),
        ] {
            if self.parse_keyword(keyword) {
                return Ok((object, self.parse_object_name(false)?, None));
            }
        }
        self.expected("comment object_type", self.peek_token())
    }

    /// Parse `CREATE COLLATION`.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createcollation.html)
    pub fn parse_create_collation(&self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;
        let definition = if self.parse_keyword(Keyword::FROM) {
            CollationDefinition::From(self.parse_object_name(false)?)
        } else {
            CollationDefinition::Options(self.parse_definition_list()?)
        };
        Ok(Statement::CreateCollation(CreateCollation {
            if_not_exists,
            name,
            definition,
        }))
    }

    /// Parse `CREATE [DEFAULT] CONVERSION`.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createconversion.html)
    pub fn parse_create_conversion(&self, default: bool) -> Result<Statement, ParserError> {
        let name = self.parse_object_name(false)?;
        self.expect_keyword_is(Keyword::FOR)?;
        let for_encoding = self.parse_value()?;
        self.expect_keyword_is(Keyword::TO)?;
        let to_encoding = self.parse_value()?;
        self.expect_keyword_is(Keyword::FROM)?;
        let function = self.parse_object_name(false)?;
        Ok(Statement::CreateConversion(CreateConversion {
            default,
            name,
            for_encoding,
            to_encoding,
            function,
        }))
    }

    /// Parse `CREATE [OR REPLACE] [TRUSTED] [PROCEDURAL] LANGUAGE`.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createlanguage.html)
    pub fn parse_create_language(&self, or_replace: bool) -> Result<Statement, ParserError> {
        let trusted = self.parse_keyword(Keyword::TRUSTED);
        let procedural = self.parse_keyword(Keyword::PROCEDURAL);
        self.expect_keyword_is(Keyword::LANGUAGE)?;
        let name = self.parse_identifier()?;
        let mut handler = None;
        let mut inline = None;
        let mut validator = None;
        if self.parse_keyword(Keyword::HANDLER) {
            handler = Some(self.parse_object_name(false)?);
            if self.parse_keyword(Keyword::INLINE) {
                inline = Some(self.parse_object_name(false)?);
            }
            if self.parse_keyword(Keyword::VALIDATOR) {
                validator = Some(self.parse_object_name(false)?);
            }
        }
        Ok(Statement::CreateLanguage(CreateLanguage {
            or_replace,
            trusted,
            procedural,
            name,
            handler,
            inline,
            validator,
        }))
    }

    /// Parse `CREATE EVENT TRIGGER`.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createeventtrigger.html)
    pub fn parse_create_event_trigger(&self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::ON)?;
        let event = self.parse_identifier()?;
        let mut conditions = vec![];
        if self.parse_keyword(Keyword::WHEN) {
            loop {
                let variable = self.parse_identifier()?;
                self.expect_keyword_is(Keyword::IN)?;
                self.expect_token(&Token::LParen)?;
                let values = self.parse_comma_separated(Parser::parse_value)?;
                self.expect_token(&Token::RParen)?;
                conditions.push(EventTriggerCondition { variable, values });
                if !self.parse_keyword(Keyword::AND) {
                    break;
                }
            }
        }
        self.expect_keyword_is(Keyword::EXECUTE)?;
        let exec_type =
            match self.expect_one_of_keywords(&[Keyword::FUNCTION, Keyword::PROCEDURE])? {
                Keyword::PROCEDURE => TriggerExecBodyType::Procedure,
                _ => TriggerExecBodyType::Function,
            };
        let func_desc = self.parse_function_desc()?;
        Ok(Statement::CreateEventTrigger(CreateEventTrigger {
            name,
            event,
            conditions,
            exec_body: TriggerExecBody {
                exec_type,
                func_desc,
            },
        }))
    }

    /// Parse `DROP AGGREGATE`.
    pub fn parse_drop_aggregate(&self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let aggregates = self.parse_comma_separated(Parser::parse_aggregate_signature)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropAggregate(DropAggregate {
            if_exists,
            aggregates,
            drop_behavior,
        }))
    }

    /// Parse `DROP OPERATOR`, `DROP OPERATOR CLASS` and `DROP OPERATOR FAMILY`.
    pub fn parse_drop_operator(&self) -> Result<Statement, ParserError> {
        let family = match self.parse_one_of_keywords(&[Keyword::CLASS, Keyword::FAMILY]) {
            Some(Keyword::CLASS) => Some(false),
            Some(_) => Some(true),
            None => None,
        };
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        if let Some(family) = family {
            let name = self.parse_object_name(false)?;
            self.expect_keyword_is(Keyword::USING)?;
            let using = self.parse_identifier()?;
            let drop_behavior = self.parse_optional_drop_behavior();
            return Ok(Statement::DropOperatorClass(DropOperatorClass {
                family,
                if_exists,
                name,
                using,
                drop_behavior,
            }));
        }
        let operators = self.parse_comma_separated(Parser::parse_operator_signature)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropOperator(DropOperator {
            if_exists,
            operators,
            drop_behavior,
        }))
    }

    /// Parse `DROP CAST`.
    pub fn parse_drop_cast(&self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let signature = self.parse_cast_signature()?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropCast(DropCast {
            if_exists,
            signature,
            drop_behavior,
        }))
    }

    /// Parse `DROP ROUTINE`.
    pub fn parse_drop_routine(&self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let routines = self.parse_comma_separated(Parser::parse_function_desc)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropRoutine(DropRoutine {
            if_exists,
            routines,
            drop_behavior,
        }))
    }

    /// Parse `DROP TRANSFORM`.
    pub fn parse_drop_transform(&self) -> Result<Statement, ParserError> {
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        self.expect_keyword_is(Keyword::FOR)?;
        let data_type = self.parse_data_type()?;
        self.expect_keyword_is(Keyword::LANGUAGE)?;
        let language = self.parse_identifier()?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropTransform(DropTransform {
            if_exists,
            data_type,
            language,
            drop_behavior,
        }))
    }

    /// Parse `DROP OWNED BY`, with `BY` already consumed.
    pub fn parse_drop_owned(&self) -> Result<Statement, ParserError> {
        let roles = self.parse_comma_separated(Parser::parse_identifier)?;
        let drop_behavior = self.parse_optional_drop_behavior();
        Ok(Statement::DropOwned(DropOwned {
            roles,
            drop_behavior,
        }))
    }

    /// Parse `REASSIGN OWNED BY role [, ...] TO role`.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-reassign-owned.html)
    pub fn parse_reassign_owned(&self) -> Result<Statement, ParserError> {
        self.expect_keywords(&[Keyword::OWNED, Keyword::BY])?;
        let roles = self.parse_comma_separated(Parser::parse_identifier)?;
        self.expect_keyword_is(Keyword::TO)?;
        let new_role = self.parse_identifier()?;
        Ok(Statement::ReassignOwned(ReassignOwned { roles, new_role }))
    }

    /// Parse one `WITH` option of `GRANT role TO role`, e.g. `ADMIN OPTION`,
    /// `INHERIT TRUE` or `SET FALSE`.
    pub fn parse_role_grant_option(&self) -> Result<RoleGrantOption, ParserError> {
        let name = self.parse_identifier()?;
        let value =
            match self.parse_one_of_keywords(&[Keyword::OPTION, Keyword::TRUE, Keyword::FALSE]) {
                Some(Keyword::OPTION) => RoleGrantOptionValue::Option,
                Some(Keyword::TRUE) => RoleGrantOptionValue::True,
                Some(Keyword::FALSE) => RoleGrantOptionValue::False,
                _ => return self.expected("OPTION, TRUE or FALSE", self.peek_token()),
            };
        Ok(RoleGrantOption { name, value })
    }

    /// Recognise the `<option> OPTION FOR` prefix of `REVOKE`, which revokes a
    /// role option rather than the role membership itself. `GRANT OPTION FOR`
    /// belongs to privilege revocation and is left alone.
    pub(super) fn parse_revoke_option_for(&self) -> Result<Option<Ident>, ParserError> {
        let is_option_for = matches!(
            &self.peek_token_ref().token,
            BorrowedToken::Word(w) if w.keyword != Keyword::GRANT
        ) && matches!(
            &self.peek_nth_token_ref(1).token,
            BorrowedToken::Word(w) if w.keyword == Keyword::OPTION
        ) && matches!(
            &self.peek_nth_token_ref(2).token,
            BorrowedToken::Word(w) if w.keyword == Keyword::FOR
        );
        if !is_option_for {
            return Ok(None);
        }
        let name = self.parse_identifier()?;
        self.expect_keywords(&[Keyword::OPTION, Keyword::FOR])?;
        Ok(Some(name))
    }

    /// Parse an aggregate name followed by its argument list.
    pub(super) fn parse_aggregate_signature(&self) -> Result<AggregateSignature, ParserError> {
        let name = self.parse_object_name(false)?;
        let args = self.parse_aggregate_args()?;
        Ok(AggregateSignature { name, args })
    }

    /// Parse the parenthesized argument list of an aggregate.
    pub(super) fn parse_aggregate_args(&self) -> Result<AggregateArgs, ParserError> {
        self.expect_token(&Token::LParen)?;
        if self.consume_token(&Token::Mul) {
            self.expect_token(&Token::RParen)?;
            return Ok(AggregateArgs::Star);
        }
        if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            let ordered = self.parse_comma_separated(Parser::parse_function_arg)?;
            self.expect_token(&Token::RParen)?;
            return Ok(AggregateArgs::OrderedSet {
                direct: vec![],
                ordered,
            });
        }
        let direct = self.parse_comma_separated0(Parser::parse_function_arg, Token::RParen)?;
        if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            let ordered = self.parse_comma_separated(Parser::parse_function_arg)?;
            self.expect_token(&Token::RParen)?;
            return Ok(AggregateArgs::OrderedSet { direct, ordered });
        }
        self.expect_token(&Token::RParen)?;
        Ok(AggregateArgs::Args(direct))
    }

    /// Parse an operator name followed by its operand types.
    pub(super) fn parse_operator_signature(&self) -> Result<OperatorSignature, ParserError> {
        let name = self.parse_operator_name()?;
        let args = self.parse_operator_operand_types()?;
        Ok(OperatorSignature { name, args })
    }

    /// Parse `( left_type , right_type )`, where either side may be `NONE`.
    pub(super) fn parse_operator_operand_types(&self) -> Result<OperatorOperandTypes, ParserError> {
        self.expect_token(&Token::LParen)?;
        let left = if self.parse_keyword(Keyword::NONE) {
            None
        } else {
            Some(self.parse_data_type()?)
        };
        self.expect_token(&Token::Comma)?;
        let right = if self.parse_keyword(Keyword::NONE) {
            None
        } else {
            Some(self.parse_data_type()?)
        };
        self.expect_token(&Token::RParen)?;
        Ok(OperatorOperandTypes { left, right })
    }

    /// Parse `( source_type AS target_type )`.
    pub(super) fn parse_cast_signature(&self) -> Result<CastSignature, ParserError> {
        self.expect_token(&Token::LParen)?;
        let source_type = self.parse_data_type()?;
        self.expect_keyword_is(Keyword::AS)?;
        let target_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(CastSignature {
            source_type,
            target_type,
        })
    }

    /// Parse the parenthesized property list that defines a collation, an
    /// aggregate or a text search object.
    pub(super) fn parse_definition_list(&self) -> Result<Vec<SqlOption>, ParserError> {
        self.expect_token(&Token::LParen)?;
        let options =
            self.parse_comma_separated0(Parser::parse_definition_option, Token::RParen)?;
        self.expect_token(&Token::RParen)?;
        Ok(options)
    }

    /// Parse one `name [= value]` property of a definition list. PostgreSQL
    /// allows the value to be a literal, a type name, an operator or `NONE`.
    pub(super) fn parse_definition_option(&self) -> Result<SqlOption, ParserError> {
        let key = self.parse_identifier()?;
        if !self.consume_token(&Token::Eq) {
            return Ok(SqlOption::Ident(key));
        }
        if let Some(value) = self.maybe_parse(|parser| parser.parse_definition_operator_value())? {
            return Ok(SqlOption::KeyValue { key, value });
        }
        if self.parse_keyword(Keyword::NONE) {
            return Ok(SqlOption::KeyValue {
                key,
                value: Expr::Identifier(Ident::new("NONE")),
            });
        }
        if let Some(value) = self.maybe_parse(|parser| {
            let expr = parser.parse_expr()?;
            parser.expect_definition_value_end()?;
            Ok(expr)
        })? {
            return Ok(SqlOption::KeyValue { key, value });
        }
        let data_type = self.parse_data_type()?;
        self.expect_definition_value_end()?;
        Ok(SqlOption::KeyValue {
            key,
            value: Expr::Identifier(Ident::new(data_type.to_string())),
        })
    }

    fn parse_definition_operator_value(&self) -> Result<Expr, ParserError> {
        let symbol = self.parse_operator_symbol()?;
        self.expect_definition_value_end()?;
        Ok(Expr::Identifier(Ident::new(symbol)))
    }

    fn expect_definition_value_end(&self) -> Result<(), ParserError> {
        match self.peek_token_ref().token {
            BorrowedToken::Comma | BorrowedToken::RParen => Ok(()),
            _ => self.expected("end of a definition value", self.peek_token()),
        }
    }

    /// Read one operator symbol, joining the adjacent tokens PostgreSQL's
    /// operator character set produces (`@+@` arrives as `@`, `+`, `@`).
    pub(super) fn parse_operator_symbol(&self) -> Result<String, ParserError> {
        let token = self.next_token();
        if !is_operator_symbol_token(&token) {
            return self.expected("an operator symbol", token);
        }
        let mut symbol = token.to_string();
        let mut end = token.span.end;
        loop {
            let next = self.peek_token_ref();
            if next.span.start != end || !is_operator_symbol_token(next) {
                break;
            }
            let next = self.next_token();
            end = next.span.end;
            symbol.push_str(&next.to_string());
        }
        Ok(symbol)
    }

    /// Read the parts of a possibly schema-qualified operator name.
    pub(super) fn parse_operator_name_parts(&self) -> Result<Vec<ObjectNamePart>, ParserError> {
        let mut parts = vec![];
        loop {
            if matches!(self.peek_token_ref().token, BorrowedToken::Word(_))
                && self.peek_nth_token_ref(1).token == BorrowedToken::Period
            {
                parts.push(ObjectNamePart::Identifier(self.parse_identifier()?));
                self.expect_token(&Token::Period)?;
                continue;
            }
            parts.push(ObjectNamePart::Identifier(Ident::new(
                self.parse_operator_symbol()?,
            )));
            return Ok(parts);
        }
    }
}

fn is_operator_symbol_token(token: &TokenWithSpan) -> bool {
    match &token.token {
        BorrowedToken::Word(_) | BorrowedToken::EOF => false,
        other => {
            let text = other.to_string();
            !text.is_empty() && text.chars().all(|ch| OPERATOR_CHARACTERS.contains(&ch))
        }
    }
}
