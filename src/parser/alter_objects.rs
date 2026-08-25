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

//! SQL Parser for the PostgreSQL `ALTER <object>` statements that target
//! neither a table nor an object with a dedicated parser entry point.

#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use super::{Parser, ParserError};
use crate::ast::helpers::attached_token::AttachedToken;
use crate::ast::{
    AllInTablespaceObjectType, AlterCollationAction, AlterConfigurationOperation,
    AlterDatabaseOption, AlterDomainAction, AlterEventTriggerAction, AlterGroupAction,
    AlterIndexOperation, AlterMaterializedViewAction, AlterMaterializedViewOperation, AlterObject,
    AlterObjectAction, AlterObjectTarget, AlterOperatorAction, AlterOperatorArgs,
    AlterRoutineAction, AlterSequenceOperation, AlterStatisticsAction,
    AlterTextSearchConfigurationAction, AlterTextSearchDictionaryAction, AlterTriggerAction,
    AlterTypeAction, AlterTypeOperation, AlterViewOperation, DataType, DatabaseOptionValue,
    DefinitionElement, DefinitionValue, EventTriggerEnableMode, Expr, FunctionBehavior,
    FunctionCalledOnNull, FunctionParallel, Ident, ObjectName, ProcedureSecurity,
    ProcedureSetConfig, ResetConfig, RoutineKind, RoutineOption, SetConfigValue,
    SetStatisticsValue, SqlOption, Statement,
};
use crate::keywords::Keyword;
use crate::tokenizer::BorrowedToken;

impl Parser<'_> {
    /// Parse `ALTER AGGREGATE name ( aggregate_signature ) action`.
    pub(super) fn parse_alter_aggregate(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_object_name(false)?;
        let signature = self.parse_aggregate_args()?;
        let action = self.parse_alter_object_action(true, true)?;
        self.build_alter_object(
            alter_token,
            AlterObjectTarget::Aggregate {
                name,
                signature,
                action,
            },
        )
    }

    /// Parse `ALTER COLLATION name { REFRESH VERSION | action }`.
    pub(super) fn parse_alter_collation(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_object_name(false)?;
        let action = if self.parse_keywords(&[Keyword::REFRESH, Keyword::VERSION]) {
            AlterCollationAction::RefreshVersion
        } else {
            AlterCollationAction::Object(self.parse_alter_object_action(true, true)?)
        };
        self.build_alter_object(alter_token, AlterObjectTarget::Collation { name, action })
    }

    /// Parse `ALTER CONVERSION name action`.
    pub(super) fn parse_alter_conversion(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_object_name(false)?;
        let action = self.parse_alter_object_action(true, true)?;
        self.build_alter_object(alter_token, AlterObjectTarget::Conversion { name, action })
    }

    /// Parse `ALTER DOMAIN name action`.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-alterdomain.html)
    pub(super) fn parse_alter_domain(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_object_name(false)?;
        let action = self.parse_alter_domain_action()?;
        self.build_alter_object(alter_token, AlterObjectTarget::Domain { name, action })
    }

    /// Parse `ALTER EVENT TRIGGER name action`, with `EVENT` already consumed.
    pub(super) fn parse_alter_event_trigger(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        self.expect_keyword_is(Keyword::TRIGGER)?;
        let name = self.parse_identifier()?;
        let action = if self.parse_keyword(Keyword::DISABLE) {
            AlterEventTriggerAction::Disable
        } else if self.parse_keyword(Keyword::ENABLE) {
            let mode = if self.parse_keyword(Keyword::REPLICA) {
                Some(EventTriggerEnableMode::Replica)
            } else if self.parse_keyword(Keyword::ALWAYS) {
                Some(EventTriggerEnableMode::Always)
            } else {
                None
            };
            AlterEventTriggerAction::Enable { mode }
        } else {
            AlterEventTriggerAction::Object(self.parse_alter_object_action(true, false)?)
        };
        self.build_alter_object(
            alter_token,
            AlterObjectTarget::EventTrigger { name, action },
        )
    }

    /// Parse `ALTER GROUP role_specification action`.
    pub(super) fn parse_alter_group(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_owner()?;
        let action = if self.parse_keywords(&[Keyword::ADD, Keyword::USER]) {
            AlterGroupAction::AddUser {
                members: self.parse_comma_separated(Parser::parse_owner)?,
            }
        } else if self.parse_keywords(&[Keyword::DROP, Keyword::USER]) {
            AlterGroupAction::DropUser {
                members: self.parse_comma_separated(Parser::parse_owner)?,
            }
        } else if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            AlterGroupAction::RenameTo {
                new_name: self.parse_identifier()?,
            }
        } else {
            return self.expected("ADD USER, DROP USER, or RENAME TO", self.peek_token());
        };
        self.build_alter_object(alter_token, AlterObjectTarget::Group { name, action })
    }

    /// Parse `ALTER [ PROCEDURAL ] LANGUAGE name action`, with the `LANGUAGE`
    /// keyword already consumed.
    pub(super) fn parse_alter_language(&self, procedural: bool) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_identifier()?;
        let action = self.parse_alter_object_action(true, false)?;
        self.build_alter_object(
            alter_token,
            AlterObjectTarget::Language {
                procedural,
                name,
                action,
            },
        )
    }

    /// Parse `ALTER OPERATOR name ( left_type, right_type ) action`.
    pub(super) fn parse_alter_operator(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_operator_name()?;
        let args = self.parse_alter_operator_args()?;
        let action = if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            AlterOperatorAction::Object(AlterObjectAction::SetSchema {
                new_schema: self.parse_object_name(false)?,
            })
        } else if self.parse_keyword(Keyword::SET) {
            self.expect_token(&BorrowedToken::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_definition_element)?;
            self.expect_token(&BorrowedToken::RParen)?;
            AlterOperatorAction::SetOptions { options }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            AlterOperatorAction::Object(AlterObjectAction::OwnerTo {
                new_owner: self.parse_owner()?,
            })
        } else {
            return self.expected("OWNER TO, SET SCHEMA, or SET (...)", self.peek_token());
        };
        self.build_alter_object(
            alter_token,
            AlterObjectTarget::Operator { name, args, action },
        )
    }

    /// Parse `ALTER { FUNCTION | PROCEDURE | ROUTINE } name [ ( args ) ] action`.
    pub(super) fn parse_alter_routine(&self, kind: RoutineKind) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let desc = self.parse_function_desc()?;
        let action = self.parse_alter_routine_action()?;
        self.build_alter_object(
            alter_token,
            AlterObjectTarget::Routine { kind, desc, action },
        )
    }

    /// Parse `ALTER STATISTICS [ IF EXISTS ] name action`.
    pub(super) fn parse_alter_statistics(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;
        let action = if self.parse_keywords(&[Keyword::SET, Keyword::STATISTICS]) {
            AlterStatisticsAction::SetStatistics {
                target: self.parse_set_statistics_value()?,
            }
        } else {
            AlterStatisticsAction::Object(self.parse_alter_object_action(true, true)?)
        };
        self.build_alter_object(
            alter_token,
            AlterObjectTarget::Statistics {
                if_exists,
                name,
                action,
            },
        )
    }

    /// Parse the `ALTER TEXT SEARCH { CONFIGURATION | DICTIONARY | PARSER |
    /// TEMPLATE }` statements, with `TEXT` already consumed.
    pub(super) fn parse_alter_text_search(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        self.expect_keyword_is(Keyword::SEARCH)?;
        let target = match self.expect_one_of_keywords(&[
            Keyword::CONFIGURATION,
            Keyword::DICTIONARY,
            Keyword::PARSER,
            Keyword::TEMPLATE,
        ])? {
            Keyword::CONFIGURATION => {
                let name = self.parse_object_name(false)?;
                let action = self.parse_alter_text_search_configuration_action()?;
                AlterObjectTarget::TextSearchConfiguration { name, action }
            }
            Keyword::DICTIONARY => {
                let name = self.parse_object_name(false)?;
                let action = if self.peek_token_ref().token == BorrowedToken::LParen {
                    self.expect_token(&BorrowedToken::LParen)?;
                    let options = self.parse_comma_separated(Parser::parse_definition_element)?;
                    self.expect_token(&BorrowedToken::RParen)?;
                    AlterTextSearchDictionaryAction::SetOptions { options }
                } else {
                    AlterTextSearchDictionaryAction::Object(
                        self.parse_alter_object_action(true, true)?,
                    )
                };
                AlterObjectTarget::TextSearchDictionary { name, action }
            }
            Keyword::PARSER => {
                let name = self.parse_object_name(false)?;
                let action = self.parse_alter_object_action(false, true)?;
                AlterObjectTarget::TextSearchParser { name, action }
            }
            _ => {
                let name = self.parse_object_name(false)?;
                let action = self.parse_alter_object_action(false, true)?;
                AlterObjectTarget::TextSearchTemplate { name, action }
            }
        };
        self.build_alter_object(alter_token, target)
    }

    /// Parse `ALTER TRIGGER name ON table_name action`.
    pub(super) fn parse_alter_trigger(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;
        let action = if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            AlterTriggerAction::RenameTo {
                new_name: self.parse_identifier()?,
            }
        } else {
            let no = self.parse_keyword(Keyword::NO);
            self.expect_keywords(&[Keyword::DEPENDS, Keyword::ON, Keyword::EXTENSION])?;
            AlterTriggerAction::DependsOnExtension {
                no,
                extension_name: self.parse_identifier()?,
            }
        };
        self.build_alter_object(
            alter_token,
            AlterObjectTarget::Trigger {
                name,
                table_name,
                action,
            },
        )
    }

    /// Parse `ALTER INDEX`, including the `ALL IN TABLESPACE` form.
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-alterindex.html)
    pub(super) fn parse_alter_index(&self) -> Result<Statement, ParserError> {
        let alter_token = self.get_alter_token();
        if self.parse_keywords(&[Keyword::ALL, Keyword::IN, Keyword::TABLESPACE]) {
            let target = self.parse_all_in_tablespace(AllInTablespaceObjectType::Index)?;
            return self.build_alter_object(alter_token, target);
        }

        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_object_name(false)?;
        let operation = if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            AlterIndexOperation::RenameIndex {
                index_name: self.parse_object_name(false)?,
            }
        } else if self.parse_keywords(&[Keyword::SET, Keyword::TABLESPACE]) {
            AlterIndexOperation::SetTablespace {
                tablespace_name: self.parse_identifier()?,
            }
        } else if self.parse_keywords(&[Keyword::ATTACH, Keyword::PARTITION]) {
            AlterIndexOperation::AttachPartition {
                partition_index: self.parse_object_name(false)?,
            }
        } else if self.parse_keyword(Keyword::SET) {
            AlterIndexOperation::SetOptions {
                options: self.parse_parenthesized_storage_parameters()?,
            }
        } else if self.parse_keyword(Keyword::RESET) {
            AlterIndexOperation::ResetOptions {
                options: self.parse_parenthesized_identifiers()?,
            }
        } else if self.parse_keyword(Keyword::ALTER) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let column_number = self.parse_number()?;
            self.expect_keywords(&[Keyword::SET, Keyword::STATISTICS])?;
            AlterIndexOperation::AlterColumnSetStatistics {
                column_number,
                statistics: self.parse_number()?,
            }
        } else {
            let no = self.parse_keyword(Keyword::NO);
            if self.parse_keywords(&[Keyword::DEPENDS, Keyword::ON, Keyword::EXTENSION]) {
                AlterIndexOperation::DependsOnExtension {
                    no,
                    extension_name: self.parse_identifier()?,
                }
            } else {
                return self.expected(
                    "RENAME TO, SET TABLESPACE, ATTACH PARTITION, SET (...), RESET (...), ALTER COLUMN, or DEPENDS ON EXTENSION after ALTER INDEX",
                    self.peek_token(),
                );
            }
        };

        Ok(Statement::AlterIndex {
            name,
            if_exists,
            operation,
        })
    }

    /// Parse the tail of `ALTER { INDEX | MATERIALIZED VIEW } ALL IN TABLESPACE`,
    /// with `ALL IN TABLESPACE` already consumed.
    pub(super) fn parse_all_in_tablespace(
        &self,
        object_type: AllInTablespaceObjectType,
    ) -> Result<AlterObjectTarget, ParserError> {
        let tablespace_name = self.parse_identifier()?;
        let owned_by = if self.parse_keywords(&[Keyword::OWNED, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_owner)?
        } else {
            vec![]
        };
        self.expect_keywords(&[Keyword::SET, Keyword::TABLESPACE])?;
        let new_tablespace = self.parse_identifier()?;
        let nowait = self.parse_keyword(Keyword::NOWAIT);
        Ok(AlterObjectTarget::AllInTablespace {
            object_type,
            tablespace_name,
            owned_by,
            new_tablespace,
            nowait,
        })
    }

    /// Parse the non-configuration forms of `ALTER SEQUENCE`, returning `None`
    /// when the statement uses the sequence-option form instead.
    pub(super) fn parse_alter_sequence_operation(
        &self,
    ) -> Result<Option<AlterSequenceOperation>, ParserError> {
        if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            Ok(Some(AlterSequenceOperation::RenameTo {
                new_name: self.parse_identifier()?,
            }))
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            Ok(Some(AlterSequenceOperation::OwnerTo {
                new_owner: self.parse_owner()?,
            }))
        } else if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            Ok(Some(AlterSequenceOperation::SetSchema {
                new_schema: self.parse_object_name(false)?,
            }))
        } else if self.parse_keywords(&[Keyword::SET, Keyword::LOGGED]) {
            Ok(Some(AlterSequenceOperation::SetLogged))
        } else if self.parse_keywords(&[Keyword::SET, Keyword::UNLOGGED]) {
            Ok(Some(AlterSequenceOperation::SetUnlogged))
        } else {
            Ok(None)
        }
    }

    /// Parse the `ALTER TYPE` forms that operate on composite-type attributes,
    /// on the type owner, or on the type schema, returning `None` when none of
    /// them is present.
    pub(super) fn parse_alter_type_attribute_operation(
        &self,
    ) -> Result<Option<AlterTypeOperation>, ParserError> {
        if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            return Ok(Some(AlterTypeOperation::OwnerTo {
                new_owner: self.parse_owner()?,
            }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            return Ok(Some(AlterTypeOperation::SetSchema {
                new_schema: self.parse_object_name(false)?,
            }));
        }
        if self.parse_keywords(&[Keyword::RENAME, Keyword::ATTRIBUTE]) {
            let old_name = self.parse_identifier()?;
            self.expect_keyword_is(Keyword::TO)?;
            let new_name = self.parse_identifier()?;
            return Ok(Some(AlterTypeOperation::RenameAttribute {
                old_name,
                new_name,
                drop_behavior: self.parse_optional_drop_behavior(),
            }));
        }
        if self.parse_keyword(Keyword::SET) {
            return Ok(Some(AlterTypeOperation::SetProperties {
                properties: self.parse_parenthesized_storage_parameters()?,
            }));
        }
        if self.peek_keyword(Keyword::ADD)
            || self.peek_keyword(Keyword::DROP)
            || self.peek_keyword(Keyword::ALTER)
        {
            let actions = self.parse_comma_separated(Parser::parse_alter_type_action)?;
            return Ok(Some(AlterTypeOperation::Actions(actions)));
        }
        Ok(None)
    }

    /// Parse the `action [, ...]` list of `ALTER MATERIALIZED VIEW`, plus the
    /// forms that are not part of that list.
    pub(super) fn parse_alter_materialized_view_extension(
        &self,
    ) -> Result<Option<AlterMaterializedViewOperation>, ParserError> {
        if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            return Ok(Some(AlterMaterializedViewOperation::RenameTo {
                new_name: self.parse_identifier()?,
            }));
        }
        if self.parse_keyword(Keyword::RENAME) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let old_column_name = self.parse_identifier()?;
            self.expect_keyword_is(Keyword::TO)?;
            return Ok(Some(AlterMaterializedViewOperation::RenameColumn {
                old_column_name,
                new_column_name: self.parse_identifier()?,
            }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            return Ok(Some(AlterMaterializedViewOperation::SetSchema {
                new_schema: self.parse_object_name(false)?,
            }));
        }
        let depends_start = self.index();
        let no = self.parse_keyword(Keyword::NO);
        if self.parse_keywords(&[Keyword::DEPENDS, Keyword::ON, Keyword::EXTENSION]) {
            return Ok(Some(AlterMaterializedViewOperation::DependsOnExtension {
                no,
                extension_name: self.parse_identifier()?,
            }));
        }
        self.rewind_to(depends_start);

        let actions = self.parse_comma_separated(Parser::parse_alter_materialized_view_action)?;
        if let [AlterMaterializedViewAction::OwnerTo { new_owner }] = actions.as_slice() {
            return Ok(Some(AlterMaterializedViewOperation::OwnerTo(
                new_owner.clone(),
            )));
        }
        Ok(Some(AlterMaterializedViewOperation::Actions(actions)))
    }

    /// Parse the `ALTER VIEW` column forms, returning `None` when the statement
    /// uses one of the other forms.
    pub(super) fn parse_alter_view_column_operation(
        &self,
    ) -> Result<Option<AlterViewOperation>, ParserError> {
        if self.parse_keyword(Keyword::ALTER) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let column_name = self.parse_identifier()?;
            if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
                return Ok(Some(AlterViewOperation::AlterColumnSetDefault {
                    column_name,
                    default: self.parse_expr()?,
                }));
            }
            self.expect_keywords(&[Keyword::DROP, Keyword::DEFAULT])?;
            return Ok(Some(AlterViewOperation::AlterColumnDropDefault {
                column_name,
            }));
        }
        if self.peek_keyword(Keyword::RENAME)
            && !self.peek_keywords(&[Keyword::RENAME, Keyword::TO])
        {
            self.expect_keyword_is(Keyword::RENAME)?;
            let _ = self.parse_keyword(Keyword::COLUMN);
            let old_column_name = self.parse_identifier()?;
            self.expect_keyword_is(Keyword::TO)?;
            return Ok(Some(AlterViewOperation::RenameColumn {
                old_column_name,
                new_column_name: self.parse_identifier()?,
            }));
        }
        Ok(None)
    }

    /// Parse the `ALTER DATABASE` forms that are not `SET`/`RESET` of a
    /// configuration parameter, returning `None` when the statement is one of
    /// those.
    pub(super) fn parse_alter_database_operation(
        &self,
    ) -> Result<Option<AlterConfigurationOperation>, ParserError> {
        if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            return Ok(Some(AlterConfigurationOperation::RenameTo {
                new_name: self.parse_identifier()?,
            }));
        }
        if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            return Ok(Some(AlterConfigurationOperation::OwnerTo {
                new_owner: self.parse_owner()?,
            }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::TABLESPACE]) {
            return Ok(Some(AlterConfigurationOperation::SetTablespace {
                tablespace_name: self.parse_identifier()?,
            }));
        }
        if self.parse_keywords(&[Keyword::REFRESH, Keyword::COLLATION, Keyword::VERSION]) {
            return Ok(Some(AlterConfigurationOperation::RefreshCollationVersion));
        }
        let with = self.parse_keyword(Keyword::WITH);
        let mut options = vec![];
        while let Some(option) = self.parse_alter_database_option()? {
            options.push(option);
        }
        if options.is_empty() {
            if with {
                return self.expected("a database option after WITH", self.peek_token());
            }
            return Ok(None);
        }
        Ok(Some(AlterConfigurationOperation::WithOptions {
            with,
            options,
        }))
    }

    fn parse_alter_database_option(&self) -> Result<Option<AlterDatabaseOption>, ParserError> {
        if self.parse_keywords(&[Keyword::CONNECTION, Keyword::LIMIT]) {
            let _ = self.consume_token(&BorrowedToken::Eq);
            return Ok(Some(AlterDatabaseOption::ConnectionLimit(
                self.parse_database_option_value()?,
            )));
        }
        let name = match self.parse_one_of_keywords(&[
            Keyword::ALLOW_CONNECTIONS,
            Keyword::IS_TEMPLATE,
            Keyword::ENCODING,
            Keyword::LOCATION,
            Keyword::OWNER,
            Keyword::TABLESPACE,
            Keyword::TEMPLATE,
        ]) {
            Some(_) => Ident::new(self.get_current_token().to_string()),
            None => match &self.peek_token_ref().token {
                BorrowedToken::Word(word) if word.keyword == Keyword::NoKeyword => {
                    self.parse_identifier()?
                }
                _ => return Ok(None),
            },
        };
        let _ = self.consume_token(&BorrowedToken::Eq);
        Ok(Some(AlterDatabaseOption::Named {
            name,
            value: self.parse_database_option_value()?,
        }))
    }

    fn parse_database_option_value(&self) -> Result<DatabaseOptionValue, ParserError> {
        if self.parse_keyword(Keyword::DEFAULT) {
            return Ok(DatabaseOptionValue::Default);
        }
        match &self.peek_token_ref().token {
            BorrowedToken::Minus | BorrowedToken::Plus | BorrowedToken::Number(_, _) => {
                Ok(DatabaseOptionValue::Value(self.parse_number()?))
            }
            _ => Ok(DatabaseOptionValue::Value(Expr::Value(self.parse_value()?))),
        }
    }

    fn build_alter_object(
        &self,
        alter_token: AttachedToken,
        target: AlterObjectTarget,
    ) -> Result<Statement, ParserError> {
        Ok(Statement::AlterObject(AlterObject {
            alter_token,
            target,
        }))
    }

    /// Parse the `RENAME TO` / `OWNER TO` / `SET SCHEMA` action shared by most
    /// `ALTER <object>` statements.
    fn parse_alter_object_action(
        &self,
        allow_owner: bool,
        allow_schema: bool,
    ) -> Result<AlterObjectAction, ParserError> {
        if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            return Ok(AlterObjectAction::RenameTo {
                new_name: self.parse_identifier()?,
            });
        }
        if allow_owner && self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            return Ok(AlterObjectAction::OwnerTo {
                new_owner: self.parse_owner()?,
            });
        }
        if allow_schema && self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            return Ok(AlterObjectAction::SetSchema {
                new_schema: self.parse_object_name(false)?,
            });
        }
        let expected = match (allow_owner, allow_schema) {
            (true, true) => "RENAME TO, OWNER TO, or SET SCHEMA",
            (true, false) => "RENAME TO or OWNER TO",
            (false, true) => "RENAME TO or SET SCHEMA",
            (false, false) => "RENAME TO",
        };
        self.expected(expected, self.peek_token())
    }

    fn parse_alter_domain_action(&self) -> Result<AlterDomainAction, ParserError> {
        if self.parse_keyword(Keyword::SET) {
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(AlterDomainAction::SetDefault {
                    value: self.parse_expr()?,
                });
            }
            if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                return Ok(AlterDomainAction::SetNotNull);
            }
            self.expect_keyword_is(Keyword::SCHEMA)?;
            return Ok(AlterDomainAction::Object(AlterObjectAction::SetSchema {
                new_schema: self.parse_object_name(false)?,
            }));
        }
        if self.parse_keyword(Keyword::DROP) {
            if self.parse_keyword(Keyword::DEFAULT) {
                return Ok(AlterDomainAction::DropDefault);
            }
            if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                return Ok(AlterDomainAction::DropNotNull);
            }
            self.expect_keyword_is(Keyword::CONSTRAINT)?;
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let name = self.parse_identifier()?;
            return Ok(AlterDomainAction::DropConstraint {
                if_exists,
                name,
                drop_behavior: self.parse_optional_drop_behavior(),
            });
        }
        if self.parse_keyword(Keyword::ADD) {
            let start = self.index();
            let constraint_name = if self.parse_keyword(Keyword::CONSTRAINT) {
                Some(self.parse_identifier()?)
            } else {
                None
            };
            if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                let _ = self.parse_keywords(&[Keyword::NO, Keyword::INHERIT]);
                return Ok(AlterDomainAction::AddNotNull {
                    constraint_name,
                    not_valid: self.parse_keywords(&[Keyword::NOT, Keyword::VALID]),
                });
            }
            self.rewind_to(start);
            let Some(constraint) = self.parse_optional_table_constraint()? else {
                return self.expected("a domain constraint after ADD", self.peek_token());
            };
            return Ok(AlterDomainAction::AddConstraint {
                constraint,
                not_valid: self.parse_keywords(&[Keyword::NOT, Keyword::VALID]),
            });
        }
        if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let old_name = self.parse_identifier()?;
                self.expect_keyword_is(Keyword::TO)?;
                return Ok(AlterDomainAction::RenameConstraint {
                    old_name,
                    new_name: self.parse_identifier()?,
                });
            }
            self.expect_keyword_is(Keyword::TO)?;
            return Ok(AlterDomainAction::Object(AlterObjectAction::RenameTo {
                new_name: self.parse_identifier()?,
            }));
        }
        if self.parse_keywords(&[Keyword::VALIDATE, Keyword::CONSTRAINT]) {
            return Ok(AlterDomainAction::ValidateConstraint {
                name: self.parse_identifier()?,
            });
        }
        if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            return Ok(AlterDomainAction::Object(AlterObjectAction::OwnerTo {
                new_owner: self.parse_owner()?,
            }));
        }
        self.expected(
            "SET, DROP, ADD, RENAME, VALIDATE CONSTRAINT, or OWNER TO after ALTER DOMAIN",
            self.peek_token(),
        )
    }

    fn parse_alter_operator_args(&self) -> Result<AlterOperatorArgs, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let left = self.parse_optional_operator_arg_type()?;
        self.expect_token(&BorrowedToken::Comma)?;
        let right = self.parse_optional_operator_arg_type()?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(AlterOperatorArgs { left, right })
    }

    fn parse_optional_operator_arg_type(&self) -> Result<Option<DataType>, ParserError> {
        if self.parse_keyword(Keyword::NONE) {
            Ok(None)
        } else {
            Ok(Some(self.parse_data_type()?))
        }
    }

    fn parse_alter_routine_action(&self) -> Result<AlterRoutineAction, ParserError> {
        if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            return Ok(AlterRoutineAction::Object(AlterObjectAction::RenameTo {
                new_name: self.parse_identifier()?,
            }));
        }
        if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            return Ok(AlterRoutineAction::Object(AlterObjectAction::OwnerTo {
                new_owner: self.parse_owner()?,
            }));
        }
        if self.parse_keywords(&[Keyword::SET, Keyword::SCHEMA]) {
            return Ok(AlterRoutineAction::Object(AlterObjectAction::SetSchema {
                new_schema: self.parse_object_name(false)?,
            }));
        }
        let depends_start = self.index();
        let no = self.parse_keyword(Keyword::NO);
        if self.parse_keywords(&[Keyword::DEPENDS, Keyword::ON, Keyword::EXTENSION]) {
            return Ok(AlterRoutineAction::DependsOnExtension {
                no,
                extension_name: self.parse_identifier()?,
            });
        }
        self.rewind_to(depends_start);

        let mut options = vec![];
        while let Some(option) = self.parse_routine_option()? {
            options.push(option);
        }
        if options.is_empty() {
            return self.expected("an ALTER ROUTINE action", self.peek_token());
        }
        Ok(AlterRoutineAction::Options {
            options,
            restrict: self.parse_keyword(Keyword::RESTRICT),
        })
    }

    fn parse_routine_option(&self) -> Result<Option<RoutineOption>, ParserError> {
        if self.parse_keywords(&[Keyword::CALLED, Keyword::ON, Keyword::NULL, Keyword::INPUT]) {
            return Ok(Some(RoutineOption::CalledOnNull(
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
            return Ok(Some(RoutineOption::CalledOnNull(
                FunctionCalledOnNull::ReturnsNullOnNullInput,
            )));
        }
        if self.parse_keyword(Keyword::STRICT) {
            return Ok(Some(RoutineOption::CalledOnNull(
                FunctionCalledOnNull::Strict,
            )));
        }
        if self.parse_keyword(Keyword::IMMUTABLE) {
            return Ok(Some(RoutineOption::Behavior(FunctionBehavior::Immutable)));
        }
        if self.parse_keyword(Keyword::STABLE) {
            return Ok(Some(RoutineOption::Behavior(FunctionBehavior::Stable)));
        }
        if self.parse_keyword(Keyword::VOLATILE) {
            return Ok(Some(RoutineOption::Behavior(FunctionBehavior::Volatile)));
        }
        if self.parse_keywords(&[Keyword::NOT, Keyword::LEAKPROOF]) {
            return Ok(Some(RoutineOption::Leakproof(false)));
        }
        if self.parse_keyword(Keyword::LEAKPROOF) {
            return Ok(Some(RoutineOption::Leakproof(true)));
        }
        if self.parse_keywords(&[Keyword::EXTERNAL, Keyword::SECURITY]) {
            return Ok(Some(RoutineOption::Security {
                external: true,
                security: self.parse_routine_security()?,
            }));
        }
        if self.parse_keyword(Keyword::SECURITY) {
            return Ok(Some(RoutineOption::Security {
                external: false,
                security: self.parse_routine_security()?,
            }));
        }
        if self.parse_keyword(Keyword::PARALLEL) {
            let parallel = if self.parse_keyword(Keyword::UNSAFE) {
                FunctionParallel::Unsafe
            } else if self.parse_keyword(Keyword::RESTRICTED) {
                FunctionParallel::Restricted
            } else if self.parse_keyword(Keyword::SAFE) {
                FunctionParallel::Safe
            } else {
                return self.expected(
                    "UNSAFE, RESTRICTED, or SAFE after PARALLEL",
                    self.peek_token(),
                );
            };
            return Ok(Some(RoutineOption::Parallel(parallel)));
        }
        if self.parse_keyword(Keyword::COST) {
            return Ok(Some(RoutineOption::Cost(self.parse_number()?)));
        }
        if self.parse_keyword(Keyword::ROWS) {
            return Ok(Some(RoutineOption::Rows(self.parse_number()?)));
        }
        if self.parse_keyword(Keyword::SUPPORT) {
            return Ok(Some(RoutineOption::Support(self.parse_object_name(false)?)));
        }
        if self.parse_keyword(Keyword::SET) {
            let config_name = self.parse_object_name(false)?;
            return Ok(Some(RoutineOption::Set(ProcedureSetConfig {
                config_name,
                config_value: self.parse_set_config_value()?,
            })));
        }
        if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::ALL) {
                return Ok(Some(RoutineOption::Reset(ResetConfig::ALL)));
            }
            return Ok(Some(RoutineOption::Reset(ResetConfig::ConfigName(
                self.parse_object_name(false)?,
            ))));
        }
        Ok(None)
    }

    /// Parse the value of a `SET configuration_parameter` clause:
    /// `FROM CURRENT`, or `{ TO | = } { DEFAULT | value [, ...] }`.
    pub(super) fn parse_set_config_value(&self) -> Result<SetConfigValue, ParserError> {
        if self.parse_keywords(&[Keyword::FROM, Keyword::CURRENT]) {
            return Ok(SetConfigValue::FromCurrent);
        }
        if !self.parse_keyword(Keyword::TO) && !self.consume_token(&BorrowedToken::Eq) {
            return self.expected("TO, =, or FROM CURRENT", self.peek_token());
        }
        if self.parse_keyword(Keyword::DEFAULT) {
            return Ok(SetConfigValue::Default);
        }
        let mut values = self.parse_comma_separated(Parser::parse_expr)?;
        if values.len() == 1 {
            match values.pop() {
                Some(value) => Ok(SetConfigValue::Value(value)),
                None => self.expected("a configuration value", self.peek_token()),
            }
        } else {
            Ok(SetConfigValue::Values(values))
        }
    }

    fn parse_routine_security(&self) -> Result<ProcedureSecurity, ParserError> {
        if self.parse_keyword(Keyword::INVOKER) {
            Ok(ProcedureSecurity::Invoker)
        } else if self.parse_keyword(Keyword::DEFINER) {
            Ok(ProcedureSecurity::Definer)
        } else {
            self.expected("INVOKER or DEFINER after SECURITY", self.peek_token())
        }
    }

    fn parse_set_statistics_value(&self) -> Result<SetStatisticsValue, ParserError> {
        if self.parse_keyword(Keyword::DEFAULT) {
            Ok(SetStatisticsValue::Default)
        } else {
            Ok(SetStatisticsValue::Value(self.parse_statistics_target()?))
        }
    }

    fn parse_alter_text_search_configuration_action(
        &self,
    ) -> Result<AlterTextSearchConfigurationAction, ParserError> {
        if self.parse_keywords(&[Keyword::ADD, Keyword::MAPPING, Keyword::FOR]) {
            let token_types = self.parse_comma_separated(Parser::parse_identifier)?;
            self.expect_keyword_is(Keyword::WITH)?;
            return Ok(AlterTextSearchConfigurationAction::AddMapping {
                token_types,
                dictionaries: self.parse_comma_separated(|p| p.parse_object_name(false))?,
            });
        }
        if self.parse_keywords(&[Keyword::ALTER, Keyword::MAPPING]) {
            if self.parse_keyword(Keyword::REPLACE) {
                let old_dictionary = self.parse_object_name(false)?;
                self.expect_keyword_is(Keyword::WITH)?;
                return Ok(AlterTextSearchConfigurationAction::ReplaceDictionary {
                    token_types: None,
                    old_dictionary,
                    new_dictionary: self.parse_object_name(false)?,
                });
            }
            self.expect_keyword_is(Keyword::FOR)?;
            let token_types = self.parse_comma_separated(Parser::parse_identifier)?;
            if self.parse_keyword(Keyword::REPLACE) {
                let old_dictionary = self.parse_object_name(false)?;
                self.expect_keyword_is(Keyword::WITH)?;
                return Ok(AlterTextSearchConfigurationAction::ReplaceDictionary {
                    token_types: Some(token_types),
                    old_dictionary,
                    new_dictionary: self.parse_object_name(false)?,
                });
            }
            self.expect_keyword_is(Keyword::WITH)?;
            return Ok(AlterTextSearchConfigurationAction::AlterMapping {
                token_types,
                dictionaries: self.parse_comma_separated(|p| p.parse_object_name(false))?,
            });
        }
        if self.parse_keywords(&[Keyword::DROP, Keyword::MAPPING]) {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            self.expect_keyword_is(Keyword::FOR)?;
            return Ok(AlterTextSearchConfigurationAction::DropMapping {
                if_exists,
                token_types: self.parse_comma_separated(Parser::parse_identifier)?,
            });
        }
        Ok(AlterTextSearchConfigurationAction::Object(
            self.parse_alter_object_action(true, true)?,
        ))
    }

    fn parse_alter_type_action(&self) -> Result<AlterTypeAction, ParserError> {
        if self.parse_keywords(&[Keyword::ADD, Keyword::ATTRIBUTE]) {
            let name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;
            let collation = self.parse_optional_attribute_collation()?;
            return Ok(AlterTypeAction::AddAttribute {
                name,
                data_type,
                collation,
                drop_behavior: self.parse_optional_drop_behavior(),
            });
        }
        if self.parse_keywords(&[Keyword::DROP, Keyword::ATTRIBUTE]) {
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let name = self.parse_identifier()?;
            return Ok(AlterTypeAction::DropAttribute {
                if_exists,
                name,
                drop_behavior: self.parse_optional_drop_behavior(),
            });
        }
        self.expect_keywords(&[Keyword::ALTER, Keyword::ATTRIBUTE])?;
        let name = self.parse_identifier()?;
        let had_set_data = self.parse_keywords(&[Keyword::SET, Keyword::DATA]);
        self.expect_keyword_is(Keyword::TYPE)?;
        let data_type = self.parse_data_type()?;
        let collation = self.parse_optional_attribute_collation()?;
        Ok(AlterTypeAction::AlterAttribute {
            name,
            had_set_data,
            data_type,
            collation,
            drop_behavior: self.parse_optional_drop_behavior(),
        })
    }

    fn parse_optional_attribute_collation(&self) -> Result<Option<ObjectName>, ParserError> {
        if self.parse_keyword(Keyword::COLLATE) {
            Ok(Some(self.parse_object_name(false)?))
        } else {
            Ok(None)
        }
    }

    fn parse_alter_materialized_view_action(
        &self,
    ) -> Result<AlterMaterializedViewAction, ParserError> {
        if self.parse_keyword(Keyword::ALTER) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let column_name = self.parse_identifier()?;
            if self.parse_keywords(&[Keyword::SET, Keyword::STATISTICS]) {
                return Ok(AlterMaterializedViewAction::AlterColumnSetStatistics {
                    column_name,
                    statistics: self.parse_number()?,
                });
            }
            if self.parse_keywords(&[Keyword::SET, Keyword::STORAGE]) {
                return Ok(AlterMaterializedViewAction::AlterColumnSetStorage {
                    column_name,
                    storage: self.parse_identifier()?,
                });
            }
            if self.parse_keywords(&[Keyword::SET, Keyword::COMPRESSION]) {
                return Ok(AlterMaterializedViewAction::AlterColumnSetCompression {
                    column_name,
                    compression: self.parse_identifier()?,
                });
            }
            if self.parse_keyword(Keyword::SET) {
                return Ok(AlterMaterializedViewAction::AlterColumnSetOptions {
                    column_name,
                    options: self.parse_parenthesized_storage_parameters()?,
                });
            }
            self.expect_keyword_is(Keyword::RESET)?;
            return Ok(AlterMaterializedViewAction::AlterColumnResetOptions {
                column_name,
                options: self.parse_parenthesized_identifiers()?,
            });
        }
        if self.parse_keywords(&[Keyword::CLUSTER, Keyword::ON]) {
            return Ok(AlterMaterializedViewAction::ClusterOn {
                index_name: self.parse_identifier()?,
            });
        }
        if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            return Ok(AlterMaterializedViewAction::OwnerTo {
                new_owner: self.parse_owner()?,
            });
        }
        if self.parse_keyword(Keyword::SET) {
            if self.parse_keywords(&[Keyword::WITHOUT, Keyword::CLUSTER]) {
                return Ok(AlterMaterializedViewAction::SetWithoutCluster);
            }
            if self.parse_keywords(&[Keyword::ACCESS, Keyword::METHOD]) {
                return Ok(AlterMaterializedViewAction::SetAccessMethod {
                    access_method: self.parse_identifier()?,
                });
            }
            if self.parse_keyword(Keyword::TABLESPACE) {
                return Ok(AlterMaterializedViewAction::SetTablespace {
                    tablespace_name: self.parse_identifier()?,
                });
            }
            return Ok(AlterMaterializedViewAction::SetOptions {
                options: self.parse_parenthesized_storage_parameters()?,
            });
        }
        self.expect_keyword_is(Keyword::RESET)?;
        Ok(AlterMaterializedViewAction::ResetOptions {
            options: self.parse_parenthesized_identifiers()?,
        })
    }

    /// Parse `( storage_parameter [= value] [, ...] )`.
    fn parse_parenthesized_storage_parameters(&self) -> Result<Vec<SqlOption>, ParserError> {
        self.expect_token(&BorrowedToken::LParen)?;
        let options = self.parse_comma_separated(Parser::parse_storage_parameter)?;
        self.expect_token(&BorrowedToken::RParen)?;
        Ok(options)
    }

    fn parse_storage_parameter(&self) -> Result<SqlOption, ParserError> {
        let key = self.parse_identifier()?;
        if self.consume_token(&BorrowedToken::Eq) {
            Ok(SqlOption::KeyValue {
                key,
                value: self.parse_expr()?,
            })
        } else {
            Ok(SqlOption::Ident(key))
        }
    }

    /// Parse PostgreSQL's `def_elem`: `name [ = value ]`.
    fn parse_definition_element(&self) -> Result<DefinitionElement, ParserError> {
        let name = self.parse_identifier()?;
        if !self.consume_token(&BorrowedToken::Eq) {
            return Ok(DefinitionElement { name, value: None });
        }
        let value = if self.parse_keyword(Keyword::NONE) {
            DefinitionValue::None
        } else {
            match &self.peek_token_ref().token {
                BorrowedToken::Minus | BorrowedToken::Plus | BorrowedToken::Number(_, _) => {
                    DefinitionValue::Literal(self.parse_number()?)
                }
                BorrowedToken::SingleQuotedString(_)
                | BorrowedToken::DollarQuotedString(_)
                | BorrowedToken::EscapedStringLiteral(_)
                | BorrowedToken::NationalStringLiteral(_) => {
                    DefinitionValue::Literal(Expr::Value(self.parse_value()?))
                }
                BorrowedToken::Word(_) => DefinitionValue::Name(self.parse_object_name(false)?),
                _ => DefinitionValue::Operator(self.parse_operator_name()?),
            }
        };
        Ok(DefinitionElement {
            name,
            value: Some(value),
        })
    }

    /// Move the token cursor back to `index`.
    fn rewind_to(&self, index: usize) {
        while self.index() > index {
            self.prev_token();
        }
    }
}
