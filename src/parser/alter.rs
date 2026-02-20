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

//! SQL Parser for ALTER

#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec};

use super::{AttachedToken, Parser, ParserError};
use crate::{
    ast::{
        helpers::key_value_options::{KeyValueOptions, KeyValueOptionsDelimiter},
        AlterConfigurationOperation, AlterPolicyOperation, AlterRoleOperation, AlterUser, Expr,
        ObjectName, Password, ResetConfig, RoleOption, SetConfigValue, Statement,
    },
    dialect::{MsSqlDialect, PostgreSqlDialect},
    keywords::Keyword,
    tokenizer::BorrowedToken,
};

impl Parser<'_> {
    pub fn parse_alter_system(&self) -> Result<Statement, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect) {
            return Err(ParserError::ParserError(
                "ALTER SYSTEM is only support for PostgreSqlDialect".into(),
            ));
        }

        // We need to get the ALTER token which was consumed before this function
        // The parser is currently at the position after SYSTEM, so we go back 2 positions
        let token = {
            let current_pos = self.index();
            self.prev_token();
            self.prev_token();
            let t = AttachedToken(self.get_current_token().clone().to_static());
            // Restore position
            while self.index() < current_pos {
                self.next_token();
            }
            t
        };

        let operation = self.parse_pg_alter_configuration_operation()?;

        Ok(Statement::AlterSystem { token, operation })
    }

    pub fn parse_alter_database(&self) -> Result<Statement, ParserError> {
        if !dialect_of!(self is PostgreSqlDialect) {
            return Err(ParserError::ParserError(
                "ALTER DATABASE is only support for PostgreSqlDialect".into(),
            ));
        }

        // We need to get the ALTER token which was consumed before this function
        // The parser is currently at the position after DATABASE, so we go back 2 positions
        let token = {
            let current_pos = self.index();
            self.prev_token();
            self.prev_token();
            let t = AttachedToken(self.get_current_token().clone().to_static());
            // Restore position
            while self.index() < current_pos {
                self.next_token();
            }
            t
        };

        let database_name = self.parse_object_name(false)?;
        let operation = self.parse_pg_alter_configuration_operation()?;

        Ok(Statement::AlterDatabase {
            token,
            database_name,
            operation,
        })
    }

    pub fn parse_alter_role(&self) -> Result<Statement, ParserError> {
        if dialect_of!(self is PostgreSqlDialect) {
            return self.parse_pg_alter_role();
        } else if dialect_of!(self is MsSqlDialect) {
            return self.parse_mssql_alter_role();
        }

        Err(ParserError::ParserError(
            "ALTER ROLE is only support for PostgreSqlDialect, MsSqlDialect".into(),
        ))
    }

    /// Parse ALTER POLICY statement
    /// ```sql
    /// ALTER POLICY policy_name ON table_name [ RENAME TO new_name ]
    /// or
    /// ALTER POLICY policy_name ON table_name
    /// [ TO { role_name | PUBLIC | CURRENT_ROLE | CURRENT_USER | SESSION_USER } [, ...] ]
    /// [ USING ( using_expression ) ]
    /// [ WITH CHECK ( check_expression ) ]
    /// ```
    ///
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-alterpolicy.html)
    pub fn parse_alter_policy(&self) -> Result<Statement, ParserError> {
        // We need to get the ALTER token which was consumed before this function
        // The parser is currently at the position after POLICY, so we go back 2 positions
        let token = {
            let current_pos = self.index();
            self.prev_token();
            self.prev_token();
            let t = AttachedToken(self.get_current_token().clone().to_static());
            // Restore position
            while self.index() < current_pos {
                self.next_token();
            }
            t
        };

        let name = self.parse_identifier()?;
        self.expect_keyword_is(Keyword::ON)?;
        let table_name = self.parse_object_name(false)?;

        if self.parse_keyword(Keyword::RENAME) {
            self.expect_keyword_is(Keyword::TO)?;
            let new_name = self.parse_identifier()?;
            Ok(Statement::AlterPolicy {
                token,
                name,
                table_name,
                operation: AlterPolicyOperation::Rename { new_name },
            })
        } else {
            let to = if self.parse_keyword(Keyword::TO) {
                Some(self.parse_comma_separated(|p| p.parse_owner())?)
            } else {
                None
            };

            let using = if self.parse_keyword(Keyword::USING) {
                self.expect_token(&BorrowedToken::LParen)?;
                let expr = self.parse_expr()?;
                self.expect_token(&BorrowedToken::RParen)?;
                Some(expr)
            } else {
                None
            };

            let with_check = if self.parse_keywords(&[Keyword::WITH, Keyword::CHECK]) {
                self.expect_token(&BorrowedToken::LParen)?;
                let expr = self.parse_expr()?;
                self.expect_token(&BorrowedToken::RParen)?;
                Some(expr)
            } else {
                None
            };
            Ok(Statement::AlterPolicy {
                token,
                name,
                table_name,
                operation: AlterPolicyOperation::Apply {
                    to,
                    using,
                    with_check,
                },
            })
        }
    }

    /// Parse an `ALTER USER` statement
    /// ```sql
    /// ALTER USER [ IF EXISTS ] [ <name> ] [ OPTIONS ]
    /// ```
    pub fn parse_alter_user(&self) -> Result<Statement, ParserError> {
        let token = AttachedToken(self.get_current_token().clone().to_static());
        let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_identifier()?;
        let rename_to = if self.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let set_props = if self.parse_keyword(Keyword::SET) {
            self.parse_key_value_options(false, &[])?
        } else {
            KeyValueOptions {
                delimiter: KeyValueOptionsDelimiter::Comma,
                options: vec![],
            }
        };

        let unset_props = if self.parse_keyword(Keyword::UNSET) {
            self.parse_comma_separated(Parser::parse_identifier)?
                .iter()
                .map(|i| i.to_string())
                .collect()
        } else {
            vec![]
        };

        Ok(Statement::AlterUser(AlterUser {
            token,
            if_exists,
            name,
            rename_to,
            set_props,
            unset_props,
        }))
    }

    fn parse_mssql_alter_role(&self) -> Result<Statement, ParserError> {
        // We need to get the ALTER token which was consumed before this function
        // The parser is currently at the position after ROLE, so we go back 2 positions
        let token = {
            let current_pos = self.index();
            self.prev_token();
            self.prev_token();
            let t = AttachedToken(self.get_current_token().clone().to_static());
            // Restore position
            while self.index() < current_pos {
                self.next_token();
            }
            t
        };

        let role_name = self.parse_identifier()?;

        let operation = if self.parse_keywords(&[Keyword::ADD, Keyword::MEMBER]) {
            let member_name = self.parse_identifier()?;
            AlterRoleOperation::AddMember { member_name }
        } else if self.parse_keywords(&[Keyword::DROP, Keyword::MEMBER]) {
            let member_name = self.parse_identifier()?;
            AlterRoleOperation::DropMember { member_name }
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::NAME]) {
            if self.consume_token(&BorrowedToken::Eq) {
                let role_name = self.parse_identifier()?;
                AlterRoleOperation::RenameRole { role_name }
            } else {
                return self.expected("= after WITH NAME ", self.peek_token());
            }
        } else {
            return self.expected("'ADD' or 'DROP' or 'WITH NAME'", self.peek_token());
        };

        Ok(Statement::AlterRole {
            token,
            name: role_name,
            operation,
        })
    }

    fn parse_pg_alter_role(&self) -> Result<Statement, ParserError> {
        // We need to get the ALTER token which was consumed before this function
        // The parser is currently at the position after ROLE, so we go back 2 positions
        let token = {
            let current_pos = self.index();
            self.prev_token();
            self.prev_token();
            let t = AttachedToken(self.get_current_token().clone().to_static());
            // Restore position
            while self.index() < current_pos {
                self.next_token();
            }
            t
        };

        let role_name = self.parse_identifier()?;

        // [ IN DATABASE _`database_name`_ ]
        let in_database = if self.parse_keywords(&[Keyword::IN, Keyword::DATABASE]) {
            self.parse_object_name(false).ok()
        } else {
            None
        };

        let operation = if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::TO) {
                let role_name = self.parse_identifier()?;
                AlterRoleOperation::RenameRole { role_name }
            } else {
                return self.expected("TO after RENAME", self.peek_token());
            }
        } else if self.peek_keyword(Keyword::SET) || self.peek_keyword(Keyword::RESET) {
            self.parse_pg_alter_set_reset_operation(in_database)?
        // option
        } else {
            // [ WITH ]
            let _ = self.parse_keyword(Keyword::WITH);
            // option
            let mut options = vec![];
            while let Some(opt) = self.maybe_parse(|parser| parser.parse_pg_role_option())? {
                options.push(opt);
            }
            // check option
            if options.is_empty() {
                return self.expected("option", self.peek_token())?;
            }

            AlterRoleOperation::WithOptions { options }
        };

        Ok(Statement::AlterRole {
            token,
            name: role_name,
            operation,
        })
    }

    fn parse_pg_alter_set_reset_operation(
        &self,
        in_database: Option<ObjectName>,
    ) -> Result<AlterRoleOperation, ParserError> {
        if self.parse_keyword(Keyword::SET) {
            let config_name = self.parse_object_name(false)?;
            // FROM CURRENT
            if self.parse_keywords(&[Keyword::FROM, Keyword::CURRENT]) {
                Ok(AlterRoleOperation::Set {
                    config_name,
                    config_value: SetConfigValue::FromCurrent,
                    in_database,
                })
            // { TO | = } { value | DEFAULT }
            } else if self.consume_token(&BorrowedToken::Eq) || self.parse_keyword(Keyword::TO) {
                if self.parse_keyword(Keyword::DEFAULT) {
                    Ok(AlterRoleOperation::Set {
                        config_name,
                        config_value: SetConfigValue::Default,
                        in_database,
                    })
                } else {
                    let expr = self.parse_expr()?;
                    Ok(AlterRoleOperation::Set {
                        config_name,
                        config_value: SetConfigValue::Value(expr),
                        in_database,
                    })
                }
            } else {
                self.expected("'TO' or '=' or 'FROM CURRENT'", self.peek_token())
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::ALL) {
                Ok(AlterRoleOperation::Reset {
                    config_name: ResetConfig::ALL,
                    in_database,
                })
            } else {
                let config_name = self.parse_object_name(false)?;
                Ok(AlterRoleOperation::Reset {
                    config_name: ResetConfig::ConfigName(config_name),
                    in_database,
                })
            }
        } else {
            self.expected("'SET' or 'RESET'", self.peek_token())
        }
    }

    fn parse_pg_alter_configuration_operation(
        &self,
    ) -> Result<AlterConfigurationOperation, ParserError> {
        if self.parse_keyword(Keyword::SET) {
            let config_name = self.parse_object_name(false)?;
            // FROM CURRENT
            if self.parse_keywords(&[Keyword::FROM, Keyword::CURRENT]) {
                Ok(AlterConfigurationOperation::Set {
                    config_name,
                    config_value: SetConfigValue::FromCurrent,
                })
            // { TO | = } { value | DEFAULT }
            } else if self.consume_token(&BorrowedToken::Eq) || self.parse_keyword(Keyword::TO) {
                if self.parse_keyword(Keyword::DEFAULT) {
                    Ok(AlterConfigurationOperation::Set {
                        config_name,
                        config_value: SetConfigValue::Default,
                    })
                } else {
                    let expr = self.parse_expr()?;
                    Ok(AlterConfigurationOperation::Set {
                        config_name,
                        config_value: SetConfigValue::Value(expr),
                    })
                }
            } else {
                self.expected("'TO' or '=' or 'FROM CURRENT'", self.peek_token())
            }
        } else if self.parse_keyword(Keyword::RESET) {
            if self.parse_keyword(Keyword::ALL) {
                Ok(AlterConfigurationOperation::Reset {
                    config_name: ResetConfig::ALL,
                })
            } else {
                let config_name = self.parse_object_name(false)?;
                Ok(AlterConfigurationOperation::Reset {
                    config_name: ResetConfig::ConfigName(config_name),
                })
            }
        } else {
            self.expected("'SET' or 'RESET'", self.peek_token())
        }
    }

    fn parse_pg_role_option(&self) -> Result<RoleOption, ParserError> {
        let option = match self.parse_one_of_keywords(&[
            Keyword::BYPASSRLS,
            Keyword::NOBYPASSRLS,
            Keyword::CONNECTION,
            Keyword::CREATEDB,
            Keyword::NOCREATEDB,
            Keyword::CREATEROLE,
            Keyword::NOCREATEROLE,
            Keyword::INHERIT,
            Keyword::NOINHERIT,
            Keyword::LOGIN,
            Keyword::NOLOGIN,
            Keyword::PASSWORD,
            Keyword::REPLICATION,
            Keyword::NOREPLICATION,
            Keyword::SUPERUSER,
            Keyword::NOSUPERUSER,
            Keyword::VALID,
        ]) {
            Some(Keyword::BYPASSRLS) => RoleOption::BypassRLS(true),
            Some(Keyword::NOBYPASSRLS) => RoleOption::BypassRLS(false),
            Some(Keyword::CONNECTION) => {
                self.expect_keyword_is(Keyword::LIMIT)?;
                RoleOption::ConnectionLimit(Expr::Value(self.parse_number_value()?))
            }
            Some(Keyword::CREATEDB) => RoleOption::CreateDB(true),
            Some(Keyword::NOCREATEDB) => RoleOption::CreateDB(false),
            Some(Keyword::CREATEROLE) => RoleOption::CreateRole(true),
            Some(Keyword::NOCREATEROLE) => RoleOption::CreateRole(false),
            Some(Keyword::INHERIT) => RoleOption::Inherit(true),
            Some(Keyword::NOINHERIT) => RoleOption::Inherit(false),
            Some(Keyword::LOGIN) => RoleOption::Login(true),
            Some(Keyword::NOLOGIN) => RoleOption::Login(false),
            Some(Keyword::PASSWORD) => {
                let password = if self.parse_keyword(Keyword::NULL) {
                    Password::NullPassword
                } else {
                    Password::Password(Expr::Value(self.parse_value()?))
                };
                RoleOption::Password(password)
            }
            Some(Keyword::REPLICATION) => RoleOption::Replication(true),
            Some(Keyword::NOREPLICATION) => RoleOption::Replication(false),
            Some(Keyword::SUPERUSER) => RoleOption::SuperUser(true),
            Some(Keyword::NOSUPERUSER) => RoleOption::SuperUser(false),
            Some(Keyword::VALID) => {
                self.expect_keyword_is(Keyword::UNTIL)?;
                RoleOption::ValidUntil(Expr::Value(self.parse_value()?))
            }
            _ => self.expected("option", self.peek_token())?,
        };

        Ok(option)
    }
}
