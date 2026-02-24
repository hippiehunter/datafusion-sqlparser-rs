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

#[cfg(not(feature = "std"))]
use alloc::{format, string::String, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{AttachedToken, ObjectName, Statement};
use crate::parser::ParserError;
use crate::tokenizer::{Token, TokenWithSpan};

/// Builder for create database statement variant ([1]).
///
/// This structure helps building and accessing a create database with more ease, without needing to:
/// - Match the enum itself a lot of times; or
/// - Moving a lot of variables around the code.
///
/// # Example
/// ```rust
/// use sqlparser::ast::helpers::stmt_create_database::CreateDatabaseBuilder;
/// use sqlparser::ast::{ColumnDef, Ident, ObjectName};
/// let builder = CreateDatabaseBuilder::new(ObjectName::from(vec![Ident::new("database_name")]))
///    .if_not_exists(true);
/// // You can access internal elements with ease
/// assert!(builder.if_not_exists);
/// // Convert to a statement
/// assert_eq!(
///    builder.build().to_string(),
///    "CREATE DATABASE IF NOT EXISTS database_name"
/// )
/// ```
///
/// [1]: Statement::CreateDatabase
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateDatabaseBuilder {
    pub db_name: ObjectName,
    pub if_not_exists: bool,
    pub location: Option<String>,
    pub managed_location: Option<String>,
    pub owner: Option<ObjectName>,
    pub or_replace: bool,
    pub transient: bool,
    pub clone: Option<ObjectName>,
    pub comment: Option<String>,
    pub catalog_sync: Option<String>,
}

impl CreateDatabaseBuilder {
    pub fn new(name: ObjectName) -> Self {
        Self {
            db_name: name,
            if_not_exists: false,
            location: None,
            managed_location: None,
            owner: None,
            or_replace: false,
            transient: false,
            clone: None,
            comment: None,
            catalog_sync: None,
        }
    }

    pub fn location(mut self, location: Option<String>) -> Self {
        self.location = location;
        self
    }

    pub fn managed_location(mut self, managed_location: Option<String>) -> Self {
        self.managed_location = managed_location;
        self
    }

    pub fn owner(mut self, owner: Option<ObjectName>) -> Self {
        self.owner = owner;
        self
    }

    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    pub fn transient(mut self, transient: bool) -> Self {
        self.transient = transient;
        self
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn clone_clause(mut self, clone: Option<ObjectName>) -> Self {
        self.clone = clone;
        self
    }

    pub fn comment(mut self, comment: Option<String>) -> Self {
        self.comment = comment;
        self
    }

    pub fn catalog_sync(mut self, catalog_sync: Option<String>) -> Self {
        self.catalog_sync = catalog_sync;
        self
    }

    pub fn build(self) -> Statement {
        Statement::CreateDatabase {
            create_token: AttachedToken(TokenWithSpan::wrap(Token::make_word("CREATE", None))),
            db_name: self.db_name,
            if_not_exists: self.if_not_exists,
            managed_location: self.managed_location,
            location: self.location,
            owner: self.owner,
            or_replace: self.or_replace,
            transient: self.transient,
            clone: self.clone,
            comment: self.comment,
            catalog_sync: self.catalog_sync,
        }
    }
}

impl TryFrom<Statement> for CreateDatabaseBuilder {
    type Error = ParserError;

    fn try_from(stmt: Statement) -> Result<Self, Self::Error> {
        match stmt {
            Statement::CreateDatabase {
                create_token: _,
                db_name,
                if_not_exists,
                location,
                managed_location,
                owner,
                or_replace,
                transient,
                clone,
                comment,
                catalog_sync,
            } => Ok(Self {
                db_name,
                if_not_exists,
                location,
                managed_location,
                owner,
                or_replace,
                transient,
                clone,
                comment,
                catalog_sync,
            }),
            _ => Err(ParserError::ParserError(format!(
                "Expected create database statement, but received: {stmt}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::helpers::stmt_create_database::CreateDatabaseBuilder;
    use crate::ast::{AttachedToken, Ident, ObjectName, Statement};
    use crate::parser::ParserError;

    #[test]
    pub fn test_from_valid_statement() {
        let builder = CreateDatabaseBuilder::new(ObjectName::from(vec![Ident::new("db_name")]));

        let stmt = builder.clone().build();

        assert_eq!(builder, CreateDatabaseBuilder::try_from(stmt).unwrap());
    }

    #[test]
    pub fn test_from_invalid_statement() {
        let stmt = Statement::Commit {
            commit_token: AttachedToken::empty(),
            chain: false,
            end: false,
            modifier: None,
        };

        assert_eq!(
            CreateDatabaseBuilder::try_from(stmt).unwrap_err(),
            ParserError::ParserError(
                "Expected create database statement, but received: COMMIT".to_owned()
            )
        );
    }
}
