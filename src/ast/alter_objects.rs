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

//! AST types for the PostgreSQL `ALTER <object>` statements that do not target
//! a table: aggregates, collations, conversions, domains, event triggers,
//! groups, languages, operators, routines, statistics, text search objects and
//! triggers.
//!
//! Many of those objects share the `RENAME TO` / `OWNER TO` / `SET SCHEMA`
//! shape, which is modelled once by [`AlterObjectAction`] and embedded in the
//! per-object action enums.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::helpers::attached_token::AttachedToken;
use crate::ast::{
    display_comma_separated, AggregateArgs, DataType, DropBehavior, Expr, FunctionBehavior,
    FunctionCalledOnNull, FunctionDesc, FunctionParallel, Ident, ObjectName, Owner,
    ProcedureSecurity, ProcedureSetConfig, ResetConfig, SqlOption, TableConstraint,
};

/// An `ALTER <object>` statement for a PostgreSQL object that is neither a
/// table nor one of the statement kinds that already have their own
/// [`Statement`](crate::ast::Statement) variant.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterObject {
    #[cfg_attr(feature = "visitor", visit(with = "visit_token"))]
    pub alter_token: AttachedToken,
    pub target: AlterObjectTarget,
}

impl fmt::Display for AlterObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ALTER {}", self.target)
    }
}

/// The object an [`AlterObject`] statement targets, together with the action
/// applied to it.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterObjectTarget {
    /// `ALTER AGGREGATE name ( aggregate_signature ) ...`
    Aggregate {
        name: ObjectName,
        signature: AggregateArgs,
        action: AlterObjectAction,
    },
    /// `ALTER { INDEX | MATERIALIZED VIEW } ALL IN TABLESPACE name
    /// [ OWNED BY role [, ...] ] SET TABLESPACE new_tablespace [ NOWAIT ]`
    AllInTablespace {
        object_type: AllInTablespaceObjectType,
        tablespace_name: Ident,
        owned_by: Vec<Owner>,
        new_tablespace: Ident,
        nowait: bool,
    },
    /// `ALTER COLLATION name ...`
    Collation {
        name: ObjectName,
        action: AlterCollationAction,
    },
    /// `ALTER CONVERSION name ...`
    Conversion {
        name: ObjectName,
        action: AlterObjectAction,
    },
    /// `ALTER DOMAIN name ...`
    Domain {
        name: ObjectName,
        action: AlterDomainAction,
    },
    /// `ALTER EVENT TRIGGER name ...`
    EventTrigger {
        name: Ident,
        action: AlterEventTriggerAction,
    },
    /// `ALTER GROUP role_specification ...`
    Group {
        name: Owner,
        action: AlterGroupAction,
    },
    /// `ALTER [ PROCEDURAL ] LANGUAGE name ...`
    Language {
        procedural: bool,
        name: Ident,
        action: AlterObjectAction,
    },
    /// `ALTER OPERATOR name ( left_type, right_type ) ...`
    Operator {
        name: ObjectName,
        args: AlterOperatorArgs,
        action: AlterOperatorAction,
    },
    /// `ALTER { FUNCTION | PROCEDURE | ROUTINE } name [ ( args ) ] ...`
    Routine {
        kind: RoutineKind,
        desc: FunctionDesc,
        action: AlterRoutineAction,
    },
    /// `ALTER STATISTICS [ IF EXISTS ] name ...`
    Statistics {
        if_exists: bool,
        name: ObjectName,
        action: AlterStatisticsAction,
    },
    /// `ALTER TEXT SEARCH CONFIGURATION name ...`
    TextSearchConfiguration {
        name: ObjectName,
        action: AlterTextSearchConfigurationAction,
    },
    /// `ALTER TEXT SEARCH DICTIONARY name ...`
    TextSearchDictionary {
        name: ObjectName,
        action: AlterTextSearchDictionaryAction,
    },
    /// `ALTER TEXT SEARCH PARSER name ...`
    TextSearchParser {
        name: ObjectName,
        action: AlterObjectAction,
    },
    /// `ALTER TEXT SEARCH TEMPLATE name ...`
    TextSearchTemplate {
        name: ObjectName,
        action: AlterObjectAction,
    },
    /// `ALTER TRIGGER name ON table_name ...`
    Trigger {
        name: Ident,
        table_name: ObjectName,
        action: AlterTriggerAction,
    },
}

impl fmt::Display for AlterObjectTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Aggregate {
                name,
                signature,
                action,
            } => write!(f, "AGGREGATE {name}{signature} {action}"),
            Self::AllInTablespace {
                object_type,
                tablespace_name,
                owned_by,
                new_tablespace,
                nowait,
            } => {
                write!(f, "{object_type} ALL IN TABLESPACE {tablespace_name}")?;
                if !owned_by.is_empty() {
                    write!(f, " OWNED BY {}", display_comma_separated(owned_by))?;
                }
                write!(f, " SET TABLESPACE {new_tablespace}")?;
                if *nowait {
                    write!(f, " NOWAIT")?;
                }
                Ok(())
            }
            Self::Collation { name, action } => write!(f, "COLLATION {name} {action}"),
            Self::Conversion { name, action } => write!(f, "CONVERSION {name} {action}"),
            Self::Domain { name, action } => write!(f, "DOMAIN {name} {action}"),
            Self::EventTrigger { name, action } => write!(f, "EVENT TRIGGER {name} {action}"),
            Self::Group { name, action } => write!(f, "GROUP {name} {action}"),
            Self::Language {
                procedural,
                name,
                action,
            } => {
                if *procedural {
                    write!(f, "PROCEDURAL ")?;
                }
                write!(f, "LANGUAGE {name} {action}")
            }
            Self::Operator { name, args, action } => {
                write!(f, "OPERATOR {name} {args} {action}")
            }
            Self::Routine { kind, desc, action } => write!(f, "{kind} {desc} {action}"),
            Self::Statistics {
                if_exists,
                name,
                action,
            } => {
                write!(f, "STATISTICS ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{name} {action}")
            }
            Self::TextSearchConfiguration { name, action } => {
                write!(f, "TEXT SEARCH CONFIGURATION {name} {action}")
            }
            Self::TextSearchDictionary { name, action } => {
                write!(f, "TEXT SEARCH DICTIONARY {name} {action}")
            }
            Self::TextSearchParser { name, action } => {
                write!(f, "TEXT SEARCH PARSER {name} {action}")
            }
            Self::TextSearchTemplate { name, action } => {
                write!(f, "TEXT SEARCH TEMPLATE {name} {action}")
            }
            Self::Trigger {
                name,
                table_name,
                action,
            } => write!(f, "TRIGGER {name} ON {table_name} {action}"),
        }
    }
}

/// The `RENAME TO` / `OWNER TO` / `SET SCHEMA` actions shared by most
/// PostgreSQL `ALTER <object>` statements.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterObjectAction {
    /// `RENAME TO new_name`
    RenameTo { new_name: Ident },
    /// `OWNER TO { new_owner | CURRENT_ROLE | CURRENT_USER | SESSION_USER }`
    OwnerTo { new_owner: Owner },
    /// `SET SCHEMA new_schema`
    SetSchema { new_schema: ObjectName },
}

impl fmt::Display for AlterObjectAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::RenameTo { new_name } => write!(f, "RENAME TO {new_name}"),
            Self::OwnerTo { new_owner } => write!(f, "OWNER TO {new_owner}"),
            Self::SetSchema { new_schema } => write!(f, "SET SCHEMA {new_schema}"),
        }
    }
}

/// The object kind of an `ALTER ... ALL IN TABLESPACE` statement.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AllInTablespaceObjectType {
    Index,
    MaterializedView,
}

impl fmt::Display for AllInTablespaceObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Index => "INDEX",
            Self::MaterializedView => "MATERIALIZED VIEW",
        })
    }
}


/// An action of `ALTER COLLATION`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterCollationAction {
    Object(AlterObjectAction),
    /// `REFRESH VERSION`
    RefreshVersion,
}

impl fmt::Display for AlterCollationAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::RefreshVersion => write!(f, "REFRESH VERSION"),
        }
    }
}

/// An action of `ALTER DOMAIN`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterDomainAction {
    Object(AlterObjectAction),
    /// `SET DEFAULT expression`
    SetDefault {
        value: Expr,
    },
    /// `DROP DEFAULT`
    DropDefault,
    /// `SET NOT NULL`
    SetNotNull,
    /// `DROP NOT NULL`
    DropNotNull,
    /// `ADD [ CONSTRAINT name ] CHECK (...) [ NOT VALID ]`
    AddConstraint {
        constraint: TableConstraint,
        not_valid: bool,
    },
    /// `ADD [ CONSTRAINT name ] NOT NULL [ NOT VALID ]`
    AddNotNull {
        constraint_name: Option<Ident>,
        not_valid: bool,
    },
    /// `DROP CONSTRAINT [ IF EXISTS ] name [ RESTRICT | CASCADE ]`
    DropConstraint {
        if_exists: bool,
        name: Ident,
        drop_behavior: Option<DropBehavior>,
    },
    /// `RENAME CONSTRAINT name TO new_name`
    RenameConstraint {
        old_name: Ident,
        new_name: Ident,
    },
    /// `VALIDATE CONSTRAINT name`
    ValidateConstraint {
        name: Ident,
    },
}

impl fmt::Display for AlterDomainAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::SetDefault { value } => write!(f, "SET DEFAULT {value}"),
            Self::DropDefault => write!(f, "DROP DEFAULT"),
            Self::SetNotNull => write!(f, "SET NOT NULL"),
            Self::DropNotNull => write!(f, "DROP NOT NULL"),
            Self::AddConstraint {
                constraint,
                not_valid,
            } => {
                write!(f, "ADD {constraint}")?;
                if *not_valid {
                    write!(f, " NOT VALID")?;
                }
                Ok(())
            }
            Self::AddNotNull {
                constraint_name,
                not_valid,
            } => {
                write!(f, "ADD ")?;
                if let Some(name) = constraint_name {
                    write!(f, "CONSTRAINT {name} ")?;
                }
                write!(f, "NOT NULL")?;
                if *not_valid {
                    write!(f, " NOT VALID")?;
                }
                Ok(())
            }
            Self::DropConstraint {
                if_exists,
                name,
                drop_behavior,
            } => {
                write!(f, "DROP CONSTRAINT ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{name}")?;
                if let Some(behavior) = drop_behavior {
                    write!(f, " {behavior}")?;
                }
                Ok(())
            }
            Self::RenameConstraint { old_name, new_name } => {
                write!(f, "RENAME CONSTRAINT {old_name} TO {new_name}")
            }
            Self::ValidateConstraint { name } => write!(f, "VALIDATE CONSTRAINT {name}"),
        }
    }
}

/// An action of `ALTER EVENT TRIGGER`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterEventTriggerAction {
    Object(AlterObjectAction),
    /// `DISABLE`
    Disable,
    /// `ENABLE [ REPLICA | ALWAYS ]`
    Enable {
        mode: Option<EventTriggerEnableMode>,
    },
}

impl fmt::Display for AlterEventTriggerAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::Disable => write!(f, "DISABLE"),
            Self::Enable { mode: None } => write!(f, "ENABLE"),
            Self::Enable { mode: Some(mode) } => write!(f, "ENABLE {mode}"),
        }
    }
}

/// The firing mode of `ALTER EVENT TRIGGER ... ENABLE`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum EventTriggerEnableMode {
    Replica,
    Always,
}

impl fmt::Display for EventTriggerEnableMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Replica => "REPLICA",
            Self::Always => "ALWAYS",
        })
    }
}

/// An action of `ALTER GROUP`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterGroupAction {
    /// `ADD USER user_name [, ...]`
    AddUser { members: Vec<Owner> },
    /// `DROP USER user_name [, ...]`
    DropUser { members: Vec<Owner> },
    /// `RENAME TO new_name`
    RenameTo { new_name: Ident },
}

impl fmt::Display for AlterGroupAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AddUser { members } => {
                write!(f, "ADD USER {}", display_comma_separated(members))
            }
            Self::DropUser { members } => {
                write!(f, "DROP USER {}", display_comma_separated(members))
            }
            Self::RenameTo { new_name } => write!(f, "RENAME TO {new_name}"),
        }
    }
}

/// The operand types of `ALTER OPERATOR name ( left_type, right_type )`,
/// where either side may be `NONE` for a unary operator.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterOperatorArgs {
    pub left: Option<DataType>,
    pub right: Option<DataType>,
}

impl fmt::Display for AlterOperatorArgs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(")?;
        match &self.left {
            Some(data_type) => write!(f, "{data_type}")?,
            None => write!(f, "NONE")?,
        }
        write!(f, ", ")?;
        match &self.right {
            Some(data_type) => write!(f, "{data_type}")?,
            None => write!(f, "NONE")?,
        }
        write!(f, ")")
    }
}

/// An action of `ALTER OPERATOR`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterOperatorAction {
    Object(AlterObjectAction),
    /// `SET ( RESTRICT = ... , JOIN = ... , COMMUTATOR = ... , HASHES, ... )`
    SetOptions {
        options: Vec<DefinitionElement>,
    },
}

impl fmt::Display for AlterOperatorAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::SetOptions { options } => {
                write!(f, "SET ({})", display_comma_separated(options))
            }
        }
    }
}

/// The routine flavor named by an `ALTER FUNCTION` / `ALTER PROCEDURE` /
/// `ALTER ROUTINE` statement.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RoutineKind {
    Function,
    Procedure,
    Routine,
}

impl fmt::Display for RoutineKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Function => "FUNCTION",
            Self::Procedure => "PROCEDURE",
            Self::Routine => "ROUTINE",
        })
    }
}

/// An action of `ALTER FUNCTION` / `ALTER PROCEDURE` / `ALTER ROUTINE`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterRoutineAction {
    Object(AlterObjectAction),
    /// `[ NO ] DEPENDS ON EXTENSION extension_name`
    DependsOnExtension {
        no: bool,
        extension_name: Ident,
    },
    /// `action [ ... ] [ RESTRICT ]`
    Options {
        options: Vec<RoutineOption>,
        restrict: bool,
    },
}

impl fmt::Display for AlterRoutineAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::DependsOnExtension { no, extension_name } => {
                if *no {
                    write!(f, "NO ")?;
                }
                write!(f, "DEPENDS ON EXTENSION {extension_name}")
            }
            Self::Options { options, restrict } => {
                let mut first = true;
                for option in options {
                    if !first {
                        write!(f, " ")?;
                    }
                    first = false;
                    write!(f, "{option}")?;
                }
                if *restrict {
                    write!(f, " RESTRICT")?;
                }
                Ok(())
            }
        }
    }
}

/// A single `action` of `ALTER FUNCTION` / `ALTER PROCEDURE` / `ALTER ROUTINE`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RoutineOption {
    /// `CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT`
    CalledOnNull(FunctionCalledOnNull),
    /// `IMMUTABLE | STABLE | VOLATILE`
    Behavior(FunctionBehavior),
    /// `[ NOT ] LEAKPROOF`
    Leakproof(bool),
    /// `[ EXTERNAL ] SECURITY { INVOKER | DEFINER }`
    Security {
        external: bool,
        security: ProcedureSecurity,
    },
    /// `PARALLEL { UNSAFE | RESTRICTED | SAFE }`
    Parallel(FunctionParallel),
    /// `COST execution_cost`
    Cost(Expr),
    /// `ROWS result_rows`
    Rows(Expr),
    /// `SUPPORT support_function`
    Support(ObjectName),
    /// `SET configuration_parameter { TO | = } { value | DEFAULT }`
    /// or `SET configuration_parameter FROM CURRENT`
    Set(ProcedureSetConfig),
    /// `RESET configuration_parameter` or `RESET ALL`
    Reset(ResetConfig),
}

impl fmt::Display for RoutineOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::CalledOnNull(value) => write!(f, "{value}"),
            Self::Behavior(value) => write!(f, "{value}"),
            Self::Leakproof(true) => write!(f, "LEAKPROOF"),
            Self::Leakproof(false) => write!(f, "NOT LEAKPROOF"),
            Self::Security { external, security } => {
                if *external {
                    write!(f, "EXTERNAL ")?;
                }
                write!(f, "{security}")
            }
            Self::Parallel(value) => write!(f, "{value}"),
            Self::Cost(value) => write!(f, "COST {value}"),
            Self::Rows(value) => write!(f, "ROWS {value}"),
            Self::Support(name) => write!(f, "SUPPORT {name}"),
            Self::Set(config) => write!(f, "{config}"),
            Self::Reset(ResetConfig::ALL) => write!(f, "RESET ALL"),
            Self::Reset(ResetConfig::ConfigName(name)) => write!(f, "RESET {name}"),
        }
    }
}

/// An action of `ALTER STATISTICS`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterStatisticsAction {
    Object(AlterObjectAction),
    /// `SET STATISTICS { new_target | DEFAULT }`
    SetStatistics {
        target: StatisticsTarget,
    },
}

impl fmt::Display for AlterStatisticsAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::SetStatistics { target } => write!(f, "SET STATISTICS {target}"),
        }
    }
}

/// The target of `SET STATISTICS`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum StatisticsTarget {
    /// `DEFAULT`
    Default,
    /// A signed integer target.
    Value(Expr),
}

impl fmt::Display for StatisticsTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Default => write!(f, "DEFAULT"),
            Self::Value(value) => write!(f, "{value}"),
        }
    }
}

/// An action of `ALTER TEXT SEARCH CONFIGURATION`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTextSearchConfigurationAction {
    Object(AlterObjectAction),
    /// `ADD MAPPING FOR token_type [, ...] WITH dictionary_name [, ...]`
    AddMapping {
        token_types: Vec<Ident>,
        dictionaries: Vec<ObjectName>,
    },
    /// `ALTER MAPPING FOR token_type [, ...] WITH dictionary_name [, ...]`
    AlterMapping {
        token_types: Vec<Ident>,
        dictionaries: Vec<ObjectName>,
    },
    /// `ALTER MAPPING [ FOR token_type [, ...] ] REPLACE old_dictionary WITH new_dictionary`
    ReplaceDictionary {
        token_types: Option<Vec<Ident>>,
        old_dictionary: ObjectName,
        new_dictionary: ObjectName,
    },
    /// `DROP MAPPING [ IF EXISTS ] FOR token_type [, ...]`
    DropMapping {
        if_exists: bool,
        token_types: Vec<Ident>,
    },
}

impl fmt::Display for AlterTextSearchConfigurationAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::AddMapping {
                token_types,
                dictionaries,
            } => write!(
                f,
                "ADD MAPPING FOR {} WITH {}",
                display_comma_separated(token_types),
                display_comma_separated(dictionaries)
            ),
            Self::AlterMapping {
                token_types,
                dictionaries,
            } => write!(
                f,
                "ALTER MAPPING FOR {} WITH {}",
                display_comma_separated(token_types),
                display_comma_separated(dictionaries)
            ),
            Self::ReplaceDictionary {
                token_types,
                old_dictionary,
                new_dictionary,
            } => {
                write!(f, "ALTER MAPPING ")?;
                if let Some(token_types) = token_types {
                    write!(f, "FOR {} ", display_comma_separated(token_types))?;
                }
                write!(f, "REPLACE {old_dictionary} WITH {new_dictionary}")
            }
            Self::DropMapping {
                if_exists,
                token_types,
            } => {
                write!(f, "DROP MAPPING ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "FOR {}", display_comma_separated(token_types))
            }
        }
    }
}

/// An action of `ALTER TEXT SEARCH DICTIONARY`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTextSearchDictionaryAction {
    Object(AlterObjectAction),
    /// `( option [ = value ] [, ...] )`
    SetOptions {
        options: Vec<DefinitionElement>,
    },
}

impl fmt::Display for AlterTextSearchDictionaryAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Object(action) => write!(f, "{action}"),
            Self::SetOptions { options } => {
                write!(f, "({})", display_comma_separated(options))
            }
        }
    }
}

/// An action of `ALTER TRIGGER`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTriggerAction {
    /// `RENAME TO new_name`
    RenameTo { new_name: Ident },
    /// `[ NO ] DEPENDS ON EXTENSION extension_name`
    DependsOnExtension { no: bool, extension_name: Ident },
}

impl fmt::Display for AlterTriggerAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::RenameTo { new_name } => write!(f, "RENAME TO {new_name}"),
            Self::DependsOnExtension { no, extension_name } => {
                if *no {
                    write!(f, "NO ")?;
                }
                write!(f, "DEPENDS ON EXTENSION {extension_name}")
            }
        }
    }
}

/// PostgreSQL's `def_elem`: an option name with an optional value, used by
/// `ALTER OPERATOR ... SET (...)` and `ALTER TEXT SEARCH DICTIONARY (...)`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DefinitionElement {
    pub name: Ident,
    pub value: Option<DefinitionValue>,
}

impl fmt::Display for DefinitionElement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(value) = &self.value {
            write!(f, " = {value}")?;
        }
        Ok(())
    }
}

/// The value of a [`DefinitionElement`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DefinitionValue {
    /// `NONE`
    None,
    /// A possibly qualified name, such as a function or type name.
    Name(ObjectName),
    /// An operator symbol, such as `||>` or `OPERATOR(public.=)`.
    Operator(ObjectName),
    /// A numeric or string literal.
    Literal(Expr),
}

impl fmt::Display for DefinitionValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::None => write!(f, "NONE"),
            Self::Name(name) => write!(f, "{name}"),
            Self::Operator(name) => write!(f, "{name}"),
            Self::Literal(value) => write!(f, "{value}"),
        }
    }
}

/// A non-configuration `ALTER DATABASE` option, as accepted by PostgreSQL's
/// `createdb_opt_list`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterDatabaseOption {
    /// `CONNECTION LIMIT { integer | DEFAULT }`
    ConnectionLimit(DatabaseOptionValue),
    /// `option_name [ = ] { value | DEFAULT }`, e.g. `ALLOW_CONNECTIONS false`
    Named {
        name: Ident,
        value: DatabaseOptionValue,
    },
}

impl fmt::Display for AlterDatabaseOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ConnectionLimit(value) => write!(f, "CONNECTION LIMIT {value}"),
            Self::Named { name, value } => write!(f, "{name} {value}"),
        }
    }
}

/// The value of an [`AlterDatabaseOption`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DatabaseOptionValue {
    /// `DEFAULT`
    Default,
    /// A numeric, string or boolean value.
    Value(Expr),
}

impl fmt::Display for DatabaseOptionValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Default => write!(f, "DEFAULT"),
            Self::Value(value) => write!(f, "{value}"),
        }
    }
}

/// A non-option `ALTER SEQUENCE` action.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterSequenceOperation {
    /// `RENAME TO new_name`
    RenameTo { new_name: Ident },
    /// `OWNER TO new_owner`
    OwnerTo { new_owner: Owner },
    /// `SET SCHEMA new_schema`
    SetSchema { new_schema: ObjectName },
    /// `SET LOGGED`
    SetLogged,
    /// `SET UNLOGGED`
    SetUnlogged,
}

impl fmt::Display for AlterSequenceOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::RenameTo { new_name } => write!(f, "RENAME TO {new_name}"),
            Self::OwnerTo { new_owner } => write!(f, "OWNER TO {new_owner}"),
            Self::SetSchema { new_schema } => write!(f, "SET SCHEMA {new_schema}"),
            Self::SetLogged => write!(f, "SET LOGGED"),
            Self::SetUnlogged => write!(f, "SET UNLOGGED"),
        }
    }
}

/// A single attribute action of `ALTER TYPE name action [, ...]`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterTypeAction {
    /// `ADD ATTRIBUTE name data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]`
    AddAttribute {
        name: Ident,
        data_type: DataType,
        collation: Option<ObjectName>,
        drop_behavior: Option<DropBehavior>,
    },
    /// `DROP ATTRIBUTE [ IF EXISTS ] name [ CASCADE | RESTRICT ]`
    DropAttribute {
        if_exists: bool,
        name: Ident,
        drop_behavior: Option<DropBehavior>,
    },
    /// `ALTER ATTRIBUTE name [ SET DATA ] TYPE data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]`
    AlterAttribute {
        name: Ident,
        /// Set when the statement spells the optional `SET DATA` keywords.
        had_set_data: bool,
        data_type: DataType,
        collation: Option<ObjectName>,
        drop_behavior: Option<DropBehavior>,
    },
}

impl fmt::Display for AlterTypeAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AddAttribute {
                name,
                data_type,
                collation,
                drop_behavior,
            } => {
                write!(f, "ADD ATTRIBUTE {name} {data_type}")?;
                if let Some(collation) = collation {
                    write!(f, " COLLATE {collation}")?;
                }
                if let Some(behavior) = drop_behavior {
                    write!(f, " {behavior}")?;
                }
                Ok(())
            }
            Self::DropAttribute {
                if_exists,
                name,
                drop_behavior,
            } => {
                write!(f, "DROP ATTRIBUTE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                write!(f, "{name}")?;
                if let Some(behavior) = drop_behavior {
                    write!(f, " {behavior}")?;
                }
                Ok(())
            }
            Self::AlterAttribute {
                name,
                had_set_data,
                data_type,
                collation,
                drop_behavior,
            } => {
                write!(f, "ALTER ATTRIBUTE {name} ")?;
                if *had_set_data {
                    write!(f, "SET DATA ")?;
                }
                write!(f, "TYPE {data_type}")?;
                if let Some(collation) = collation {
                    write!(f, " COLLATE {collation}")?;
                }
                if let Some(behavior) = drop_behavior {
                    write!(f, " {behavior}")?;
                }
                Ok(())
            }
        }
    }
}

/// A single action of `ALTER MATERIALIZED VIEW name action [, ...]`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AlterMaterializedViewAction {
    /// `ALTER [ COLUMN ] column_name SET STATISTICS integer`
    AlterColumnSetStatistics {
        column_name: Ident,
        statistics: Expr,
    },
    /// `ALTER [ COLUMN ] column_name SET ( attribute_option = value [, ...] )`
    AlterColumnSetOptions {
        column_name: Ident,
        options: Vec<SqlOption>,
    },
    /// `ALTER [ COLUMN ] column_name RESET ( attribute_option [, ...] )`
    AlterColumnResetOptions {
        column_name: Ident,
        options: Vec<Ident>,
    },
    /// `ALTER [ COLUMN ] column_name SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN | DEFAULT }`
    AlterColumnSetStorage { column_name: Ident, storage: Ident },
    /// `ALTER [ COLUMN ] column_name SET COMPRESSION compression_method`
    AlterColumnSetCompression {
        column_name: Ident,
        compression: Ident,
    },
    /// `CLUSTER ON index_name`
    ClusterOn { index_name: Ident },
    /// `SET WITHOUT CLUSTER`
    SetWithoutCluster,
    /// `SET ACCESS METHOD new_access_method`
    SetAccessMethod { access_method: Ident },
    /// `SET TABLESPACE new_tablespace`
    SetTablespace { tablespace_name: Ident },
    /// `SET ( storage_parameter [= value] [, ...] )`
    SetOptions { options: Vec<SqlOption> },
    /// `RESET ( storage_parameter [, ...] )`
    ResetOptions { options: Vec<Ident> },
    /// `OWNER TO new_owner`
    OwnerTo { new_owner: Owner },
}

impl fmt::Display for AlterMaterializedViewAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AlterColumnSetStatistics {
                column_name,
                statistics,
            } => write!(f, "ALTER COLUMN {column_name} SET STATISTICS {statistics}"),
            Self::AlterColumnSetOptions {
                column_name,
                options,
            } => write!(
                f,
                "ALTER COLUMN {column_name} SET ({})",
                display_comma_separated(options)
            ),
            Self::AlterColumnResetOptions {
                column_name,
                options,
            } => write!(
                f,
                "ALTER COLUMN {column_name} RESET ({})",
                display_comma_separated(options)
            ),
            Self::AlterColumnSetStorage {
                column_name,
                storage,
            } => write!(f, "ALTER COLUMN {column_name} SET STORAGE {storage}"),
            Self::AlterColumnSetCompression {
                column_name,
                compression,
            } => write!(
                f,
                "ALTER COLUMN {column_name} SET COMPRESSION {compression}"
            ),
            Self::ClusterOn { index_name } => write!(f, "CLUSTER ON {index_name}"),
            Self::SetWithoutCluster => write!(f, "SET WITHOUT CLUSTER"),
            Self::SetAccessMethod { access_method } => {
                write!(f, "SET ACCESS METHOD {access_method}")
            }
            Self::SetTablespace { tablespace_name } => {
                write!(f, "SET TABLESPACE {tablespace_name}")
            }
            Self::SetOptions { options } => {
                write!(f, "SET ({})", display_comma_separated(options))
            }
            Self::ResetOptions { options } => {
                write!(f, "RESET ({})", display_comma_separated(options))
            }
            Self::OwnerTo { new_owner } => write!(f, "OWNER TO {new_owner}"),
        }
    }
}
