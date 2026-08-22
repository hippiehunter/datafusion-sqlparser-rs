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

//! AST nodes for the PostgreSQL utility and transaction statements:
//! `VACUUM`, `ANALYZE`, `LOCK`, `TABLE`, and the two-phase commit commands.

use super::*;

/// A PostgreSQL `relation_expr`: a table reference that controls whether the
/// command descends into inheritance children.
///
/// ```sql
/// name | name * | ONLY name | ONLY ( name )
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PgRelationExpr {
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub name: ObjectName,
    /// `ONLY name` — do not descend into inheritance children.
    pub only: bool,
    /// `ONLY ( name )` — the parenthesized spelling of `ONLY`.
    pub parenthesized: bool,
    /// `name *` — the explicit spelling of the default, descend into children.
    pub descendants: bool,
}

impl PgRelationExpr {
    /// A plain relation reference with neither `ONLY` nor a trailing `*`.
    pub fn new(name: ObjectName) -> Self {
        Self {
            name,
            only: false,
            parenthesized: false,
            descendants: false,
        }
    }
}

impl fmt::Display for PgRelationExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.only {
            if self.parenthesized {
                return write!(f, "ONLY ({})", self.name);
            }
            return write!(f, "ONLY {}", self.name);
        }
        write!(f, "{}", self.name)?;
        if self.descendants {
            write!(f, " *")?;
        }
        Ok(())
    }
}

/// One entry of the table list of `VACUUM` or `ANALYZE`: a relation plus the
/// optional column list restricting which columns get statistics.
///
/// ```sql
/// [ ONLY ] name [ * ] [ ( column [, ...] ) ]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct VacuumRelation {
    pub relation: PgRelationExpr,
    pub columns: Vec<Ident>,
}

impl fmt::Display for VacuumRelation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.relation)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        Ok(())
    }
}

/// The name of an option inside the parenthesized option list of `VACUUM` or
/// `ANALYZE`.
///
/// PostgreSQL parses any non-reserved word here and only rejects unknown names
/// while executing the command, so [`VacuumOptionName::Other`] carries the
/// names that have no dedicated variant.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum VacuumOptionName {
    Full,
    Freeze,
    Verbose,
    Analyze,
    DisablePageSkipping,
    SkipLocked,
    IndexCleanup,
    ProcessMain,
    ProcessToast,
    Truncate,
    Parallel,
    BufferUsageLimit,
    SkipDatabaseStats,
    OnlyDatabaseStats,
    Other(Ident),
}

impl fmt::Display for VacuumOptionName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Full => "FULL",
            Self::Freeze => "FREEZE",
            Self::Verbose => "VERBOSE",
            Self::Analyze => "ANALYZE",
            Self::DisablePageSkipping => "DISABLE_PAGE_SKIPPING",
            Self::SkipLocked => "SKIP_LOCKED",
            Self::IndexCleanup => "INDEX_CLEANUP",
            Self::ProcessMain => "PROCESS_MAIN",
            Self::ProcessToast => "PROCESS_TOAST",
            Self::Truncate => "TRUNCATE",
            Self::Parallel => "PARALLEL",
            Self::BufferUsageLimit => "BUFFER_USAGE_LIMIT",
            Self::SkipDatabaseStats => "SKIP_DATABASE_STATS",
            Self::OnlyDatabaseStats => "ONLY_DATABASE_STATS",
            Self::Other(name) => return write!(f, "{name}"),
        })
    }
}

/// The value given to a `VACUUM` or `ANALYZE` option.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum VacuumOptionValue {
    /// The `TRUE` or `FALSE` keyword.
    Boolean(bool),
    /// Any other bare word, such as `ON`, `OFF` or the `AUTO` of
    /// `INDEX_CLEANUP AUTO`.
    Word(Ident),
    /// A possibly signed numeric literal, such as the `2` of `PARALLEL 2`.
    Number(Value),
    /// A string literal, such as the `'512 kB'` of `BUFFER_USAGE_LIMIT`.
    StringLiteral(Value),
}

impl VacuumOptionValue {
    /// The value read as a boolean the way PostgreSQL's `defGetBoolean` reads
    /// it, or `None` when it does not spell one.
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(*value),
            Self::Word(word) => match word.value.to_ascii_lowercase().as_str() {
                "true" | "on" => Some(true),
                "false" | "off" => Some(false),
                _ => None,
            },
            Self::Number(Value::Number(value, _)) => match value.to_string().as_str() {
                "1" => Some(true),
                "0" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }
}

impl fmt::Display for VacuumOptionValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Boolean(true) => f.write_str("TRUE"),
            Self::Boolean(false) => f.write_str("FALSE"),
            Self::Word(word) => write!(f, "{word}"),
            Self::Number(value) | Self::StringLiteral(value) => write!(f, "{value}"),
        }
    }
}

/// One entry of the parenthesized option list of `VACUUM` or `ANALYZE`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct VacuumOption {
    pub name: VacuumOptionName,
    pub value: Option<VacuumOptionValue>,
}

impl VacuumOption {
    /// Whether this option turns its flag on: an omitted value means `TRUE`.
    pub fn is_enabled(&self) -> bool {
        match &self.value {
            None => true,
            Some(value) => value.as_boolean().unwrap_or(false),
        }
    }
}

impl fmt::Display for VacuumOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(value) = &self.value {
            write!(f, " {value}")?;
        }
        Ok(())
    }
}

/// Which of the three two-phase commit commands a
/// [`PreparedTransactionStatement`] spells.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum PreparedTransactionAction {
    /// `PREPARE TRANSACTION 'gid'`
    Prepare,
    /// `COMMIT PREPARED 'gid'`
    Commit,
    /// `ROLLBACK PREPARED 'gid'`
    Rollback,
}

impl fmt::Display for PreparedTransactionAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Prepare => "PREPARE TRANSACTION",
            Self::Commit => "COMMIT PREPARED",
            Self::Rollback => "ROLLBACK PREPARED",
        })
    }
}

/// A PostgreSQL two-phase commit command.
///
/// ```sql
/// PREPARE TRANSACTION 'gid'
/// COMMIT PREPARED 'gid'
/// ROLLBACK PREPARED 'gid'
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PreparedTransactionStatement {
    #[cfg_attr(feature = "visitor", visit(with = "visit_token"))]
    pub token: AttachedToken,
    pub action: PreparedTransactionAction,
    /// The global transaction identifier; PostgreSQL requires a string literal.
    pub gid: Value,
}

impl fmt::Display for PreparedTransactionStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.action, self.gid)
    }
}

impl From<PreparedTransactionStatement> for Statement {
    fn from(statement: PreparedTransactionStatement) -> Self {
        Self::PreparedTransaction(statement)
    }
}
