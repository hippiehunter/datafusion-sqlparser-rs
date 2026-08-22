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

//! AST types for PostgreSQL table-shaped DDL: `CREATE TABLE` and its
//! relatives, `ALTER TABLE`, foreign tables and their options, domains and
//! typed tables.

use core::fmt;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{
    display_comma_separated, ColumnOption, ColumnOptionDef, ConstraintCharacteristics,
    CreateTableLike, Expr, GeneratedAs, Ident, ObjectName, Owner, SequenceOptions, Spanned,
    TableConstraint,
};
use crate::tokenizer::Span;

/// `SET STATISTICS { <integer> | DEFAULT }` on an `ALTER TABLE ... ALTER COLUMN`.
///
/// [PostgreSQL](https://www.postgresql.org/docs/current/sql-altertable.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetStatisticsValue {
    /// A signed target, e.g. `1000` or `-1`.
    Value(i64),
    /// `DEFAULT`
    Default,
}

impl fmt::Display for SetStatisticsValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetStatisticsValue::Value(value) => write!(f, "{value}"),
            SetStatisticsValue::Default => f.write_str("DEFAULT"),
        }
    }
}

/// A single `alter_identity_column_option` of
/// `ALTER TABLE ... ALTER COLUMN <col> <options>`.
///
/// [PostgreSQL](https://www.postgresql.org/docs/current/sql-altertable.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IdentityColumnOption {
    /// `RESTART [ [ WITH ] <value> ]`
    Restart { with: bool, value: Option<Expr> },
    /// `SET <sequence option>`
    SetSequenceOption(SequenceOptions),
    /// `SET GENERATED { ALWAYS | BY DEFAULT }`
    SetGenerated(GeneratedAs),
}

impl fmt::Display for IdentityColumnOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IdentityColumnOption::Restart { with, value } => {
                f.write_str("RESTART")?;
                if *with {
                    f.write_str(" WITH")?;
                }
                if let Some(value) = value {
                    write!(f, " {value}")?;
                }
                Ok(())
            }
            // `SequenceOptions` renders with a leading space.
            IdentityColumnOption::SetSequenceOption(option) => write!(f, "SET{option}"),
            IdentityColumnOption::SetGenerated(generated_as) => {
                write!(f, "SET GENERATED {}", GeneratedWhen(generated_as))
            }
        }
    }
}

/// Renders the `generated_when` production (`ALWAYS` / `BY DEFAULT`).
pub(crate) struct GeneratedWhen<'a>(pub &'a GeneratedAs);

impl fmt::Display for GeneratedWhen<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self.0 {
            GeneratedAs::ByDefault => "BY DEFAULT",
            GeneratedAs::ByDefaultOnNull => "BY DEFAULT ON NULL",
            _ => "ALWAYS",
        })
    }
}

/// `SET COMPRESSION { <method> | DEFAULT }`, also usable as a column-definition
/// clause (`<col> <type> COMPRESSION <method>`).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ColumnCompression {
    /// A named compression method, e.g. `pglz` or `lz4`.
    Method(Ident),
    /// `DEFAULT`
    Default,
}

impl fmt::Display for ColumnCompression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColumnCompression::Method(name) => write!(f, "{name}"),
            ColumnCompression::Default => f.write_str("DEFAULT"),
        }
    }
}

/// A `reloption_elem`: a relation or attribute storage parameter, whose name
/// may be qualified (`toast.autovacuum_enabled`) and whose value may be absent
/// (`WITH (oids)`).
///
/// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RelationOption {
    pub name: ObjectName,
    pub value: Option<Expr>,
}

impl fmt::Display for RelationOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(value) = &self.value {
            write!(f, " = {value}")?;
        }
        Ok(())
    }
}

/// A standalone constraint attribute, which PostgreSQL parses as its own
/// element of a column's qualifier list rather than as part of the constraint
/// that precedes it: `b int CHECK (b > 0) NOT ENFORCED`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ConstraintAttribute {
    Deferrable,
    NotDeferrable,
    InitiallyDeferred,
    InitiallyImmediate,
    Enforced,
    NotEnforced,
}

impl fmt::Display for ConstraintAttribute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ConstraintAttribute::Deferrable => "DEFERRABLE",
            ConstraintAttribute::NotDeferrable => "NOT DEFERRABLE",
            ConstraintAttribute::InitiallyDeferred => "INITIALLY DEFERRED",
            ConstraintAttribute::InitiallyImmediate => "INITIALLY IMMEDIATE",
            ConstraintAttribute::Enforced => "ENFORCED",
            ConstraintAttribute::NotEnforced => "NOT ENFORCED",
        })
    }
}

/// The index-related tail shared by `UNIQUE`, `PRIMARY KEY` and `EXCLUDE`
/// table constraints.
///
/// ```sql
/// UNIQUE USING INDEX <index>
/// UNIQUE (<cols>) INCLUDE (<cols>) WITH (<params>) USING INDEX TABLESPACE <ts>
/// ```
#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IndexConstraintDetails {
    /// `USING INDEX <index_name>`: adopt an existing index as the constraint's index.
    pub using_index: Option<Ident>,
    /// `INCLUDE (<column>, ...)`: non-key payload columns.
    pub include: Vec<Ident>,
    /// `WITH (<storage parameter>, ...)`
    pub with_options: Vec<RelationOption>,
    /// `USING INDEX TABLESPACE <name>`
    pub index_tablespace: Option<Ident>,
}

impl IndexConstraintDetails {
    pub fn is_empty(&self) -> bool {
        self.using_index.is_none()
            && self.include.is_empty()
            && self.with_options.is_empty()
            && self.index_tablespace.is_none()
    }

    pub fn into_option(self) -> Option<Self> {
        if self.is_empty() {
            None
        } else {
            Some(self)
        }
    }

    /// Renders the part that follows the constraint's column list.
    pub fn fmt_tail(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.include.is_empty() {
            write!(f, " INCLUDE ({})", display_comma_separated(&self.include))?;
        }
        if !self.with_options.is_empty() {
            write!(f, " WITH ({})", display_comma_separated(&self.with_options))?;
        }
        if let Some(tablespace) = &self.index_tablespace {
            write!(f, " USING INDEX TABLESPACE {tablespace}")?;
        }
        Ok(())
    }
}

/// A PostgreSQL 18 table-level `NOT NULL` constraint:
/// `[ CONSTRAINT <name> ] NOT NULL <column> [ NO INHERIT ]`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct NotNullConstraint {
    pub name: Option<Ident>,
    pub column: Ident,
    pub no_inherit: bool,
}

impl fmt::Display for NotNullConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "CONSTRAINT {name} ")?;
        }
        write!(f, "NOT NULL {}", self.column)?;
        if self.no_inherit {
            f.write_str(" NO INHERIT")?;
        }
        Ok(())
    }
}

impl Spanned for NotNullConstraint {
    fn span(&self) -> Span {
        self.column
            .span
            .union_opt(&self.name.as_ref().map(|name| name.span))
    }
}

/// A `LIKE <source> [ { INCLUDING | EXCLUDING } <option> ]...` element inside a
/// `CREATE TABLE` column list, together with the number of column definitions
/// that precede it.
///
/// PostgreSQL copies the source table's columns into the new table at the point
/// the clause appears, so the position is part of the statement's meaning.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableLikeElement {
    /// How many entries of `CreateTable::columns` come before this clause.
    pub after_columns: u32,
    pub source: CreateTableLike,
}

impl fmt::Display for TableLikeElement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.source)
    }
}

/// A `CREATE TABLE ... OF <type> ( ... )` element.
///
/// ```sql
/// CREATE TABLE persons OF person_type (id WITH OPTIONS PRIMARY KEY, UNIQUE (name))
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TypedTableElement {
    /// `<column> [ WITH OPTIONS ] <column constraint>...`
    Column(TypedTableColumn),
    /// A table constraint.
    Constraint(TableConstraint),
}

impl fmt::Display for TypedTableElement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TypedTableElement::Column(column) => column.fmt(f),
            TypedTableElement::Constraint(constraint) => constraint.fmt(f),
        }
    }
}

/// A column of a typed table, which names a column of the underlying composite
/// type and attaches constraints to it. The column has no type of its own.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TypedTableColumn {
    pub name: Ident,
    /// Whether the redundant `WITH OPTIONS` spelling was used.
    pub with_options: bool,
    pub options: Vec<ColumnOptionDef>,
}

impl fmt::Display for TypedTableColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if self.with_options {
            f.write_str(" WITH OPTIONS")?;
        }
        for option in &self.options {
            write!(f, " {option}")?;
        }
        Ok(())
    }
}

/// `CREATE TABLE <name> [ (<column alias>, ...) ] AS EXECUTE <name> [ (<params>) ]`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateTableAsExecute {
    pub name: Ident,
    pub parameters: Vec<Expr>,
}

impl fmt::Display for CreateTableAsExecute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXECUTE {}", self.name)?;
        if !self.parameters.is_empty() {
            write!(f, "({})", display_comma_separated(&self.parameters))?;
        }
        Ok(())
    }
}

/// A domain constraint as PostgreSQL parses it: the full column-qualifier
/// grammar, so that forms which are syntactically legal but semantically
/// rejected (`CONSTRAINT c GENERATED BY DEFAULT AS IDENTITY`) still reach the
/// server as a typed node.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DomainConstraint {
    pub name: Option<Ident>,
    pub option: ColumnOption,
    /// `NO INHERIT` written after `NOT NULL` or `CHECK (...)`.
    pub no_inherit: bool,
}

impl fmt::Display for DomainConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "CONSTRAINT {name} ")?;
        }
        write!(f, "{}", self.option)?;
        if self.no_inherit {
            f.write_str(" NO INHERIT")?;
        }
        Ok(())
    }
}

/// `ALTER TABLE ... ALTER CONSTRAINT <name> ...` — the inheritability half of
/// the action, which PostgreSQL spells either `INHERIT` or `NO INHERIT`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ConstraintInheritability {
    Inherit,
    NoInherit,
}

impl fmt::Display for ConstraintInheritability {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ConstraintInheritability::Inherit => "INHERIT",
            ConstraintInheritability::NoInherit => "NO INHERIT",
        })
    }
}

/// `ALTER TABLE ... ALTER CONSTRAINT <name> <attributes>`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterConstraint {
    pub name: Ident,
    pub characteristics: Option<ConstraintCharacteristics>,
    pub inheritability: Option<ConstraintInheritability>,
}

impl fmt::Display for AlterConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ALTER CONSTRAINT {}", self.name)?;
        if let Some(characteristics) = &self.characteristics {
            write!(f, " {characteristics}")?;
        }
        if let Some(inheritability) = &self.inheritability {
            write!(f, " {inheritability}")?;
        }
        Ok(())
    }
}

/// `SET ACCESS METHOD { <name> | DEFAULT }`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetAccessMethod {
    Name(Ident),
    Default,
}

impl fmt::Display for SetAccessMethod {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetAccessMethod::Name(name) => write!(f, "{name}"),
            SetAccessMethod::Default => f.write_str("DEFAULT"),
        }
    }
}

/// `WITH [ NO ] DATA` on `CREATE TABLE ... AS`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateTableWithData {
    WithData,
    WithNoData,
}

impl fmt::Display for CreateTableWithData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            CreateTableWithData::WithData => "WITH DATA",
            CreateTableWithData::WithNoData => "WITH NO DATA",
        })
    }
}

/// `WITH [ CASCADED | LOCAL ] CHECK OPTION` on `CREATE VIEW`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ViewCheckOption {
    /// `WITH CHECK OPTION` — PostgreSQL records this as `CASCADED`, but the
    /// spelling is preserved so the statement round-trips as written.
    Unqualified,
    Cascaded,
    Local,
}

impl fmt::Display for ViewCheckOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ViewCheckOption::Unqualified => "WITH CHECK OPTION",
            ViewCheckOption::Cascaded => "WITH CASCADED CHECK OPTION",
            ViewCheckOption::Local => "WITH LOCAL CHECK OPTION",
        })
    }
}

/// One `INCLUDING`/`EXCLUDING` item of a `CREATE TABLE ... LIKE` clause.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableLikeOptionKind {
    Comments,
    Compression,
    Constraints,
    Defaults,
    Generated,
    Identity,
    Indexes,
    Statistics,
    Storage,
    All,
}

impl fmt::Display for TableLikeOptionKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            TableLikeOptionKind::Comments => "COMMENTS",
            TableLikeOptionKind::Compression => "COMPRESSION",
            TableLikeOptionKind::Constraints => "CONSTRAINTS",
            TableLikeOptionKind::Defaults => "DEFAULTS",
            TableLikeOptionKind::Generated => "GENERATED",
            TableLikeOptionKind::Identity => "IDENTITY",
            TableLikeOptionKind::Indexes => "INDEXES",
            TableLikeOptionKind::Statistics => "STATISTICS",
            TableLikeOptionKind::Storage => "STORAGE",
            TableLikeOptionKind::All => "ALL",
        })
    }
}

/// ```sql
/// ALTER TABLE ALL IN TABLESPACE <name> [ OWNED BY <role>, ... ]
///     SET TABLESPACE <new_tablespace> [ NOWAIT ]
/// ```
///
/// [PostgreSQL](https://www.postgresql.org/docs/current/sql-altertable.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterTableAllInTablespace {
    pub tablespace: Ident,
    pub owned_by: Vec<Owner>,
    pub new_tablespace: Ident,
    pub nowait: bool,
}

impl fmt::Display for AlterTableAllInTablespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ALTER TABLE ALL IN TABLESPACE {}", self.tablespace)?;
        if !self.owned_by.is_empty() {
            write!(f, " OWNED BY {}", display_comma_separated(&self.owned_by))?;
        }
        write!(f, " SET TABLESPACE {}", self.new_tablespace)?;
        if self.nowait {
            f.write_str(" NOWAIT")?;
        }
        Ok(())
    }
}
