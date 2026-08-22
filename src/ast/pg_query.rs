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

//! AST types for the PostgreSQL query and DML grammar: grouping elements,
//! table function column definition lists, `ON CONFLICT` inference clauses,
//! `RETURNING WITH (OLD/NEW ...)` aliases and column targets that carry an
//! indirection.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use super::{display_comma_separated, AccessExpr, DataType, Expr, Ident, ObjectName};
use crate::ast::OrderByOptions;

/// The set quantifier that may precede the grouping element list of a
/// PostgreSQL `GROUP BY` clause.
///
/// ```sql
/// GROUP BY DISTINCT ROLLUP(a, b), ROLLUP(a)
/// ```
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GroupBySetQuantifier {
    All,
    Distinct,
}

impl fmt::Display for GroupBySetQuantifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GroupBySetQuantifier::All => f.write_str("ALL"),
            GroupBySetQuantifier::Distinct => f.write_str("DISTINCT"),
        }
    }
}

/// A column of a PostgreSQL `INSERT` target list or of an `UPDATE` assignment,
/// which may select into a field or subscript of the column rather than
/// replacing the whole column.
///
/// ```sql
/// INSERT INTO t (f2[1], f3.if1, f3.if2[1]) VALUES (...)
/// UPDATE t SET a[1:2] = '{16,25}', js['a']['b'] = '1'
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ColumnTarget {
    /// The column being written to.
    pub column: ObjectName,
    /// The field selections and subscripts applied to it, in source order.
    pub indirection: Vec<AccessExpr>,
}

impl fmt::Display for ColumnTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.column)?;
        for access in &self.indirection {
            write!(f, "{access}")?;
        }
        Ok(())
    }
}

/// One element of a PostgreSQL `ON CONFLICT` inference clause.
///
/// ```sql
/// ON CONFLICT (lower(fruit) COLLATE "C" text_pattern_ops DESC NULLS LAST)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ConflictIndexElement {
    /// A column name, or a parenthesized index expression.
    pub expr: Expr,
    /// `COLLATE collation`
    pub collation: Option<ObjectName>,
    /// The operator class the index was declared with.
    pub opclass: Option<ObjectName>,
    /// `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`.
    pub options: OrderByOptions,
}

impl fmt::Display for ConflictIndexElement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(collation) = &self.collation {
            write!(f, " COLLATE {collation}")?;
        }
        if let Some(opclass) = &self.opclass {
            write!(f, " {opclass}")?;
        }
        write!(f, "{}", self.options)
    }
}

/// The PostgreSQL `ON CONFLICT` inference clause: the index elements the
/// arbiter index is inferred from, plus its partial-index predicate.
///
/// ```sql
/// INSERT INTO t VALUES (1, 'Blueberry')
///     ON CONFLICT (key) WHERE fruit LIKE '%berry' DO NOTHING
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ConflictInference {
    pub elements: Vec<ConflictIndexElement>,
    /// `WHERE index_predicate`
    pub predicate: Option<Expr>,
}

impl fmt::Display for ConflictInference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({})", display_comma_separated(&self.elements))?;
        if let Some(predicate) = &self.predicate {
            write!(f, " WHERE {predicate}")?;
        }
        Ok(())
    }
}

/// Which version of the row a PostgreSQL 18 `RETURNING WITH (...)` alias names.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ReturningRowVersion {
    Old,
    New,
}

impl fmt::Display for ReturningRowVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReturningRowVersion::Old => f.write_str("OLD"),
            ReturningRowVersion::New => f.write_str("NEW"),
        }
    }
}

/// One `OLD AS name` / `NEW AS name` entry of a PostgreSQL 18
/// `RETURNING WITH ( ... )` clause.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReturningRowAlias {
    pub version: ReturningRowVersion,
    pub alias: Ident,
}

impl fmt::Display for ReturningRowAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS {}", self.version, self.alias)
    }
}

/// One column of a table function's column definition list.
///
/// ```sql
/// SELECT * FROM record_returning_fn(5) AS (a int, b numeric, c text)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableFunctionColumnDef {
    pub name: Ident,
    pub data_type: DataType,
    /// `COLLATE collation`
    pub collation: Option<ObjectName>,
}

impl fmt::Display for TableFunctionColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if let Some(collation) = &self.collation {
            write!(f, " COLLATE {collation}")?;
        }
        Ok(())
    }
}

/// A single function of a PostgreSQL table function reference, with the column
/// definition list it was given, if any.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableFunctionItem {
    pub function: Expr,
    /// `AS (a int, b text)`; empty when the function was given no column
    /// definition list.
    pub column_defs: Vec<TableFunctionColumnDef>,
}

impl fmt::Display for TableFunctionItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.function)?;
        if !self.column_defs.is_empty() {
            write!(f, " AS ({})", display_comma_separated(&self.column_defs))?;
        }
        Ok(())
    }
}

/// The `VERSION` argument of an `XMLROOT` call.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum XmlRootVersion {
    /// `VERSION NO VALUE`
    NoValue,
    /// `VERSION <expr>`
    Version(Expr),
}

impl fmt::Display for XmlRootVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            XmlRootVersion::NoValue => f.write_str("VERSION NO VALUE"),
            XmlRootVersion::Version(expr) => write!(f, "VERSION {expr}"),
        }
    }
}

/// The `STANDALONE` argument of an `XMLROOT` call.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum XmlRootStandalone {
    Yes,
    No,
    /// `STANDALONE NO VALUE`
    NoValue,
}

impl fmt::Display for XmlRootStandalone {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            XmlRootStandalone::Yes => f.write_str("STANDALONE YES"),
            XmlRootStandalone::No => f.write_str("STANDALONE NO"),
            XmlRootStandalone::NoValue => f.write_str("STANDALONE NO VALUE"),
        }
    }
}
