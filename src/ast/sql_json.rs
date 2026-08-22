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

//! AST nodes for the SQL/JSON grammar shared by the SQL standard and
//! PostgreSQL: the `FORMAT JSON` clause, the query-function clauses
//! (`PASSING`, quotes behavior) and the `JSON_TABLE` table function.
//!
//! Reference: <https://www.postgresql.org/docs/18/functions-json.html>

#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{
    display_comma_separated, Box, DataType, Expr, ExprWithAlias, Ident, JsonOnBehavior,
    JsonQueryWrapper, TableAlias, Value,
};

/// The character encoding named by a [`JsonFormatClause`].
///
/// PostgreSQL parses the encoding as a bare name and rejects unknown names
/// during parse analysis, so names outside the standard set are retained.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JsonEncoding {
    Utf8,
    Utf16,
    Utf32,
    /// An encoding name that is not one of the standard ones.
    Custom(Ident),
}

impl fmt::Display for JsonEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonEncoding::Utf8 => f.write_str("UTF8"),
            JsonEncoding::Utf16 => f.write_str("UTF16"),
            JsonEncoding::Utf32 => f.write_str("UTF32"),
            JsonEncoding::Custom(name) => write!(f, "{name}"),
        }
    }
}

/// `FORMAT JSON [ENCODING <encoding>]`
///
/// ```sql
/// SELECT JSON('{"a": 1}' FORMAT JSON ENCODING UTF8)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct JsonFormatClause {
    pub encoding: Option<JsonEncoding>,
}

impl fmt::Display for JsonFormatClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FORMAT JSON")?;
        if let Some(encoding) = &self.encoding {
            write!(f, " ENCODING {encoding}")?;
        }
        Ok(())
    }
}

/// An expression carrying an explicit `FORMAT JSON` clause, the SQL/JSON
/// `<JSON value expression>`.
///
/// ```sql
/// SELECT JSON_SERIALIZE('1' FORMAT JSON)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct JsonFormattedExpr {
    pub expr: Box<Expr>,
    pub format: JsonFormatClause,
}

impl fmt::Display for JsonFormattedExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.expr, self.format)
    }
}

/// Whether a SQL/JSON query function keeps or omits the quotes around a
/// scalar string result.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JsonQuotesBehavior {
    Keep,
    Omit,
}

impl fmt::Display for JsonQuotesBehavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonQuotesBehavior::Keep => f.write_str("KEEP"),
            JsonQuotesBehavior::Omit => f.write_str("OMIT"),
        }
    }
}

/// `{KEEP | OMIT} QUOTES [ON SCALAR STRING]`
///
/// ```sql
/// SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text OMIT QUOTES ON SCALAR STRING)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct JsonQuotesClause {
    pub behavior: JsonQuotesBehavior,
    /// Whether the noise phrase `ON SCALAR STRING` was written.
    pub on_scalar_string: bool,
}

impl fmt::Display for JsonQuotesClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} QUOTES", self.behavior)?;
        if self.on_scalar_string {
            f.write_str(" ON SCALAR STRING")?;
        }
        Ok(())
    }
}

/// The SQL/JSON `JSON_TABLE` table function as defined by the SQL standard
/// and implemented by PostgreSQL.
///
/// ```sql
/// SELECT * FROM JSON_TABLE(
///     jsonb '{"b": 7, "n": [1, 2]}',
///     '$' AS root
///     COLUMNS (
///         b INT PATH '$.b',
///         NESTED PATH '$.n[*]' AS nested COLUMNS (c INT PATH '$')
///     )
///     EMPTY ARRAY ON ERROR
/// ) AS jt
/// ```
///
/// Reference: <https://www.postgresql.org/docs/18/functions-json.html#SQLJSON-TABLE>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SqlJsonTable {
    /// The context item the path expression is evaluated against.
    pub context_item: Expr,
    /// The path expression. PostgreSQL accepts any expression here and
    /// rejects everything but a string constant during parse analysis.
    pub path: Expr,
    /// `AS <json_path_name>` naming the top level path.
    pub path_name: Option<Ident>,
    /// `PASSING <value> AS <varname>, ...`
    pub passing: Vec<ExprWithAlias>,
    pub columns: Vec<SqlJsonTableColumn>,
    /// The table level `<behavior> ON ERROR` clause following `COLUMNS (...)`.
    pub on_error: Option<JsonOnBehavior>,
    pub alias: Option<TableAlias>,
}

impl fmt::Display for SqlJsonTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JSON_TABLE({}, {}", self.context_item, self.path)?;
        if let Some(path_name) = &self.path_name {
            write!(f, " AS {path_name}")?;
        }
        if !self.passing.is_empty() {
            write!(f, " PASSING {}", display_comma_separated(&self.passing))?;
        }
        write!(f, " COLUMNS ({})", display_comma_separated(&self.columns))?;
        if let Some(on_error) = &self.on_error {
            write!(f, " {on_error} ON ERROR")?;
        }
        f.write_str(")")?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

/// A single column definition inside a [`SqlJsonTable`] `COLUMNS` list.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SqlJsonTableColumn {
    /// `<name> FOR ORDINALITY`
    ForOrdinality(Ident),
    /// `<name> <type> [FORMAT JSON] [PATH ...] ...`
    Regular(SqlJsonTableRegularColumn),
    /// `<name> <type> EXISTS [PATH ...] [<behavior> ON ERROR]`
    Exists(SqlJsonTableExistsColumn),
    /// `NESTED [PATH] <path> [AS <name>] COLUMNS (...)`
    Nested(SqlJsonTableNestedColumn),
}

impl fmt::Display for SqlJsonTableColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlJsonTableColumn::ForOrdinality(name) => write!(f, "{name} FOR ORDINALITY"),
            SqlJsonTableColumn::Regular(column) => write!(f, "{column}"),
            SqlJsonTableColumn::Exists(column) => write!(f, "{column}"),
            SqlJsonTableColumn::Nested(column) => write!(f, "{column}"),
        }
    }
}

/// An ordinary [`SqlJsonTable`] column, whose value is the result of applying
/// its path to the row's context item.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SqlJsonTableRegularColumn {
    pub name: Ident,
    pub data_type: DataType,
    pub format: Option<JsonFormatClause>,
    /// `PATH <path>`; when absent the path defaults to the column name.
    pub path: Option<Value>,
    pub wrapper: Option<JsonQueryWrapper>,
    pub quotes: Option<JsonQuotesClause>,
    pub on_empty: Option<JsonOnBehavior>,
    pub on_error: Option<JsonOnBehavior>,
}

impl fmt::Display for SqlJsonTableRegularColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if let Some(format) = &self.format {
            write!(f, " {format}")?;
        }
        if let Some(path) = &self.path {
            write!(f, " PATH {path}")?;
        }
        if let Some(wrapper) = &self.wrapper {
            write!(f, " {wrapper}")?;
        }
        if let Some(quotes) = &self.quotes {
            write!(f, " {quotes}")?;
        }
        if let Some(on_empty) = &self.on_empty {
            write!(f, " {on_empty} ON EMPTY")?;
        }
        if let Some(on_error) = &self.on_error {
            write!(f, " {on_error} ON ERROR")?;
        }
        Ok(())
    }
}

/// A [`SqlJsonTable`] column reporting whether its path matches anything.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SqlJsonTableExistsColumn {
    pub name: Ident,
    pub data_type: DataType,
    /// `PATH <path>`; when absent the path defaults to the column name.
    pub path: Option<Value>,
    pub on_error: Option<JsonOnBehavior>,
}

impl fmt::Display for SqlJsonTableExistsColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} EXISTS", self.name, self.data_type)?;
        if let Some(path) = &self.path {
            write!(f, " PATH {path}")?;
        }
        if let Some(on_error) = &self.on_error {
            write!(f, " {on_error} ON ERROR")?;
        }
        Ok(())
    }
}

/// A nested `COLUMNS` list, which expands a nested array or object into
/// additional rows.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SqlJsonTableNestedColumn {
    pub path: Value,
    /// `AS <json_path_name>` naming the nested path.
    pub path_name: Option<Ident>,
    pub columns: Vec<SqlJsonTableColumn>,
}

impl fmt::Display for SqlJsonTableNestedColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NESTED PATH {}", self.path)?;
        if let Some(path_name) = &self.path_name {
            write!(f, " AS {path_name}")?;
        }
        write!(f, " COLUMNS ({})", display_comma_separated(&self.columns))
    }
}
