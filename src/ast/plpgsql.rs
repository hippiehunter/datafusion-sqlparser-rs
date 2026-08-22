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

//! AST nodes for PL/pgSQL routine bodies and for the parts of
//! `CREATE FUNCTION` / `CREATE PROCEDURE` that PostgreSQL shares between the
//! two commands.
//!
//! References:
//! - <https://www.postgresql.org/docs/current/plpgsql.html>
//! - <https://www.postgresql.org/docs/current/sql-createfunction.html>

use super::*;

/// A PL/pgSQL `ASSERT` statement.
///
/// ```sql
/// ASSERT condition [, message]
/// ```
///
/// This is distinct from [`Statement::Assert`], which spells the message with
/// `AS` rather than a comma.
///
/// <https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-ASSERT>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PlpgsqlAssert {
    #[cfg_attr(feature = "visitor", visit(with = "visit_token"))]
    pub assert_token: AttachedToken,
    pub condition: Expr,
    pub message: Option<Expr>,
}

impl fmt::Display for PlpgsqlAssert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ASSERT {}", self.condition)?;
        if let Some(message) = &self.message {
            write!(f, ", {message}")?;
        }
        Ok(())
    }
}

/// A PL/pgSQL assignment whose right hand side is a query rather than a plain
/// expression.
///
/// PL/pgSQL parses `target := rest-of-line` as `SELECT rest-of-line`, so the
/// right hand side is a full select list optionally followed by `FROM`,
/// `WHERE`, set operations, `ORDER BY` and the other query clauses:
///
/// ```sql
/// v := data FROM tab WHERE id = 2;
/// a := id, id FROM tab;
/// ```
///
/// [`SqlPsmQueryAssignment::query`] holds the equivalent `SELECT` statement.
/// A right hand side that is a plain expression is represented by
/// [`SqlPsmAssignment`] instead.
///
/// <https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-ASSIGNMENT>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SqlPsmQueryAssignment {
    pub target: Expr,
    pub query: Box<Query>,
}

impl fmt::Display for SqlPsmQueryAssignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // The source form omits the leading `SELECT` that PL/pgSQL prepends,
        // and PostgreSQL rejects the statement if it is written out.
        let query = self.query.to_string();
        let select_list_and_tail = query.strip_prefix("SELECT ").unwrap_or(query.as_str());
        write!(f, "{} := {select_list_and_tail}", self.target)
    }
}

/// The `BEGIN ATOMIC ... END` body of a SQL-language routine.
///
/// ```sql
/// CREATE FUNCTION f(x int) RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT x * 2; END
/// ```
///
/// Every statement in the block is terminated by a semicolon, including the
/// last one.
///
/// <https://www.postgresql.org/docs/current/sql-createfunction.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AtomicBlock {
    pub statements: Vec<Statement>,
}

impl fmt::Display for AtomicBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BEGIN ATOMIC")?;
        for statement in &self.statements {
            write!(f, " {statement};")?;
        }
        write!(f, " END")
    }
}

/// A routine attribute from PostgreSQL's `createfunc_opt_item` list that has no
/// dedicated field on the statement it belongs to.
///
/// `CREATE FUNCTION` and `CREATE PROCEDURE` share one grammar in PostgreSQL, so
/// both accept the whole list; a procedure that carries a function-only
/// attribute is rejected by the server, not by the parser.
///
/// <https://www.postgresql.org/docs/current/sql-createprocedure.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RoutineAttribute {
    /// `IMMUTABLE` | `STABLE` | `VOLATILE`
    Behavior(FunctionBehavior),
    /// `CALLED ON NULL INPUT` | `RETURNS NULL ON NULL INPUT` | `STRICT`
    CalledOnNull(FunctionCalledOnNull),
    /// `PARALLEL { UNSAFE | RESTRICTED | SAFE }`
    Parallel(FunctionParallel),
    /// `LEAKPROOF` | `NOT LEAKPROOF`
    Leakproof(bool),
    /// `WINDOW`
    Window,
    /// `COST execution_cost`
    Cost(Expr),
    /// `ROWS result_rows`
    Rows(Expr),
    /// `SUPPORT support_function`
    Support(ObjectName),
    /// `TRANSFORM { FOR TYPE type_name } [, ...]`
    Transform(Vec<DataType>),
}

impl fmt::Display for RoutineAttribute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RoutineAttribute::Behavior(behavior) => write!(f, "{behavior}"),
            RoutineAttribute::CalledOnNull(called_on_null) => write!(f, "{called_on_null}"),
            RoutineAttribute::Parallel(parallel) => write!(f, "{parallel}"),
            RoutineAttribute::Leakproof(true) => write!(f, "LEAKPROOF"),
            RoutineAttribute::Leakproof(false) => write!(f, "NOT LEAKPROOF"),
            RoutineAttribute::Window => write!(f, "WINDOW"),
            RoutineAttribute::Cost(cost) => write!(f, "COST {cost}"),
            RoutineAttribute::Rows(rows) => write!(f, "ROWS {rows}"),
            RoutineAttribute::Support(support) => write!(f, "SUPPORT {support}"),
            RoutineAttribute::Transform(types) => {
                write!(f, "TRANSFORM ")?;
                for (index, data_type) in types.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "FOR TYPE {data_type}")?;
                }
                Ok(())
            }
        }
    }
}
