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

//! AST types for PostgreSQL object DDL: `COMMENT ON`, the `CREATE` forms of
//! miscellaneous database objects, the typed `DROP` forms whose target is more
//! than a name, and ownership transfer between roles.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{
    display_comma_separated, DataType, DropBehavior, FunctionDesc, Ident, ObjectName,
    OperateFunctionArg, Spanned, SqlOption, TriggerExecBody, ValueWithSpan,
};
use crate::tokenizer::Span;

/// The part of a `COMMENT ON` target that its name alone cannot express.
///
/// `COMMENT ON CAST`, `COMMENT ON TRANSFORM` and `COMMENT ON LARGE OBJECT`
/// name no object at all; for those the statement's `object_name` is empty and
/// this value identifies the target on its own.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CommentObjectDetail {
    /// `CONSTRAINT`/`POLICY`/`RULE`/`TRIGGER` name `ON` relation
    On(ObjectName),
    /// `CONSTRAINT` name `ON DOMAIN` domain
    OnDomain(ObjectName),
    /// `OPERATOR CLASS`/`OPERATOR FAMILY` name `USING` index_method
    Using(Ident),
    /// The parenthesized argument list of a `FUNCTION`, `PROCEDURE` or `ROUTINE`
    Arguments(Vec<OperateFunctionArg>),
    /// The parenthesized argument list of an `AGGREGATE`
    AggregateArguments(AggregateArgs),
    /// The parenthesized operand types of an `OPERATOR`
    OperatorArguments(OperatorOperandTypes),
    /// `CAST (source_type AS target_type)`
    Cast(CastSignature),
    /// `TRANSFORM FOR type LANGUAGE lang`
    Transform {
        data_type: DataType,
        language: Ident,
    },
    /// `LARGE OBJECT oid`
    LargeObject(ValueWithSpan),
}

impl fmt::Display for CommentObjectDetail {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommentObjectDetail::On(name) => write!(f, " ON {name}"),
            CommentObjectDetail::OnDomain(name) => write!(f, " ON DOMAIN {name}"),
            CommentObjectDetail::Using(method) => write!(f, " USING {method}"),
            CommentObjectDetail::Arguments(args) => {
                write!(f, "({})", display_comma_separated(args))
            }
            CommentObjectDetail::AggregateArguments(args) => write!(f, "{args}"),
            CommentObjectDetail::OperatorArguments(args) => write!(f, " {args}"),
            CommentObjectDetail::Cast(signature) => write!(f, " {signature}"),
            CommentObjectDetail::Transform {
                data_type,
                language,
            } => write!(f, " FOR {data_type} LANGUAGE {language}"),
            CommentObjectDetail::LargeObject(oid) => write!(f, " {oid}"),
        }
    }
}

/// The argument list of an aggregate, as written after the aggregate's name.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AggregateArgs {
    /// `(*)`
    Star,
    /// `(arg [, ...])`, possibly empty
    Args(Vec<OperateFunctionArg>),
    /// `([arg [, ...]] ORDER BY arg [, ...])`, the signature of an ordered-set
    /// or hypothetical-set aggregate
    OrderedSet {
        direct: Vec<OperateFunctionArg>,
        ordered: Vec<OperateFunctionArg>,
    },
}

impl fmt::Display for AggregateArgs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AggregateArgs::Star => f.write_str("(*)"),
            AggregateArgs::Args(args) => write!(f, "({})", display_comma_separated(args)),
            AggregateArgs::OrderedSet { direct, ordered } => {
                f.write_str("(")?;
                if !direct.is_empty() {
                    write!(f, "{} ", display_comma_separated(direct))?;
                }
                write!(f, "ORDER BY {})", display_comma_separated(ordered))
            }
        }
    }
}

/// An aggregate named together with its argument list, as used by
/// `DROP AGGREGATE` and `COMMENT ON AGGREGATE`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AggregateSignature {
    pub name: ObjectName,
    pub args: AggregateArgs,
}

impl fmt::Display for AggregateSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.name, self.args)
    }
}

/// The operand types of an operator. `NONE`, which PostgreSQL uses to spell the
/// missing operand of a unary operator, is represented by `None`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OperatorOperandTypes {
    pub left: Option<DataType>,
    pub right: Option<DataType>,
}

impl fmt::Display for OperatorOperandTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("(")?;
        match &self.left {
            Some(data_type) => write!(f, "{data_type}")?,
            None => f.write_str("NONE")?,
        }
        f.write_str(", ")?;
        match &self.right {
            Some(data_type) => write!(f, "{data_type}")?,
            None => f.write_str("NONE")?,
        }
        f.write_str(")")
    }
}

/// An operator named together with its operand types.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OperatorSignature {
    pub name: ObjectName,
    pub args: OperatorOperandTypes,
}

impl fmt::Display for OperatorSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.args)
    }
}

/// `(source_type AS target_type)`, the way a cast is named.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CastSignature {
    pub source_type: DataType,
    pub target_type: DataType,
}

impl fmt::Display for CastSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({} AS {})", self.source_type, self.target_type)
    }
}

/// `CREATE COLLATION`
///
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createcollation.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateCollation {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub definition: CollationDefinition,
}

impl fmt::Display for CreateCollation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE COLLATION {}{} {}",
            if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.definition
        )
    }
}

/// How a collation is defined: from a list of properties, or copied from an
/// existing collation.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CollationDefinition {
    Options(Vec<SqlOption>),
    From(ObjectName),
}

impl fmt::Display for CollationDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CollationDefinition::Options(options) => {
                write!(f, "({})", display_comma_separated(options))
            }
            CollationDefinition::From(name) => write!(f, "FROM {name}"),
        }
    }
}

/// `CREATE [DEFAULT] CONVERSION`
///
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createconversion.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateConversion {
    pub default: bool,
    pub name: ObjectName,
    pub for_encoding: ValueWithSpan,
    pub to_encoding: ValueWithSpan,
    pub function: ObjectName,
}

impl fmt::Display for CreateConversion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE {}CONVERSION {} FOR {} TO {} FROM {}",
            if self.default { "DEFAULT " } else { "" },
            self.name,
            self.for_encoding,
            self.to_encoding,
            self.function
        )
    }
}

/// `CREATE [OR REPLACE] [TRUSTED] [PROCEDURAL] LANGUAGE`
///
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createlanguage.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateLanguage {
    pub or_replace: bool,
    pub trusted: bool,
    pub procedural: bool,
    pub name: Ident,
    /// `HANDLER call_handler`; absent for the parameterless form, which
    /// PostgreSQL treats as a request to load an extension of the same name.
    pub handler: Option<ObjectName>,
    pub inline: Option<ObjectName>,
    pub validator: Option<ObjectName>,
}

impl fmt::Display for CreateLanguage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE {}{}{}LANGUAGE {}",
            if self.or_replace { "OR REPLACE " } else { "" },
            if self.trusted { "TRUSTED " } else { "" },
            if self.procedural { "PROCEDURAL " } else { "" },
            self.name
        )?;
        if let Some(handler) = &self.handler {
            write!(f, " HANDLER {handler}")?;
        }
        if let Some(inline) = &self.inline {
            write!(f, " INLINE {inline}")?;
        }
        if let Some(validator) = &self.validator {
            write!(f, " VALIDATOR {validator}")?;
        }
        Ok(())
    }
}

/// `CREATE EVENT TRIGGER`
///
/// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createeventtrigger.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateEventTrigger {
    pub name: Ident,
    pub event: Ident,
    /// The `WHEN` filter conditions, combined with `AND`
    pub conditions: Vec<EventTriggerCondition>,
    pub exec_body: TriggerExecBody,
}

impl fmt::Display for CreateEventTrigger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE EVENT TRIGGER {} ON {}", self.name, self.event)?;
        if !self.conditions.is_empty() {
            write!(f, " WHEN {}", display_separated_and(&self.conditions))?;
        }
        write!(f, " EXECUTE {}", self.exec_body)
    }
}

/// `filter_variable IN ('value' [, ...])` inside the `WHEN` clause of an event
/// trigger.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct EventTriggerCondition {
    pub variable: Ident,
    pub values: Vec<ValueWithSpan>,
}

impl fmt::Display for EventTriggerCondition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} IN ({})",
            self.variable,
            display_comma_separated(&self.values)
        )
    }
}

struct AndSeparated<'a, T>(&'a [T]);

impl<T: fmt::Display> fmt::Display for AndSeparated<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, item) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(" AND ")?;
            }
            write!(f, "{item}")?;
        }
        Ok(())
    }
}

fn display_separated_and<T: fmt::Display>(slice: &[T]) -> AndSeparated<'_, T> {
    AndSeparated(slice)
}

/// `DROP AGGREGATE`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropAggregate {
    pub if_exists: bool,
    pub aggregates: Vec<AggregateSignature>,
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropAggregate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP AGGREGATE {}{}",
            if self.if_exists { "IF EXISTS " } else { "" },
            display_comma_separated(&self.aggregates)
        )?;
        if let Some(behavior) = &self.drop_behavior {
            write!(f, " {behavior}")?;
        }
        Ok(())
    }
}

/// `DROP OPERATOR`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropOperator {
    pub if_exists: bool,
    pub operators: Vec<OperatorSignature>,
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP OPERATOR {}{}",
            if self.if_exists { "IF EXISTS " } else { "" },
            display_comma_separated(&self.operators)
        )?;
        if let Some(behavior) = &self.drop_behavior {
            write!(f, " {behavior}")?;
        }
        Ok(())
    }
}

/// `DROP OPERATOR CLASS` and `DROP OPERATOR FAMILY`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropOperatorClass {
    /// True for `DROP OPERATOR FAMILY`, false for `DROP OPERATOR CLASS`
    pub family: bool,
    pub if_exists: bool,
    pub name: ObjectName,
    pub using: Ident,
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropOperatorClass {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP OPERATOR {} {}{} USING {}",
            if self.family { "FAMILY" } else { "CLASS" },
            if self.if_exists { "IF EXISTS " } else { "" },
            self.name,
            self.using
        )?;
        if let Some(behavior) = &self.drop_behavior {
            write!(f, " {behavior}")?;
        }
        Ok(())
    }
}

/// `DROP CAST`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropCast {
    pub if_exists: bool,
    pub signature: CastSignature,
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropCast {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP CAST {}{}",
            if self.if_exists { "IF EXISTS " } else { "" },
            self.signature
        )?;
        if let Some(behavior) = &self.drop_behavior {
            write!(f, " {behavior}")?;
        }
        Ok(())
    }
}

/// `DROP ROUTINE`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropRoutine {
    pub if_exists: bool,
    pub routines: Vec<FunctionDesc>,
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropRoutine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP ROUTINE {}{}",
            if self.if_exists { "IF EXISTS " } else { "" },
            display_comma_separated(&self.routines)
        )?;
        if let Some(behavior) = &self.drop_behavior {
            write!(f, " {behavior}")?;
        }
        Ok(())
    }
}

/// `DROP TRANSFORM`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropTransform {
    pub if_exists: bool,
    pub data_type: DataType,
    pub language: Ident,
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropTransform {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP TRANSFORM {}FOR {} LANGUAGE {}",
            if self.if_exists { "IF EXISTS " } else { "" },
            self.data_type,
            self.language
        )?;
        if let Some(behavior) = &self.drop_behavior {
            write!(f, " {behavior}")?;
        }
        Ok(())
    }
}

/// `DROP OWNED BY role [, ...] [CASCADE | RESTRICT]`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropOwned {
    pub roles: Vec<Ident>,
    pub drop_behavior: Option<DropBehavior>,
}

impl fmt::Display for DropOwned {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DROP OWNED BY {}", display_comma_separated(&self.roles))?;
        if let Some(behavior) = &self.drop_behavior {
            write!(f, " {behavior}")?;
        }
        Ok(())
    }
}

/// `REASSIGN OWNED BY role [, ...] TO role`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReassignOwned {
    pub roles: Vec<Ident>,
    pub new_role: Ident,
}

impl fmt::Display for ReassignOwned {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "REASSIGN OWNED BY {} TO {}",
            display_comma_separated(&self.roles),
            self.new_role
        )
    }
}

/// One `name value` pair of the `WITH` clause of `GRANT role TO role`, for
/// example `ADMIN OPTION`, `INHERIT TRUE` or `SET FALSE`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RoleGrantOption {
    pub name: Ident,
    pub value: RoleGrantOptionValue,
}

impl fmt::Display for RoleGrantOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.name, self.value)
    }
}

/// The value of a [`RoleGrantOption`].
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RoleGrantOptionValue {
    Option,
    True,
    False,
}

impl fmt::Display for RoleGrantOptionValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            RoleGrantOptionValue::Option => "OPTION",
            RoleGrantOptionValue::True => "TRUE",
            RoleGrantOptionValue::False => "FALSE",
        })
    }
}

/// Whether a `CREATE ROLE` password was written with the `ENCRYPTED` or
/// `UNENCRYPTED` noise word.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum PasswordEncryption {
    Encrypted,
    Unencrypted,
}

impl fmt::Display for PasswordEncryption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            PasswordEncryption::Encrypted => "ENCRYPTED",
            PasswordEncryption::Unencrypted => "UNENCRYPTED",
        })
    }
}

macro_rules! empty_span {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Spanned for $ty {
                fn span(&self) -> Span {
                    Span::empty()
                }
            }
        )*
    };
}

empty_span!(
    CreateCollation,
    CreateConversion,
    CreateLanguage,
    CreateEventTrigger,
    DropAggregate,
    DropOperator,
    DropOperatorClass,
    DropCast,
    DropRoutine,
    DropTransform,
    DropOwned,
    ReassignOwned,
);
