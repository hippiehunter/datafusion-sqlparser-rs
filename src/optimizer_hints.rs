//! Parser-owned optimizer-hint grammar.
//!
//! A `/*+ ... */` comment is lexical, but it is not commentary: Oracle gives it
//! meaning, so the tokenizer keeps it and the parser attaches it to the
//! statement it introduces. Hints therefore travel in the syntax tree like any
//! other clause and survive rendering a statement back to SQL, which is what
//! every replan and prepared-statement path does.

use crate::tokenizer::Span;
#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// One optimizer hint written after a statement's leading keyword.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OptimizerHint {
    pub directive: OptimizerHintDirective,
    /// Source span of the containing `/*+ ... */` block.
    pub span: Span,
}

impl fmt::Display for OptimizerHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.directive)
    }
}

/// Typed optimizer-hint directives. Unknown directives remain observable so
/// consumers can preserve forward compatibility without accepting semantics
/// they do not implement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OptimizerHintDirective {
    UseIndex { table: String, index: String },
    ForceIndex { table: String, index: String },
    NoIndex { table: String, index: String },
    HashJoin { tables: Vec<String> },
    NestedLoop { tables: Vec<String> },
    SortMergeJoin { tables: Vec<String> },
    NoHashJoin { tables: Vec<String> },
    NoNestedLoop { tables: Vec<String> },
    NoParallel,
    Parallel { dop: u16 },
    Leading { tables: Vec<String> },
    NoPlanCache,
    Reoptimize,
    Unknown { name: String, args: Vec<HintArgument> },
}

impl OptimizerHintDirective {
    /// The directive's name as it is written in a hint block.
    pub fn name(&self) -> &str {
        match self {
            Self::UseIndex { .. } => "USE_INDEX",
            Self::ForceIndex { .. } => "FORCE_INDEX",
            Self::NoIndex { .. } => "NO_INDEX",
            Self::HashJoin { .. } => "HASH_JOIN",
            Self::NestedLoop { .. } => "NESTED_LOOP",
            Self::SortMergeJoin { .. } => "SORT_MERGE_JOIN",
            Self::NoHashJoin { .. } => "NO_HASH_JOIN",
            Self::NoNestedLoop { .. } => "NO_NESTED_LOOP",
            Self::NoParallel => "NO_PARALLEL",
            Self::Parallel { .. } => "PARALLEL",
            Self::Leading { .. } => "LEADING",
            Self::NoPlanCache => "NO_PLAN_CACHE",
            Self::Reoptimize => "REOPTIMIZE",
            Self::Unknown { name, .. } => name,
        }
    }
}

impl fmt::Display for OptimizerHintDirective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())?;
        match self {
            Self::UseIndex { table, index }
            | Self::ForceIndex { table, index }
            | Self::NoIndex { table, index } => write!(f, "({table} {index})"),
            Self::HashJoin { tables }
            | Self::NestedLoop { tables }
            | Self::SortMergeJoin { tables }
            | Self::NoHashJoin { tables }
            | Self::NoNestedLoop { tables }
            | Self::Leading { tables } => write!(f, "({})", tables.join(" ")),
            Self::Parallel { dop } => write!(f, "({dop})"),
            Self::NoParallel | Self::NoPlanCache | Self::Reoptimize => Ok(()),
            Self::Unknown { args, .. } => {
                if args.is_empty() {
                    return Ok(());
                }
                let rendered = args
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ");
                write!(f, "({rendered})")
            }
        }
    }
}

/// One argument of a hint directive.
///
/// Oracle distinguishes `IGNORE_ROW_ON_DUPKEY_INDEX(<table> <index>)` from
/// `IGNORE_ROW_ON_DUPKEY_INDEX(<table> (<columns>))`: the first names an index,
/// the second a column list. Both are the same sequence of identifiers, so the
/// grouping is the only thing that tells them apart and it is kept rather than
/// flattened away.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum HintArgument {
    Name(String),
    Group(Vec<String>),
}

impl HintArgument {
    /// Every identifier this argument spells, in order.
    pub fn names(&self) -> &[String] {
        match self {
            Self::Name(name) => core::slice::from_ref(name),
            Self::Group(names) => names,
        }
    }
}

impl fmt::Display for HintArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => f.write_str(name),
            Self::Group(names) => write!(f, "({})", names.join(", ")),
        }
    }
}

/// Render a statement's hints as the `/*+ ... */` block it was written as, so
/// displaying a statement and parsing it again yields the same hints.
pub(crate) fn display_hint_block(
    f: &mut fmt::Formatter<'_>,
    hints: &[OptimizerHint],
) -> fmt::Result {
    if hints.is_empty() {
        return Ok(());
    }
    let rendered = hints
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    write!(f, " /*+ {rendered} */")
}

/// The identifiers a directive's arguments spell, with grouping flattened
/// away. Directives that take only names read their arguments this way.
fn argument_names(args: &[HintArgument]) -> Vec<String> {
    args.iter()
        .flat_map(|argument| argument.names().iter().cloned())
        .collect()
}

/// Parse the directives inside one hint comment's content, which is the text
/// between `/*+` and `*/`.
pub(crate) fn parse_hint_content(content: &str, span: Span, hints: &mut Vec<OptimizerHint>) {
    let chars = content.chars().collect::<Vec<_>>();
    let mut pos = 0;

    while pos < chars.len() {
        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos == chars.len() {
            break;
        }

        let name_start = pos;
        while pos < chars.len() && (chars[pos].is_alphanumeric() || chars[pos] == '_') {
            pos += 1;
        }
        if pos == name_start {
            pos += 1;
            continue;
        }
        let name = chars[name_start..pos].iter().collect::<String>();

        while pos < chars.len() && chars[pos].is_whitespace() {
            pos += 1;
        }
        let args = if pos < chars.len() && chars[pos] == '(' {
            pos += 1;
            let argument_start = pos;
            // A directive may group part of its argument list, as Oracle's
            // `IGNORE_ROW_ON_DUPKEY_INDEX(<table> (<columns>))` names a column
            // list rather than an index. Closing the directive on the first
            // `)` would end it inside that group and leave the rest of the
            // arguments unread, so the depth is tracked.
            let mut depth = 1usize;
            while pos < chars.len() {
                match chars[pos] {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                pos += 1;
            }
            let raw = chars[argument_start..pos].iter().collect::<String>();
            if pos < chars.len() {
                pos += 1;
            }
            split_hint_arguments(&raw)
        } else {
            Vec::new()
        };

        hints.push(OptimizerHint {
            directive: parse_directive(&name.to_ascii_uppercase(), args),
            span,
        });
    }
}

/// Split a directive's argument text into bare names and parenthesized groups.
fn split_hint_arguments(raw: &str) -> Vec<HintArgument> {
    let chars = raw.chars().collect::<Vec<_>>();
    let mut arguments = Vec::new();
    let mut pos = 0;
    while pos < chars.len() {
        if chars[pos].is_whitespace() || chars[pos] == ',' {
            pos += 1;
        } else if chars[pos] == '(' {
            pos += 1;
            let group_start = pos;
            let mut depth = 1usize;
            while pos < chars.len() {
                match chars[pos] {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                pos += 1;
            }
            let group = chars[group_start..pos].iter().collect::<String>();
            if pos < chars.len() {
                pos += 1;
            }
            arguments.push(HintArgument::Group(
                group
                    .split(',')
                    .flat_map(str::split_whitespace)
                    .map(str::to_owned)
                    .collect(),
            ));
        } else {
            let name_start = pos;
            while pos < chars.len()
                && !chars[pos].is_whitespace()
                && chars[pos] != ','
                && chars[pos] != '('
            {
                pos += 1;
            }
            arguments.push(HintArgument::Name(
                chars[name_start..pos].iter().collect::<String>(),
            ));
        }
    }
    arguments
}

fn parse_directive(name: &str, args: Vec<HintArgument>) -> OptimizerHintDirective {
    let names = argument_names(&args);
    match name {
        "USE_INDEX" if names.len() >= 2 => OptimizerHintDirective::UseIndex {
            table: names[0].clone(),
            index: names[1].clone(),
        },
        "FORCE_INDEX" if names.len() >= 2 => OptimizerHintDirective::ForceIndex {
            table: names[0].clone(),
            index: names[1].clone(),
        },
        "NO_INDEX" if names.len() >= 2 => OptimizerHintDirective::NoIndex {
            table: names[0].clone(),
            index: names[1].clone(),
        },
        "HASH_JOIN" => OptimizerHintDirective::HashJoin { tables: names },
        "NESTED_LOOP" => OptimizerHintDirective::NestedLoop { tables: names },
        "SORT_MERGE_JOIN" => OptimizerHintDirective::SortMergeJoin { tables: names },
        "NO_HASH_JOIN" => OptimizerHintDirective::NoHashJoin { tables: names },
        "NO_NESTED_LOOP" => OptimizerHintDirective::NoNestedLoop { tables: names },
        "NO_PARALLEL" => OptimizerHintDirective::NoParallel,
        "PARALLEL" => OptimizerHintDirective::Parallel {
            dop: names
                .first()
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(1),
        },
        "LEADING" => OptimizerHintDirective::Leading { tables: names },
        "NO_PLAN_CACHE" => OptimizerHintDirective::NoPlanCache,
        "REOPTIMIZE" => OptimizerHintDirective::Reoptimize,
        _ => OptimizerHintDirective::Unknown {
            name: name.to_owned(),
            args,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Statement, Insert};
    use crate::dialect::PostgreSqlDialect;
    use crate::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .expect("tokenize")
            .parse_statement()
            .expect("parse")
    }

    fn insert_of(statement: Statement) -> Insert {
        match statement {
            Statement::Insert(insert) => insert,
            other => panic!("expected an INSERT, got {other:?}"),
        }
    }

    #[test]
    fn a_hint_survives_rendering_the_statement_back_to_sql() {
        // Every replan and prepared-statement path renders the statement and
        // parses it again. A hint that lived only in the source comment was
        // lost on that trip, which left the whole hint surface unreachable.
        let sql = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(t (a, b)) */ INTO t VALUES (1, 2)";
        let parsed = insert_of(parse_one(sql));
        assert_eq!(parsed.hints.len(), 1);

        let rendered = Statement::Insert(parsed.clone()).to_string();
        let reparsed = insert_of(parse_one(&rendered));
        assert_eq!(reparsed.hints, parsed.hints);
    }

    #[test]
    fn a_grouped_argument_is_a_column_list_and_a_bare_one_is_an_index() {
        // OraDB spells `IGNORE_ROW_ON_DUPKEY_INDEX(<table> <index>)` and
        // `IGNORE_ROW_ON_DUPKEY_INDEX(<table> (<columns>))` with the same
        // identifiers, so only the grouping tells the two apart.
        let grouped = insert_of(parse_one(
            "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(t (v)) */ INTO t VALUES (1)",
        ));
        let OptimizerHintDirective::Unknown { name, args } = &grouped.hints[0].directive else {
            panic!("expected an unknown directive");
        };
        assert_eq!(name, "IGNORE_ROW_ON_DUPKEY_INDEX");
        assert_eq!(
            args,
            &[
                HintArgument::Name("t".to_string()),
                HintArgument::Group(vec!["v".to_string()]),
            ]
        );

        let named = insert_of(parse_one(
            "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(t t_pk) */ INTO t VALUES (1)",
        ));
        let OptimizerHintDirective::Unknown { args, .. } = &named.hints[0].directive else {
            panic!("expected an unknown directive");
        };
        assert_eq!(
            args,
            &[
                HintArgument::Name("t".to_string()),
                HintArgument::Name("t_pk".to_string()),
            ]
        );
    }

    #[test]
    fn every_statement_that_admits_a_hint_block_carries_it() {
        let select = parse_one("SELECT /*+ USE_INDEX(t idx_x) NO_PARALLEL */ * FROM t");
        let Statement::Query(query) = &select else {
            panic!("expected a query");
        };
        let crate::ast::SetExpr::Select(inner) = query.body.as_ref() else {
            panic!("expected a select");
        };
        assert_eq!(inner.hints.len(), 2);
        assert!(matches!(
            &inner.hints[0].directive,
            OptimizerHintDirective::UseIndex { table, index } if table == "t" && index == "idx_x"
        ));
        assert_eq!(select.to_string(), parse_one(&select.to_string()).to_string());

        let update = parse_one("UPDATE /*+ NO_PARALLEL */ t SET v = 1");
        let Statement::Update(update) = &update else {
            panic!("expected an update");
        };
        assert_eq!(update.hints.len(), 1);

        let delete = parse_one("DELETE /*+ NO_PARALLEL */ FROM t WHERE v = 1");
        let Statement::Delete(delete) = &delete else {
            panic!("expected a delete");
        };
        assert_eq!(delete.hints.len(), 1);
    }

    #[test]
    fn sql_literals_and_ordinary_comments_are_not_hints() {
        let statement = parse_one(
            "SELECT '/*+ NO_PLAN_CACHE */' /* ordinary */ FROM t",
        );
        let Statement::Query(query) = &statement else {
            panic!("expected a query");
        };
        let crate::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a select");
        };
        assert!(select.hints.is_empty());
    }
}
