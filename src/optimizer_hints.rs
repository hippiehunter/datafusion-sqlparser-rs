//! Parser-owned optimizer-hint grammar.
//!
//! Hint comments are lexical SQL constructs. Keeping their recognition here
//! prevents downstream consumers from scanning, masking, or rewriting source
//! text before it reaches the parser.

use crate::dialect::Dialect;
use crate::tokenizer::{BorrowedToken, Location, Tokenizer, TokenizerError, Whitespace};

/// One optimizer hint parsed from a `/*+ ... */` comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerHint {
    pub directive: OptimizerHintDirective,
    /// Exact UTF-8 byte offsets `[start, end)` of the containing hint block.
    pub span: (usize, usize),
}

/// Typed optimizer-hint directives. Unknown directives remain observable so
/// consumers can preserve forward compatibility without accepting semantics
/// they do not implement.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    Unknown { name: String, args: Vec<String> },
}

/// Parse every optimizer-hint comment in `source` without changing `source`.
/// Strings, quoted identifiers, dollar strings, and ordinary comments are
/// distinguished by the normal dialect tokenizer rather than a second SQL
/// scanner.
pub fn parse_optimizer_hints(
    dialect: &dyn Dialect,
    source: &str,
) -> Result<Vec<OptimizerHint>, TokenizerError> {
    let tokens = Tokenizer::new(dialect, source).tokenize_with_location()?;
    let mut hints = Vec::new();

    for token in tokens {
        let BorrowedToken::Whitespace(Whitespace::MultiLineComment(comment)) = token.token else {
            continue;
        };
        let Some(content) = comment.strip_prefix('+') else {
            continue;
        };
        let Some(start) = source_location_to_offset(source, token.span.start) else {
            continue;
        };
        let Some(end) = source_location_to_offset(source, token.span.end) else {
            continue;
        };
        parse_hint_content(content, (start, end), &mut hints);
    }

    Ok(hints)
}

fn parse_hint_content(content: &str, span: (usize, usize), hints: &mut Vec<OptimizerHint>) {
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
            while pos < chars.len() && chars[pos] != ')' {
                pos += 1;
            }
            let raw = chars[argument_start..pos].iter().collect::<String>();
            if pos < chars.len() {
                pos += 1;
            }
            raw.split(',')
                .flat_map(str::split_whitespace)
                .filter(|argument| !argument.is_empty())
                .map(str::to_owned)
                .collect()
        } else {
            Vec::new()
        };

        hints.push(OptimizerHint {
            directive: parse_directive(&name.to_ascii_uppercase(), args),
            span,
        });
    }
}

fn parse_directive(name: &str, args: Vec<String>) -> OptimizerHintDirective {
    match name {
        "USE_INDEX" if args.len() >= 2 => OptimizerHintDirective::UseIndex {
            table: args[0].clone(),
            index: args[1].clone(),
        },
        "FORCE_INDEX" if args.len() >= 2 => OptimizerHintDirective::ForceIndex {
            table: args[0].clone(),
            index: args[1].clone(),
        },
        "NO_INDEX" if args.len() >= 2 => OptimizerHintDirective::NoIndex {
            table: args[0].clone(),
            index: args[1].clone(),
        },
        "HASH_JOIN" => OptimizerHintDirective::HashJoin { tables: args },
        "NESTED_LOOP" => OptimizerHintDirective::NestedLoop { tables: args },
        "SORT_MERGE_JOIN" => OptimizerHintDirective::SortMergeJoin { tables: args },
        "NO_HASH_JOIN" => OptimizerHintDirective::NoHashJoin { tables: args },
        "NO_NESTED_LOOP" => OptimizerHintDirective::NoNestedLoop { tables: args },
        "NO_PARALLEL" => OptimizerHintDirective::NoParallel,
        "PARALLEL" => OptimizerHintDirective::Parallel {
            dop: args
                .first()
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(1),
        },
        "LEADING" => OptimizerHintDirective::Leading { tables: args },
        "NO_PLAN_CACHE" => OptimizerHintDirective::NoPlanCache,
        "REOPTIMIZE" => OptimizerHintDirective::Reoptimize,
        _ => OptimizerHintDirective::Unknown {
            name: name.to_owned(),
            args,
        },
    }
}

fn source_location_to_offset(source: &str, location: Location) -> Option<usize> {
    if location.line == 0 || location.column == 0 {
        return None;
    }
    let target_line = usize::try_from(location.line - 1).ok()?;
    let line_start = if target_line == 0 {
        0
    } else {
        source
            .match_indices('\n')
            .nth(target_line - 1)
            .map(|(offset, _)| offset + 1)?
    };
    let line_end = source[line_start..]
        .find('\n')
        .map(|offset| line_start + offset)
        .unwrap_or(source.len());
    let line = source.get(line_start..line_end)?;
    let target_column = usize::try_from(location.column - 1).ok()?;
    if let Some((offset, _)) = line.char_indices().nth(target_column) {
        return Some(line_start + offset);
    }
    (line.chars().count() == target_column).then_some(line_end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::PostgreSqlDialect;

    #[test]
    fn parses_typed_hints_with_exact_source_spans() {
        let sql = "SELECT /*+ USE_INDEX(t idx_x) NO_PARALLEL */ * FROM t";
        let hints = parse_optimizer_hints(&PostgreSqlDialect {}, sql).unwrap();
        assert_eq!(hints.len(), 2);
        assert!(matches!(
            &hints[0].directive,
            OptimizerHintDirective::UseIndex { table, index }
                if table == "t" && index == "idx_x"
        ));
        assert_eq!(
            &sql[hints[0].span.0..hints[0].span.1],
            "/*+ USE_INDEX(t idx_x) NO_PARALLEL */"
        );
        assert!(matches!(
            hints[1].directive,
            OptimizerHintDirective::NoParallel
        ));
    }

    #[test]
    fn sql_literals_and_ordinary_comments_are_not_hints() {
        let sql = "SELECT '/*+ NO_PLAN_CACHE */', $$/*+ REOPTIMIZE */$$ /* ordinary */";
        assert!(parse_optimizer_hints(&PostgreSqlDialect {}, sql)
            .unwrap()
            .is_empty());
    }
}
