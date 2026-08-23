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

//! Tests for the PostgreSQL query and DML grammar.
//!
//! References:
//! - <https://www.postgresql.org/docs/current/sql-select.html>
//! - <https://www.postgresql.org/docs/current/sql-insert.html>
//! - <https://www.postgresql.org/docs/current/sql-update.html>
//! - <https://www.postgresql.org/docs/current/sql-merge.html>

use sqlparser::ast::{
    AccessExpr, AssignmentTarget, BinaryOperator, ConflictTarget, Expr, GroupByExpr,
    GroupBySetQuantifier, JoinConstraint, MergeAction, MergeInsertKind, OverridingKind, Query,
    ReturningRowVersion, Select, SelectItem, SetExpr, Statement, Subscript, TableFactor,
    TableSampleKind, Value, XmlRootStandalone, XmlRootVersion,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::test_utils::TestedDialects;

fn pg() -> TestedDialects {
    TestedDialects::new(vec![std::boxed::Box::new(PostgreSqlDialect {})])
}

fn query_of(stmt: &Statement) -> &Query {
    match stmt {
        Statement::Query(query) => query,
        other => panic!("expected a query, got {other:?}"),
    }
}

fn select_of(stmt: &Statement) -> &Select {
    match query_of(stmt).body.as_ref() {
        SetExpr::Select(select) => select,
        other => panic!("expected a SELECT, got {other:?}"),
    }
}

fn only_projection(stmt: &Statement) -> &Expr {
    match &select_of(stmt).projection[..] {
        [SelectItem::UnnamedExpr(expr)] => expr,
        other => panic!("expected a single unnamed projection, got {other:?}"),
    }
}

// =============================================================================
// GROUP BY
// =============================================================================

#[test]
fn parse_group_by_set_quantifier() {
    // https://www.postgresql.org/docs/current/sql-select.html#SQL-GROUPBY
    let stmt = pg().verified_stmt("SELECT a, b FROM t GROUP BY ALL ROLLUP(a, b)");
    match &select_of(&stmt).group_by {
        GroupByExpr::Quantified {
            quantifier,
            expressions,
            modifiers,
        } => {
            assert_eq!(*quantifier, GroupBySetQuantifier::All);
            assert!(matches!(expressions[..], [Expr::Rollup(_)]));
            assert!(modifiers.is_empty());
        }
        other => panic!("expected a quantified GROUP BY, got {other:?}"),
    }

    let stmt = pg().verified_stmt("SELECT a, b FROM t GROUP BY DISTINCT ROLLUP(a, b), ROLLUP(a)");
    match &select_of(&stmt).group_by {
        GroupByExpr::Quantified {
            quantifier,
            expressions,
            ..
        } => {
            assert_eq!(*quantifier, GroupBySetQuantifier::Distinct);
            assert_eq!(expressions.len(), 2);
        }
        other => panic!("expected a quantified GROUP BY, got {other:?}"),
    }
}

#[test]
fn parse_group_by_all_without_grouping_elements() {
    let stmt = pg().verified_stmt("SELECT a FROM t GROUP BY ALL");
    assert!(matches!(select_of(&stmt).group_by, GroupByExpr::All(_)));
}

#[test]
fn parse_grouping_sets_of_parenthesized_lists() {
    let stmt = pg().verified_stmt("SELECT a, b FROM t GROUP BY GROUPING SETS ((a, b), (a), ())");
    match &select_of(&stmt).group_by {
        GroupByExpr::Expressions(exprs, _) => match &exprs[..] {
            [Expr::GroupingSets(sets)] => assert_eq!(sets.len(), 3),
            other => panic!("expected GROUPING SETS, got {other:?}"),
        },
        other => panic!("expected expression GROUP BY, got {other:?}"),
    }
}

#[test]
fn parse_grouping_sets_of_bare_expressions() {
    let stmt = pg().verified_stmt("SELECT a, b FROM t GROUP BY GROUPING SETS (a, b, ())");
    match &select_of(&stmt).group_by {
        GroupByExpr::Expressions(exprs, _) => match &exprs[..] {
            [Expr::GroupingSetsElements(elements)] => assert_eq!(elements.len(), 3),
            other => panic!("expected GROUPING SETS elements, got {other:?}"),
        },
        other => panic!("expected expression GROUP BY, got {other:?}"),
    }
    pg().verified_stmt("SELECT a + b, sum(v) FROM t GROUP BY GROUPING SETS (a + b, a)");
}

#[test]
fn parse_nested_grouping_sets() {
    let stmt = pg().verified_stmt(
        "SELECT sum(v) FROM t GROUP BY GROUPING SETS ((), GROUPING SETS ((), GROUPING SETS (())))",
    );
    match &select_of(&stmt).group_by {
        GroupByExpr::Expressions(exprs, _) => match &exprs[..] {
            [Expr::GroupingSetsElements(elements)] => {
                assert!(matches!(elements[1], Expr::GroupingSetsElements(_)));
            }
            other => panic!("expected GROUPING SETS elements, got {other:?}"),
        },
        other => panic!("expected expression GROUP BY, got {other:?}"),
    }
    pg().verified_stmt("SELECT sum(v) FROM t GROUP BY GROUPING SETS (GROUPING SETS ((a, b)))");
    pg().verified_stmt("SELECT a FROM t GROUP BY GROUPING SETS (ROLLUP(a, b), CUBE(c), (), a)");
}

#[test]
fn parse_grouping_sets_mixed_with_plain_expressions() {
    let stmt = pg().verified_stmt("SELECT a, b FROM t GROUP BY a, b, GROUPING SETS (a)");
    match &select_of(&stmt).group_by {
        GroupByExpr::Expressions(exprs, _) => {
            assert_eq!(exprs.len(), 3);
            assert!(matches!(exprs[2], Expr::GroupingSetsElements(_)));
        }
        other => panic!("expected expression GROUP BY, got {other:?}"),
    }
}

// =============================================================================
// ORDER BY ... USING
// =============================================================================

#[test]
fn parse_order_by_using_operator() {
    // https://www.postgresql.org/docs/current/queries-order.html
    let stmt = pg().verified_stmt("SELECT a FROM t ORDER BY a USING >");
    let order_by = query_of(&stmt).order_by.as_ref().expect("ORDER BY");
    match &order_by.kind {
        sqlparser::ast::OrderByKind::Expressions(exprs) => {
            assert_eq!(exprs[0].using, Some(BinaryOperator::Gt));
            assert_eq!(exprs[0].options.asc, None);
        }
        other => panic!("expected ORDER BY expressions, got {other:?}"),
    }

    pg().verified_stmt("SELECT a, b FROM t ORDER BY b USING <, a USING >");
    pg().verified_stmt("SELECT a FROM t ORDER BY a USING > NULLS FIRST");
    pg().verified_stmt("SELECT a FROM t ORDER BY a USING < NULLS LAST");
    pg().verified_stmt("SELECT a FROM t ORDER BY a USING OPERATOR(pg_catalog.<)");
    pg().verified_stmt("SELECT sum(v) OVER (ORDER BY a USING <) FROM t");
}

#[test]
fn parse_order_by_using_user_defined_operator() {
    let stmt = pg().verified_stmt("SELECT a FROM t ORDER BY a USING ~<~");
    let order_by = query_of(&stmt).order_by.as_ref().expect("ORDER BY");
    match &order_by.kind {
        sqlparser::ast::OrderByKind::Expressions(exprs) => assert_eq!(
            exprs[0].using,
            Some(BinaryOperator::Custom("~<~".to_string()))
        ),
        other => panic!("expected ORDER BY expressions, got {other:?}"),
    }
}

#[test]
fn parse_order_by_using_any_operator_name() {
    let stmt = pg().verified_stmt("SELECT a FROM t ORDER BY a USING @>");
    match &query_of(&stmt).order_by.as_ref().expect("ORDER BY").kind {
        sqlparser::ast::OrderByKind::Expressions(exprs) => {
            assert_eq!(
                exprs[0].using,
                Some(BinaryOperator::Custom("@>".to_string()))
            )
        }
        other => panic!("expected ORDER BY expressions, got {other:?}"),
    }
}

// =============================================================================
// JOIN ... USING (...) AS alias
// =============================================================================

#[test]
fn parse_join_using_alias() {
    // https://www.postgresql.org/docs/current/sql-select.html#SQL-FROM
    let stmt = pg().verified_stmt("SELECT * FROM j1 JOIN j2 USING(i) AS x WHERE x.i = 1");
    let join = &select_of(&stmt).from[0].joins[0];
    match join.join_operator.clone() {
        sqlparser::ast::JoinOperator::Join(JoinConstraint::UsingWithAlias { columns, alias }) => {
            assert_eq!(columns.len(), 1);
            assert_eq!(alias.value, "x");
        }
        other => panic!("expected a USING alias join, got {other:?}"),
    }
    pg().verified_stmt("SELECT x.* FROM j1 LEFT JOIN j2 USING(i, j) AS x");
}

// =============================================================================
// TABLESAMPLE
// =============================================================================

#[test]
fn parse_tablesample_repeatable_expression() {
    // https://www.postgresql.org/docs/current/sql-select.html#SQL-FROM
    pg().verified_stmt("SELECT id FROM t TABLESAMPLE BERNOULLI (50) REPEATABLE (NULL)");
    let stmt =
        pg().verified_stmt("SELECT count(*) FROM t TABLESAMPLE SYSTEM (100) REPEATABLE (2 + 0.4)");
    match &select_of(&stmt).from[0].relation {
        TableFactor::Table {
            sample: Some(TableSampleKind::AfterTableAlias(sample)),
            ..
        }
        | TableFactor::Table {
            sample: Some(TableSampleKind::BeforeTableAlias(sample)),
            ..
        } => {
            let seed = sample.seed.as_ref().expect("REPEATABLE");
            assert_eq!(seed.value, Value::Null);
            assert!(matches!(seed.expr, Some(Expr::BinaryOp { .. })));
        }
        other => panic!("expected a sampled table, got {other:?}"),
    }
}

// =============================================================================
// Window frames
// =============================================================================

#[test]
fn parse_window_frame_bounds_with_arbitrary_expressions() {
    // https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
    pg().verified_stmt(
        "SELECT sum(v) OVER (ORDER BY d RANGE BETWEEN '-1 year'::INTERVAL PRECEDING AND '1 year'::INTERVAL FOLLOWING) FROM t",
    );
    pg().verified_stmt(
        "SELECT sum(v) OVER (ORDER BY f RANGE BETWEEN 1.1 PRECEDING AND 'NaN'::FLOAT8 FOLLOWING) FROM t",
    );
    pg().verified_stmt(
        "SELECT sum(v) OVER (ORDER BY d RANGE BETWEEN '1 year'::INTERVAL PRECEDING AND '1 year'::INTERVAL FOLLOWING EXCLUDE NO OTHERS) FROM t",
    );
    pg().verified_stmt(
        "SELECT sum(v) OVER (ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM t",
    );
    pg().verified_stmt(
        "SELECT sum(v) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t",
    );
    pg().verified_stmt(
        "SELECT sum(v) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
    );
}

#[test]
fn parse_window_frame_interval_literal_bound_keeps_interval_shape() {
    let stmt =
        pg().verified_stmt("SELECT sum(v) OVER (ORDER BY d RANGE '1 year' PRECEDING) FROM t");
    assert!(format!("{stmt:?}").contains("Interval"));
}

// =============================================================================
// SUBSTRING / TRIM / COLLATION FOR
// =============================================================================

#[test]
fn parse_substring_similar_escape() {
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-SIMILARTO-REGEXP
    let stmt = pg().verified_stmt("SELECT SUBSTRING('abcdefg' SIMILAR 'a#\"(b_d)#\"%' ESCAPE '#')");
    assert!(matches!(
        only_projection(&stmt),
        Expr::SubstringSimilar { .. }
    ));
    pg().verified_stmt("SELECT SUBSTRING(NULL SIMILAR '%' ESCAPE '#')");
    pg().verified_stmt("SELECT SUBSTRING('abcdefg' SIMILAR NULL ESCAPE '#')");
    pg().verified_stmt("SELECT SUBSTRING('abcdefg' SIMILAR '%' ESCAPE NULL)");
}

#[test]
fn parse_substring_from_for_still_parses() {
    pg().verified_stmt("SELECT SUBSTRING('abcdefg' FROM 2 FOR 3)");
    pg().verified_stmt("SELECT SUBSTRING('abcdefg' FROM 'a#\"%#\"g' FOR '#')");
}

#[test]
fn parse_trim_without_trim_characters() {
    // https://www.postgresql.org/docs/current/functions-string.html
    let stmt = pg().one_statement_parses_to(
        "SELECT TRIM(BOTH FROM '  padded  ')",
        "SELECT TRIM(BOTH '  padded  ')",
    );
    match only_projection(&stmt) {
        Expr::Trim {
            trim_where: Some(_),
            trim_what: None,
            trim_characters: None,
            ..
        } => {}
        other => panic!("expected a TRIM with no trim characters, got {other:?}"),
    }
    pg().one_statement_parses_to(
        "SELECT TRIM(LEADING FROM ' x ')",
        "SELECT TRIM(LEADING ' x ')",
    );
    pg().one_statement_parses_to(
        "SELECT TRIM(TRAILING FROM ' x ')",
        "SELECT TRIM(TRAILING ' x ')",
    );
    pg().one_statement_parses_to("SELECT TRIM(FROM ' x ')", "SELECT TRIM(' x ')");
}

#[test]
fn parse_trim_character_list() {
    let stmt = pg().verified_stmt("SELECT TRIM('xax', 'x')");
    match only_projection(&stmt) {
        Expr::Trim {
            trim_characters: Some(characters),
            ..
        } => assert_eq!(characters.len(), 1),
        other => panic!("expected TRIM characters, got {other:?}"),
    }
}

#[test]
fn parse_collation_for() {
    // https://www.postgresql.org/docs/current/functions-info.html
    let stmt = pg().verified_stmt("SELECT COLLATION FOR ('foo')");
    assert!(matches!(only_projection(&stmt), Expr::CollationFor(_)));
    pg().verified_stmt("SELECT COLLATION FOR ('foo'::TEXT)");
    pg().verified_stmt("SELECT COLLATION FOR ((SELECT a FROM t LIMIT 1))");
}

// =============================================================================
// FROM clause
// =============================================================================

#[test]
fn parse_table_inheritance_star() {
    // `name *` and a bare `name` both select the descendants, which is also how
    // PostgreSQL's own parse tree records them.
    let starred = pg().one_statement_parses_to(
        "SELECT aa FROM parent* ORDER BY aa",
        "SELECT aa FROM parent ORDER BY aa",
    );
    let bare = pg().verified_stmt("SELECT aa FROM parent ORDER BY aa");
    assert_eq!(starred, bare);

    let stmt = pg().verified_stmt("SELECT aa FROM ONLY parent");
    match &select_of(&stmt).from[0].relation {
        TableFactor::Table { only, .. } => assert!(*only),
        other => panic!("expected a table, got {other:?}"),
    }
}

#[test]
fn parse_function_table_with_column_definition_list() {
    // https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-TABLEFUNCTIONS
    let stmt =
        pg().verified_stmt("SELECT a, b, c FROM dyn_record(5) AS (a INT, b NUMERIC, c TEXT)");
    match &select_of(&stmt).from[0].relation {
        TableFactor::RowsFrom {
            rows_from,
            functions,
            with_ordinality,
            alias,
            ..
        } => {
            assert!(!rows_from);
            assert!(!with_ordinality);
            assert!(alias.is_none());
            assert_eq!(functions[0].column_defs.len(), 3);
        }
        other => panic!("expected a table function, got {other:?}"),
    }
    pg().verified_stmt("SELECT a FROM dyn_record(5) WITH ORDINALITY AS (a INT)");
    pg().verified_stmt("SELECT a FROM dyn_record(5) AS z (a INT, b INT)");
    pg().verified_stmt("SELECT a FROM json_to_record('{}') AS x (a INT, b TEXT COLLATE \"C\")");
    pg().verified_stmt("SELECT a FROM ROWS FROM (f() AS (a INT COLLATE \"C\"))");
}

#[test]
fn parse_rows_from() {
    // https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-TABLEFUNCTIONS
    let stmt = pg().verified_stmt(
        "SELECT a, b, o FROM ROWS FROM (generate_series(1, 2), generate_series(5, 6)) WITH ORDINALITY AS z (a, b, o) ORDER BY o",
    );
    match &select_of(&stmt).from[0].relation {
        TableFactor::RowsFrom {
            rows_from,
            functions,
            with_ordinality,
            alias,
            ..
        } => {
            assert!(rows_from);
            assert!(with_ordinality);
            assert_eq!(functions.len(), 2);
            assert_eq!(alias.as_ref().expect("alias").columns.len(), 3);
        }
        other => panic!("expected ROWS FROM, got {other:?}"),
    }
    pg().verified_stmt("SELECT a FROM ROWS FROM (f(1) AS (a INT), g() AS (b TEXT, c INT))");
    pg().verified_stmt("SELECT a FROM LATERAL ROWS FROM (f()) AS z");
    pg().verified_stmt(
        "SELECT a, b, o FROM ROWS FROM (unnest(ARRAY[10, 20], ARRAY['foo', 'bar'])) WITH ORDINALITY AS z (a, b, o)",
    );
}

// =============================================================================
// Set operations over an empty select list
// =============================================================================

#[test]
fn parse_set_operation_of_empty_selects() {
    // PostgreSQL's `opt_target_list` may be empty.
    for sql in [
        "SELECT UNION SELECT",
        "SELECT INTERSECT SELECT",
        "SELECT EXCEPT SELECT",
    ] {
        let stmt = pg().verified_stmt(sql);
        match query_of(&stmt).body.as_ref() {
            SetExpr::SetOperation { left, right, .. } => {
                for side in [left, right] {
                    match side.as_ref() {
                        SetExpr::Select(select) => assert!(select.projection.is_empty()),
                        other => panic!("expected a SELECT, got {other:?}"),
                    }
                }
            }
            other => panic!("expected a set operation, got {other:?}"),
        }
    }
    pg().verified_stmt("SELECT WHERE false");
    pg().verified_stmt("SELECT FROM t");
}

// =============================================================================
// Literals
// =============================================================================

#[test]
fn parse_generic_typed_string_literal() {
    // PostgreSQL's `func_name Sconst` production.
    let stmt = pg().verified_stmt("SELECT name 'name string' = name 'name string'");
    match only_projection(&stmt) {
        Expr::BinaryOp { left, right, .. } => {
            assert!(matches!(left.as_ref(), Expr::TypedString(_)));
            assert!(matches!(right.as_ref(), Expr::TypedString(_)));
        }
        other => panic!("expected a comparison, got {other:?}"),
    }
    pg().verified_stmt("SELECT CAST(name 'namefield' AS TEXT)");
    pg().verified_stmt("SELECT xml '<a/>'");
    pg().verified_stmt("SELECT pg_catalog.text 'x'");
}

#[test]
fn reserved_keywords_never_introduce_a_typed_string_literal() {
    let stmt = pg().verified_stmt("SELECT NOT 'a' LIKE 'b'");
    assert!(matches!(only_projection(&stmt), Expr::UnaryOp { .. }));
}

#[test]
fn parse_non_decimal_integer_literals() {
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC
    for sql in [
        "SELECT (0b100101)::TEXT",
        "SELECT (0o273)::TEXT",
        "SELECT (0xff)::TEXT",
        "SELECT (0b_10_0101)::TEXT",
        "SELECT (0o2_73)::TEXT",
        "SELECT (-0o20000000001)::TEXT",
        "SELECT (1_000.5e0_1)::TEXT",
    ] {
        pg().verified_stmt(sql);
    }
    let stmt = pg().verified_stmt("SELECT 0b1010");
    match only_projection(&stmt) {
        Expr::Value(value) => assert_eq!(value.value, Value::Number("0b1010".to_string(), false)),
        other => panic!("expected a number, got {other:?}"),
    }
}

#[test]
fn parse_unicode_literals_with_uescape() {
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-UESCAPE
    let stmt = pg().one_statement_parses_to(
        "SELECT U&'d!0061t\\+000061' UESCAPE '!'",
        "SELECT U&'dat\\\\+000061'",
    );
    match only_projection(&stmt) {
        Expr::Value(value) => assert_eq!(
            value.value,
            Value::UnicodeStringLiteral("dat\\+000061".to_string())
        ),
        other => panic!("expected a unicode string, got {other:?}"),
    }

    let stmt = pg().one_statement_parses_to(
        "SELECT 'tricky' AS U&\"d\\0061t\\+000061\"",
        "SELECT 'tricky' AS \"data\"",
    );
    match &select_of(&stmt).projection[..] {
        [SelectItem::ExprWithAlias { alias, .. }] => assert_eq!(alias.value, "data"),
        other => panic!("expected an aliased projection, got {other:?}"),
    }
}

#[test]
fn parse_at_local() {
    // https://www.postgresql.org/docs/current/functions-datetime.html
    let stmt = pg().verified_stmt("SELECT (timetz '15:36:39-04' AT LOCAL)::TEXT");
    assert!(format!("{stmt:?}").contains("AtLocal"));
    pg().verified_stmt("SELECT (timestamptz '2001-02-16 20:38:40+00' AT LOCAL)::TEXT");
}

#[test]
fn parse_user_defined_operators() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    let stmt = pg().verified_stmt("SELECT 1::BIGINT === 1::BIGINT");
    match only_projection(&stmt) {
        Expr::BinaryOp { op, .. } => {
            assert_eq!(*op, BinaryOperator::Custom("===".to_string()))
        }
        other => panic!("expected a binary operation, got {other:?}"),
    }
    let stmt = pg().verified_stmt("SELECT 1::BIGINT !== 2::BIGINT");
    match only_projection(&stmt) {
        Expr::BinaryOp { op, .. } => {
            assert_eq!(*op, BinaryOperator::Custom("!==".to_string()))
        }
        other => panic!("expected a binary operation, got {other:?}"),
    }
}

#[test]
fn operator_names_do_not_swallow_a_trailing_sign() {
    // `=-` is not a legal operator name, so `a =- 1` is a comparison with -1.
    let stmt = pg().one_statement_parses_to("SELECT a =- 1", "SELECT a = -1");
    match only_projection(&stmt) {
        Expr::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Eq),
        other => panic!("expected a comparison, got {other:?}"),
    }
}

#[test]
fn parse_is_document() {
    // https://www.postgresql.org/docs/current/functions-xml.html
    let stmt = pg().verified_stmt("SELECT xml '<foo>bar</foo>' IS DOCUMENT");
    assert!(matches!(
        only_projection(&stmt),
        Expr::IsDocument { negated: false, .. }
    ));
    pg().verified_stmt("SELECT xml '<foo>bar</foo>' IS NOT DOCUMENT");
}

#[test]
fn parse_xml_constructors_with_keyword_arguments() {
    // https://www.postgresql.org/docs/current/functions-xml.html
    pg().verified_stmt("SELECT (XMLELEMENT(NAME foo, xml 'b<a/>r'))::TEXT");
    let stmt = pg().verified_stmt("SELECT XMLROOT(xml '<foo/>', VERSION '2.0')");
    match only_projection(&stmt) {
        Expr::XmlRoot {
            version,
            standalone,
            ..
        } => {
            assert!(matches!(version.as_ref(), XmlRootVersion::Version(_)));
            assert!(standalone.is_none());
        }
        other => panic!("expected XMLROOT, got {other:?}"),
    }
    let stmt = pg().verified_stmt("SELECT XMLROOT(xml '<foo/>', VERSION NO VALUE, STANDALONE YES)");
    match only_projection(&stmt) {
        Expr::XmlRoot {
            version,
            standalone,
            ..
        } => {
            assert!(matches!(version.as_ref(), XmlRootVersion::NoValue));
            assert_eq!(*standalone, Some(XmlRootStandalone::Yes));
        }
        other => panic!("expected XMLROOT, got {other:?}"),
    }
    pg().verified_stmt("SELECT XMLROOT(xml '<foo/>', VERSION '1.0', STANDALONE NO VALUE)");
    pg().verified_stmt("SELECT XMLROOT(xml '<foo/>', VERSION '1.0', STANDALONE NO)");
}

// =============================================================================
// INSERT
// =============================================================================

fn insert_of(stmt: &Statement) -> &sqlparser::ast::Insert {
    match stmt {
        Statement::Insert(insert) => insert,
        other => panic!("expected an INSERT, got {other:?}"),
    }
}

#[test]
fn parse_insert_column_targets_with_indirection() {
    // https://www.postgresql.org/docs/current/sql-insert.html
    let stmt = pg().verified_stmt("INSERT INTO t (f2[1], f2[2]) VALUES (1, 2)");
    let insert = insert_of(&stmt);
    let targets = insert.column_targets.as_ref().expect("column targets");
    assert_eq!(targets.len(), 2);
    assert!(matches!(
        targets[0].indirection[..],
        [AccessExpr::Subscript(Subscript::Index { .. })]
    ));
    assert_eq!(insert.columns.len(), 2);

    let stmt = pg().verified_stmt("INSERT INTO t (f3.if1, f3.if2) VALUES (1, ARRAY['foo'])");
    let targets = insert_of(&stmt)
        .column_targets
        .as_ref()
        .expect("column targets");
    assert!(matches!(targets[0].indirection[..], [AccessExpr::Dot(_)]));

    pg().verified_stmt("INSERT INTO t (f3.if2[1], f3.if2[2]) VALUES ('bear', 'beer')");
    pg().verified_stmt("INSERT INTO t (id, a[1:3]) VALUES (1, '{1,2,3}')");
    pg().verified_stmt("INSERT INTO t (id, fn.first) VALUES (2, 'Joe')");
    pg().verified_stmt("INSERT INTO t (f2[1], f2[2]) VALUES (1, DEFAULT)");
    pg().verified_stmt("INSERT INTO t (f2[1], f2[2]) SELECT 7, 8");
}

#[test]
fn parse_insert_from_select_followed_by_on_conflict() {
    // PostgreSQL reserves `ON`, so it cannot be read as the select item alias.
    let stmt = pg().verified_stmt("INSERT INTO t (a) SELECT 1 ON CONFLICT DO NOTHING");
    assert!(insert_of(&stmt).on.is_some());
    pg().verified_stmt("INSERT INTO t (a) SELECT 1 FROM u ON CONFLICT (a) DO NOTHING");
}

#[test]
fn parse_select_item_is_not_a_reserved_keyword() {
    pg().verified_stmt("SELECT 1 FOR UPDATE");
    pg().verified_stmt("SELECT 1 WINDOW w AS ()");
}

#[test]
fn parse_insert_plain_column_list_keeps_its_shape() {
    let stmt = pg().verified_stmt("INSERT INTO t (a, b) VALUES (1, 2)");
    let insert = insert_of(&stmt);
    assert!(insert.column_targets.is_none());
    assert_eq!(insert.columns.len(), 2);
}

#[test]
fn parse_on_conflict_inference_clause() {
    // https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT
    let stmt = pg().verified_stmt(
        "INSERT INTO t VALUES (2, 'apple') ON CONFLICT (lower(fruit)) DO UPDATE SET fruit = excluded.fruit",
    );
    match insert_of(&stmt).on.as_ref().expect("ON CONFLICT") {
        sqlparser::ast::OnInsert::OnConflict(conflict) => match &conflict.conflict_target {
            Some(ConflictTarget::Inference(inference)) => {
                assert_eq!(inference.elements.len(), 1);
                assert!(inference.predicate.is_none());
            }
            other => panic!("expected an inference clause, got {other:?}"),
        },
        other => panic!("expected ON CONFLICT, got {other:?}"),
    }

    let stmt = pg().verified_stmt(
        "INSERT INTO t VALUES (1, 'Blueberry') ON CONFLICT (key) WHERE fruit LIKE '%berry' DO UPDATE SET fruit = excluded.fruit",
    );
    match insert_of(&stmt).on.as_ref().expect("ON CONFLICT") {
        sqlparser::ast::OnInsert::OnConflict(conflict) => match &conflict.conflict_target {
            Some(ConflictTarget::Inference(inference)) => assert!(inference.predicate.is_some()),
            other => panic!("expected an inference clause, got {other:?}"),
        },
        other => panic!("expected ON CONFLICT, got {other:?}"),
    }

    pg().verified_stmt("INSERT INTO t VALUES (2, 'apple') ON CONFLICT (lower(fruit)) DO NOTHING");
    pg().verified_stmt(
        "INSERT INTO t VALUES (1) ON CONFLICT (a COLLATE \"C\" text_pattern_ops) DO NOTHING",
    );
    pg().verified_stmt("INSERT INTO t VALUES (1) ON CONFLICT (a DESC NULLS LAST) DO NOTHING");
}

#[test]
fn parse_on_conflict_plain_column_list_keeps_its_shape() {
    let stmt = pg().verified_stmt("INSERT INTO t VALUES (1, 2) ON CONFLICT (a, b) DO NOTHING");
    match insert_of(&stmt).on.as_ref().expect("ON CONFLICT") {
        sqlparser::ast::OnInsert::OnConflict(conflict) => assert!(matches!(
            conflict.conflict_target,
            Some(ConflictTarget::Columns(_))
        )),
        other => panic!("expected ON CONFLICT, got {other:?}"),
    }
}

// =============================================================================
// RETURNING WITH (OLD/NEW)
// =============================================================================

#[test]
fn parse_returning_row_aliases() {
    // https://www.postgresql.org/docs/current/sql-update.html
    let stmt = pg().verified_stmt(
        "UPDATE t SET f3 = f3 * 2 RETURNING WITH (OLD AS o, NEW AS n) o.f3::TEXT, n.f3::TEXT",
    );
    let returning = match &stmt {
        Statement::Update(update) => update.returning.as_ref().expect("RETURNING"),
        other => panic!("expected an UPDATE, got {other:?}"),
    };
    let aliases = returning.row_aliases.as_ref().expect("row aliases");
    assert_eq!(aliases[0].version, ReturningRowVersion::Old);
    assert_eq!(aliases[0].alias.value, "o");
    assert_eq!(aliases[1].version, ReturningRowVersion::New);

    // PostgreSQL parses duplicate aliases and rejects them semantically.
    pg().verified_stmt("INSERT INTO t VALUES (1) RETURNING WITH (OLD AS x, NEW AS x) f1");
    pg().verified_stmt("DELETE FROM t RETURNING WITH (OLD AS o) o.a");
    pg().verified_stmt(
        "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN UPDATE SET a = 1 RETURNING WITH (OLD AS o) o.a",
    );
}

// =============================================================================
// UPDATE assignment targets
// =============================================================================

#[test]
fn parse_update_slice_assignment_targets() {
    // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-MODIFYING
    let stmt = pg().verified_stmt("UPDATE t SET a[1:2] = '{16,25}' WHERE id = 1");
    match &update_assignment_target(&stmt) {
        AssignmentTarget::Indirection(target) => {
            assert!(matches!(
                target.indirection[..],
                [AccessExpr::Subscript(Subscript::Slice { .. })]
            ));
        }
        other => panic!("expected an indirection target, got {other:?}"),
    }
    pg().verified_stmt("UPDATE t SET a[:3] = '{11,12,13}'");
    pg().verified_stmt("UPDATE t SET a[4:] = '{24,25}'");
    pg().verified_stmt("UPDATE t SET a[:] = '{31,32}'");
}

#[test]
fn parse_update_multi_subscript_assignment_targets() {
    let stmt = pg().verified_stmt("UPDATE t SET js['a']['b']['c'] = '1' WHERE id = 4");
    match &update_assignment_target(&stmt) {
        AssignmentTarget::Indirection(target) => assert_eq!(target.indirection.len(), 3),
        other => panic!("expected an indirection target, got {other:?}"),
    }
    pg().verified_stmt("UPDATE t SET js['a']['b'] = '1'");
    pg().verified_stmt("UPDATE t SET a[1][2] = 1");
}

#[test]
fn parse_update_target_mixing_subscripts_and_field_selections() {
    let stmt = pg().verified_stmt("UPDATE t SET a.b[1].c = 1");
    match &update_assignment_target(&stmt) {
        AssignmentTarget::Indirection(target) => {
            assert_eq!(target.column.to_string(), "a.b");
            assert!(matches!(
                target.indirection[..],
                [AccessExpr::Subscript(_), AccessExpr::Dot(_)]
            ));
        }
        other => panic!("expected an indirection target, got {other:?}"),
    }
}

#[test]
fn parse_update_single_index_target_desugars_to_array_set() {
    pg().one_statement_parses_to(
        "UPDATE t SET a[2] = 5",
        "UPDATE t SET a = array_set(a, 2, 5)",
    );
}

fn update_assignment_target(stmt: &Statement) -> AssignmentTarget {
    match stmt {
        Statement::Update(update) => update.assignments[0].target.clone(),
        other => panic!("expected an UPDATE, got {other:?}"),
    }
}

// =============================================================================
// MERGE
// =============================================================================

#[test]
fn parse_merge_insert_overriding_and_default_values() {
    // https://www.postgresql.org/docs/current/sql-merge.html
    let stmt = pg().verified_stmt(
        "MERGE INTO t USING (SELECT 20 AS s_a, 'user value' AS s_b) AS s ON t.a = s.s_a WHEN NOT MATCHED THEN INSERT (a, b) OVERRIDING USER VALUE VALUES (s.s_a, s.s_b)",
    );
    match merge_action(&stmt) {
        MergeAction::Insert(insert) => {
            assert_eq!(insert.overriding, Some(OverridingKind::UserValue));
            assert!(matches!(insert.kind, MergeInsertKind::Values(_)));
        }
        other => panic!("expected a MERGE INSERT, got {other:?}"),
    }

    let stmt = pg().verified_stmt(
        "MERGE INTO t USING s ON t.tid = s.sid WHEN NOT MATCHED THEN INSERT DEFAULT VALUES",
    );
    match merge_action(&stmt) {
        MergeAction::Insert(insert) => {
            assert!(matches!(insert.kind, MergeInsertKind::DefaultValues));
        }
        other => panic!("expected a MERGE INSERT, got {other:?}"),
    }

    pg().verified_stmt(
        "MERGE INTO t USING s ON t.a = s.a WHEN NOT MATCHED THEN INSERT (a, b) OVERRIDING SYSTEM VALUE VALUES (1, 2)",
    );
}

#[test]
fn parse_merge_joined_source() {
    // https://www.postgresql.org/docs/current/sql-merge.html
    let stmt = pg().verified_stmt(
        "MERGE INTO target AS t USING source1 AS s1 INNER JOIN source2 AS s2 ON s1.sid = s2.sid ON t.tid = s1.sid WHEN MATCHED THEN UPDATE SET val = s1.val",
    );
    match &stmt {
        Statement::Merge { source_joins, .. } => assert_eq!(source_joins.len(), 1),
        other => panic!("expected a MERGE, got {other:?}"),
    }
}

fn merge_action(stmt: &Statement) -> MergeAction {
    match stmt {
        Statement::Merge { clauses, .. } => clauses[0].action.clone(),
        other => panic!("expected a MERGE, got {other:?}"),
    }
}

/// The first projected string literal of `sql`, which is not required to
/// round-trip because the literal was spelled across several lines.
fn single_string_literal(sql: &str) -> String {
    let select = pg().verified_only_select_with_canonical(sql, "");
    match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Value(value))
        | SelectItem::ExprWithAlias {
            expr: Expr::Value(value),
            ..
        } => match &value.value {
            Value::SingleQuotedString(text) => text.clone(),
            other => panic!("expected a string literal, got {other:?}"),
        },
        other => panic!("expected a literal projection, got {other:?}"),
    }
}

#[test]
fn string_literals_continue_across_a_newline() {
    assert_eq!(
        single_string_literal("SELECT 'first line'\n' - next line'\n' - third line' AS s"),
        "first line - next line - third line"
    );
    assert_eq!(single_string_literal("SELECT 'a'  \t\n\t 'b'"), "ab");
    assert_eq!(single_string_literal("SELECT 'a'\n-- a comment\n'b'"), "ab");
    assert_eq!(single_string_literal("SELECT 'a' -- a comment\n'b'"), "ab");
    assert!(pg()
        .parse_sql_statements("SELECT 'a'\n/* block */\n'b' AS b")
        .is_err());
}
