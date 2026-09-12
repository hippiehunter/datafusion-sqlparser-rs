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

use sqlparser::ast::{
    Expr, FunctionArguments, Select, SelectItem, SetExpr, Statement, TopQuantity,
};
use sqlparser::dialect::{Dialect, MsSqlDialect, MySqlDialect, OracleDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;

fn check_select(dialect: &dyn Dialect, sql: &str, check: impl FnOnce(&Select)) {
    let statements = Parser::parse_sql(dialect, sql)
        .unwrap_or_else(|error| panic!("{dialect:?}: {sql}: {error}"));
    assert_eq!(statements.len(), 1);
    let Statement::Query(query) = &statements[0] else {
        panic!("expected SELECT: {sql}");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected SELECT body: {sql}");
    };
    check(select);
    let rendered = statements[0].to_string();
    let reparsed = Parser::parse_sql(dialect, &rendered).unwrap();
    assert_eq!(statements[0], reparsed[0], "{sql} -> {rendered}");
}

#[test]
fn pgtap_skip_body_is_a_function_call_with_ordered_parameters() {
    check_select(&PostgreSqlDialect {}, "SELECT skip($2, $1)", |select| {
        assert!(select.top.is_none());
        let [SelectItem::UnnamedExpr(Expr::Function(function))] = select.projection.as_slice()
        else {
            panic!("expected skip function call");
        };
        assert_eq!(function.name.to_string(), "skip");
        let FunctionArguments::List(arguments) = &function.args else {
            panic!("expected function arguments");
        };
        assert_eq!(
            arguments
                .args
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["$2", "$1"]
        );
    });
}

#[test]
fn skip_function_calls_and_columns_are_not_select_modifiers() {
    for sql in [
        "SELECT skip()",
        "SELECT skip(1)",
        "SELECT skip (1) AS result",
        "SELECT skip /* comment */ (1, 2)",
        "SELECT skip(NULL, $1)",
        "SELECT DISTINCT skip($1)",
        "SELECT ALL skip($1)",
        "SELECT skip(skip(1), 2)",
        "SELECT skip(1) + 2",
        "SELECT skip(1) * 2",
        "SELECT skip(1)::text",
        "SELECT skip(1) AS result FROM records",
        "SELECT skip(1) result FROM records",
        "SELECT skip(1), skip(2) FROM records",
        "SELECT skip FROM records",
        "SELECT skip AS result FROM records",
        "SELECT skip, id FROM records",
        "SELECT skip + 1 FROM records",
        "SELECT public.skip($2, $1)",
        "SELECT \"skip\"($2, $1)",
    ] {
        check_select(&PostgreSqlDialect {}, sql, |select| {
            assert!(select.top.is_none(), "{sql}");
        });
    }
}

#[test]
fn skip_calls_are_preserved_in_every_builtin_dialect() {
    for dialect in [
        &PostgreSqlDialect {} as &dyn Dialect,
        &MsSqlDialect {},
        &MySqlDialect {},
        &OracleDialect {},
    ] {
        check_select(
            dialect,
            "SELECT skip(1), skip(2, 3) FROM records",
            |select| {
                assert!(select.top.is_none());
                assert_eq!(select.projection.len(), 2);
                assert!(select
                    .projection
                    .iter()
                    .all(|item| matches!(item, SelectItem::UnnamedExpr(Expr::Function(_)))));
            },
        );
    }
}

#[test]
fn synergy_literal_skip_and_top_clauses_still_roundtrip() {
    for (sql, skip, top) in [
        // PostgreSQL permits an empty target list, including after SKIP.
        ("SELECT SKIP 2", 2, None),
        ("SELECT SKIP 0 id FROM records", 0, None),
        ("SELECT SKIP 2 id FROM records", 2, None),
        ("SELECT SKIP 2 TOP 5 id FROM records", 2, Some(5)),
        ("SELECT TOP 5 SKIP 2 id FROM records", 2, Some(5)),
        ("SELECT DISTINCT SKIP 2 TOP 5 id FROM records", 2, Some(5)),
        ("SELECT SKIP 2 TOP 5 skip(1) FROM records", 2, Some(5)),
        ("SELECT TOP 5 SKIP 2 skip(1) FROM records", 2, Some(5)),
        ("SELECT SKIP 2 42 FROM records", 2, None),
    ] {
        check_select(&PostgreSqlDialect {}, sql, |select| {
            let clause = select.top.as_ref().expect("expected row-limit clause");
            assert_eq!(clause.skip, Some(TopQuantity::Constant(skip)));
            assert_eq!(clause.quantity, top.map(TopQuantity::Constant));
        });
    }
}

#[test]
fn top_keeps_parenthesized_quantities_without_eating_skip_projections() {
    for dialect in [&PostgreSqlDialect {} as &dyn Dialect, &MsSqlDialect {}] {
        for sql in [
            "SELECT TOP (5) skip(1)",
            "SELECT TOP 5 skip(1, 2)",
            "SELECT TOP 5 skip FROM records",
            "SELECT TOP (5) PERCENT WITH TIES skip(1, 2) FROM records",
        ] {
            check_select(dialect, sql, |select| {
                let top = select.top.as_ref().expect("expected TOP clause");
                assert!(top.quantity.is_some());
                assert!(top.skip.is_none());
                assert_eq!(select.projection.len(), 1);
            });
        }
    }
}

#[test]
fn invalid_literal_skip_quantities_are_rejected() {
    for sql in [
        "SELECT SKIP 1.5 id FROM records",
        "SELECT SKIP 18446744073709551616 id FROM records",
    ] {
        assert!(
            Parser::parse_sql(&PostgreSqlDialect {}, sql).is_err(),
            "{sql}"
        );
    }
}
