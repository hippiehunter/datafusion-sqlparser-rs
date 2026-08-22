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

//! Tests for the SQL/JSON query, constructor and table functions.
//!
//! Reference: <https://www.postgresql.org/docs/18/functions-json.html>

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments, JoinConstraint,
    JsonEncoding, JsonFormatClause, JsonOnBehavior, JsonPredicateUniqueKeyConstraint,
    JsonQueryWrapper, JsonQuotesBehavior, SelectItem, SqlJsonTable, SqlJsonTableColumn,
    TableFactor, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::test_utils::TestedDialects;

fn pg() -> TestedDialects {
    TestedDialects::new(vec![Box::new(PostgreSqlDialect {})])
}

/// Parses a `SELECT` with a single `JSON_TABLE` in its `FROM` clause and
/// returns that table function.
fn json_table(sql: &str) -> SqlJsonTable {
    let select = pg().verified_only_select(sql);
    match select.from[0].relation.clone() {
        TableFactor::SqlJsonTable(table) => table,
        other => panic!("expected a SQL/JSON table, got {other:?}"),
    }
}

/// Parses a `SELECT` whose single projection is a function call and returns
/// that call's clauses.
fn function_clauses(sql: &str) -> Vec<FunctionArgumentClause> {
    let select = pg().verified_only_select(sql);
    let expr = match &select.projection[0] {
        SelectItem::UnnamedExpr(expr) => expr.clone(),
        other => panic!("expected an unnamed projection, got {other:?}"),
    };
    match expr {
        Expr::Function(function) => match function.args {
            FunctionArguments::List(list) => list.clauses,
            other => panic!("expected a function argument list, got {other:?}"),
        },
        other => panic!("expected a function call, got {other:?}"),
    }
}

fn string_value(text: &str) -> Value {
    Value::SingleQuotedString(text.to_string())
}

// =============================================================================
// JSON_TABLE
// =============================================================================

#[test]
fn json_table_column_path_is_optional() {
    let table = json_table("SELECT foo FROM JSON_TABLE(NULL::JSONB, '$' COLUMNS (foo INT)) AS jt");
    match &table.columns[0] {
        SqlJsonTableColumn::Regular(column) => {
            assert_eq!(column.name.value, "foo");
            assert_eq!(column.path, None);
            assert_eq!(column.format, None);
            assert_eq!(column.wrapper, None);
            assert_eq!(column.quotes, None);
        }
        other => panic!("expected an ordinary column, got {other:?}"),
    }
}

#[test]
fn json_table_mixes_columns_with_and_without_a_path() {
    let table = json_table(
        "SELECT item, foo FROM JSON_TABLE(JSONB '{\"item\": 123}', '$' COLUMNS (item INT PATH '$.item', foo INT)) AS jt",
    );
    assert_eq!(table.columns.len(), 2);
    match &table.columns[0] {
        SqlJsonTableColumn::Regular(column) => {
            assert_eq!(column.path, Some(string_value("$.item")))
        }
        other => panic!("expected an ordinary column, got {other:?}"),
    }
}

#[test]
fn json_table_names_its_path() {
    let table = json_table(
        "SELECT js2 FROM JSON_TABLE(JSONB '\"1.23\"', '$' AS jt_root COLUMNS (js2 NUMERIC PATH '$')) AS jt",
    );
    assert_eq!(table.path_name.unwrap().value, "jt_root");
    assert_eq!(table.path, Expr::value(string_value("$")));
}

#[test]
fn json_table_passes_variables_to_the_path() {
    let table = json_table(
        "SELECT a FROM JSON_TABLE(JSONB '[1, 2, 3]', '$[*] ? (@ > $min)' PASSING 2 AS min COLUMNS (a INT PATH '$')) AS jt",
    );
    assert_eq!(table.passing.len(), 1);
    assert_eq!(table.passing[0].alias.as_ref().unwrap().value, "min");
}

#[test]
fn json_table_passes_several_variables() {
    let table = json_table(
        "SELECT a FROM JSON_TABLE(JSONB '[1]', '$[*]' PASSING 1 AS x, '{\"a\": 1}' FORMAT JSON AS y COLUMNS (a INT PATH '$')) AS jt",
    );
    assert_eq!(table.passing.len(), 2);
    assert!(matches!(table.passing[1].expr, Expr::JsonFormatted(_)));
}

#[test]
fn json_table_context_item_carries_a_format_clause() {
    let table = json_table(
        "SELECT a FROM JSON_TABLE('[1]' FORMAT JSON ENCODING UTF8, '$[*]' COLUMNS (a INT)) AS jt",
    );
    match table.context_item {
        Expr::JsonFormatted(value) => {
            assert_eq!(value.format.encoding, Some(JsonEncoding::Utf8))
        }
        other => panic!("expected a formatted context item, got {other:?}"),
    }
}

#[test]
fn json_table_column_for_ordinality() {
    let table = json_table(
        "SELECT n FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (n FOR ORDINALITY, a INT PATH '$')) AS jt",
    );
    match &table.columns[0] {
        SqlJsonTableColumn::ForOrdinality(name) => assert_eq!(name.value, "n"),
        other => panic!("expected an ordinality column, got {other:?}"),
    }
}

#[test]
fn json_table_column_format_json() {
    let table = json_table(
        "SELECT jst FROM JSON_TABLE(JSONB '[\"2\"]', 'lax $[*]' COLUMNS (jst TEXT FORMAT JSON PATH '$')) AS jt",
    );
    match &table.columns[0] {
        SqlJsonTableColumn::Regular(column) => {
            assert_eq!(column.format, Some(JsonFormatClause { encoding: None }));
            assert_eq!(column.path, Some(string_value("$")));
        }
        other => panic!("expected an ordinary column, got {other:?}"),
    }
}

#[test]
fn json_table_column_format_json_with_encoding() {
    json_table(
        "SELECT a FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (a TEXT FORMAT JSON ENCODING UTF16 PATH '$')) AS jt",
    );
    json_table(
        "SELECT a FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (a TEXT FORMAT JSON ENCODING UTF32 PATH '$')) AS jt",
    );
}

#[test]
fn json_table_column_wrapper_behaviors() {
    for (wrapper, expected) in [
        ("WITHOUT WRAPPER", JsonQueryWrapper::Without),
        ("WITHOUT ARRAY WRAPPER", JsonQueryWrapper::WithoutArray),
        ("WITH WRAPPER", JsonQueryWrapper::With),
        ("WITH ARRAY WRAPPER", JsonQueryWrapper::WithArray),
        (
            "WITH CONDITIONAL WRAPPER",
            JsonQueryWrapper::WithConditional,
        ),
        (
            "WITH UNCONDITIONAL WRAPPER",
            JsonQueryWrapper::WithUnconditional,
        ),
        (
            "WITH CONDITIONAL ARRAY WRAPPER",
            JsonQueryWrapper::WithConditionalArray,
        ),
        (
            "WITH UNCONDITIONAL ARRAY WRAPPER",
            JsonQueryWrapper::WithUnconditionalArray,
        ),
    ] {
        let table = json_table(&format!(
            "SELECT item FROM JSON_TABLE(JSONB '\"world\"', '$' COLUMNS (item TEXT FORMAT JSON PATH '$' {wrapper})) AS jt"
        ));
        match &table.columns[0] {
            SqlJsonTableColumn::Regular(column) => {
                assert_eq!(column.wrapper.as_ref(), Some(&expected))
            }
            other => panic!("expected an ordinary column, got {other:?}"),
        }
    }
}

#[test]
fn json_table_column_quotes_behavior() {
    for (clause, behavior, on_scalar_string) in [
        ("KEEP QUOTES", JsonQuotesBehavior::Keep, false),
        (
            "KEEP QUOTES ON SCALAR STRING",
            JsonQuotesBehavior::Keep,
            true,
        ),
        ("OMIT QUOTES", JsonQuotesBehavior::Omit, false),
        (
            "OMIT QUOTES ON SCALAR STRING",
            JsonQuotesBehavior::Omit,
            true,
        ),
    ] {
        let table = json_table(&format!(
            "SELECT jst FROM JSON_TABLE(JSONB '[\"2\"]', 'lax $[*]' COLUMNS (jst TEXT FORMAT JSON PATH '$' {clause})) AS jt"
        ));
        match &table.columns[0] {
            SqlJsonTableColumn::Regular(column) => {
                let quotes = column.quotes.as_ref().unwrap();
                assert_eq!(quotes.behavior, behavior);
                assert_eq!(quotes.on_scalar_string, on_scalar_string);
            }
            other => panic!("expected an ordinary column, got {other:?}"),
        }
    }
}

#[test]
fn json_table_column_on_empty_and_on_error() {
    let table = json_table(
        "SELECT a FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (a INT PATH '$' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) AS jt",
    );
    match &table.columns[0] {
        SqlJsonTableColumn::Regular(column) => {
            assert!(matches!(column.on_empty, Some(JsonOnBehavior::Default(_))));
            assert!(matches!(column.on_error, Some(JsonOnBehavior::Default(_))));
        }
        other => panic!("expected an ordinary column, got {other:?}"),
    }

    for behavior in ["ERROR", "NULL", "EMPTY ARRAY", "EMPTY OBJECT", "DEFAULT 1"] {
        json_table(&format!(
            "SELECT a FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (a JSONB PATH '$' {behavior} ON EMPTY)) AS jt"
        ));
        json_table(&format!(
            "SELECT a FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (a JSONB PATH '$' {behavior} ON ERROR)) AS jt"
        ));
    }
}

#[test]
fn json_table_exists_column() {
    let table = json_table(
        "SELECT e FROM JSON_TABLE(JSONB '1', '$' COLUMNS (e TEXT EXISTS PATH 'strict $.aaa' FALSE ON ERROR)) AS jt",
    );
    match &table.columns[0] {
        SqlJsonTableColumn::Exists(column) => {
            assert_eq!(column.name.value, "e");
            assert_eq!(column.path, Some(string_value("strict $.aaa")));
            assert_eq!(column.on_error, Some(JsonOnBehavior::False));
        }
        other => panic!("expected an EXISTS column, got {other:?}"),
    }
}

#[test]
fn json_table_exists_column_on_error_behaviors() {
    for (clause, expected) in [
        ("ERROR", JsonOnBehavior::Error),
        ("TRUE", JsonOnBehavior::True),
        ("FALSE", JsonOnBehavior::False),
        ("UNKNOWN", JsonOnBehavior::Unknown),
    ] {
        let table = json_table(&format!(
            "SELECT e FROM JSON_TABLE(JSONB '1', '$' COLUMNS (e INT EXISTS PATH 'strict $.aaa' {clause} ON ERROR)) AS jt"
        ));
        match &table.columns[0] {
            SqlJsonTableColumn::Exists(column) => {
                assert_eq!(column.on_error.as_ref(), Some(&expected))
            }
            other => panic!("expected an EXISTS column, got {other:?}"),
        }
    }
}

#[test]
fn json_table_exists_column_path_is_optional() {
    let table = json_table("SELECT e FROM JSON_TABLE(JSONB '1', '$' COLUMNS (e INT EXISTS)) AS jt");
    match &table.columns[0] {
        SqlJsonTableColumn::Exists(column) => assert_eq!(column.path, None),
        other => panic!("expected an EXISTS column, got {other:?}"),
    }
}

#[test]
fn json_table_nested_columns() {
    let table = json_table(
        "SELECT b, c FROM JSON_TABLE(JSONB '{\"b\": 7, \"n\": [1, 2]}', '$' COLUMNS (b INT PATH '$.b', NESTED PATH '$.n[*]' AS n_a COLUMNS (c INT PATH '$'))) AS jt",
    );
    match &table.columns[1] {
        SqlJsonTableColumn::Nested(column) => {
            assert_eq!(column.path, string_value("$.n[*]"));
            assert_eq!(column.path_name.as_ref().unwrap().value, "n_a");
            assert_eq!(column.columns.len(), 1);
        }
        other => panic!("expected a NESTED column, got {other:?}"),
    }
}

#[test]
fn json_table_nested_columns_nest_further() {
    let table = json_table(
        "SELECT * FROM JSON_TABLE(JSONB '[]', '$' COLUMNS (NESTED PATH '$[*]' AS n COLUMNS (NESTED PATH '$' AS m COLUMNS (a INT)))) AS jt",
    );
    match &table.columns[0] {
        SqlJsonTableColumn::Nested(column) => {
            assert!(matches!(column.columns[0], SqlJsonTableColumn::Nested(_)))
        }
        other => panic!("expected a NESTED column, got {other:?}"),
    }
}

#[test]
fn json_table_nested_path_keyword_is_noise() {
    pg().one_statement_parses_to(
        "SELECT * FROM JSON_TABLE(JSONB '[]', '$' COLUMNS (NESTED '$[*]' COLUMNS (a INT))) AS jt",
        "SELECT * FROM JSON_TABLE(JSONB '[]', '$' COLUMNS (NESTED PATH '$[*]' COLUMNS (a INT))) AS jt",
    );
}

#[test]
fn json_table_column_may_be_named_nested() {
    let table = json_table(
        "SELECT nested FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (nested INT PATH '$')) AS jt",
    );
    match &table.columns[0] {
        SqlJsonTableColumn::Regular(column) => assert_eq!(column.name.value, "nested"),
        other => panic!("expected an ordinary column, got {other:?}"),
    }
}

#[test]
fn json_table_on_error_follows_the_column_list() {
    let table = json_table(
        "SELECT js2 FROM JSON_TABLE(JSONB '[]', 'strict $.a' COLUMNS (js2 INT PATH '$') ERROR ON ERROR) AS jt",
    );
    assert_eq!(table.on_error, Some(JsonOnBehavior::Error));
}

#[test]
fn json_table_bare_empty_on_error_means_empty_array() {
    let table = pg().one_statement_parses_to(
        "SELECT js2 FROM JSON_TABLE(JSONB '[]', 'strict $.a' COLUMNS (js2 INT PATH '$') EMPTY ON ERROR) AS jt",
        "SELECT js2 FROM JSON_TABLE(JSONB '[]', 'strict $.a' COLUMNS (js2 INT PATH '$') EMPTY ARRAY ON ERROR) AS jt",
    );
    match &pg().verified_only_select(&table.to_string()).from[0].relation {
        TableFactor::SqlJsonTable(table) => {
            assert_eq!(table.on_error, Some(JsonOnBehavior::EmptyArray))
        }
        other => panic!("expected a SQL/JSON table, got {other:?}"),
    }
}

#[test]
fn json_table_alias_may_rename_columns() {
    let table = json_table(
        "SELECT * FROM JSON_TABLE(NULL::JSONB, '$' COLUMNS (v1 TIMESTAMP)) AS f (v1, v2)",
    );
    let alias = table.alias.unwrap();
    assert_eq!(alias.name.value, "f");
    assert_eq!(alias.columns.len(), 2);
}

#[test]
fn json_table_accepts_an_implicit_alias() {
    let table = json_table("SELECT a FROM JSON_TABLE(JSONB '[1]', '$[*]' COLUMNS (a INT)) AS jt");
    assert_eq!(table.alias.unwrap().name.value, "jt");
}

// =============================================================================
// JSON_EXISTS
// =============================================================================

#[test]
fn json_exists_passes_variables() {
    let clauses = function_clauses(
        "SELECT JSON_EXISTS(JSONB '{\"a\": 1, \"b\": 2}', '$.* ? (@ > $x && @ < $y)' PASSING 0 AS x, 2 AS y)",
    );
    match &clauses[0] {
        FunctionArgumentClause::JsonPassing(bindings) => {
            assert_eq!(bindings.len(), 2);
            assert_eq!(bindings[0].alias.as_ref().unwrap().value, "x");
            assert_eq!(bindings[1].alias.as_ref().unwrap().value, "y");
        }
        other => panic!("expected a PASSING clause, got {other:?}"),
    }
}

#[test]
fn json_exists_on_error_behaviors() {
    for (clause, expected) in [
        ("ERROR", JsonOnBehavior::Error),
        ("TRUE", JsonOnBehavior::True),
        ("FALSE", JsonOnBehavior::False),
        ("UNKNOWN", JsonOnBehavior::Unknown),
    ] {
        let clauses = function_clauses(&format!(
            "SELECT JSON_EXISTS(JSONB '1', '$' {clause} ON ERROR)"
        ));
        assert_eq!(clauses, vec![FunctionArgumentClause::JsonOnError(expected)]);
    }
}

// =============================================================================
// JSON_QUERY
// =============================================================================

#[test]
fn json_query_accepts_the_full_clause_list() {
    let clauses = function_clauses(
        "SELECT JSON_QUERY(JSONB '[1]', '$' PASSING 1 AS x RETURNING JSONB FORMAT JSON WITH CONDITIONAL ARRAY WRAPPER KEEP QUOTES ON SCALAR STRING EMPTY ARRAY ON EMPTY ERROR ON ERROR)",
    );
    assert!(matches!(clauses[0], FunctionArgumentClause::JsonPassing(_)));
    match &clauses[1] {
        FunctionArgumentClause::JsonReturningClause(returning) => {
            assert_eq!(returning.format, Some(JsonFormatClause { encoding: None }))
        }
        other => panic!("expected a RETURNING clause, got {other:?}"),
    }
    assert_eq!(
        clauses[2],
        FunctionArgumentClause::JsonQueryWrapper(JsonQueryWrapper::WithConditionalArray)
    );
    assert!(matches!(clauses[3], FunctionArgumentClause::JsonQuotes(_)));
    assert_eq!(
        clauses[4],
        FunctionArgumentClause::JsonOnEmpty(JsonOnBehavior::EmptyArray)
    );
    assert_eq!(
        clauses[5],
        FunctionArgumentClause::JsonOnError(JsonOnBehavior::Error)
    );
}

#[test]
fn json_query_quotes_behavior() {
    for clause in [
        "KEEP QUOTES",
        "KEEP QUOTES ON SCALAR STRING",
        "OMIT QUOTES",
        "OMIT QUOTES ON SCALAR STRING",
    ] {
        let clauses = function_clauses(&format!(
            "SELECT JSON_QUERY(JSONB '\"aaa\"', '$' RETURNING TEXT {clause})"
        ));
        assert!(matches!(clauses[1], FunctionArgumentClause::JsonQuotes(_)));
    }
}

#[test]
fn json_query_wrapper_precedes_quotes() {
    let clauses = function_clauses(
        "SELECT JSON_QUERY(JSONB '[\"1\"]', '$[*]' WITH CONDITIONAL WRAPPER KEEP QUOTES)",
    );
    assert_eq!(
        clauses[0],
        FunctionArgumentClause::JsonQueryWrapper(JsonQueryWrapper::WithConditional)
    );
    assert!(matches!(clauses[1], FunctionArgumentClause::JsonQuotes(_)));
}

// =============================================================================
// JSON_VALUE
// =============================================================================

#[test]
fn json_value_defaults_may_be_negative() {
    let select = pg().verified_only_select(
        "SELECT JSON_VALUE(JSONB '{\"a\": 1, \"b\": 2}', '$.* ? (@ > $x)' PASSING x AS x RETURNING INT DEFAULT -1 ON EMPTY DEFAULT -2 ON ERROR) FROM generate_series(1, 1) AS x",
    );
    let clauses = match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Function(function)) => match &function.args {
            FunctionArguments::List(list) => list.clauses.clone(),
            other => panic!("expected a function argument list, got {other:?}"),
        },
        other => panic!("expected a function call, got {other:?}"),
    };
    assert!(matches!(clauses[0], FunctionArgumentClause::JsonPassing(_)));
    assert!(matches!(
        clauses[2],
        FunctionArgumentClause::JsonOnEmpty(JsonOnBehavior::Default(_))
    ));
    assert!(matches!(
        clauses[3],
        FunctionArgumentClause::JsonOnError(JsonOnBehavior::Default(_))
    ));
}

#[test]
fn json_value_on_empty_and_on_error_behaviors() {
    for clause in ["ERROR", "NULL", "DEFAULT 1"] {
        pg().verified_only_select(&format!(
            "SELECT JSON_VALUE(JSONB '1', '$' RETURNING INT {clause} ON EMPTY {clause} ON ERROR)"
        ));
    }
}

// =============================================================================
// JSON, JSON_SCALAR and JSON_SERIALIZE
// =============================================================================

#[test]
fn json_constructor_takes_a_format_clause() {
    let select =
        pg().verified_only_select("SELECT JSON('{ \"a\" : 1 } ' FORMAT JSON ENCODING UTF8)");
    let args = match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Function(function)) => match &function.args {
            FunctionArguments::List(list) => list.args.clone(),
            other => panic!("expected a function argument list, got {other:?}"),
        },
        other => panic!("expected a function call, got {other:?}"),
    };
    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::JsonFormatted(value))) => {
            assert_eq!(value.format.encoding, Some(JsonEncoding::Utf8))
        }
        other => panic!("expected a formatted argument, got {other:?}"),
    }
}

#[test]
fn json_constructor_key_uniqueness() {
    for (clause, expected) in [
        (
            "WITH UNIQUE KEYS",
            JsonPredicateUniqueKeyConstraint::WithUniqueKeys,
        ),
        (
            "WITHOUT UNIQUE KEYS",
            JsonPredicateUniqueKeyConstraint::WithoutUniqueKeys,
        ),
    ] {
        let clauses = function_clauses(&format!("SELECT JSON('{{}}' FORMAT JSON {clause})"));
        assert_eq!(
            clauses,
            vec![FunctionArgumentClause::JsonUniqueKeys(expected)]
        );
    }
}

#[test]
fn json_scalar_takes_a_single_expression() {
    pg().verified_only_select("SELECT JSON_SCALAR(1)");
    pg().verified_only_select("SELECT JSON_SCALAR(a + 1) FROM t");
}

#[test]
fn json_serialize_returns_a_formatted_type() {
    pg().verified_only_select("SELECT JSON_SERIALIZE('1' FORMAT JSON)");
    let clauses = function_clauses(
        "SELECT JSON_SERIALIZE('1' FORMAT JSON RETURNING BYTEA FORMAT JSON ENCODING UTF8)",
    );
    match &clauses[0] {
        FunctionArgumentClause::JsonReturningClause(returning) => assert_eq!(
            returning.format,
            Some(JsonFormatClause {
                encoding: Some(JsonEncoding::Utf8)
            })
        ),
        other => panic!("expected a RETURNING clause, got {other:?}"),
    }
}

// =============================================================================
// JSON_OBJECT and JSON_ARRAY
// =============================================================================

#[test]
fn json_object_returning_may_be_its_only_clause() {
    let clauses =
        function_clauses("SELECT JSON_OBJECT(RETURNING BYTEA FORMAT JSON ENCODING UTF16)");
    match &clauses[0] {
        FunctionArgumentClause::JsonReturningClause(returning) => assert_eq!(
            returning.format,
            Some(JsonFormatClause {
                encoding: Some(JsonEncoding::Utf16)
            })
        ),
        other => panic!("expected a RETURNING clause, got {other:?}"),
    }
}

#[test]
fn json_object_keys_need_not_be_strings() {
    let select = pg()
        .verified_only_select("SELECT JSON_OBJECT(1: 1, '1': NULL WITH UNIQUE RETURNING JSONB)");
    let (args, clauses) = match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Function(function)) => match &function.args {
            FunctionArguments::List(list) => (list.args.clone(), list.clauses.clone()),
            other => panic!("expected a function argument list, got {other:?}"),
        },
        other => panic!("expected a function call, got {other:?}"),
    };
    assert_eq!(args.len(), 2);
    assert!(matches!(args[0], FunctionArg::ExprNamed { .. }));
    assert_eq!(
        clauses[0],
        FunctionArgumentClause::JsonUniqueKeys(JsonPredicateUniqueKeyConstraint::WithUniqueKeys)
    );
    assert!(matches!(
        clauses[1],
        FunctionArgumentClause::JsonReturningClause(_)
    ));
}

#[test]
fn json_object_values_may_be_formatted() {
    pg().verified_only_select(
        "SELECT JSON_OBJECT('a': '1' FORMAT JSON NULL ON NULL WITHOUT UNIQUE KEYS RETURNING TEXT FORMAT JSON)",
    );
    pg().verified_only_select("SELECT JSON_OBJECT('a' VALUE '1' FORMAT JSON ABSENT ON NULL)");
}

#[test]
fn json_array_takes_a_query() {
    let select =
        pg().verified_only_select("SELECT JSON_ARRAY(SELECT i FROM generate_series(1, 3) AS i)");
    let args = match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Function(function)) => match &function.args {
            FunctionArguments::List(list) => list.args.clone(),
            other => panic!("expected a function argument list, got {other:?}"),
        },
        other => panic!("expected a function call, got {other:?}"),
    };
    assert!(matches!(
        args[0],
        FunctionArg::Unnamed(FunctionArgExpr::Query(_))
    ));
}

#[test]
fn json_array_query_takes_a_returning_clause() {
    let clauses = function_clauses("SELECT JSON_ARRAY(SELECT 1 RETURNING JSONB)");
    assert!(matches!(
        clauses[0],
        FunctionArgumentClause::JsonReturningClause(_)
    ));
}

#[test]
fn json_array_values_may_be_formatted() {
    pg().verified_only_select("SELECT JSON_ARRAY(1, '2' FORMAT JSON NULL ON NULL RETURNING JSONB)");
    pg().verified_only_select("SELECT JSON_ARRAY(ABSENT ON NULL)");
}

#[test]
fn json_arrayagg_clauses() {
    pg().verified_only_select("SELECT JSON_ARRAYAGG(a) FROM t");
    pg().verified_only_select(
        "SELECT JSON_ARRAYAGG(a FORMAT JSON ORDER BY a NULL ON NULL RETURNING JSONB) FROM t",
    );
    pg().verified_only_select(
        "SELECT JSON_ARRAYAGG(a ABSENT ON NULL RETURNING TEXT FORMAT JSON) FROM t",
    );
}

#[test]
fn json_objectagg_clauses() {
    pg().verified_only_select("SELECT JSON_OBJECTAGG(k: v) FROM t");
    pg().verified_only_select(
        "SELECT JSON_OBJECTAGG(k VALUE v FORMAT JSON ABSENT ON NULL WITH UNIQUE KEYS RETURNING JSONB) FROM t",
    );
}

// =============================================================================
// Table function column definition lists and USING aliases
// =============================================================================

#[test]
fn function_alias_may_be_a_bare_column_definition_list() {
    let select = pg().verified_only_select(
        "SELECT x, y FROM json_populate_record(NULL::RECORD, '{\"x\": 776}') AS (x INT, y INT)",
    );
    let alias = match &select.from[0].relation {
        TableFactor::Table { alias, .. } => alias.clone().unwrap(),
        other => panic!("expected a table function, got {other:?}"),
    };
    assert_eq!(alias.name.value, "");
    assert_eq!(alias.columns.len(), 2);
    assert!(alias.columns[0].data_type.is_some());
}

#[test]
fn function_alias_may_name_the_table_and_its_columns() {
    pg().verified_only_select(
        "SELECT x FROM jsonb_populate_recordset(NULL::RECORD, '[{\"x\": 776}]'::JSONB) AS r (x INT, y INT)",
    );
}

#[test]
fn join_using_may_carry_an_alias() {
    let select = pg().verified_only_select(
        "SELECT row_to_json(x.*)::TEXT FROM qy_scf_j1 JOIN qy_scf_j2 USING(i) AS x WHERE qy_scf_j1.t = 'one'",
    );
    match &select.from[0].joins[0].join_operator {
        sqlparser::ast::JoinOperator::Join(JoinConstraint::UsingWithAlias { columns, alias }) => {
            assert_eq!(columns.len(), 1);
            assert_eq!(alias.value, "x");
        }
        other => panic!("expected a USING join with an alias, got {other:?}"),
    }
}
