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

//! Tests for the PL/pgSQL body grammar and for the `CREATE FUNCTION` /
//! `CREATE PROCEDURE` / `DO` forms that carry one.
//!
//! References:
//! - <https://www.postgresql.org/docs/current/plpgsql.html>
//! - <https://www.postgresql.org/docs/current/sql-createfunction.html>
//! - <https://www.postgresql.org/docs/current/sql-createprocedure.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{
    ArgMode, AtomicBlock, BeginEndStatements, ConditionalStatements, CreateFunctionBody, DataType,
    DiagnosticsItem, ExecuteInto, Expr, ForLoopVariant, FunctionBehavior, FunctionCalledOnNull,
    FunctionParallel, GetDiagnosticsKind, PlSqlDeclaration, RoutineAttribute, SqlPsmDataType,
    Statement,
};

// =============================================================================
// Helpers
// =============================================================================

/// Parses `sql` and asserts that the Display form re-parses to an equal AST.
fn round_trip(sql: &str) -> Statement {
    let statement = verified_pg_stmt(sql);
    let rendered = statement.to_string();
    let reparsed = verified_pg_stmt(&rendered);
    assert_eq!(
        statement, reparsed,
        "Display did not round-trip: {rendered}"
    );
    statement
}

/// As [`round_trip`], and additionally requires the Display form to match the
/// source text exactly.
fn round_trip_exact(sql: &str) -> Statement {
    let statement = round_trip(sql);
    assert_eq!(statement.to_string(), sql);
    statement
}

/// The structured body of a PL/pgSQL function.
fn function_block(sql: &str) -> BeginEndStatements {
    let statement = round_trip(sql);
    match extract_create_function(&statement).function_body.clone() {
        Some(CreateFunctionBody::AsBeginEnd(block)) => block,
        other => panic!("expected a PL/pgSQL block body, got {other:?}"),
    }
}

/// The structured body of a `DO` block.
fn do_block(sql: &str) -> BeginEndStatements {
    match round_trip(sql) {
        Statement::Do(statement) => match statement.body {
            sqlparser::ast::DoBody::Block(block) => block,
            other => panic!("expected a structured DO body, got {other:?}"),
        },
        other => panic!("expected Statement::Do, got {other:?}"),
    }
}

/// The single statement a PL/pgSQL function body contains.
fn only_body_statement(sql: &str) -> Statement {
    let block = function_block(sql);
    assert_eq!(block.statements.len(), 1, "expected exactly one statement");
    block.statements[0].clone()
}

// =============================================================================
// Block and loop labels
// =============================================================================

#[test]
fn test_label_before_outermost_declare_section() {
    // https://www.postgresql.org/docs/current/plpgsql-structure.html
    let block = function_block(
        "CREATE FUNCTION f(param1 INT) RETURNS TEXT LANGUAGE plpgsql \
         AS $$ <<outerblock>> DECLARE param1 INT := 1; BEGIN RETURN param1; END $$",
    );
    assert_eq!(
        block.label.as_ref().map(|label| label.value.as_str()),
        Some("outerblock")
    );
    assert_eq!(block.declarations.len(), 1);
}

#[test]
fn test_label_on_nested_declare_block_and_matching_end_label() {
    // https://www.postgresql.org/docs/current/plpgsql-structure.html
    let block = function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ BEGIN <<inner>> DECLARE v INT := 1; BEGIN RETURN v; END inner; END $$",
    );
    match &block.statements[0] {
        Statement::PlSqlBlock(nested) => {
            assert_eq!(
                nested.label.as_ref().map(|label| label.value.as_str()),
                Some("inner")
            );
            assert_eq!(
                nested.end_label.as_ref().map(|label| label.value.as_str()),
                Some("inner")
            );
            assert_eq!(nested.declarations.len(), 1);
        }
        other => panic!("expected a nested block, got {other:?}"),
    }
}

#[test]
fn test_unlabelled_nested_declare_block() {
    // PL/pgSQL reads a statement-initial DECLARE as a nested block.
    let block = function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ BEGIN DECLARE v INT := 5; BEGIN RETURN v; END; END $$",
    );
    assert!(matches!(block.statements[0], Statement::PlSqlBlock(_)));
}

#[test]
fn test_label_before_begin_block_without_declarations() {
    let block = function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ BEGIN <<blk>> BEGIN RETURN 1; END; RETURN 2; END $$",
    );
    match &block.statements[0] {
        Statement::PlSqlBlock(nested) => assert_eq!(
            nested.label.as_ref().map(|label| label.value.as_str()),
            Some("blk")
        ),
        other => panic!("expected a nested block, got {other:?}"),
    }
}

#[test]
fn test_label_before_integer_for_loop() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-INTEGER-FOR
    let block = function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE n INT := 0; BEGIN <<outerloop>> FOR i IN 1..2 LOOP n := n + i; END LOOP; \
         RETURN n; END $$",
    );
    match &block.statements[0] {
        Statement::PlSqlLabeled { label, statement } => {
            assert_eq!(label.value, "outerloop");
            assert!(matches!(**statement, Statement::For(_)));
        }
        other => panic!("expected a labeled loop, got {other:?}"),
    }
}

#[test]
fn test_label_before_while_loop_and_end_loop_label() {
    let block = function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE i INT := 0; BEGIN <<w>> WHILE i < 3 LOOP i := i + 1; END LOOP w; \
         RETURN i; END $$",
    );
    match &block.statements[0] {
        Statement::PlSqlLabeled { label, statement } => {
            assert_eq!(label.value, "w");
            match &**statement {
                Statement::While(while_statement) => {
                    assert!(while_statement.has_loop_keyword);
                    assert_eq!(
                        while_statement
                            .end_label
                            .as_ref()
                            .map(|label| label.value.as_str()),
                        Some("w")
                    );
                }
                other => panic!("expected a WHILE loop, got {other:?}"),
            }
        }
        other => panic!("expected a labeled loop, got {other:?}"),
    }
}

#[test]
fn test_label_before_unconditional_loop() {
    let block = function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE i INT := 0; BEGIN <<l>> LOOP i := i + 1; EXIT l WHEN i > 3; END LOOP l; \
         RETURN i; END $$",
    );
    match &block.statements[0] {
        Statement::PlSqlLabeled { label, statement } => {
            assert_eq!(label.value, "l");
            assert!(matches!(**statement, Statement::Loop(_)));
        }
        other => panic!("expected a labeled loop, got {other:?}"),
    }
}

#[test]
fn test_label_before_foreach_loop() {
    let block = function_block(
        "CREATE FUNCTION f(a INT[]) RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE x INT[]; BEGIN <<fe>> FOREACH x SLICE 1 IN ARRAY a LOOP NULL; END LOOP fe; \
         RETURN 1; END $$",
    );
    match &block.statements[0] {
        Statement::PlSqlLabeled { label, statement } => {
            assert_eq!(label.value, "fe");
            match &**statement {
                Statement::Foreach(foreach) => assert_eq!(foreach.slice, Some(1)),
                other => panic!("expected a FOREACH loop, got {other:?}"),
            }
        }
        other => panic!("expected a labeled loop, got {other:?}"),
    }
}

#[test]
fn test_exit_and_continue_with_label_and_condition() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-EXIT
    let block = function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ BEGIN <<outerloop>> FOR i IN 1..3 LOOP FOR j IN 1..3 LOOP \
         CONTINUE outerloop WHEN j = 2; EXIT outerloop WHEN i = 2; EXIT; CONTINUE; \
         END LOOP; END LOOP; RETURN 1; END $$",
    );
    let inner = match &block.statements[0] {
        Statement::PlSqlLabeled { statement, .. } => match &**statement {
            Statement::For(outer) => outer.body.statements()[0].clone(),
            other => panic!("expected a FOR loop, got {other:?}"),
        },
        other => panic!("expected a labeled loop, got {other:?}"),
    };
    let statements = match &inner {
        Statement::For(inner) => inner.body.statements().clone(),
        other => panic!("expected a FOR loop, got {other:?}"),
    };
    match &statements[0] {
        Statement::Continue(statement) => {
            assert_eq!(
                statement.label.as_ref().map(|label| label.value.as_str()),
                Some("outerloop")
            );
            assert!(statement.condition.is_some());
        }
        other => panic!("expected CONTINUE, got {other:?}"),
    }
    match &statements[1] {
        Statement::Exit(statement) => {
            assert_eq!(
                statement.label.as_ref().map(|label| label.value.as_str()),
                Some("outerloop")
            );
            assert!(statement.condition.is_some());
        }
        other => panic!("expected EXIT, got {other:?}"),
    }
    assert!(matches!(&statements[2], Statement::Exit(statement)
        if statement.label.is_none() && statement.condition.is_none()));
    assert!(matches!(&statements[3], Statement::Continue(statement)
        if statement.label.is_none() && statement.condition.is_none()));
}

#[test]
fn test_label_qualified_names_in_expressions() {
    // A block label qualifies the variables declared in that block.
    function_block(
        "CREATE FUNCTION f(param1 INT) RETURNS TEXT LANGUAGE plpgsql \
         AS $$ <<outerblock>> DECLARE param1 INT := 1; BEGIN <<innerblock>> DECLARE param1 INT := 2; \
         BEGIN RETURN param1 || ',' || innerblock.param1 || ',' || outerblock.param1 || ',' || f.param1; \
         END; END $$",
    );
}

#[test]
fn test_function_body_end_label() {
    let block = function_block(
        "CREATE FUNCTION f() RETURNS TEXT LANGUAGE plpgsql \
         AS $$ <<blbl>> BEGIN <<flbl>> FOR i IN 1..3 LOOP EXIT flbl; END LOOP flbl; \
         RETURN 'ok'; END blbl $$",
    );
    assert_eq!(
        block.end_label.as_ref().map(|label| label.value.as_str()),
        Some("blbl")
    );
}

// =============================================================================
// ASSERT
// =============================================================================

#[test]
fn test_assert_with_message() {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-ASSERT
    let block = do_block("DO $$ BEGIN ASSERT 1 = 0, 'custom failure'; END $$");
    match &block.statements[0] {
        Statement::PlpgsqlAssert(assert) => {
            assert!(assert.message.is_some());
        }
        other => panic!("expected ASSERT, got {other:?}"),
    }
}

#[test]
fn test_assert_without_message() {
    let block = do_block("DO $$ BEGIN ASSERT TRUE; END $$");
    match &block.statements[0] {
        Statement::PlpgsqlAssert(assert) => assert!(assert.message.is_none()),
        other => panic!("expected ASSERT, got {other:?}"),
    }
}

#[test]
fn test_assert_message_is_an_expression() {
    let block =
        do_block("DO $$ DECLARE v TEXT := 'x'; BEGIN ASSERT v = 'x', 'unexpected ' || v; END $$");
    assert!(matches!(&block.statements[0], Statement::PlpgsqlAssert(_)));
}

// =============================================================================
// EXECUTE
// =============================================================================

#[test]
fn test_execute_into_record() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-EXECUTING-DYN
    let statement = only_body_statement(
        "CREATE FUNCTION f(q TEXT) RETURNS TEXT LANGUAGE plpgsql \
         AS $$ DECLARE r RECORD; BEGIN EXECUTE q INTO r; END $$",
    );
    match statement {
        Statement::ExecuteDynamic { into, using, .. } => {
            assert_eq!(
                into,
                Some(ExecuteInto {
                    strict: false,
                    targets: vec!["r".into()],
                })
            );
            assert!(using.is_none());
        }
        other => panic!("expected dynamic EXECUTE, got {other:?}"),
    }
}

#[test]
fn test_execute_into_strict() {
    let statement = only_body_statement(
        "CREATE FUNCTION f(q TEXT) RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE n INT; BEGIN EXECUTE q INTO STRICT n; END $$",
    );
    match statement {
        Statement::ExecuteDynamic { into, .. } => {
            assert_eq!(into.map(|into| into.strict), Some(true));
        }
        other => panic!("expected dynamic EXECUTE, got {other:?}"),
    }
}

#[test]
fn test_execute_into_and_using_in_either_order() {
    let with_into_first = only_body_statement(
        "CREATE FUNCTION f(q TEXT) RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE n INT; BEGIN EXECUTE q INTO n USING 1, 2; END $$",
    );
    let with_using_first = only_body_statement(
        "CREATE FUNCTION f(q TEXT) RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE n INT; BEGIN EXECUTE q USING 1, 2 INTO n; END $$",
    );
    assert_eq!(with_into_first, with_using_first);
    match with_into_first {
        Statement::ExecuteDynamic { using, .. } => {
            assert_eq!(using.map(|using| using.len()), Some(2));
        }
        other => panic!("expected dynamic EXECUTE, got {other:?}"),
    }
}

#[test]
fn test_execute_without_into_or_using() {
    let statement = only_body_statement(
        "CREATE FUNCTION f(q TEXT) RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE q; END $$",
    );
    match statement {
        Statement::ExecuteDynamic { into, using, .. } => {
            assert!(into.is_none());
            assert!(using.is_none());
        }
        other => panic!("expected dynamic EXECUTE, got {other:?}"),
    }
}

#[test]
fn test_select_into_strict_target() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-SQL-ONEROW
    function_block(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE v INT; BEGIN SELECT id INTO STRICT v FROM t; RETURN v; END $$",
    );
}

// =============================================================================
// GET DIAGNOSTICS
// =============================================================================

fn diagnostics_items(sql: &str) -> (bool, Vec<DiagnosticsItem>) {
    let block = function_block(sql);
    let statement = block
        .exception_handlers
        .as_ref()
        .and_then(|handlers| handlers.first())
        .map(|handler| handler.statements[0].clone())
        .unwrap_or_else(|| block.statements[0].clone());
    match statement {
        Statement::GetDiagnostics(statement) => {
            let assignments = match statement.kind {
                GetDiagnosticsKind::Statement(assignments) => assignments,
                GetDiagnosticsKind::Condition { assignments, .. } => assignments,
            };
            (
                statement.stacked,
                assignments
                    .into_iter()
                    .map(|assignment| assignment.item)
                    .collect(),
            )
        }
        other => panic!("expected GET DIAGNOSTICS, got {other:?}"),
    }
}

#[test]
fn test_get_current_diagnostics_items() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-DIAGNOSTICS
    let (stacked, items) = diagnostics_items(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql \
         AS $$ DECLARE n INT; c TEXT; r oid; \
         BEGIN GET CURRENT DIAGNOSTICS n := ROW_COUNT, c := PG_CONTEXT, r := PG_ROUTINE_OID; END $$",
    );
    assert!(!stacked);
    assert_eq!(
        items,
        vec![
            DiagnosticsItem::RowCount,
            DiagnosticsItem::PgContext,
            DiagnosticsItem::PgRoutineOid,
        ]
    );
}

#[test]
fn test_get_diagnostics_without_area_keyword() {
    let (stacked, items) = diagnostics_items(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql \
         AS $$ DECLARE n INT; BEGIN GET DIAGNOSTICS n = ROW_COUNT; END $$",
    );
    assert!(!stacked);
    assert_eq!(items, vec![DiagnosticsItem::RowCount]);
}

#[test]
fn test_get_stacked_diagnostics_items() {
    let (stacked, items) = diagnostics_items(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql \
         AS $$ DECLARE a TEXT; b TEXT; c TEXT; d TEXT; e TEXT; g TEXT; h TEXT; i TEXT; j TEXT; k TEXT; \
         BEGIN NULL; EXCEPTION WHEN others THEN GET STACKED DIAGNOSTICS \
         a = RETURNED_SQLSTATE, b = COLUMN_NAME, c = CONSTRAINT_NAME, d = PG_DATATYPE_NAME, \
         e = MESSAGE_TEXT, g = TABLE_NAME, h = SCHEMA_NAME, i = PG_EXCEPTION_DETAIL, \
         j = PG_EXCEPTION_HINT, k = PG_EXCEPTION_CONTEXT; END $$",
    );
    assert!(stacked);
    assert_eq!(
        items,
        vec![
            DiagnosticsItem::ReturnedSqlstate,
            DiagnosticsItem::ColumnName,
            DiagnosticsItem::ConstraintName,
            DiagnosticsItem::PgDatatypeName,
            DiagnosticsItem::MessageText,
            DiagnosticsItem::TableName,
            DiagnosticsItem::SchemaName,
            DiagnosticsItem::PgExceptionDetail,
            DiagnosticsItem::PgExceptionHint,
            DiagnosticsItem::PgExceptionContext,
        ]
    );
}

#[test]
fn test_diagnostics_item_names_stay_usable_as_identifiers() {
    // PostgreSQL matches these names as unreserved words.
    round_trip("SELECT table_name, column_name, constraint_name, schema_name FROM t");
    do_block("DO $$ DECLARE table_name TEXT; BEGIN table_name := 'x'; END $$");
}

// =============================================================================
// FOR loops
// =============================================================================

#[test]
fn test_for_over_scalar_target_list() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
    let block = function_block(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql \
         AS $$ DECLARE a INT; b TEXT; c TEXT; \
         BEGIN FOR a, b, c IN SELECT 1, 'B', 'C' LOOP NULL; END LOOP; END $$",
    );
    match &block.statements[0] {
        Statement::For(statement) => {
            assert_eq!(statement.loop_name.value, "a");
            let names: Vec<_> = statement
                .additional_loop_names
                .iter()
                .map(|name| name.value.as_str())
                .collect();
            assert_eq!(names, vec!["b", "c"]);
            assert!(matches!(statement.variant, ForLoopVariant::InQuery(_)));
        }
        other => panic!("expected a FOR loop, got {other:?}"),
    }
}

#[test]
fn test_for_over_query() {
    let block = function_block(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql \
         AS $$ DECLARE r RECORD; BEGIN FOR r IN SELECT 1 AS a LOOP NULL; END LOOP; END $$",
    );
    match &block.statements[0] {
        Statement::For(statement) => {
            assert!(matches!(statement.variant, ForLoopVariant::InQuery(_)))
        }
        other => panic!("expected a FOR loop, got {other:?}"),
    }
}

#[test]
fn test_for_over_data_modifying_statements() {
    // A FOR loop may iterate any command that returns rows.
    for command in [
        "UPDATE t SET x = x * 2 RETURNING x",
        "INSERT INTO t VALUES (1) RETURNING x",
        "DELETE FROM t RETURNING x",
        "MERGE INTO t USING (SELECT 1 AS x) AS s ON t.x = s.x \
         WHEN MATCHED THEN UPDATE SET x = s.x RETURNING merge_action()",
    ] {
        let sql = format!(
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql \
             AS $$ DECLARE r RECORD; BEGIN FOR r IN {command} LOOP NULL; END LOOP; END $$"
        );
        let block = function_block(&sql);
        match &block.statements[0] {
            Statement::For(statement) => assert!(
                matches!(statement.variant, ForLoopVariant::StatementQuery { .. }),
                "{command} did not produce a statement query"
            ),
            other => panic!("expected a FOR loop, got {other:?}"),
        }
    }
}

#[test]
fn test_for_over_dynamic_query() {
    let block = function_block(
        "CREATE FUNCTION f(q TEXT) RETURNS void LANGUAGE plpgsql \
         AS $$ DECLARE r RECORD; BEGIN FOR r IN EXECUTE q USING 1 LOOP NULL; END LOOP; END $$",
    );
    match &block.statements[0] {
        Statement::For(statement) => match &statement.variant {
            ForLoopVariant::DynamicQuery { using, .. } => {
                assert_eq!(using.as_ref().map(|using| using.len()), Some(1))
            }
            other => panic!("expected a dynamic query loop, got {other:?}"),
        },
        other => panic!("expected a FOR loop, got {other:?}"),
    }
}

#[test]
fn test_for_over_integer_range_with_reverse_and_step() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-INTEGER-FOR
    let block = function_block(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql \
         AS $$ BEGIN FOR i IN REVERSE 10..1 BY 2 LOOP NULL; END LOOP; END $$",
    );
    match &block.statements[0] {
        Statement::For(statement) => match &statement.variant {
            ForLoopVariant::IntegerRange { reverse, step, .. } => {
                assert!(*reverse);
                assert!(step.is_some());
            }
            other => panic!("expected an integer range loop, got {other:?}"),
        },
        other => panic!("expected a FOR loop, got {other:?}"),
    }
}

#[test]
fn test_foreach_over_array() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-FOREACH-ARRAY
    let block = function_block(
        "CREATE FUNCTION f(a INT[]) RETURNS void LANGUAGE plpgsql \
         AS $$ DECLARE x INT; BEGIN FOREACH x IN ARRAY a LOOP NULL; END LOOP; END $$",
    );
    match &block.statements[0] {
        Statement::Foreach(statement) => {
            assert_eq!(statement.loop_name.value, "x");
            assert!(statement.slice.is_none());
        }
        other => panic!("expected FOREACH, got {other:?}"),
    }
}

// =============================================================================
// CASE
// =============================================================================

#[test]
fn test_case_with_several_values_per_when() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS-CASE-SIMPLE
    let block = function_block(
        "CREATE FUNCTION f(x BIGINT) RETURNS TEXT LANGUAGE plpgsql \
         AS $$ DECLARE a INT = 10; b INT = 1; BEGIN CASE x WHEN 1 THEN RETURN 'one'; \
         WHEN 3, 4, 3 + 5 THEN RETURN 'three, four or eight'; \
         WHEN a + b, a + b + 1 THEN RETURN 'eleven, twelve'; END CASE; END $$",
    );
    match &block.statements[0] {
        Statement::Case(statement) => {
            assert!(statement.match_expr.is_some());
            assert_eq!(statement.when_blocks.len(), 3);
            let lengths: Vec<_> = statement
                .when_values
                .iter()
                .map(|values| values.as_ref().map(|values| values.len()))
                .collect();
            assert_eq!(lengths, vec![None, Some(3), Some(2)]);
            assert!(statement.when_blocks[0].condition.is_some());
            assert!(statement.when_blocks[1].condition.is_none());
        }
        other => panic!("expected a CASE statement, got {other:?}"),
    }
}

#[test]
fn test_case_with_single_value_per_when_keeps_the_condition() {
    let block = function_block(
        "CREATE FUNCTION f(x INT) RETURNS TEXT LANGUAGE plpgsql \
         AS $$ BEGIN CASE x WHEN 1 THEN RETURN 'a'; ELSE RETURN 'b'; END CASE; END $$",
    );
    match &block.statements[0] {
        Statement::Case(statement) => {
            assert!(statement.when_values.is_empty());
            assert!(statement.when_blocks[0].condition.is_some());
            assert!(statement.else_block.is_some());
        }
        other => panic!("expected a CASE statement, got {other:?}"),
    }
}

#[test]
fn test_searched_case() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS-CASE-SEARCHED
    let block = function_block(
        "CREATE FUNCTION f(x INT) RETURNS TEXT LANGUAGE plpgsql \
         AS $$ BEGIN CASE WHEN x > 1 THEN RETURN 'big'; ELSE RETURN 'small'; END CASE; END $$",
    );
    match &block.statements[0] {
        Statement::Case(statement) => {
            assert!(statement.match_expr.is_none());
            assert!(statement.when_values.is_empty());
        }
        other => panic!("expected a CASE statement, got {other:?}"),
    }
}

// =============================================================================
// Declarations
// =============================================================================

fn declarations(sql: &str) -> Vec<PlSqlDeclaration> {
    function_block(sql).declarations
}

#[test]
fn test_declare_constant_not_null_with_default() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html
    let declarations = declarations(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE a CONSTANT INT NOT NULL := 1; b INT = 2; c INT DEFAULT 3; BEGIN RETURN a; END $$",
    );
    match &declarations[0] {
        PlSqlDeclaration::Variable(declaration) => {
            assert!(declaration.constant);
            assert!(declaration.not_null);
            assert!(declaration.default.is_some());
        }
        other => panic!("expected a variable declaration, got {other:?}"),
    }
    assert_eq!(declarations.len(), 3);
}

#[test]
fn test_declare_column_and_row_types() {
    let declarations = declarations(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE a t.id%TYPE; b t%ROWTYPE; BEGIN RETURN 1; END $$",
    );
    match &declarations[0] {
        PlSqlDeclaration::Variable(declaration) => {
            assert!(matches!(declaration.data_type, SqlPsmDataType::TypeOf(_)))
        }
        other => panic!("expected a variable declaration, got {other:?}"),
    }
    match &declarations[1] {
        PlSqlDeclaration::Variable(declaration) => {
            assert!(matches!(
                declaration.data_type,
                SqlPsmDataType::RowTypeOf(_)
            ))
        }
        other => panic!("expected a variable declaration, got {other:?}"),
    }
}

#[test]
fn test_declare_array_types() {
    let declarations = declarations(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE a RECORD[]; b t.id%TYPE[]; c t%ROWTYPE[]; BEGIN RETURN 1; END $$",
    );
    for declaration in &declarations {
        match declaration {
            PlSqlDeclaration::Variable(declaration) => {
                assert!(matches!(declaration.data_type, SqlPsmDataType::Array(_)))
            }
            other => panic!("expected a variable declaration, got {other:?}"),
        }
    }
}

#[test]
fn test_declare_array_of_type_written_with_array_keyword() {
    // PostgreSQL ignores the declared dimensions, so `ARRAY[3]` is an array.
    one_statement_parses_to_pg(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE e t.id%TYPE ARRAY[3]; BEGIN RETURN 1; END $$",
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE e t.id%TYPE[]; BEGIN RETURN 1; END $$",
    );
}

#[test]
fn test_declare_cursor_variants() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-DECLARATIONS
    let declarations = declarations(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE c1 CURSOR FOR SELECT 1; c2 NO SCROLL CURSOR FOR SELECT 2; \
         c3 SCROLL CURSOR(k INT) FOR SELECT k; BEGIN RETURN 1; END $$",
    );
    assert_eq!(declarations.len(), 3);
    for declaration in &declarations {
        match declaration {
            PlSqlDeclaration::Variable(declaration) => {
                assert!(matches!(declaration.data_type, SqlPsmDataType::Cursor(_)))
            }
            other => panic!("expected a cursor declaration, got {other:?}"),
        }
    }
}

#[test]
fn test_declare_alias_for() {
    // https://www.postgresql.org/docs/current/plpgsql-declarations.html#PLPGSQL-DECLARATION-ALIAS
    let declarations = declarations(
        "CREATE FUNCTION f(INT) RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE n ALIAS FOR $1; BEGIN RETURN n; END $$",
    );
    match &declarations[0] {
        PlSqlDeclaration::Variable(declaration) => {
            assert!(matches!(declaration.data_type, SqlPsmDataType::Alias(_)))
        }
        other => panic!("expected a variable declaration, got {other:?}"),
    }
}

#[test]
fn test_declare_alias_for_a_local_variable() {
    // The aliased datum need not be a routine parameter.
    declarations(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE a INT := 1; b ALIAS FOR a; BEGIN RETURN b; END $$",
    );
}

// =============================================================================
// Assignment
// =============================================================================

#[test]
fn test_assignment_from_a_query_with_a_from_clause() {
    // https://www.postgresql.org/docs/current/plpgsql-statements.html#PLPGSQL-STATEMENTS-ASSIGNMENT
    let statement = only_body_statement(
        "CREATE FUNCTION f() RETURNS TEXT LANGUAGE plpgsql \
         AS $$ DECLARE v TEXT; BEGIN v := data FROM t WHERE id = 2; END $$",
    );
    match statement {
        Statement::SqlPsmQueryAssignment(assignment) => {
            assert_eq!(assignment.target, Expr::Identifier("v".into()));
            assert_eq!(
                assignment.query.to_string(),
                "SELECT data FROM t WHERE id = 2"
            );
        }
        other => panic!("expected a query assignment, got {other:?}"),
    }
}

#[test]
fn test_assignment_from_a_multi_item_select_list() {
    let statement = only_body_statement(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE a INT; BEGIN a := id, id FROM t; END $$",
    );
    match statement {
        Statement::SqlPsmQueryAssignment(assignment) => {
            assert_eq!(assignment.query.to_string(), "SELECT id, id FROM t")
        }
        other => panic!("expected a query assignment, got {other:?}"),
    }
}

#[test]
fn test_assignment_from_a_query_with_order_by_and_limit() {
    let statement = only_body_statement(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE a INT; BEGIN a := id FROM t ORDER BY 1 LIMIT 1; END $$",
    );
    assert!(matches!(statement, Statement::SqlPsmQueryAssignment(_)));
}

#[test]
fn test_assignment_from_a_plain_expression() {
    let statement = only_body_statement(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE a INT; BEGIN a := 1 + 2; END $$",
    );
    assert!(matches!(statement, Statement::SqlPsmAssignment(_)));
}

#[test]
fn test_assignment_to_a_record_field() {
    let statement = only_body_statement(
        "CREATE FUNCTION f() RETURNS INT LANGUAGE plpgsql \
         AS $$ DECLARE r RECORD; BEGIN r.a := 1; END $$",
    );
    match statement {
        Statement::SqlPsmAssignment(assignment) => {
            assert!(matches!(assignment.target, Expr::CompoundIdentifier(_)))
        }
        other => panic!("expected an assignment, got {other:?}"),
    }
}

// =============================================================================
// SQL-standard function bodies
// =============================================================================

#[test]
fn test_function_begin_atomic_body() {
    // https://www.postgresql.org/docs/current/sql-createfunction.html
    let statement = round_trip_exact(
        "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql BEGIN ATOMIC SELECT x * 2; END",
    );
    match extract_create_function(&statement).function_body.clone() {
        Some(CreateFunctionBody::BeginAtomic(AtomicBlock { statements })) => {
            assert_eq!(statements.len(), 1)
        }
        other => panic!("expected a BEGIN ATOMIC body, got {other:?}"),
    }
}

#[test]
fn test_function_begin_atomic_body_with_several_statements() {
    let statement = round_trip_exact(
        "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql BEGIN ATOMIC INSERT INTO t VALUES (x); SELECT x; END",
    );
    match extract_create_function(&statement).function_body.clone() {
        Some(CreateFunctionBody::BeginAtomic(AtomicBlock { statements })) => {
            assert_eq!(statements.len(), 2)
        }
        other => panic!("expected a BEGIN ATOMIC body, got {other:?}"),
    }
}

#[test]
fn test_function_begin_atomic_body_may_be_empty() {
    let statement =
        round_trip_exact("CREATE FUNCTION f() RETURNS void LANGUAGE sql BEGIN ATOMIC END");
    match extract_create_function(&statement).function_body.clone() {
        Some(CreateFunctionBody::BeginAtomic(AtomicBlock { statements })) => {
            assert!(statements.is_empty())
        }
        other => panic!("expected a BEGIN ATOMIC body, got {other:?}"),
    }
}

#[test]
fn test_procedure_begin_atomic_body() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    let statement = round_trip_exact(
        "CREATE PROCEDURE p(x INT) LANGUAGE sql BEGIN ATOMIC INSERT INTO t VALUES (x); END",
    );
    match statement {
        Statement::CreateProcedure { body, .. } => match body {
            ConditionalStatements::BeginAtomic(AtomicBlock { statements }) => {
                assert_eq!(statements.len(), 1)
            }
            other => panic!("expected a BEGIN ATOMIC body, got {other:?}"),
        },
        other => panic!("expected CREATE PROCEDURE, got {other:?}"),
    }
}

#[test]
fn test_function_return_body() {
    let statement =
        round_trip_exact("CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql RETURN x + 1");
    assert!(matches!(
        extract_create_function(&statement).function_body,
        Some(CreateFunctionBody::Return(_))
    ));
}

// =============================================================================
// CREATE FUNCTION bodies PostgreSQL parses and rejects later
// =============================================================================

#[test]
fn test_function_with_both_an_as_body_and_a_return_body() {
    // PostgreSQL parses this and raises 42P13 when the function is created.
    let statement = round_trip_exact(
        "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x * 2 $$ RETURN x * 3",
    );
    match extract_create_function(&statement).function_body.clone() {
        Some(CreateFunctionBody::Multiple(bodies)) => {
            assert_eq!(bodies.len(), 2);
            assert!(matches!(bodies[0], CreateFunctionBody::AsBeforeOptions(_)));
            assert!(matches!(bodies[1], CreateFunctionBody::Return(_)));
        }
        other => panic!("expected two bodies, got {other:?}"),
    }
}

#[test]
fn test_function_with_two_as_items() {
    // `AS 'obj_file', 'link_symbol'` names a C function.
    let statement =
        round_trip_exact("CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE c AS 'obj', 'sym'");
    assert!(matches!(
        extract_create_function(&statement).function_body,
        Some(CreateFunctionBody::AsObjectFileLinkSymbol { .. })
    ));
}

#[test]
fn test_function_with_two_as_clauses() {
    let statement = round_trip_exact(
        "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT 1 $$ AS $$ SELECT 2 $$",
    );
    match extract_create_function(&statement).function_body.clone() {
        Some(CreateFunctionBody::Multiple(bodies)) => assert_eq!(bodies.len(), 2),
        other => panic!("expected two bodies, got {other:?}"),
    }
}

// =============================================================================
// Routine attributes and parameters
// =============================================================================

fn procedure_attributes(sql: &str) -> Vec<RoutineAttribute> {
    match round_trip(sql) {
        Statement::CreateProcedure { attributes, .. } => attributes,
        other => panic!("expected CREATE PROCEDURE, got {other:?}"),
    }
}

#[test]
fn test_procedure_with_function_only_attributes() {
    // PostgreSQL parses these and rejects them when the procedure is created.
    assert_eq!(
        procedure_attributes("CREATE PROCEDURE p() LANGUAGE sql STRICT AS $$ SELECT 1 $$"),
        vec![RoutineAttribute::CalledOnNull(FunctionCalledOnNull::Strict)]
    );
    assert_eq!(
        procedure_attributes("CREATE PROCEDURE p() LANGUAGE sql WINDOW AS $$ SELECT 1 $$"),
        vec![RoutineAttribute::Window]
    );
}

#[test]
fn test_procedure_with_the_full_attribute_list() {
    let attributes = procedure_attributes(
        "CREATE PROCEDURE p() LANGUAGE sql IMMUTABLE PARALLEL SAFE COST 100 ROWS 10 LEAKPROOF \
         AS $$ SELECT 1 $$",
    );
    assert_eq!(attributes.len(), 5);
    assert_eq!(
        attributes[0],
        RoutineAttribute::Behavior(FunctionBehavior::Immutable)
    );
    assert_eq!(
        attributes[1],
        RoutineAttribute::Parallel(FunctionParallel::Safe)
    );
    assert!(matches!(attributes[2], RoutineAttribute::Cost(_)));
    assert!(matches!(attributes[3], RoutineAttribute::Rows(_)));
    assert_eq!(attributes[4], RoutineAttribute::Leakproof(true));
}

#[test]
fn test_procedure_with_transform() {
    let attributes = procedure_attributes(
        "CREATE PROCEDURE p() LANGUAGE sql TRANSFORM FOR TYPE INT4 AS $$ SELECT 1 $$",
    );
    match &attributes[0] {
        RoutineAttribute::Transform(types) => assert_eq!(types.len(), 1),
        other => panic!("expected TRANSFORM, got {other:?}"),
    }
}

#[test]
fn test_function_with_transform_for_several_types() {
    let statement = round_trip(
        "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql TRANSFORM FOR TYPE INT4, FOR TYPE TEXT \
         AS $$ SELECT x $$",
    );
    match &extract_create_function(&statement).attributes[..] {
        [RoutineAttribute::Transform(types)] => assert_eq!(types.len(), 2),
        other => panic!("expected TRANSFORM, got {other:?}"),
    }
}

#[test]
fn test_procedure_parameter_modes_written_after_the_name() {
    let statement = round_trip(
        "CREATE PROCEDURE p(a VARIADIC INT[], b OUT INT) LANGUAGE sql AS $$ SELECT a[1] $$",
    );
    let extract = extract_create_procedure(&statement);
    let params = extract.params.as_ref().expect("parameters");
    assert_eq!(params[0].mode, Some(ArgMode::Variadic));
    assert_eq!(params[0].name.value, "a");
    assert_eq!(params[1].mode, Some(ArgMode::Out));
    assert_eq!(params[1].name.value, "b");
}

#[test]
fn test_function_parameter_mode_written_after_the_name() {
    let statement = round_trip(
        "CREATE FUNCTION f(a VARIADIC INT[]) RETURNS INT LANGUAGE sql AS $$ SELECT a[1] $$",
    );
    let args = extract_create_function(&statement)
        .args
        .as_ref()
        .expect("arguments");
    assert_eq!(args[0].mode, Some(ArgMode::Variadic));
    assert_eq!(
        args[0].name.as_ref().map(|name| name.value.as_str()),
        Some("a")
    );
}

#[test]
fn test_function_parameter_mode_written_before_the_name() {
    let statement = round_trip_exact(
        "CREATE FUNCTION f(VARIADIC a INT[]) RETURNS INT LANGUAGE sql AS $$ SELECT a[1] $$",
    );
    let args = extract_create_function(&statement)
        .args
        .as_ref()
        .expect("arguments");
    assert_eq!(args[0].mode, Some(ArgMode::Variadic));
}

#[test]
fn test_column_type_in_a_parameter() {
    // PostgreSQL's `func_type` allows `table.column%TYPE`.
    let statement = round_trip_exact(
        "CREATE FUNCTION f(x t.id%TYPE) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN x + 1; END $$",
    );
    let args = extract_create_function(&statement)
        .args
        .as_ref()
        .expect("arguments");
    assert!(matches!(args[0].data_type, DataType::TypeOf(_)));
}

#[test]
fn test_column_type_in_the_return_type() {
    let statement =
        round_trip_exact("CREATE FUNCTION f() RETURNS t.id%TYPE LANGUAGE sql AS $$ SELECT 1 $$");
    assert!(matches!(
        extract_create_function(&statement).return_type,
        Some(DataType::TypeOf(_))
    ));
}

#[test]
fn test_column_type_in_a_setof_return_type() {
    let statement = round_trip_exact(
        "CREATE FUNCTION f() RETURNS SETOF t.id%TYPE LANGUAGE sql AS $$ SELECT 1 $$",
    );
    match &extract_create_function(&statement).return_type {
        Some(DataType::SetOf(element)) => assert!(matches!(**element, DataType::TypeOf(_))),
        other => panic!("expected SETOF of a column type, got {other:?}"),
    }
}

// =============================================================================
// DO
// =============================================================================

#[test]
fn test_do_block_with_a_label() {
    // https://www.postgresql.org/docs/current/sql-do.html
    let block = do_block("DO $$ <<top>> DECLARE v INT := 1; BEGIN NULL; END top $$");
    assert_eq!(
        block.label.as_ref().map(|label| label.value.as_str()),
        Some("top")
    );
    assert_eq!(
        block.end_label.as_ref().map(|label| label.value.as_str()),
        Some("top")
    );
}

#[test]
fn test_do_block_with_language_before_the_body() {
    let block = do_block(
        "DO LANGUAGE plpgsql $$ DECLARE r RECORD; \
         BEGIN FOR r IN UPDATE t SET x = x * 2 RETURNING x LOOP COMMIT; END LOOP; END $$",
    );
    assert_eq!(block.declarations.len(), 1);
}
