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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use sqlparser::ast::{
    AlternativeQuotedString, DataType, Expr, Ident, SelectItem, SetExpr, Statement, Value,
};
use sqlparser::dialect::OracleDialect;
use sqlparser::parser::Parser;

fn parse_one(sql: &str) -> Statement {
    let mut statements = Parser::parse_sql(&OracleDialect {}, sql).expect(sql);
    assert_eq!(statements.len(), 1, "expected one statement: {sql}");
    statements.pop().unwrap()
}

#[test]
fn oracle_unquoted_identifiers_fold_to_uppercase() {
    let statement = parse_one("SELECT employee_id, \"MixedCase\" FROM hr.employees");
    let Statement::Query(ref query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };

    assert_eq!(
        select.projection[0].to_string(),
        Ident::new("EMPLOYEE_ID").to_string()
    );
    assert_eq!(select.projection[1].to_string(), "\"MixedCase\"");
    assert_eq!(select.from[0].relation.to_string(), "HR.EMPLOYEES");
}

#[test]
fn oracle_hierarchical_query_builds_structured_ast() {
    let statement = parse_one(
        "SELECT employee_id FROM employees \
         START WITH manager_id IS NULL \
         CONNECT BY PRIOR employee_id = manager_id",
    );
    let Statement::Query(query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };

    let connect_by = select.connect_by.as_ref().expect("CONNECT BY AST");
    assert!(!connect_by.nocycle);
    assert!(connect_by.condition.is_some());
    assert_eq!(connect_by.relationships.len(), 1);
    let Expr::BinaryOp { left, .. } = &connect_by.relationships[0] else {
        panic!("expected CONNECT BY comparison");
    };
    assert!(matches!(left.as_ref(), Expr::Prior(_)));
}

#[test]
fn oracle_legacy_outer_join_builds_structured_ast() {
    let statement = parse_one("SELECT a.id FROM a, b WHERE a.id = b.id(+) AND b.active(+) = 1");
    let Statement::Query(query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };

    let selection = select.selection.as_ref().expect("WHERE expression");
    assert!(
        format!("{selection:?}").contains("OuterJoin"),
        "expected legacy outer join nodes: {selection:?}"
    );
}

#[test]
fn oracle_alternative_quoted_strings_preserve_content_and_delimiter() {
    let statement = parse_one("SELECT q'[Sam's string]', q'{SELECT ''x'' FROM dual}' FROM dual");
    let Statement::Query(ref query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };

    let expected = [
        AlternativeQuotedString {
            value: "Sam's string".into(),
            delimiter: '[',
        },
        AlternativeQuotedString {
            value: "SELECT ''x'' FROM dual".into(),
            delimiter: '{',
        },
    ];
    for (item, expected) in select.projection.iter().zip(expected) {
        let SelectItem::UnnamedExpr(Expr::Value(value)) = item else {
            panic!("expected an alternative quoted value, got {item:?}");
        };
        assert_eq!(
            value.value,
            Value::AlternativeQuotedString(expected.clone())
        );
        assert_eq!(value.to_string(), expected.to_string());
    }

    assert_eq!(
        statement.to_string(),
        "SELECT q'[Sam's string]', q'{SELECT ''x'' FROM dual}' FROM DUAL"
    );
}

#[test]
fn oracle_long_and_long_raw_are_distinct_typed_data_types() {
    let statement = parse_one("CREATE TABLE legacy_values (text_value LONG, raw_value LONG RAW)");
    let Statement::CreateTable(create) = statement else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(create.columns[0].data_type, DataType::Long);
    assert_eq!(create.columns[1].data_type, DataType::LongRaw);
    assert_eq!(create.columns[0].data_type.to_string(), "LONG");
    assert_eq!(create.columns[1].data_type.to_string(), "LONG RAW");
}

#[cfg(feature = "visitor")]
#[test]
fn data_type_visitor_observes_oracle_types_in_nested_ast_positions() {
    use core::ops::ControlFlow;
    use sqlparser::ast::{Visit, Visitor};

    struct TypeVisitor(Vec<DataType>);

    impl Visitor for TypeVisitor {
        type Break = ();

        fn pre_visit_data_type(&mut self, data_type: &DataType) -> ControlFlow<Self::Break> {
            self.0.push(data_type.clone());
            ControlFlow::Continue(())
        }
    }

    let statement = parse_one("CREATE TABLE legacy_values (text_value LONG, raw_value LONG RAW)");
    let mut visitor = TypeVisitor(Vec::new());
    assert!(statement.visit(&mut visitor).is_continue());
    assert!(visitor.0.contains(&DataType::Long));
    assert!(visitor.0.contains(&DataType::LongRaw));
}

#[test]
fn oracle_bind_variables_are_typed_placeholders() {
    let statement =
        parse_one("SELECT :department_id, :1 FROM dual WHERE employee_id = :employee_id");
    let Statement::Query(query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };

    let placeholders = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Value(value)) => &value.value,
            _ => panic!("expected bind value, got {item:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        placeholders,
        vec![
            &Value::Placeholder(":DEPARTMENT_ID".into()),
            &Value::Placeholder(":1".into())
        ]
    );

    let selection = select.selection.as_ref().expect("WHERE expression");
    let Expr::BinaryOp { right, .. } = selection.as_ref() else {
        panic!("expected WHERE comparison");
    };
    assert!(matches!(
        right.as_ref(),
        Expr::Value(value) if value.value == Value::Placeholder(":EMPLOYEE_ID".into())
    ));
}

#[test]
fn oracle_subquery_and_local_time_zone_expressions_are_typed() {
    let statement = parse_one(
        "SELECT CURSOR(SELECT employee_id FROM employees), \
         CAST(MULTISET(SELECT department_name FROM departments) AS name_list_t), \
         order_ts AT LOCAL FROM orders",
    );
    let Statement::Query(query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };

    assert!(matches!(
        &select.projection[0],
        SelectItem::UnnamedExpr(Expr::Cursor(query))
            if matches!(query.body.as_ref(), SetExpr::Select(_))
    ));
    assert!(matches!(
        &select.projection[1],
        SelectItem::UnnamedExpr(Expr::Cast {
            expr,
            data_type: sqlparser::ast::DataType::Custom(_, _),
            ..
        }) if matches!(expr.as_ref(), Expr::Multiset(query)
            if matches!(query.body.as_ref(), SetExpr::Select(_)))
    ));
    assert!(matches!(
        &select.projection[2],
        SelectItem::UnnamedExpr(Expr::AtLocal { timestamp })
            if matches!(timestamp.as_ref(), Expr::Identifier(identifier)
                if identifier.value == "ORDER_TS")
    ));
}

#[test]
fn oracle_hierarchical_query_modifiers_are_typed() {
    let statement = parse_one(
        "SELECT employee_id FROM employees \
         START WITH manager_id IS NULL \
         CONNECT BY NOCYCLE PRIOR employee_id = manager_id \
         ORDER SIBLINGS BY last_name",
    );
    let Statement::Query(query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };

    let connect_by = select.connect_by.as_ref().expect("CONNECT BY AST");
    assert!(connect_by.nocycle);
    assert!(connect_by.condition.is_some());
    assert!(matches!(
        query.order_by.as_ref().map(|order_by| &order_by.kind),
        Some(sqlparser::ast::OrderByKind::Siblings(expressions))
            if expressions.len() == 1
    ));

    let statement =
        parse_one("SELECT employee_id FROM employees CONNECT BY PRIOR employee_id = manager_id");
    let Statement::Query(query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    let connect_by = select.connect_by.as_ref().expect("CONNECT BY AST");
    assert!(connect_by.condition.is_none());
    assert!(!connect_by.nocycle);
}

#[test]
fn oracle_dml_returning_into_is_structured() {
    let cases = [
        (
            "INSERT INTO employees (employee_id) VALUES (1001) \
             RETURNING employee_id INTO :new_id",
            ":NEW_ID",
        ),
        (
            "UPDATE employees SET salary = salary + 100 \
             RETURNING salary INTO :new_salary",
            ":NEW_SALARY",
        ),
        (
            "DELETE FROM employees WHERE employee_id = 100 \
             RETURNING last_name INTO :old_name",
            ":OLD_NAME",
        ),
    ];

    for (sql, expected_target) in cases {
        let statement = parse_one(sql);
        let returning = match &statement {
            Statement::Insert(insert) => insert.returning.as_ref(),
            Statement::Update(update) => update.returning.as_ref(),
            Statement::Delete(delete) => delete.returning.as_ref(),
            _ => None,
        }
        .expect("RETURNING clause");

        assert_eq!(returning.expressions.len(), 1);
        assert!(matches!(
            returning.into.as_deref(),
            Some([Expr::Value(value)])
                if value.value == Value::Placeholder(expected_target.into())
        ));
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_partition_extensions_are_preserved() {
    let statement =
        parse_one("INSERT INTO sales PARTITION (sales_q3_2026) (sale_id, amount) VALUES (1, 100)");
    let Statement::Insert(insert) = statement else {
        panic!("expected insert");
    };
    assert_eq!(
        insert
            .partitioned
            .as_ref()
            .expect("partition extension")
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["SALES_Q3_2026"]
    );
    assert_eq!(
        insert
            .after_columns
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["SALE_ID", "AMOUNT"]
    );

    for sql in [
        "SELECT * FROM sales PARTITION (sales_q1_2026)",
        "DELETE FROM sales PARTITION (sales_q1_2026) WHERE amount = 0",
    ] {
        let statement = parse_one(sql);
        let relation = match &statement {
            Statement::Query(query) => {
                let SetExpr::Select(select) = query.body.as_ref() else {
                    panic!("expected select");
                };
                &select.from[0].relation
            }
            Statement::Delete(delete) => match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(from) => &from[0].relation,
                _ => panic!("expected DELETE FROM"),
            },
            _ => panic!("expected query or delete"),
        };
        assert!(matches!(
            relation,
            sqlparser::ast::TableFactor::Table { partitions, .. }
                if partitions == &[Ident::new("SALES_Q1_2026")]
        ));
    }
}

#[test]
fn oracle_plsql_procedure_invocations_are_typed() {
    let statement = parse_one("BEGIN DBMS_OUTPUT.PUT_LINE('hello'); risky_operation; END;");
    let Statement::PlSqlBlock(sqlparser::ast::BeginEndStatements { statements, .. }) = statement
    else {
        panic!("expected PL/SQL block");
    };

    assert_eq!(statements.len(), 2);
    assert!(matches!(
        &statements[0],
        Statement::PlSqlProcedureCall(function)
            if function.name.to_string() == "DBMS_OUTPUT.PUT_LINE"
                && matches!(&function.args, sqlparser::ast::FunctionArguments::List(arguments)
                    if arguments.args.len() == 1)
    ));
    assert!(matches!(
        &statements[1],
        Statement::PlSqlProcedureCall(function)
            if function.name.to_string() == "RISKY_OPERATION"
                && matches!(function.args, sqlparser::ast::FunctionArguments::None)
    ));
}

#[test]
fn oracle_bare_begin_is_a_transaction_but_begin_end_is_plsql() {
    for sql in ["BEGIN", "BEGIN;", "BEGIN TRANSACTION", "BEGIN WORK"] {
        assert!(
            matches!(parse_one(sql), Statement::StartTransaction { .. }),
            "expected a transaction for {sql}"
        );
    }

    assert!(matches!(
        parse_one("BEGIN NULL; END;"),
        Statement::PlSqlBlock(_)
    ));
}

#[test]
fn oracle_plsql_block_declarations_are_typed() {
    let statement = parse_one(
        "DECLARE \
         amount NUMBER(12,2) NOT NULL := 0; \
         tax_rate CONSTANT PLS_INTEGER := 7; \
         employee_name employees.last_name%TYPE; \
         employee_row employees%ROWTYPE; \
         BEGIN NULL; END;",
    );
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };

    assert_eq!(block.declarations.len(), 4);
    assert!(matches!(
        &block.declarations[0],
        sqlparser::ast::PlSqlDeclaration::Variable(declaration)
            if declaration.not_null && declaration.default.is_some()
    ));
    assert!(matches!(
        &block.declarations[1],
        sqlparser::ast::PlSqlDeclaration::Variable(declaration) if declaration.constant
    ));
    assert!(matches!(
        &block.declarations[2],
        sqlparser::ast::PlSqlDeclaration::Variable(declaration)
            if matches!(&declaration.data_type,
                sqlparser::ast::SqlPsmDataType::TypeOf(name)
            if name.to_string() == "EMPLOYEES.LAST_NAME"
        )
    ));
    assert!(matches!(
        &block.declarations[3],
        sqlparser::ast::PlSqlDeclaration::Variable(declaration)
            if matches!(&declaration.data_type,
                sqlparser::ast::SqlPsmDataType::RowTypeOf(name)
            if name.to_string() == "EMPLOYEES"
        )
    ));
    assert_eq!(block.statements, vec![Statement::Null]);
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_plsql_type_and_cursor_declarations_are_typed() {
    let statement = parse_one(
        "DECLARE \
         SUBTYPE short_text IS VARCHAR2(30) NOT NULL; \
         TYPE employee_rec_t IS RECORD (employee_id NUMBER, name VARCHAR2(100)); \
         TYPE salary_map_t IS TABLE OF NUMBER INDEX BY VARCHAR2(100); \
         TYPE colors_t IS VARRAY(3) OF VARCHAR2(20); \
         TYPE employee_cursor_t IS REF CURSOR RETURN employees%ROWTYPE; \
         CURSOR employee_cursor(p_department_id departments.department_id%TYPE) \
           RETURN employees%ROWTYPE IS SELECT * FROM employees WHERE department_id = p_department_id; \
         BEGIN NULL; END;",
    );
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };

    use sqlparser::ast::{PlSqlCollectionKind, PlSqlDeclaration};
    assert!(matches!(
        &block.declarations[0],
        PlSqlDeclaration::Subtype { not_null: true, .. }
    ));
    assert!(matches!(
        &block.declarations[1],
        PlSqlDeclaration::RecordType { fields, .. } if fields.len() == 2
    ));
    assert!(matches!(
        &block.declarations[2],
        PlSqlDeclaration::CollectionType {
            kind: PlSqlCollectionKind::NestedTable,
            index_by: Some(_),
            ..
        }
    ));
    assert!(matches!(
        &block.declarations[3],
        PlSqlDeclaration::CollectionType {
            kind: PlSqlCollectionKind::Varray(size),
            index_by: None,
            ..
        } if size.to_string() == "3"
    ));
    assert!(matches!(
        &block.declarations[4],
        PlSqlDeclaration::RefCursorType {
            return_type: Some(sqlparser::ast::SqlPsmDataType::RowTypeOf(name)),
            ..
        } if name.to_string() == "EMPLOYEES"
    ));
    assert!(matches!(
        &block.declarations[5],
        PlSqlDeclaration::Cursor {
            parameters,
            return_type: Some(sqlparser::ast::SqlPsmDataType::RowTypeOf(_)),
            query,
            ..
        } if parameters.len() == 1 && matches!(query.body.as_ref(), SetExpr::Select(_))
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_plsql_labels_and_goto_are_typed() {
    let statement = parse_one(
        "<<main>> DECLARE n NUMBER := 0; BEGIN \
         GOTO finished; \
         <<scan>> LOOP EXIT scan WHEN n > 1; END LOOP scan; \
         <<finished>> NULL; \
         END main;",
    );
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };
    assert_eq!(block.label, Some(Ident::new("MAIN")));
    assert_eq!(block.end_label, Some(Ident::new("MAIN")));
    assert!(matches!(
        &block.statements[0],
        Statement::PlSqlGoto(label) if label.value == "FINISHED"
    ));
    assert!(matches!(
        &block.statements[1],
        Statement::PlSqlLabeled { label, statement }
            if label.value == "SCAN"
                && matches!(statement.as_ref(), Statement::Loop(loop_statement)
                    if loop_statement.end_label == Some(Ident::new("SCAN")))
    ));
    assert!(matches!(
        &block.statements[2],
        Statement::PlSqlLabeled { label, statement }
            if label.value == "FINISHED" && matches!(statement.as_ref(), Statement::Null)
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_plsql_pragmas_preserve_expression_arguments() {
    let statement = parse_one(
        "DECLARE deadlock_detected EXCEPTION; \
         PRAGMA EXCEPTION_INIT(deadlock_detected, -60); \
         BEGIN PRAGMA INLINE(calculate_total, 'YES'); NULL; END;",
    );
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.declarations[1],
        sqlparser::ast::PlSqlDeclaration::Pragma(pragma)
            if pragma.name.to_string() == "EXCEPTION_INIT"
                && pragma.arguments.len() == 2
                && matches!(&pragma.arguments[0], Expr::Identifier(name)
                    if name.value == "DEADLOCK_DETECTED")
                && pragma.arguments[1].to_string() == "-60"
    ));
    assert!(matches!(
        &block.statements[0],
        Statement::Pragma { pragma, .. }
            if pragma.name.to_string() == "INLINE"
                && pragma.arguments.len() == 2
                && matches!(&pragma.arguments[0], Expr::Identifier(name)
                    if name.value == "CALCULATE_TOTAL")
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_plsql_while_loop_is_not_nested_loop_syntax() {
    let statement =
        parse_one("BEGIN WHILE counter < 10 LOOP counter := counter + 1; END LOOP; END;");
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.statements[0],
        Statement::While(while_statement)
            if while_statement.has_loop_keyword
                && !while_statement.has_do_keyword
                && while_statement.while_block.is_none()
                && while_statement.body.statements().len() == 1
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_commit_options_and_rename_are_typed() {
    let statement = parse_one("COMMIT WRITE IMMEDIATE NOWAIT");
    assert!(matches!(
        &statement,
        Statement::Commit {
            oracle: Some(sqlparser::ast::OracleCommitOptions {
                write: Some(sqlparser::ast::OracleCommitWrite {
                    wait: Some(false),
                    immediate: Some(true),
                }),
                ..
            }),
            ..
        }
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);

    let statement = parse_one("COMMIT COMMENT 'batch-42'");
    assert!(matches!(
        &statement,
        Statement::Commit {
            oracle: Some(sqlparser::ast::OracleCommitOptions {
                comment: Some(comment),
                ..
            }),
            ..
        } if comment == "batch-42"
    ));

    let statement = parse_one("RENAME employees_stage TO employees_ready");
    let Statement::RenameTable(rename) = &statement else {
        panic!("expected rename");
    };
    assert_eq!(rename.len(), 1);
    assert_eq!(rename[0].old_name.to_string(), "EMPLOYEES_STAGE");
    assert_eq!(rename[0].new_name.to_string(), "EMPLOYEES_READY");
}

#[test]
fn oracle_plsql_bulk_and_dynamic_statements_are_typed() {
    let statement = parse_one(
        "DECLARE CURSOR c IS SELECT employee_id FROM employees; \
         TYPE ids_t IS TABLE OF NUMBER; ids ids_t; \
         BEGIN \
         FETCH c BULK COLLECT INTO ids LIMIT 100; \
         FORALL i IN INDICES OF ids SAVE EXCEPTIONS \
           DELETE FROM employees WHERE employee_id = ids(i); \
         EXECUTE IMMEDIATE 'SELECT last_name FROM employees' BULK COLLECT INTO ids; \
         EXECUTE IMMEDIATE 'UPDATE employees SET salary = :1' \
           USING 100 RETURNING INTO ids; \
         PIPE ROW(number_t(1)); \
         END;",
    );
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };

    assert!(matches!(
        &block.statements[0],
        Statement::PlSqlFetch(fetch)
            if fetch.bulk_collect && fetch.targets.len() == 1 && fetch.limit.is_some()
    ));
    assert!(matches!(
        &block.statements[1],
        Statement::PlSqlForAll(forall)
            if forall.save_exceptions
                && matches!(forall.bounds, sqlparser::ast::PlSqlForAllBounds::IndicesOf { .. })
                && matches!(forall.statement.as_ref(), Statement::Delete(_))
    ));
    assert!(matches!(
        &block.statements[2],
        Statement::PlSqlExecuteImmediate(execute)
            if execute.bulk_collect && execute.into.len() == 1
    ));
    assert!(matches!(
        &block.statements[3],
        Statement::PlSqlExecuteImmediate(execute)
            if execute.using.len() == 1 && execute.returning_into.len() == 1
    ));
    assert!(matches!(
        &block.statements[4],
        Statement::PlSqlPipeRow(row) if row.to_string() == "NUMBER_T(1)"
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_bulk_collect_clauses_are_typed() {
    let statement =
        parse_one("SELECT last_name BULK COLLECT INTO names FROM employees ORDER BY last_name");
    let Statement::Query(query) = statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(select.into.as_ref().is_some_and(|into| into.bulk_collect));

    let statement = parse_one(
        "UPDATE employees SET active = TRUE WHERE department_id = 10 \
         RETURNING employee_id BULK COLLECT INTO changed_ids",
    );
    let Statement::Update(update) = statement else {
        panic!("expected update");
    };
    let returning = update.returning.expect("RETURNING clause");
    assert!(returning.bulk_collect);
    assert_eq!(returning.into.as_ref().map(Vec::len), Some(1));
}

#[test]
fn oracle_standalone_routine_headers_and_bodies_are_typed() {
    let statement = parse_one(
        "CREATE OR REPLACE EDITIONABLE PROCEDURE run_report(\
         p_id IN employees.employee_id%TYPE, p_result OUT NOCOPY SYS_REFCURSOR) \
         AUTHID CURRENT_USER ACCESSIBLE BY (PACKAGE reporting_api) \
         IS local_id NUMBER := p_id; BEGIN OPEN p_result FOR \
         SELECT * FROM employees WHERE employee_id = local_id; END run_report;",
    );
    let Statement::OracleCreatePlSqlRoutine(create) = &statement else {
        panic!("expected Oracle routine");
    };
    assert!(create.or_replace);
    assert_eq!(create.editionable, Some(true));
    assert_eq!(
        create.routine.kind,
        sqlparser::ast::OraclePlSqlRoutineKind::Procedure
    );
    assert_eq!(create.routine.parameters.len(), 2);
    assert_eq!(
        create.routine.parameters[0].mode,
        Some(sqlparser::ast::PlSqlParameterMode::In)
    );
    assert!(matches!(
        create.routine.parameters[0].data_type,
        sqlparser::ast::SqlPsmDataType::TypeOf(_)
    ));
    assert_eq!(
        create.routine.parameters[1].mode,
        Some(sqlparser::ast::PlSqlParameterMode::Out)
    );
    assert!(create.routine.parameters[1].nocopy);
    assert!(matches!(
        &create.routine.clauses[..],
        [
            sqlparser::ast::OraclePlSqlRoutineClause::Authid(
                sqlparser::ast::OraclePlSqlAuthid::CurrentUser
            ),
            sqlparser::ast::OraclePlSqlRoutineClause::AccessibleBy(accessors)
        ] if accessors.len() == 1
    ));
    assert!(matches!(
        &create.routine.body,
        sqlparser::ast::OraclePlSqlRoutineBody::Block {
            declarations,
            block,
            ..
        } if declarations.len() == 1 && block.statements.len() == 1
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_function_variants_are_structurally_distinct() {
    let statement = parse_one(
        "CREATE OR REPLACE FUNCTION department_name(p_id NUMBER) RETURN VARCHAR2 \
         RESULT_CACHE RELIES_ON (departments) AUTHID DEFINER DETERMINISTIC \
         IS BEGIN RETURN 'name'; END;",
    );
    let Statement::OracleCreatePlSqlRoutine(create) = &statement else {
        panic!("expected Oracle function");
    };
    assert_eq!(
        create.routine.kind,
        sqlparser::ast::OraclePlSqlRoutineKind::Function
    );
    assert!(create.routine.return_type.is_some());
    assert!(matches!(
        &create.routine.clauses[0],
        sqlparser::ast::OraclePlSqlRoutineClause::ResultCache { relies_on }
            if relies_on.len() == 1
    ));
    assert!(matches!(
        create.routine.clauses[1],
        sqlparser::ast::OraclePlSqlRoutineClause::Authid(
            sqlparser::ast::OraclePlSqlAuthid::Definer
        )
    ));
    assert!(matches!(
        create.routine.clauses[2],
        sqlparser::ast::OraclePlSqlRoutineClause::Deterministic
    ));

    let aggregate = parse_one(
        "CREATE FUNCTION second_max(input NUMBER) RETURN NUMBER \
         PARALLEL_ENABLE AGGREGATE USING second_max_impl_t",
    );
    assert!(matches!(
        &aggregate,
        Statement::OracleCreatePlSqlRoutine(create)
            if matches!(
                create.routine.body,
                sqlparser::ast::OraclePlSqlRoutineBody::AggregateUsing(_)
            )
    ));

    let call_spec = parse_one(
        "CREATE OR REPLACE FUNCTION c_hash(value VARCHAR2) RETURN BINARY_INTEGER \
         AS LANGUAGE C NAME \"hash_value\" LIBRARY hash_lib \
         PARAMETERS (value STRING, RETURN INT)",
    );
    assert!(matches!(
        &call_spec,
        Statement::OracleCreatePlSqlRoutine(create)
            if matches!(
                &create.routine.body,
                sqlparser::ast::OraclePlSqlRoutineBody::CallSpec(spec)
                    if spec.parameters.len() == 2
                        && spec.parameters[1].return_value
                        && spec.library.is_some()
            )
    ));
    assert_eq!(parse_one(&aggregate.to_string()), aggregate);
    assert_eq!(parse_one(&call_spec.to_string()), call_spec);
}

#[test]
fn oracle_nested_routines_are_typed_declarations() {
    let statement = parse_one(
        "DECLARE result NUMBER; \
         FUNCTION twice(n NUMBER) RETURN NUMBER IS BEGIN RETURN n * 2; END; \
         PROCEDURE save_result(value NUMBER) IS BEGIN result := value; END; \
         BEGIN save_result(twice(21)); END;",
    );
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.declarations[1],
        sqlparser::ast::PlSqlDeclaration::Routine(routine)
            if routine.kind == sqlparser::ast::OraclePlSqlRoutineKind::Function
                && routine.return_type.is_some()
    ));
    assert!(matches!(
        &block.declarations[2],
        sqlparser::ast::PlSqlDeclaration::Routine(routine)
            if routine.kind == sqlparser::ast::OraclePlSqlRoutineKind::Procedure
                && routine.return_type.is_none()
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_packages_and_unit_lifecycle_are_typed() {
    let package = parse_one(
        "CREATE OR REPLACE EDITIONABLE PACKAGE employee_api AUTHID DEFINER \
         ACCESSIBLE BY (PROCEDURE app_entry) IS \
         TYPE employee_ids_t IS TABLE OF NUMBER INDEX BY PLS_INTEGER; \
         PROCEDURE raise_salary(p_id NUMBER); \
         FUNCTION employee_name(p_id NUMBER) RETURN VARCHAR2; \
         END employee_api;",
    );
    let Statement::OracleCreatePackage(create) = &package else {
        panic!("expected Oracle package");
    };
    assert_eq!(create.editionable, Some(true));
    assert!(!create.is_body);
    assert_eq!(create.declarations.len(), 3);
    assert!(matches!(
        &create.declarations[1],
        sqlparser::ast::PlSqlDeclaration::Routine(routine)
            if matches!(
                routine.body,
                sqlparser::ast::OraclePlSqlRoutineBody::Declaration
            )
    ));
    assert_eq!(parse_one(&package.to_string()), package);

    let alter = parse_one("ALTER PACKAGE employee_api COMPILE SPECIFICATION DEBUG REUSE SETTINGS");
    assert!(matches!(
        &alter,
        Statement::OracleAlterPlSqlUnit(unit)
            if unit.kind == sqlparser::ast::OraclePlSqlUnitKind::Package
                && matches!(
                    unit.action,
                    sqlparser::ast::OracleAlterPlSqlUnitAction::Compile {
                        target: Some(
                            sqlparser::ast::OraclePlSqlCompileTarget::Specification
                        ),
                        debug: true,
                        reuse_settings: true,
                        ..
                    }
                )
    ));

    let drop = parse_one("DROP PACKAGE BODY employee_api");
    assert!(matches!(
        &drop,
        Statement::OracleDropPlSqlUnit(unit)
            if unit.kind == sqlparser::ast::OraclePlSqlUnitKind::Package && unit.body
    ));
    assert_eq!(parse_one(&alter.to_string()), alter);
    assert_eq!(parse_one(&drop.to_string()), drop);
}

#[test]
fn oracle_triggers_types_and_libraries_are_typed() {
    let trigger = parse_one(
        "CREATE OR REPLACE TRIGGER employees_biu BEFORE INSERT OR UPDATE OF salary \
         ON employees FOR EACH ROW WHEN (NEW.salary < 0) \
         BEGIN :NEW.salary := 0; END;",
    );
    let Statement::OracleCreateTrigger(create) = &trigger else {
        panic!("expected Oracle trigger");
    };
    assert_eq!(
        create.timing,
        Some(sqlparser::ast::OracleTriggerTiming::Before)
    );
    assert!(matches!(
        &create.events[..],
        [
            sqlparser::ast::OracleTriggerEvent::Insert,
            sqlparser::ast::OracleTriggerEvent::Update(columns)
        ] if columns == &[Ident::new("SALARY")]
    ));
    assert!(create.for_each_row);
    assert!(create.when.is_some());
    assert!(matches!(
        &create.body,
        sqlparser::ast::OracleTriggerBody::Block(block)
            if matches!(block.statements[0], Statement::SqlPsmAssignment(_))
    ));

    let data_type = parse_one(
        "CREATE OR REPLACE TYPE employee_t AS OBJECT (\
         employee_id NUMBER, MEMBER FUNCTION display_name RETURN VARCHAR2) NOT FINAL",
    );
    assert!(matches!(
        &data_type,
        Statement::OracleCreateType(create)
            if matches!(
                &create.definition,
                sqlparser::ast::OracleTypeDefinition::Object {
                    elements,
                    not_final: true
                } if elements.len() == 2
            )
    ));

    let library = parse_one(
        "CREATE OR REPLACE LIBRARY hash_lib AS '/opt/libhash.so' \
         AGENT 'extproc' CREDENTIAL app_credential",
    );
    assert!(matches!(
        &library,
        Statement::OracleCreateLibrary(create)
            if create.agent.is_some() && create.credential.is_some()
    ));
    assert_eq!(parse_one(&trigger.to_string()), trigger);
    assert_eq!(parse_one(&data_type.to_string()), data_type);
    assert_eq!(parse_one(&library.to_string()), library);
}

#[test]
fn oracle_extended_controls_and_directives_are_typed() {
    let statement = parse_one(
        "BEGIN CASE selector WHEN 1, 2 THEN result := 'small'; \
         WHEN > 2, <= 5 THEN result := 'medium'; ELSE result := 'large'; \
         END CASE; END;",
    );
    let Statement::PlSqlBlock(block) = &statement else {
        panic!("expected PL/SQL block");
    };
    let Statement::Case(case) = &block.statements[0] else {
        panic!("expected CASE");
    };
    assert_eq!(case.oracle_when_controls.len(), 2);
    assert!(matches!(
        &case.oracle_when_controls[0],
        Some(controls) if controls.len() == 2
    ));
    assert!(matches!(
        &case.oracle_when_controls[1],
        Some(controls)
            if matches!(
                controls[0],
                sqlparser::ast::OracleCaseControl::Comparison { .. }
            ) && matches!(
                controls[1],
                sqlparser::ast::OracleCaseControl::Comparison { .. }
            )
    ));

    let compilation = parse_one(
        "BEGIN $IF $$PLSQL_DEBUG $THEN DBMS_OUTPUT.PUT_LINE('debug'); \
         $ELSE NULL; $END END;",
    );
    let Statement::PlSqlBlock(block) = &compilation else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.statements[0],
        Statement::PlSqlConditionalCompilation(compilation)
            if compilation.branches.len() == 1
                && compilation.else_statements.len() == 1
                && matches!(
                    compilation.branches[0].condition,
                    Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: Value::PlSqlInquiryDirective(_),
                        ..
                    })
                )
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
    assert_eq!(parse_one(&compilation.to_string()), compilation);
}

#[test]
fn oracle_administrative_statements_have_structured_ast() {
    let statement =
        parse_one("ALTER JSON RELATIONAL DUALITY VIEW orders_dv ENABLE LOGICAL REPLICATION");
    let Statement::OracleAlter(alter) = &statement else {
        panic!("expected structured Oracle ALTER statement");
    };
    assert_eq!(
        alter.object_type,
        sqlparser::ast::OracleAlterObjectType::JsonRelationalDualityView
    );
    assert!(matches!(
        alter.operation,
        sqlparser::ast::OracleAlterOperation::EnableLogicalReplication
    ));

    let statement =
        parse_one("DROP TABLESPACE app_data INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS");
    let Statement::OracleDrop(drop) = &statement else {
        panic!("expected structured Oracle DROP statement");
    };
    assert_eq!(
        drop.object_type,
        sqlparser::ast::OracleDropObjectType::Tablespace
    );
    assert_eq!(drop.options.len(), 2);

    let statement = parse_one("LOCK TABLE employees IN SHARE ROW EXCLUSIVE MODE NOWAIT");
    let Statement::OracleLockTable(lock) = &statement else {
        panic!("expected structured Oracle LOCK TABLE statement");
    };
    assert_eq!(lock.mode, sqlparser::ast::OracleLockMode::ShareRowExclusive);
    assert!(matches!(
        lock.wait,
        Some(sqlparser::ast::OracleLockWait::Nowait)
    ));

    let statement =
        parse_one("ASSOCIATE STATISTICS WITH COLUMNS employees.salary USING salary_stats_type");
    assert!(matches!(
        statement,
        Statement::OracleCommand(sqlparser::ast::OracleCommandStatement {
            command: sqlparser::ast::OracleCommand::AssociateColumnStatistics { .. },
            ..
        })
    ));

    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_structured_statements_reject_invalid_clause_sequences() {
    let invalid = [
        "ALTER JSON RELATIONAL DUALITY VIEW orders_dv ENABLE REPLICATION LOGICAL",
        "DROP DOMAIN email_domain PRESERVE FORCE",
        "DROP TABLESPACE app_data CASCADE CONSTRAINTS INCLUDING CONTENTS AND DATAFILES",
        "LOCK TABLE employees IN SHARE BANANA MODE",
        "AUDIT POLICY app_audit BY app_user WHENEVER MAYBE",
        "TRUNCATE CLUSTER employee_cluster KEEP STORAGE",
    ];
    for sql in invalid {
        assert!(
            Parser::parse_sql(&OracleDialect {}, sql).is_err(),
            "Oracle parser accepted invalid clause sequence: {sql}"
        );
    }
}

#[test]
fn oracle_create_families_build_nested_ast() {
    let analytic = parse_one(
        "CREATE ANALYTIC VIEW sales_av USING sales \
         DIMENSION BY (time_attr_dim KEY month REFERENCES month) \
         MEASURES (sales_amount FACT sales.amount)",
    );
    let Statement::OracleCreate(create) = &analytic else {
        panic!("expected Oracle CREATE");
    };
    let sqlparser::ast::OracleCreateDefinition::AnalyticView {
        dimensions,
        measures,
        ..
    } = &create.definition
    else {
        panic!("expected analytic view");
    };
    assert_eq!(dimensions.len(), 1);
    assert_eq!(dimensions[0].key.value, "MONTH");
    assert_eq!(measures.len(), 1);
    assert_eq!(measures[0].fact.to_string(), "SALES.AMOUNT");

    let attribute = parse_one(
        "CREATE ATTRIBUTE DIMENSION time_attr_dim USING calendar \
         ATTRIBUTES (year, month) LEVEL year KEY year \
         LEVEL month KEY month DETERMINES (year)",
    );
    let Statement::OracleCreate(create) = &attribute else {
        panic!("expected Oracle CREATE");
    };
    let sqlparser::ast::OracleCreateDefinition::AttributeDimension {
        attributes, levels, ..
    } = &create.definition
    else {
        panic!("expected attribute dimension");
    };
    assert_eq!(attributes.len(), 2);
    assert_eq!(levels.len(), 2);
    assert_eq!(levels[1].determines[0].value, "YEAR");

    let graph = parse_one(
        "CREATE PROPERTY GRAPH social_graph \
         VERTEX TABLES (persons KEY (person_id)) \
         EDGE TABLES (follows KEY (follow_id) \
         SOURCE KEY (from_id) REFERENCES persons(person_id) \
         DESTINATION KEY (to_id) REFERENCES persons(person_id))",
    );
    let Statement::OracleCreate(create) = &graph else {
        panic!("expected Oracle CREATE");
    };
    assert!(matches!(
        &create.definition,
        sqlparser::ast::OracleCreateDefinition::PropertyGraph {
            vertices,
            edges,
            ..
        } if vertices.len() == 1
            && edges.len() == 1
            && edges[0].source_key[0].value == "FROM_ID"
            && edges[0].destination_key[0].value == "TO_ID"
    ));

    let schema = parse_one(
        "CREATE SCHEMA AUTHORIZATION app_user \
         CREATE TABLE settings (name VARCHAR2(100), value VARCHAR2(100))",
    );
    let Statement::OracleCreate(create) = &schema else {
        panic!("expected Oracle CREATE");
    };
    assert!(matches!(
        &create.definition,
        sqlparser::ast::OracleCreateDefinition::Schema { statements, .. }
            if matches!(statements.as_slice(), [Statement::CreateTable(_)])
    ));

    for statement in [analytic, attribute, graph, schema] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_transaction_privilege_and_explain_commands_are_typed() {
    let transaction = parse_one("SET TRANSACTION READ ONLY NAME 'reporting'");
    assert!(matches!(
        &transaction,
        Statement::OracleCommand(sqlparser::ast::OracleCommandStatement {
            command: sqlparser::ast::OracleCommand::SetTransaction { modes, name },
            ..
        }) if modes == &[sqlparser::ast::TransactionMode::AccessMode(
            sqlparser::ast::TransactionAccessMode::ReadOnly
        )] && name.is_some()
    ));

    let role = parse_one("SET ROLE reporting_role IDENTIFIED BY secret");
    assert!(matches!(
        &role,
        Statement::OracleCommand(sqlparser::ast::OracleCommandStatement {
            command: sqlparser::ast::OracleCommand::SetRole { role, password },
            ..
        }) if role.to_string() == "REPORTING_ROLE" && password.value == "SECRET"
    ));

    let grant = parse_one("GRANT CREATE SESSION, CREATE TABLE TO app_user WITH ADMIN OPTION");
    assert!(matches!(
        &grant,
        Statement::OracleCommand(sqlparser::ast::OracleCommandStatement {
            command: sqlparser::ast::OracleCommand::GrantSystemPrivileges {
                privileges,
                grantees,
                admin_option: true,
            },
            ..
        }) if privileges == &[
            sqlparser::ast::OracleSystemPrivilege::CreateSession,
            sqlparser::ast::OracleSystemPrivilege::CreateTable,
        ] && grantees.len() == 1
    ));

    let explain = parse_one(
        "EXPLAIN PLAN SET STATEMENT_ID = 'q1' INTO plan_table \
         FOR SELECT employee_id FROM employees",
    );
    assert!(matches!(
        &explain,
        Statement::OracleCommand(sqlparser::ast::OracleCommandStatement {
            command: sqlparser::ast::OracleCommand::ExplainPlan {
                statement_id: Some(_),
                into,
                statement,
            },
            ..
        }) if into.to_string() == "PLAN_TABLE"
            && matches!(statement.as_ref(), Statement::Query(_))
    ));

    for statement in [transaction, role, grant, explain] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_interval_and_character_data_types_are_typed() {
    let statement = parse_one(
        "CREATE TABLE typed_values (\
         local_ts TIMESTAMP(6) WITH LOCAL TIME ZONE, \
         year_span INTERVAL YEAR(4) TO MONTH, \
         day_span INTERVAL DAY(3) TO SECOND(6), \
         char_value VARCHAR2(40 CHAR), byte_value VARCHAR2(80 BYTE), \
         national_value NVARCHAR2(30), raw_value RAW(16), \
         long_raw_value LONG RAW, file_value BFILE)",
    );
    let Statement::CreateTable(create) = &statement else {
        panic!("expected CREATE TABLE");
    };
    assert!(matches!(
        create.columns[0].data_type,
        sqlparser::ast::DataType::Timestamp(
            Some(6),
            sqlparser::ast::TimezoneInfo::WithLocalTimeZone
        )
    ));
    assert!(matches!(
        create.columns[1].data_type,
        sqlparser::ast::DataType::OracleInterval {
            fields: sqlparser::ast::IntervalFields::YearToMonth,
            leading_precision: Some(4),
            fractional_seconds_precision: None,
        }
    ));
    assert!(matches!(
        create.columns[2].data_type,
        sqlparser::ast::DataType::OracleInterval {
            fields: sqlparser::ast::IntervalFields::DayToSecond,
            leading_precision: Some(3),
            fractional_seconds_precision: Some(6),
        }
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_synonym_directory_link_and_sequence_lifecycle_is_typed() {
    let synonym = parse_one("CREATE OR REPLACE PUBLIC SYNONYM emp FOR hr.employees");
    assert!(matches!(
        &synonym,
        Statement::OracleCreate(sqlparser::ast::OracleCreateStatement {
            or_replace: true,
            definition: sqlparser::ast::OracleCreateDefinition::Synonym {
                public: true,
                name,
                target,
            },
            ..
        }) if name.to_string() == "EMP" && target.to_string() == "HR.EMPLOYEES"
    ));

    let directory = parse_one("CREATE OR REPLACE DIRECTORY data_dir AS '/srv/data' SHARING = NONE");
    assert!(matches!(
        &directory,
        Statement::OracleCreate(sqlparser::ast::OracleCreateStatement {
            definition: sqlparser::ast::OracleCreateDefinition::Directory {
                sharing: Some(sharing),
                ..
            },
            ..
        }) if sharing.value == "NONE"
    ));

    let database_link = parse_one(
        "CREATE PUBLIC DATABASE LINK reporting CONNECT TO report_user \
         IDENTIFIED BY password USING 'reporting_service'",
    );
    assert!(matches!(
        &database_link,
        Statement::OracleCreate(sqlparser::ast::OracleCreateStatement {
            definition: sqlparser::ast::OracleCreateDefinition::DatabaseLink {
                public: true,
                user,
                ..
            },
            ..
        }) if user.to_string() == "REPORT_USER"
    ));

    let altered = parse_one(
        "ALTER PUBLIC DATABASE LINK reporting \
         CONNECT TO report_user IDENTIFIED BY new_password",
    );
    assert!(matches!(
        &altered,
        Statement::OracleAlter(sqlparser::ast::OracleAlterStatement {
            object_type: sqlparser::ast::OracleAlterObjectType::PublicDatabaseLink,
            operation: sqlparser::ast::OracleAlterOperation::ConnectTo { user, .. },
            ..
        }) if user.to_string() == "REPORT_USER"
    ));

    let sequence =
        parse_one("CREATE SEQUENCE audit_seq NOMINVALUE MAXVALUE 999 NOCACHE ORDER CYCLE");
    assert!(matches!(
        &sequence,
        Statement::CreateSequence {
            sequence_options,
            ..
        } if matches!(
            sequence_options.as_slice(),
            [
                sqlparser::ast::SequenceOptions::MinValue(None),
                sqlparser::ast::SequenceOptions::MaxValue(Some(_)),
                sqlparser::ast::SequenceOptions::NoCache,
                sqlparser::ast::SequenceOptions::Order(false),
                sqlparser::ast::SequenceOptions::Cycle(false),
            ]
        )
    ));

    for statement in [synonym, directory, database_link, altered, sequence] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_pattern_floating_object_and_translation_expressions_are_typed() {
    let statement = parse_one(
        "SELECT TRANSLATE(name USING NCHAR_CS), \
         TREAT(VALUE(p) AS employee_t).employee_id \
         FROM persons p \
         WHERE name NOT LIKE4 N'Z%' \
         AND reading IS NOT NAN \
         AND VALUE(p) IS OF (ONLY employee_t, contractor_t) \
         AND tag_t('urgent') MEMBER OF p.tags",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.projection[0],
        SelectItem::UnnamedExpr(Expr::OracleTranslateUsing {
            character_set,
            ..
        }) if character_set.to_string() == "NCHAR_CS"
    ));
    assert!(format!("{:?}", select.projection[1]).contains("OracleTreat"));

    let selection = select.selection.as_ref().expect("WHERE expression");
    let debug = format!("{selection:?}");
    assert!(debug.contains("OracleLike"));
    assert!(debug.contains("kind: Like4"));
    assert!(debug.contains("OracleIs"));
    assert!(debug.contains("Nan"));
    assert!(debug.contains("only: true"));
    assert!(debug.contains("OracleMemberOf"));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_flashback_partitioned_join_and_qualify_are_typed() {
    let flashback =
        parse_one("SELECT * FROM employees VERSIONS BETWEEN TIMESTAMP :start_time AND :end_time");
    let Statement::Query(query) = &flashback else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.from[0].relation,
        sqlparser::ast::TableFactor::Table {
            version: Some(version),
            ..
        } if matches!(
            version.as_ref(),
            sqlparser::ast::TableVersion::OracleVersionsBetween {
                kind: sqlparser::ast::OracleFlashbackVersionKind::Timestamp,
                start: sqlparser::ast::OracleFlashbackBoundary::Expr(_),
                end: sqlparser::ast::OracleFlashbackBoundary::Expr(_),
            }
        )
    ));

    let partitioned = parse_one(
        "SELECT d.day, s.amount FROM calendar d \
         LEFT OUTER JOIN sales s PARTITION BY (s.product_id) ON s.day = d.day",
    );
    let Statement::Query(query) = &partitioned else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.from[0].joins[0].join_operator,
        sqlparser::ast::JoinOperator::OraclePartitioned {
            kind: sqlparser::ast::OraclePartitionedJoinKind::LeftOuter,
            partition_by,
            constraint: sqlparser::ast::JoinConstraint::On(_),
        } if partition_by.len() == 1
    ));

    let qualify = parse_one(
        "SELECT employee_id, ROW_NUMBER() OVER (ORDER BY salary DESC) AS position \
         FROM employees WHERE active IS TRUE QUALIFY position = 1",
    );
    let Statement::Query(query) = &qualify else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(select.selection.is_some());
    assert!(select.qualify.is_some());

    for statement in [flashback, partitioned, qualify] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_drop_and_truncate_modifiers_are_typed() {
    let drop_user = parse_one("DROP USER IF EXISTS app_user CASCADE");
    assert!(matches!(
        &drop_user,
        Statement::OracleDrop(sqlparser::ast::OracleDropStatement {
            object_type: sqlparser::ast::OracleDropObjectType::User,
            if_exists: true,
            options,
            ..
        }) if options == &[sqlparser::ast::OracleDropOption::Cascade]
    ));

    let drop_table = parse_one("DROP TABLE employees_stage CASCADE CONSTRAINTS PURGE");
    assert!(matches!(
        &drop_table,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Table,
            purge: true,
            oracle: Some(sqlparser::ast::OracleDropOptions {
                cascade_constraints: true,
                ..
            }),
            ..
        }
    ));

    let drop_view = parse_one("DROP VIEW active_employees CASCADE CONSTRAINTS");
    assert!(matches!(
        &drop_view,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::View,
            oracle: Some(sqlparser::ast::OracleDropOptions {
                cascade_constraints: true,
                ..
            }),
            ..
        }
    ));

    let drop_materialized = parse_one("DROP MATERIALIZED VIEW department_totals PRESERVE TABLE");
    assert!(matches!(
        &drop_materialized,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::MaterializedView,
            oracle: Some(sqlparser::ast::OracleDropOptions {
                preserve_table: true,
                ..
            }),
            ..
        }
    ));

    let truncate = parse_one("TRUNCATE TABLE employees_stage DROP ALL STORAGE CASCADE");
    assert!(matches!(
        &truncate,
        Statement::Truncate(sqlparser::ast::Truncate {
            oracle_storage: Some(sqlparser::ast::OracleTruncateStorage::DropAll),
            cascade: Some(sqlparser::ast::CascadeOption::Cascade),
            ..
        })
    ));

    for statement in [
        drop_user,
        drop_table,
        drop_view,
        drop_materialized,
        truncate,
    ] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_index_families_are_typed() {
    let standard = parse_one(
        "CREATE UNIQUE INDEX employees_email_uix \
         ON employees (LOWER(email), employee_id DESC) ONLINE",
    );
    assert!(matches!(
        &standard,
        Statement::OracleCreate(sqlparser::ast::OracleCreateStatement {
            definition: sqlparser::ast::OracleCreateDefinition::Index {
                kind: sqlparser::ast::OracleIndexKind::Standard,
                unique: true,
                columns,
                options: sqlparser::ast::OracleIndexOptions {
                    online: true,
                    ..
                },
                ..
            },
            ..
        }) if columns.len() == 2
    ));

    let domain = parse_one(
        "CREATE INDEX documents_text_ix ON documents (body) \
         INDEXTYPE IS ctxsys.context PARAMETERS ('SYNC (ON COMMIT)')",
    );
    assert!(matches!(
        &domain,
        Statement::OracleCreate(sqlparser::ast::OracleCreateStatement {
            definition: sqlparser::ast::OracleCreateDefinition::Index {
                options: sqlparser::ast::OracleIndexOptions {
                    indextype: Some(indextype),
                    parameters: Some(_),
                    ..
                },
                ..
            },
            ..
        }) if indextype.to_string() == "CTXSYS.CONTEXT"
    ));

    let vector = parse_one(
        "CREATE VECTOR INDEX items_embedding_hnsw ON items (embedding) \
         ORGANIZATION INMEMORY NEIGHBOR GRAPH DISTANCE COSINE \
         WITH TARGET ACCURACY 95 PARAMETERS (TYPE HNSW, NEIGHBORS 32)",
    );
    assert!(matches!(
        &vector,
        Statement::OracleCreate(sqlparser::ast::OracleCreateStatement {
            definition: sqlparser::ast::OracleCreateDefinition::Index {
                kind: sqlparser::ast::OracleIndexKind::Vector,
                options: sqlparser::ast::OracleIndexOptions {
                    vector_distance: Some(distance),
                    vector_parameters,
                    ..
                },
                ..
            },
            ..
        }) if distance.value == "COSINE" && vector_parameters.len() == 2
    ));

    let altered = parse_one("ALTER INDEX employees_name_ix REBUILD ONLINE");
    assert!(matches!(
        &altered,
        Statement::OracleAlter(sqlparser::ast::OracleAlterStatement {
            object_type: sqlparser::ast::OracleAlterObjectType::Index,
            operation: sqlparser::ast::OracleAlterOperation::RebuildIndex { online: true },
            ..
        })
    ));

    for statement in [standard, domain, vector, altered] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_identity_and_parenthesized_column_changes_are_typed() {
    let create = parse_one(
        "CREATE TABLE messages (\
         message_id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY \
         (START WITH 100 CACHE 20), body CLOB)",
    );
    let Statement::CreateTable(create_table) = &create else {
        panic!("expected CREATE TABLE");
    };
    assert!(matches!(
        &create_table.columns[0].options[0].option,
        sqlparser::ast::ColumnOption::Generated {
            generated_as: sqlparser::ast::GeneratedAs::ByDefaultOnNull,
            sequence_options: Some(options),
            ..
        } if options.len() == 2
    ));

    let add = parse_one(
        "ALTER TABLE employees ADD \
         (preferred_name VARCHAR2(100), active BOOLEAN DEFAULT TRUE NOT NULL)",
    );
    assert!(matches!(
        &add,
        Statement::AlterTable(sqlparser::ast::AlterTable { operations, .. })
            if matches!(
            operations.as_slice(),
            [sqlparser::ast::AlterTableOperation::OracleAddColumns { columns }]
                if columns.len() == 2
                    && columns[0].name.value == "PREFERRED_NAME"
                    && columns[1].name.value == "ACTIVE"
        )
    ));

    let modify = parse_one(
        "ALTER TABLE employees MODIFY \
         (last_name VARCHAR2(200 CHAR) COLLATE BINARY_CI)",
    );
    assert!(matches!(
        &modify,
        Statement::AlterTable(sqlparser::ast::AlterTable { operations, .. })
            if matches!(
            operations.as_slice(),
            [sqlparser::ast::AlterTableOperation::OracleModifyColumns { columns }]
                if columns.len() == 1
                    && columns[0].name.value == "LAST_NAME"
                    && columns[0].options.len() == 1
        )
    ));

    for statement in [create, add, modify] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_recursive_search_and_cycle_are_typed() {
    let statement = parse_one(
        "WITH org(emp_id, manager_id) AS (\
         SELECT employee_id, manager_id FROM employees \
         UNION ALL \
         SELECT e.employee_id, e.manager_id FROM employees e \
         JOIN org o ON e.manager_id = o.emp_id) \
         SEARCH DEPTH FIRST BY emp_id SET order_col \
         CYCLE emp_id SET is_cycle TO 1 DEFAULT 0 \
         SELECT * FROM org",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let with = query.with.as_ref().expect("WITH clause");
    assert!(matches!(
        &with.search,
        Some(sqlparser::ast::SearchClause {
            order: sqlparser::ast::SearchOrder::DepthFirst,
            by_columns,
            set_column,
        }) if by_columns.len() == 1 && set_column.value == "ORDER_COL"
    ));
    assert!(matches!(
        &with.cycle,
        Some(sqlparser::ast::CycleClause {
            columns,
            set_column,
            cycle_value: Some(_),
            non_cycle_value: Some(_),
            using_column: None,
        }) if columns.len() == 1 && set_column.value == "IS_CYCLE"
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_keep_dense_rank_is_typed() {
    let statement = parse_one(
        "SELECT department_id, \
         MAX(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct DESC, employee_id) \
         FROM employees GROUP BY department_id",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.projection[1],
        SelectItem::UnnamedExpr(Expr::OracleKeep {
            aggregate,
            rank: sqlparser::ast::OracleKeepRank::Last,
            order_by,
        }) if matches!(aggregate.as_ref(), Expr::Function(_)) && order_by.len() == 2
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_pivot_xml_is_typed() {
    let statement = parse_one(
        "SELECT * FROM (SELECT product, quarter, amount FROM sales) \
         PIVOT XML (SUM(amount) FOR quarter IN (ANY))",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.from[0].relation,
        sqlparser::ast::TableFactor::Pivot {
            xml: true,
            value_source: sqlparser::ast::PivotValueSource::Any(order_by),
            aggregate_functions,
            ..
        } if order_by.is_empty() && aggregate_functions.len() == 1
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_fetch_approximate_is_typed() {
    let statement = parse_one(
        "SELECT item_id FROM items \
         ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE) \
         FETCH APPROXIMATE FIRST 10 ROWS ONLY",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    assert!(matches!(
        query.fetch.as_deref(),
        Some(sqlparser::ast::Fetch {
            approximate: true,
            with_ties: false,
            percent: false,
            quantity: Some(_),
        })
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_group_by_vector_is_typed() {
    let statement = parse_one(
        "SELECT department_id, job_id, SUM(salary) FROM employees \
         GROUP BY VECTOR ((department_id), (job_id, manager_id), ())",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.group_by,
        sqlparser::ast::GroupByExpr::OracleVector(vectors)
            if vectors.len() == 3
                && vectors[0].len() == 1
                && vectors[1].len() == 2
                && vectors[2].is_empty()
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_json_exists_clauses_are_typed() {
    let statement = parse_one(
        "SELECT * FROM orders WHERE \
         JSON_EXISTS(payload, '$?(@.total > $min)' \
         PASSING 100 AS \"min\", threshold AS \"threshold\" ERROR ON ERROR)",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        select.selection.as_deref(),
        Some(Expr::Function(sqlparser::ast::Function {
            args: sqlparser::ast::FunctionArguments::List(
                sqlparser::ast::FunctionArgumentList { args, clauses, .. }
            ),
            ..
        })) if args.len() == 2
            && matches!(
                clauses.as_slice(),
                [
                    sqlparser::ast::FunctionArgumentClause::OracleJsonPassing(bindings),
                    sqlparser::ast::FunctionArgumentClause::JsonOnError(
                        sqlparser::ast::JsonOnBehavior::Error
                    ),
                ] if bindings.len() == 2
                    && bindings[0].alias.as_ref().is_some_and(|alias| {
                        alias.value == "min" && alias.quote_style == Some('"')
                    })
            )
    ));

    for behavior in ["TRUE", "FALSE", "UNKNOWN"] {
        let sql = format!("SELECT JSON_EXISTS(payload, '$.id' {behavior} ON ERROR) FROM orders");
        let parsed = parse_one(&sql);
        assert_eq!(parse_one(&parsed.to_string()), parsed);
    }
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_split_partition_is_typed() {
    let statement = parse_one(
        "ALTER TABLE sales SPLIT PARTITION future AT (DATE '2027-01-01') \
         INTO (PARTITION sales_2026, PARTITION future) UPDATE GLOBAL INDEXES",
    );
    assert!(matches!(
        &statement,
        Statement::AlterTable(sqlparser::ast::AlterTable { operations, .. })
            if matches!(
                operations.as_slice(),
                [sqlparser::ast::AlterTableOperation::SplitPartition {
                    at: Some(Expr::TypedString(_)),
                    into,
                    update_global_indexes: true,
                    ..
                }] if into.len() == 2 && into.iter().all(|target| target.bound.is_none())
            )
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_alter_view_compile_is_typed() {
    let statement = parse_one("ALTER VIEW active_employees COMPILE");
    assert!(matches!(
        &statement,
        Statement::OracleAlter(sqlparser::ast::OracleAlterStatement {
            object_type: sqlparser::ast::OracleAlterObjectType::View,
            target: sqlparser::ast::OracleAlterTarget::Name(name),
            operation: sqlparser::ast::OracleAlterOperation::Compile,
            ..
        }) if name.to_string() == "ACTIVE_EMPLOYEES"
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_alter_materialized_view_refresh_is_typed() {
    let statement = parse_one("ALTER MATERIALIZED VIEW department_totals REFRESH FAST ON DEMAND");
    assert!(matches!(
        &statement,
        Statement::AlterMaterializedView {
            operation: sqlparser::ast::AlterMaterializedViewOperation::OracleRefresh {
                method: sqlparser::ast::OracleMaterializedViewRefreshMethod::Fast,
                mode: sqlparser::ast::OracleMaterializedViewRefreshMode::Demand,
            },
            ..
        }
    ));
    for sql in [
        "ALTER MATERIALIZED VIEW department_totals REFRESH COMPLETE ON COMMIT",
        "ALTER MATERIALIZED VIEW department_totals REFRESH FORCE ON DEMAND",
    ] {
        let parsed = parse_one(sql);
        assert_eq!(parse_one(&parsed.to_string()), parsed);
    }
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_relational_and_object_views_are_typed() {
    let relational = parse_one(
        "CREATE OR REPLACE FORCE EDITIONING VIEW active_employees \
         (employee_id, last_name) AS \
         SELECT employee_id, last_name FROM employees WHERE active IS TRUE \
         WITH READ ONLY",
    );
    assert!(matches!(
        &relational,
        Statement::CreateView(sqlparser::ast::CreateView {
            oracle: Some(sqlparser::ast::OracleCreateViewOptions {
                force: Some(true),
                editioning: Some(true),
                constraint: Some(sqlparser::ast::OracleViewConstraint::ReadOnly),
                ..
            }),
            columns,
            ..
        }) if columns.len() == 2
    ));

    let object = parse_one(
        "CREATE VIEW employee_objects OF employee_t \
         WITH OBJECT IDENTIFIER (employee_id) AS \
         SELECT employee_id, last_name FROM employees",
    );
    assert!(matches!(
        &object,
        Statement::CreateView(sqlparser::ast::CreateView {
            oracle: Some(sqlparser::ast::OracleCreateViewOptions {
                object: Some(sqlparser::ast::OracleObjectView {
                    data_type,
                    object_identifier,
                }),
                ..
            }),
            ..
        }) if data_type.to_string() == "EMPLOYEE_T" && object_identifier.len() == 1
    ));

    for statement in [relational, object] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_create_materialized_view_options_are_typed() {
    let statement = parse_one(
        "CREATE MATERIALIZED VIEW department_totals \
         BUILD IMMEDIATE REFRESH FAST ON COMMIT ENABLE QUERY REWRITE AS \
         SELECT department_id, COUNT(*) employee_count FROM employees \
         GROUP BY department_id",
    );
    assert!(matches!(
        &statement,
        Statement::CreateView(sqlparser::ast::CreateView {
            materialized: true,
            oracle: Some(sqlparser::ast::OracleCreateViewOptions {
                materialized: Some(sqlparser::ast::OracleCreateMaterializedViewOptions {
                    build: Some(sqlparser::ast::OracleMaterializedViewBuild::Immediate),
                    refresh_method: Some(sqlparser::ast::OracleMaterializedViewRefreshMethod::Fast),
                    refresh_mode: Some(sqlparser::ast::OracleMaterializedViewRefreshMode::Commit),
                    query_rewrite: Some(true),
                }),
                ..
            }),
            ..
        })
    ));
    for sql in [
        "CREATE MATERIALIZED VIEW mv BUILD DEFERRED REFRESH COMPLETE ON DEMAND DISABLE QUERY REWRITE AS SELECT 1 FROM dual",
        "CREATE MATERIALIZED VIEW mv REFRESH FORCE ON DEMAND AS SELECT 1 FROM dual",
    ] {
        let parsed = parse_one(sql);
        assert_eq!(parse_one(&parsed.to_string()), parsed);
    }
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_materialized_view_log_options_are_typed() {
    let statement = parse_one(
        "CREATE MATERIALIZED VIEW LOG ON employees \
         WITH PRIMARY KEY, ROWID, SEQUENCE (department_id, salary) \
         INCLUDING NEW VALUES",
    );
    assert!(matches!(
        &statement,
        Statement::CreateMaterializedViewLog {
            table_name,
            with_primary_key: true,
            with_rowid: true,
            with_sequence: true,
            columns,
            including_new_values: true,
        } if table_name.to_string() == "EMPLOYEES" && columns.len() == 2
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_dml_error_logging_is_typed() {
    let insert = parse_one(
        "INSERT INTO target_table SELECT * FROM source_table \
         LOG ERRORS INTO err$_target_table ('load-1') REJECT LIMIT UNLIMITED",
    );
    assert!(matches!(
        &insert,
        Statement::Insert(sqlparser::ast::Insert {
            error_logging:
                Some(sqlparser::ast::OracleErrorLoggingClause {
                    table: Some(table),
                    tag: Some(_),
                    reject_limit: Some(sqlparser::ast::OracleRejectLimit::Unlimited),
                }),
            ..
        }) if table.to_string() == "ERR$_TARGET_TABLE"
    ));

    let update = parse_one(
        "UPDATE employees SET salary = salary * 2 \
         LOG ERRORS INTO err$_employees ('raise') REJECT LIMIT 10",
    );
    assert!(matches!(
        &update,
        Statement::Update(sqlparser::ast::Update {
            error_logging: Some(sqlparser::ast::OracleErrorLoggingClause {
                reject_limit: Some(sqlparser::ast::OracleRejectLimit::Value(_)),
                ..
            }),
            ..
        })
    ));

    let merge = parse_one(
        "MERGE INTO target d USING source s ON (d.id = s.id) \
         WHEN MATCHED THEN UPDATE SET d.value = s.value \
         LOG ERRORS INTO err$_target REJECT LIMIT UNLIMITED",
    );
    assert!(matches!(
        &merge,
        Statement::Merge {
            error_logging: Some(sqlparser::ast::OracleErrorLoggingClause { .. }),
            ..
        }
    ));

    for statement in [insert, update, merge] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_merge_action_conditions_are_typed() {
    let statement = parse_one(
        "MERGE INTO bonuses d \
         USING (SELECT employee_id, salary FROM employees) s \
         ON (d.employee_id = s.employee_id) \
         WHEN MATCHED THEN UPDATE SET d.bonus = s.salary * 0.1 \
         DELETE WHERE s.salary = 0 \
         WHEN NOT MATCHED THEN INSERT (employee_id, bonus) \
         VALUES (s.employee_id, s.salary * 0.05) WHERE s.salary > 0",
    );
    assert!(matches!(
        &statement,
        Statement::Merge { clauses, .. }
            if matches!(
                clauses.as_slice(),
                [
                    sqlparser::ast::MergeClause {
                        action: sqlparser::ast::MergeAction::Update {
                            delete_where: Some(_),
                            ..
                        },
                        ..
                    },
                    sqlparser::ast::MergeClause {
                        action: sqlparser::ast::MergeAction::Insert(
                            sqlparser::ast::MergeInsertExpr {
                                where_clause: Some(_),
                                ..
                            }
                        ),
                        ..
                    },
                ]
            )
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_multitable_insert_is_typed() {
    let all = parse_one(
        "INSERT ALL \
         INTO employees_history VALUES (employee_id, hire_date) \
         INTO audit_log VALUES (employee_id, 'COPIED') \
         SELECT employee_id, hire_date FROM employees",
    );
    assert!(matches!(
        &all,
        Statement::OracleMultiTableInsert(sqlparser::ast::OracleMultiTableInsert {
            mode: sqlparser::ast::OracleMultiTableInsertMode::All,
            branches,
            else_targets,
            ..
        }) if branches.len() == 1
            && branches[0].condition.is_none()
            && branches[0].targets.len() == 2
            && else_targets.is_empty()
    ));

    let conditional = parse_one(
        "INSERT FIRST \
         WHEN salary > 20000 THEN \
         INTO high_earners VALUES (employee_id, salary) \
         WHEN salary > 10000 THEN \
         INTO mid_earners VALUES (employee_id, salary) \
         ELSE INTO other_earners VALUES (employee_id, salary) \
         SELECT employee_id, salary FROM employees",
    );
    assert!(matches!(
        &conditional,
        Statement::OracleMultiTableInsert(sqlparser::ast::OracleMultiTableInsert {
            mode: sqlparser::ast::OracleMultiTableInsertMode::First,
            branches,
            else_targets,
            ..
        }) if branches.len() == 2
            && branches.iter().all(|branch| branch.condition.is_some())
            && else_targets.len() == 1
    ));

    for statement in [all, conditional] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_graph_table_is_label_patterns_are_typed() {
    let statement = parse_one(
        "SELECT * FROM GRAPH_TABLE (social_graph \
         MATCH (a IS person) -[e IS follows]-> (b IS person) \
         COLUMNS (a.name AS person_a, b.name AS person_b))",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.from[0].relation,
        sqlparser::ast::TableFactor::GraphTable { match_clause, .. }
            if matches!(
                match_clause.patterns[0].expr,
                sqlparser::ast::GraphPatternExpr::Chain(ref elements)
                    if matches!(
                        elements.as_slice(),
                        [
                            sqlparser::ast::GraphPatternElement::Node(
                                sqlparser::ast::NodePattern {
                                    is_label_syntax: true,
                                    ..
                                }
                            ),
                            sqlparser::ast::GraphPatternElement::Edge(
                                sqlparser::ast::EdgePattern {
                                    is_label_syntax: true,
                                    ..
                                }
                            ),
                            sqlparser::ast::GraphPatternElement::Node(
                                sqlparser::ast::NodePattern {
                                    is_label_syntax: true,
                                    ..
                                }
                            ),
                        ]
                    )
            )
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_with_plsql_function_is_typed() {
    let statement = parse_one(
        "WITH FUNCTION twice(n NUMBER) RETURN NUMBER IS \
         BEGIN RETURN n * 2; END; \
         SELECT twice(21) FROM dual",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let with = query.with.as_ref().expect("WITH clause");
    assert!(with.cte_tables.is_empty());
    assert!(matches!(
        with.oracle_declarations.as_slice(),
        [sqlparser::ast::OraclePlSqlRoutine {
            kind: sqlparser::ast::OraclePlSqlRoutineKind::Function,
            return_type: Some(_),
            body: sqlparser::ast::OraclePlSqlRoutineBody::Block { .. },
            ..
        }]
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_interval_and_reference_partitioning_are_typed() {
    let interval = parse_one(
        "CREATE TABLE sales (sale_id NUMBER, sold_on DATE, amount NUMBER) \
         PARTITION BY RANGE (sold_on) \
         INTERVAL (NUMTOYMINTERVAL(1, 'MONTH')) \
         (PARTITION p0 VALUES LESS THAN (DATE '2026-01-01'))",
    );
    assert!(matches!(
        &interval,
        Statement::CreateTable(sqlparser::ast::CreateTable {
            partition_by:
                Some(sqlparser::ast::PartitionByClause {
                    strategy: sqlparser::ast::PartitionStrategy::Range,
                    interval: Some(Expr::Function(_)),
                    partitions,
                    ..
                }),
            ..
        }) if partitions.len() == 1 && partitions[0].values_less_than.len() == 1
    ));

    let reference = parse_one(
        "CREATE TABLE order_items (\
         order_id NUMBER, item_id NUMBER, \
         CONSTRAINT oi_order_fk FOREIGN KEY (order_id) REFERENCES orders(order_id)) \
         PARTITION BY REFERENCE (oi_order_fk)",
    );
    assert!(matches!(
        &reference,
        Statement::CreateTable(sqlparser::ast::CreateTable {
            partition_by:
                Some(sqlparser::ast::PartitionByClause {
                    strategy: sqlparser::ast::PartitionStrategy::Reference,
                    columns,
                    interval: None,
                    ..
                }),
            ..
        }) if columns.len() == 1
    ));

    for statement in [interval, reference] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_special_table_kinds_are_typed() {
    let private = parse_one(
        "CREATE PRIVATE TEMPORARY TABLE ora$ptt_work (item_id NUMBER) \
         ON COMMIT DROP DEFINITION",
    );
    assert!(matches!(
        &private,
        Statement::OracleCreateTable(sqlparser::ast::OracleCreateTable {
            kind: sqlparser::ast::OracleCreateTableKind::PrivateTemporary,
            options: sqlparser::ast::OracleCreateTableOptions::PrivateTemporary {
                drop_definition: true,
            },
            ..
        })
    ));

    let blockchain = parse_one(
        "CREATE BLOCKCHAIN TABLE ledger (id NUMBER, payload JSON) \
         NO DROP UNTIL 31 DAYS IDLE \
         NO DELETE UNTIL 31 DAYS AFTER INSERT \
         HASHING USING SHA2_512 VERSION v1",
    );
    assert!(matches!(
        &blockchain,
        Statement::OracleCreateTable(sqlparser::ast::OracleCreateTable {
            kind: sqlparser::ast::OracleCreateTableKind::Blockchain,
            options: sqlparser::ast::OracleCreateTableOptions::Retention {
                no_delete_until: Some(_),
                no_delete_after_insert: true,
                hashing: Some(_),
                ..
            },
            ..
        })
    ));

    let immutable = parse_one(
        "CREATE IMMUTABLE TABLE audit_events (event_id NUMBER, payload JSON) \
         NO DROP UNTIL 30 DAYS IDLE NO DELETE",
    );
    assert!(matches!(
        &immutable,
        Statement::OracleCreateTable(sqlparser::ast::OracleCreateTable {
            kind: sqlparser::ast::OracleCreateTableKind::Immutable,
            options: sqlparser::ast::OracleCreateTableOptions::Retention {
                no_delete_until: None,
                hashing: None,
                ..
            },
            ..
        })
    ));

    for statement in [private, blockchain, immutable] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_external_tables_are_typed() {
    let create = parse_one(
        "CREATE TABLE ext_employees (employee_id NUMBER, last_name VARCHAR2(100)) \
         ORGANIZATION EXTERNAL (\
         TYPE ORACLE_LOADER DEFAULT DIRECTORY data_dir \
         ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE) \
         LOCATION ('employees.csv')) REJECT LIMIT UNLIMITED",
    );
    assert!(matches!(
        &create,
        Statement::OracleCreateExternalTable {
            definition:
                sqlparser::ast::OracleExternalTableDefinition {
                    columns,
                    access_driver,
                    access_parameters,
                    locations,
                    reject_limit: Some(sqlparser::ast::OracleRejectLimit::Unlimited),
                    ..
                },
            ..
        } if columns.len() == 2
            && access_driver.value == "ORACLE_LOADER"
            && access_parameters.len() == 1
            && locations.len() == 1
    ));

    let inline = parse_one(
        "SELECT * FROM EXTERNAL ((\
         employee_id NUMBER, last_name VARCHAR2(100)) \
         TYPE ORACLE_LOADER DEFAULT DIRECTORY data_dir \
         ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE) \
         LOCATION ('employees.csv'))",
    );
    let Statement::Query(query) = &inline else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    assert!(matches!(
        &select.from[0].relation,
        sqlparser::ast::TableFactor::OracleExternal {
            definition: sqlparser::ast::OracleExternalTableDefinition { columns, .. },
            ..
        } if columns.len() == 2
    ));

    for statement in [create, inline] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_model_clause_is_typed() {
    let statement = parse_one(
        "SELECT country, year, sales FROM sales_view \
         MODEL PARTITION BY (country, region) \
         DIMENSION BY (year) \
         MEASURES (sales, margin) \
         RULES (sales[2027] = sales[2026] * 1.1, margin[2027] = margin[2026] + 10)",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::OracleModel { select, model } = query.body.as_ref() else {
        panic!("expected typed MODEL query");
    };

    assert_eq!(select.from[0].relation.to_string(), "SALES_VIEW");
    assert_eq!(model.partition_by.len(), 2);
    assert_eq!(model.dimension_by.len(), 1);
    assert_eq!(model.measures.len(), 2);
    assert_eq!(model.rules.len(), 2);
    assert!(matches!(
        &model.rules[0].target,
        sqlparser::ast::OracleModelRuleTarget::Expr(Expr::CompoundFieldAccess { .. })
    ));
    assert!(matches!(&model.rules[0].value, Expr::BinaryOp { .. }));
    assert_eq!(parse_one(&statement.to_string()), statement);

    let advanced = parse_one(
        "SELECT dim_col, cur_val FROM model_input \
         MODEL IGNORE NAV RETURN UPDATED ROWS MAIN forecast \
         DIMENSION BY (dim_col AS dimension_key) \
         MEASURES (cur_val AS value_to_model) UNIQUE DIMENSION \
         RULES UPDATE SEQUENTIAL ORDER ITERATE (1000) \
         UNTIL (PREVIOUS(value_to_model[1]) - value_to_model[1] < 1) \
         (UPSERT value_to_model[1] ORDER BY dim_col = value_to_model[1] / 2)",
    );
    let Statement::Query(query) = &advanced else {
        panic!("expected query");
    };
    let SetExpr::OracleModel { model, .. } = query.body.as_ref() else {
        panic!("expected MODEL query");
    };
    assert_eq!(
        model.global_options.nav,
        Some(sqlparser::ast::OracleModelNav::Ignore)
    );
    assert_eq!(
        model.return_rows,
        Some(sqlparser::ast::OracleModelReturnRows::Updated)
    );
    assert_eq!(
        model.name.as_ref().map(|name| name.value.as_str()),
        Some("FORECAST")
    );
    assert_eq!(
        model.dimension_by[0]
            .alias
            .as_ref()
            .map(|alias| alias.value.as_str()),
        Some("DIMENSION_KEY")
    );
    assert_eq!(
        model.rule_mode,
        Some(sqlparser::ast::OracleModelRuleMode::Update)
    );
    assert_eq!(
        model.rule_order,
        Some(sqlparser::ast::OracleModelRuleOrder::Sequential)
    );
    assert!(model
        .iterate
        .as_ref()
        .is_some_and(|iterate| iterate.until.is_some()));
    assert_eq!(
        model.rules[0].mode,
        Some(sqlparser::ast::OracleModelRuleMode::Upsert)
    );
    assert_eq!(model.rules[0].order_by.len(), 1);
    assert_eq!(parse_one(&advanced.to_string()), advanced);

    let reference = parse_one(
        "SELECT country, year, sales FROM sales_view \
         MODEL REFERENCE historical ON \
         (SELECT country, year, sales FROM sales_history) \
         PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) \
         UNIQUE SINGLE REFERENCE \
         PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) \
         (sales[2027] = historical.sales[2026])",
    );
    let Statement::Query(query) = &reference else {
        panic!("expected query");
    };
    let SetExpr::OracleModel { model, .. } = query.body.as_ref() else {
        panic!("expected MODEL query");
    };
    assert!(matches!(
        model.reference_models.as_slice(),
        [sqlparser::ast::OracleReferenceModel {
            cell_reference_options: sqlparser::ast::OracleModelCellReferenceOptions {
                unique: Some(sqlparser::ast::OracleModelUnique::SingleReference),
                ..
            },
            ..
        }]
    ));
    assert_eq!(parse_one(&reference.to_string()), reference);
}

#[test]
fn oracle_model_for_loops_are_typed() {
    let statement = parse_one(
        "SELECT product, year, sales FROM sales_view \
         MODEL DIMENSION BY (product, year) MEASURES (sales) RULES (\
         sales[FOR product IN ('Mouse', 'Keyboard'), 2027] = 1, \
         sales[FOR year FROM 2027 TO 2030 INCREMENT 1] = 2, \
         sales[FOR (product, year) IN (('Mouse', 2027), ('Keyboard', 2028))] = 3, \
         sales[FOR product IN (SELECT product_name FROM forecast_targets)] = 4)",
    );
    let Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let SetExpr::OracleModel { model, .. } = query.body.as_ref() else {
        panic!("expected MODEL query");
    };
    assert!(matches!(
        &model.rules[0].target,
        sqlparser::ast::OracleModelRuleTarget::ForLoop(
            sqlparser::ast::OracleModelForLoopAssignment {
                selectors: sqlparser::ast::OracleModelForLoopSelectors::Items(items),
                ..
            }
        ) if matches!(
            items.as_slice(),
            [
                sqlparser::ast::OracleModelCellSelector::For(
                    sqlparser::ast::OracleModelSingleColumnForLoop {
                        values: sqlparser::ast::OracleModelSingleColumnForLoopValues::InList(values),
                        ..
                    }
                ),
                sqlparser::ast::OracleModelCellSelector::Expr(_)
            ] if values.len() == 2
        )
    ));
    assert!(matches!(
        &model.rules[1].target,
        sqlparser::ast::OracleModelRuleTarget::ForLoop(
            sqlparser::ast::OracleModelForLoopAssignment {
                selectors: sqlparser::ast::OracleModelForLoopSelectors::Items(items),
                ..
            }
        ) if matches!(
            items.as_slice(),
            [sqlparser::ast::OracleModelCellSelector::For(
                sqlparser::ast::OracleModelSingleColumnForLoop {
                    values: sqlparser::ast::OracleModelSingleColumnForLoopValues::Range {
                        direction: sqlparser::ast::OracleModelForLoopDirection::Increment,
                        ..
                    },
                    ..
                }
            )]
        )
    ));
    assert!(matches!(
        &model.rules[2].target,
        sqlparser::ast::OracleModelRuleTarget::ForLoop(
            sqlparser::ast::OracleModelForLoopAssignment {
                selectors: sqlparser::ast::OracleModelForLoopSelectors::MultiColumn(
                    sqlparser::ast::OracleModelMultiColumnForLoop {
                        dimensions,
                        values: sqlparser::ast::OracleModelMultiColumnForLoopValues::Rows(rows),
                    }
                ),
                ..
            }
        ) if dimensions.len() == 2 && rows.len() == 2
    ));
    assert!(matches!(
        &model.rules[3].target,
        sqlparser::ast::OracleModelRuleTarget::ForLoop(
            sqlparser::ast::OracleModelForLoopAssignment {
                selectors: sqlparser::ast::OracleModelForLoopSelectors::Items(items),
                ..
            }
        ) if matches!(
            items.as_slice(),
            [sqlparser::ast::OracleModelCellSelector::For(
                sqlparser::ast::OracleModelSingleColumnForLoop {
                    values: sqlparser::ast::OracleModelSingleColumnForLoopValues::InQuery(_),
                    ..
                }
            )]
        )
    ));
    assert_eq!(parse_one(&statement.to_string()), statement);
}

#[test]
fn oracle_dynamic_open_collection_calls_and_trigger_aliases_are_typed() {
    let dynamic_open = parse_one(
        "DECLARE c SYS_REFCURSOR; statement_text VARCHAR2(4000); output_value NUMBER; \
         BEGIN OPEN c FOR statement_text USING IN 20, OUT output_value; END;",
    );
    let Statement::PlSqlBlock(block) = &dynamic_open else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.statements[0],
        Statement::Open(sqlparser::ast::OpenStatement {
            open_for:
                Some(sqlparser::ast::OpenFor::OracleDynamic {
                    query_expr,
                    using,
                }),
            ..
        }) if matches!(query_expr.as_ref(), Expr::Identifier(_))
            && using.len() == 2
            && using[0].mode == sqlparser::ast::PlSqlParameterMode::In
            && using[1].mode == sqlparser::ast::PlSqlParameterMode::Out
    ));

    let static_open = parse_one(
        "DECLARE c SYS_REFCURSOR; BEGIN \
         OPEN c FOR SELECT * FROM employees WHERE department_id = :1 USING 20; END;",
    );
    let Statement::PlSqlBlock(block) = &static_open else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.statements[0],
        Statement::Open(sqlparser::ast::OpenStatement {
            open_for: Some(sqlparser::ast::OpenFor::OracleQuery { using, .. }),
            ..
        }) if using.len() == 1
    ));

    let bound_open = parse_one(
        "DECLARE CURSOR c(p_id NUMBER, p_active BOOLEAN DEFAULT TRUE) IS \
         SELECT employee_id FROM employees WHERE employee_id = p_id; \
         BEGIN OPEN c(p_active => FALSE, p_id => 20); END;",
    );
    let Statement::PlSqlBlock(block) = &bound_open else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.statements[0],
        Statement::Open(sqlparser::ast::OpenStatement {
            open_for: Some(sqlparser::ast::OpenFor::BoundCursorArgs(arguments)),
            ..
        }) if arguments.len() == 2
            && arguments.iter().all(|argument| argument.name.is_some())
    ));

    let collection_call = parse_one(
        "DECLARE TYPE values_t IS TABLE OF NUMBER; values_list values_t; \
         BEGIN values_list.TRIM; END;",
    );
    let Statement::PlSqlBlock(block) = &collection_call else {
        panic!("expected PL/SQL block");
    };
    assert!(matches!(
        &block.statements[0],
        Statement::PlSqlProcedureCall(function)
            if function.name.to_string() == "VALUES_LIST.TRIM"
                && matches!(function.args, sqlparser::ast::FunctionArguments::None)
    ));

    let trigger = parse_one(
        "CREATE TRIGGER employees_history AFTER UPDATE ON employees \
         REFERENCING OLD AS old_row NEW new_row \
         FOR EACH ROW BEGIN NULL; END;",
    );
    assert!(matches!(
        &trigger,
        Statement::OracleCreateTrigger(sqlparser::ast::OracleCreateTrigger {
            referencing,
            ..
        }) if matches!(
            referencing.as_slice(),
            [
                sqlparser::ast::OracleTriggerReferencing {
                    kind: sqlparser::ast::OracleTriggerReferencingKind::Old,
                    is_as: true,
                    ..
                },
                sqlparser::ast::OracleTriggerReferencing {
                    kind: sqlparser::ast::OracleTriggerReferencingKind::New,
                    is_as: false,
                    ..
                },
            ]
        )
    ));

    let ordered_trigger = parse_one(
        "CREATE TRIGGER IF NOT EXISTS employees_forward BEFORE UPDATE ON employees \
         FOR EACH ROW FORWARD CROSSEDITION FOLLOWS employees_legacy, employees_audit \
         DISABLE BEGIN NULL; END;",
    );
    assert!(matches!(
        &ordered_trigger,
        Statement::OracleCreateTrigger(sqlparser::ast::OracleCreateTrigger {
            if_not_exists: true,
            crossedition: Some(sqlparser::ast::OracleTriggerCrossedition::Forward),
            ordering: Some(sqlparser::ast::OracleTriggerOrdering {
                kind: sqlparser::ast::OracleTriggerOrderingKind::Follows,
                triggers,
            }),
            enabled: Some(false),
            ..
        }) if triggers.len() == 2
    ));

    let call_trigger = parse_one(
        "CREATE TRIGGER employees_call AFTER INSERT ON employees \
         FOR EACH ROW CALL audit_employee_change(NEW.employee_id)",
    );
    assert!(matches!(
        &call_trigger,
        Statement::OracleCreateTrigger(sqlparser::ast::OracleCreateTrigger {
            body: sqlparser::ast::OracleTriggerBody::Call(Expr::Function(_)),
            ..
        })
    ));

    for statement in [
        dynamic_open,
        static_open,
        bound_open,
        collection_call,
        trigger,
        ordered_trigger,
        call_trigger,
    ] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}

#[test]
fn oracle_trigger_event_families_and_targets_are_typed() {
    let ddl = parse_one(
        "CREATE TRIGGER schema_ddl_events AFTER ANALYZE OR ASSOCIATE STATISTICS \
         OR DISASSOCIATE STATISTICS OR DDL ON SCHEMA BEGIN NULL; END;",
    );
    assert!(matches!(
        &ddl,
        Statement::OracleCreateTrigger(sqlparser::ast::OracleCreateTrigger {
            events,
            target: sqlparser::ast::OracleTriggerTarget::Schema,
            ..
        }) if matches!(
            events.as_slice(),
            [
                sqlparser::ast::OracleTriggerEvent::Analyze,
                sqlparser::ast::OracleTriggerEvent::AssociateStatistics,
                sqlparser::ast::OracleTriggerEvent::DisassociateStatistics,
                sqlparser::ast::OracleTriggerEvent::Ddl,
            ]
        )
    ));

    let database = parse_one(
        "CREATE TRIGGER pdb_events AFTER STARTUP OR DB_ROLE_CHANGE OR SERVERERROR \
         OR SET CONTAINER ON PLUGGABLE DATABASE BEGIN NULL; END;",
    );
    assert!(matches!(
        &database,
        Statement::OracleCreateTrigger(sqlparser::ast::OracleCreateTrigger {
            events,
            target: sqlparser::ast::OracleTriggerTarget::PluggableDatabase,
            ..
        }) if matches!(
            events.as_slice(),
            [
                sqlparser::ast::OracleTriggerEvent::Startup,
                sqlparser::ast::OracleTriggerEvent::DbRoleChange,
                sqlparser::ast::OracleTriggerEvent::ServerError,
                sqlparser::ast::OracleTriggerEvent::SetContainer,
            ]
        )
    ));

    let named_schema =
        parse_one("CREATE TRIGGER schema_logon AFTER LOGON ON hr.SCHEMA BEGIN NULL; END;");
    assert!(matches!(
        &named_schema,
        Statement::OracleCreateTrigger(sqlparser::ast::OracleCreateTrigger {
            target: sqlparser::ast::OracleTriggerTarget::NamedSchema(schema),
            ..
        }) if schema.value == "HR"
    ));

    for statement in [ddl, database, named_schema] {
        assert_eq!(parse_one(&statement.to_string()), statement);
    }
}
