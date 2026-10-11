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

use sqlparser::ast::{CreateTableOptions, SqlOption, Statement};

use super::common::parse_oracle;

fn default_collation(sql: &str) -> Option<String> {
    let statements = parse_oracle(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    let [Statement::CreateTable(create)] = statements.as_slice() else {
        panic!("{sql}: not one CREATE TABLE");
    };
    let CreateTableOptions::Plain(options) = &create.table_options else {
        return None;
    };
    options.iter().find_map(|option| match option {
        SqlOption::DefaultCollation(name) => Some(name.value.clone()),
        _ => None,
    })
}

#[test]
fn a_table_names_the_collation_its_character_columns_default_to() {
    assert_eq!(
        default_collation("CREATE TABLE t (c VARCHAR2(10), d VARCHAR2(5)) DEFAULT COLLATION BINARY_CI")
            .as_deref(),
        Some("BINARY_CI")
    );
    assert_eq!(
        default_collation("CREATE TABLE t (c VARCHAR2(10) COLLATE BINARY_AI) DEFAULT COLLATION USING_NLS_COMP")
            .as_deref(),
        Some("USING_NLS_COMP")
    );
    assert_eq!(default_collation("CREATE TABLE t (c VARCHAR2(10))"), None);
}

#[test]
fn the_default_collation_clause_prints_as_written() {
    let statements = parse_oracle("CREATE TABLE t (c VARCHAR2(10)) DEFAULT COLLATION BINARY_CI")
        .expect("the statement parses");
    let printed = statements[0].to_string();
    assert!(printed.ends_with("DEFAULT COLLATION BINARY_CI"), "{printed}");
    assert_eq!(
        default_collation(&printed).as_deref(),
        Some("BINARY_CI"),
        "{printed}"
    );
}

fn only(sql: &str) -> Statement {
    let mut statements = parse_oracle(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    assert_eq!(statements.len(), 1, "{sql}");
    statements.remove(0)
}

#[test]
fn a_view_names_the_collation_its_expression_columns_default_to() {
    let statement = only(
        "CREATE VIEW v DEFAULT COLLATION BINARY_AI AS SELECT 'abc' AS c FROM dual",
    );
    let Statement::CreateView(view) = &statement else {
        panic!("not a CREATE VIEW");
    };
    let collation = view
        .oracle
        .as_ref()
        .and_then(|oracle| oracle.default_collation.as_ref())
        .map(|name| name.value.as_str());
    assert_eq!(collation, Some("BINARY_AI"));
    assert_eq!(only(&statement.to_string()), statement);
    let plain = only("CREATE VIEW v AS SELECT 1 AS c FROM dual");
    let Statement::CreateView(view) = &plain else {
        panic!("not a CREATE VIEW");
    };
    assert!(view.oracle.is_none(), "{view:?}");
}

#[test]
fn a_table_alteration_changes_the_default_collation() {
    let statement = only("ALTER TABLE t DEFAULT COLLATION BINARY_CI");
    let Statement::AlterTable(alter) = &statement else {
        panic!("not an ALTER TABLE");
    };
    assert!(matches!(
        alter.operations.as_slice(),
        [sqlparser::ast::AlterTableOperation::OracleDefaultCollation { collation }]
            if collation.value == "BINARY_CI"
    ));
    assert_eq!(only(&statement.to_string()), statement);
}

#[test]
fn an_alter_session_sets_several_parameters_separated_by_white_space() {
    let statement = only("ALTER SESSION SET NLS_COMP = LINGUISTIC NLS_SORT = 'BINARY_AI'");
    let Statement::OracleAlter(alter) = &statement else {
        panic!("not an Oracle ALTER");
    };
    let sqlparser::ast::OracleAlterOperation::SetParameter { assignments, scope } =
        &alter.operation
    else {
        panic!("not a SET");
    };
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].parameter.to_string(), "NLS_COMP");
    assert_eq!(assignments[1].parameter.to_string(), "NLS_SORT");
    assert!(scope.is_none());
    assert_eq!(only(&statement.to_string()), statement);
    let one = only("ALTER SESSION SET NLS_SORT = BINARY_CI");
    assert_eq!(only(&one.to_string()), one);
    let system = only("ALTER SYSTEM SET open_cursors = 300 SCOPE = BOTH");
    assert_eq!(only(&system.to_string()), system);
}
