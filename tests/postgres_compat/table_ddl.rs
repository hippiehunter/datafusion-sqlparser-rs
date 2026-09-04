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

//! Tests for PostgreSQL table-shaped DDL: `CREATE TABLE` and its relatives,
//! `ALTER TABLE`, `ALTER FOREIGN TABLE`, foreign-data options, domains,
//! indexes, schemas, triggers and views.
//!
//! References:
//! - <https://www.postgresql.org/docs/current/sql-createtable.html>
//! - <https://www.postgresql.org/docs/current/sql-altertable.html>
//! - <https://www.postgresql.org/docs/current/sql-createforeigntable.html>
//! - <https://www.postgresql.org/docs/current/sql-alterforeigntable.html>
//! - <https://www.postgresql.org/docs/current/sql-createdomain.html>
//! - <https://www.postgresql.org/docs/current/sql-createindex.html>
//! - <https://www.postgresql.org/docs/current/sql-createschema.html>
//! - <https://www.postgresql.org/docs/current/sql-createview.html>

use crate::postgres_compat::common::try_parse_pg;
use sqlparser::ast::table_ddl::{
    ColumnCompression, ConstraintAttribute, ConstraintInheritability, IdentityColumnOption,
    SetAccessMethod, SetStatisticsValue, TableLikeOptionKind, TypedTableElement, ViewCheckOption,
};
use sqlparser::ast::{
    AlterColumnOperation, AlterForeignDataWrapperOperation, AlterForeignTableOperation,
    AlterTableOperation, ColumnOption, CreateTable, CreateTableLike, CreateTableLikeKind,
    CreateTableLikeOption, CreateTableOptions, CreateTableWithData, GeneratedAs, Ident, ObjectName,
    ReferentialAction, SequenceOptions, SetExpr, SqlMedOptionAction, SqlOption, Statement,
    TableConstraint, TableFactor, TriggerGroup, UserDefinedTypeStorage,
};

/// Parses `sql`, asserts that rendering the AST reproduces `sql` exactly, and
/// asserts that re-parsing the rendered text yields the same AST.
#[track_caller]
fn verified(sql: &str) -> Statement {
    let mut statements = try_parse_pg(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(statements.len(), 1, "expected one statement: {sql}");
    let statement = statements.pop().expect("one statement");
    assert_eq!(statement.to_string(), sql, "Display did not round-trip");
    let mut reparsed =
        try_parse_pg(&statement.to_string()).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(
        reparsed.pop().expect("one statement"),
        statement,
        "re-parsing the rendered text produced a different AST"
    );
    statement
}

/// Parses `sql` and asserts that it renders as `canonical`, which must itself
/// round-trip.
#[track_caller]
fn parses_to(sql: &str, canonical: &str) -> Statement {
    let mut statements = try_parse_pg(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(statements.len(), 1, "expected one statement: {sql}");
    let statement = statements.pop().expect("one statement");
    assert_eq!(statement.to_string(), canonical);
    assert_eq!(verified(canonical), statement);
    statement
}

#[track_caller]
fn alter_table_operations(sql: &str) -> Vec<AlterTableOperation> {
    match verified(sql) {
        Statement::AlterTable(alter) => alter.operations,
        other => panic!("expected ALTER TABLE, got {other:?}"),
    }
}

#[track_caller]
fn alter_column_op(sql: &str) -> AlterColumnOperation {
    match alter_table_operations(sql).remove(0) {
        AlterTableOperation::AlterColumn { op, .. } => op,
        other => panic!("expected ALTER COLUMN, got {other:?}"),
    }
}

#[track_caller]
fn sole_like(create: &CreateTable) -> &CreateTableLike {
    match create.like.as_ref() {
        Some(CreateTableLikeKind::Parenthesized(like)) => like,
        other => panic!("expected a parenthesized LIKE clause, got {other:?}"),
    }
}

#[track_caller]
fn create_table(sql: &str) -> CreateTable {
    match verified(sql) {
        Statement::CreateTable(create) => create,
        other => panic!("expected CREATE TABLE, got {other:?}"),
    }
}

// =============================================================================
// ALTER TABLE — relation-level actions
// =============================================================================

#[test]
fn alter_table_set_schema() {
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET SCHEMA other"),
        vec![AlterTableOperation::SetSchema {
            new_schema: ObjectName::from(vec![Ident::new("other")]),
        }]
    );
    verified("ALTER TABLE src.moved SET SCHEMA public");
    verified("ALTER TABLE IF EXISTS missing SET SCHEMA dest");
}

#[test]
fn alter_table_set_logged_and_unlogged() {
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET LOGGED"),
        vec![AlterTableOperation::SetLogged]
    );
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET UNLOGGED"),
        vec![AlterTableOperation::SetUnlogged]
    );
}

#[test]
fn alter_table_cluster_actions() {
    assert_eq!(
        alter_table_operations("ALTER TABLE t CLUSTER ON t_pkey"),
        vec![AlterTableOperation::ClusterOn {
            index_name: Ident::new("t_pkey"),
        }]
    );
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET WITHOUT CLUSTER"),
        vec![AlterTableOperation::SetWithoutCluster]
    );
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET WITHOUT OIDS"),
        vec![AlterTableOperation::SetWithoutOids]
    );
}

#[test]
fn alter_table_set_and_reset_storage_parameters() {
    let operations = alter_table_operations("ALTER TABLE t SET (toast.autovacuum_enabled = off)");
    match &operations[0] {
        AlterTableOperation::SetOptionsParens { options } => match &options[0] {
            SqlOption::Reloption(option) => {
                assert_eq!(option.name.to_string(), "toast.autovacuum_enabled");
                assert_eq!(
                    option.value.as_ref().map(ToString::to_string),
                    Some("off".into())
                );
            }
            other => panic!("expected a qualified relation option, got {other:?}"),
        },
        other => panic!("expected SET (...), got {other:?}"),
    }

    verified("ALTER TABLE t SET (fillfactor = 70)");
    let operations =
        alter_table_operations("ALTER TABLE t RESET (fillfactor, toast.autovacuum_enabled)");
    match &operations[0] {
        AlterTableOperation::ResetOptionsParens { options } => assert_eq!(options.len(), 2),
        other => panic!("expected RESET (...), got {other:?}"),
    }
}

#[test]
fn alter_table_set_access_method_and_tablespace() {
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET ACCESS METHOD heap"),
        vec![AlterTableOperation::SetAccessMethod {
            method: SetAccessMethod::Name(Ident::new("heap")),
        }]
    );
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET ACCESS METHOD DEFAULT"),
        vec![AlterTableOperation::SetAccessMethod {
            method: SetAccessMethod::Default,
        }]
    );
    assert_eq!(
        alter_table_operations("ALTER TABLE t SET TABLESPACE fast"),
        vec![AlterTableOperation::SetTablespace {
            name: Ident::new("fast"),
        }]
    );
}

#[test]
fn alter_table_inheritance_and_typed_table_actions() {
    match &alter_table_operations("ALTER TABLE child INHERIT parent")[0] {
        AlterTableOperation::Inherit { parent } => assert_eq!(parent.to_string(), "parent"),
        other => panic!("expected INHERIT, got {other:?}"),
    }
    match &alter_table_operations("ALTER TABLE child NO INHERIT parent")[0] {
        AlterTableOperation::NoInherit { parent } => assert_eq!(parent.to_string(), "parent"),
        other => panic!("expected NO INHERIT, got {other:?}"),
    }
    match &alter_table_operations("ALTER TABLE t OF person_type")[0] {
        AlterTableOperation::OfType { type_name } => {
            assert_eq!(type_name.to_string(), "person_type")
        }
        other => panic!("expected OF, got {other:?}"),
    }
    assert_eq!(
        alter_table_operations("ALTER TABLE t NOT OF"),
        vec![AlterTableOperation::NotOf]
    );
}

#[test]
fn alter_table_no_inherit_does_not_shadow_no_force_row_level_security() {
    assert_eq!(
        alter_table_operations("ALTER TABLE t NO FORCE ROW LEVEL SECURITY"),
        vec![AlterTableOperation::NoForceRowLevelSecurity]
    );
}

#[test]
fn alter_table_enable_and_disable_trigger_groups() {
    assert_eq!(
        alter_table_operations("ALTER TABLE t ENABLE TRIGGER ALL"),
        vec![AlterTableOperation::EnableTriggerGroup {
            group: TriggerGroup::All,
        }]
    );
    assert_eq!(
        alter_table_operations("ALTER TABLE t ENABLE TRIGGER USER"),
        vec![AlterTableOperation::EnableTriggerGroup {
            group: TriggerGroup::User,
        }]
    );
    assert_eq!(
        alter_table_operations("ALTER TABLE t DISABLE TRIGGER ALL"),
        vec![AlterTableOperation::DisableTriggerGroup {
            group: TriggerGroup::All,
        }]
    );
    // A trigger name is still a name, not a group.
    match &alter_table_operations("ALTER TABLE t DISABLE TRIGGER tg")[0] {
        AlterTableOperation::DisableTrigger { name } => assert_eq!(name.value, "tg"),
        other => panic!("expected DISABLE TRIGGER <name>, got {other:?}"),
    }
}

#[test]
fn alter_table_only_and_descendants_prefixes() {
    match verified("ALTER TABLE ONLY t ADD COLUMN a INT") {
        Statement::AlterTable(alter) => {
            assert!(alter.only);
            assert!(!alter.descendants);
        }
        other => panic!("expected ALTER TABLE, got {other:?}"),
    }
    match verified("ALTER TABLE t * ADD COLUMN a INT") {
        Statement::AlterTable(alter) => {
            assert!(!alter.only);
            assert!(alter.descendants);
        }
        other => panic!("expected ALTER TABLE, got {other:?}"),
    }
    verified("ALTER TABLE IF EXISTS ONLY t DROP COLUMN a");
}

#[test]
fn alter_table_alter_constraint() {
    match &alter_table_operations("ALTER TABLE t ALTER CONSTRAINT c DEFERRABLE INITIALLY DEFERRED")
        [0]
    {
        AlterTableOperation::AlterConstraint(alter) => {
            assert_eq!(alter.name.value, "c");
            let characteristics = alter.characteristics.as_ref().expect("characteristics");
            assert_eq!(characteristics.deferrable, Some(true));
            assert!(alter.inheritability.is_none());
        }
        other => panic!("expected ALTER CONSTRAINT, got {other:?}"),
    }
    verified("ALTER TABLE t ALTER CONSTRAINT c NOT DEFERRABLE");
    verified("ALTER TABLE t ALTER CONSTRAINT c DEFERRABLE INITIALLY IMMEDIATE");
    verified("ALTER TABLE t ALTER CONSTRAINT c ENFORCED");
    verified("ALTER TABLE t ALTER CONSTRAINT c NOT ENFORCED");
    match &alter_table_operations("ALTER TABLE t ALTER CONSTRAINT c INHERIT")[0] {
        AlterTableOperation::AlterConstraint(alter) => {
            assert_eq!(
                alter.inheritability,
                Some(ConstraintInheritability::Inherit)
            );
        }
        other => panic!("expected ALTER CONSTRAINT, got {other:?}"),
    }
    match &alter_table_operations("ALTER TABLE t ALTER CONSTRAINT c NO INHERIT")[0] {
        AlterTableOperation::AlterConstraint(alter) => {
            assert_eq!(
                alter.inheritability,
                Some(ConstraintInheritability::NoInherit)
            );
        }
        other => panic!("expected ALTER CONSTRAINT, got {other:?}"),
    }
}

#[test]
fn alter_table_generic_options() {
    match &alter_table_operations("ALTER TABLE t OPTIONS (a '1', SET b '2', DROP c)")[0] {
        AlterTableOperation::Options { options } => {
            assert_eq!(options.len(), 3);
            assert!(matches!(options[0], SqlMedOptionAction::Implicit { .. }));
            assert!(matches!(options[1], SqlMedOptionAction::Set { .. }));
            assert!(matches!(options[2], SqlMedOptionAction::Drop { .. }));
        }
        other => panic!("expected OPTIONS (...), got {other:?}"),
    }
}

#[test]
fn alter_table_multiple_actions() {
    let operations = alter_table_operations("ALTER TABLE t ADD COLUMN b INT, ADD NOT NULL b");
    assert_eq!(operations.len(), 2);
    assert!(matches!(
        operations[0],
        AlterTableOperation::AddColumn { .. }
    ));
    match &operations[1] {
        AlterTableOperation::AddConstraint { constraint, .. } => match constraint {
            TableConstraint::NotNull(not_null) => {
                assert_eq!(not_null.column.value, "b");
                assert!(not_null.name.is_none());
            }
            other => panic!("expected NOT NULL constraint, got {other:?}"),
        },
        other => panic!("expected ADD NOT NULL, got {other:?}"),
    }
}

#[test]
fn alter_table_all_in_tablespace() {
    match verified("ALTER TABLE ALL IN TABLESPACE old SET TABLESPACE new") {
        Statement::AlterTableAllInTablespace(alter) => {
            assert_eq!(alter.tablespace.value, "old");
            assert_eq!(alter.new_tablespace.value, "new");
            assert!(alter.owned_by.is_empty());
            assert!(!alter.nowait);
        }
        other => panic!("expected ALTER TABLE ALL IN TABLESPACE, got {other:?}"),
    }
    match verified(
        "ALTER TABLE ALL IN TABLESPACE old OWNED BY alice, CURRENT_USER SET TABLESPACE new NOWAIT",
    ) {
        Statement::AlterTableAllInTablespace(alter) => {
            assert_eq!(alter.owned_by.len(), 2);
            assert!(alter.nowait);
        }
        other => panic!("expected ALTER TABLE ALL IN TABLESPACE, got {other:?}"),
    }
    // A table may still be named `all`.
    verified("ALTER TABLE all_things ADD COLUMN a INT");
}

// =============================================================================
// ALTER TABLE — column actions
// =============================================================================

#[test]
fn alter_column_set_statistics() {
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a SET STATISTICS 1000"),
        AlterColumnOperation::SetStatistics {
            value: SetStatisticsValue::Value(1000),
        }
    );
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a SET STATISTICS -1"),
        AlterColumnOperation::SetStatistics {
            value: SetStatisticsValue::Value(-1),
        }
    );
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a SET STATISTICS DEFAULT"),
        AlterColumnOperation::SetStatistics {
            value: SetStatisticsValue::Default,
        }
    );
    // The COLUMN keyword is optional.
    parses_to(
        "ALTER TABLE t ALTER a SET STATISTICS 0",
        "ALTER TABLE t ALTER COLUMN a SET STATISTICS 0",
    );
}

#[test]
fn alter_column_set_storage() {
    for (sql, expected) in [
        ("PLAIN", UserDefinedTypeStorage::Plain),
        ("EXTERNAL", UserDefinedTypeStorage::External),
        ("EXTENDED", UserDefinedTypeStorage::Extended),
        ("MAIN", UserDefinedTypeStorage::Main),
        ("DEFAULT", UserDefinedTypeStorage::Default),
    ] {
        let statement = format!("ALTER TABLE t ALTER COLUMN a SET STORAGE {expected}");
        assert_eq!(
            alter_column_op(&statement),
            AlterColumnOperation::SetStorage(expected),
            "{sql}"
        );
    }
}

#[test]
fn alter_column_set_compression() {
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a SET COMPRESSION pglz"),
        AlterColumnOperation::SetCompression {
            compression: ColumnCompression::Method(Ident::new("pglz")),
        }
    );
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a SET COMPRESSION DEFAULT"),
        AlterColumnOperation::SetCompression {
            compression: ColumnCompression::Default,
        }
    );
}

#[test]
fn alter_column_attribute_options() {
    match alter_column_op(
        "ALTER TABLE t ALTER COLUMN a SET (n_distinct = 1, n_distinct_inherited = 2)",
    ) {
        AlterColumnOperation::SetOptionsParens { options } => assert_eq!(options.len(), 2),
        other => panic!("expected SET (...), got {other:?}"),
    }
    match alter_column_op("ALTER TABLE t ALTER COLUMN a RESET (n_distinct_inherited)") {
        AlterColumnOperation::ResetOptionsParens { options } => assert_eq!(options.len(), 1),
        other => panic!("expected RESET (...), got {other:?}"),
    }
    match alter_column_op("ALTER TABLE t ALTER COLUMN a OPTIONS (ADD x 'y', DROP z)") {
        AlterColumnOperation::Options { options } => assert_eq!(options.len(), 2),
        other => panic!("expected OPTIONS (...), got {other:?}"),
    }
}

#[test]
fn alter_column_expression_actions() {
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a DROP EXPRESSION"),
        AlterColumnOperation::DropExpression { if_exists: false }
    );
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a DROP EXPRESSION IF EXISTS"),
        AlterColumnOperation::DropExpression { if_exists: true }
    );
    match alter_column_op("ALTER TABLE t ALTER COLUMN b SET EXPRESSION AS (a * 100)") {
        AlterColumnOperation::SetExpression { expr } => assert_eq!(expr.to_string(), "a * 100"),
        other => panic!("expected SET EXPRESSION, got {other:?}"),
    }
}

#[test]
fn alter_column_identity_actions() {
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a DROP IDENTITY"),
        AlterColumnOperation::DropIdentity { if_exists: false }
    );
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a DROP IDENTITY IF EXISTS"),
        AlterColumnOperation::DropIdentity { if_exists: true }
    );
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a RESTART"),
        AlterColumnOperation::IdentityOptions {
            options: vec![IdentityColumnOption::Restart {
                with: false,
                value: None,
            }],
        }
    );
    match alter_column_op("ALTER TABLE t ALTER COLUMN a RESTART WITH 50") {
        AlterColumnOperation::IdentityOptions { options } => match &options[0] {
            IdentityColumnOption::Restart { with, value } => {
                assert!(*with);
                assert_eq!(value.as_ref().map(ToString::to_string), Some("50".into()));
            }
            other => panic!("expected RESTART, got {other:?}"),
        },
        other => panic!("expected identity options, got {other:?}"),
    }
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a SET GENERATED BY DEFAULT"),
        AlterColumnOperation::IdentityOptions {
            options: vec![IdentityColumnOption::SetGenerated(GeneratedAs::ByDefault)],
        }
    );
    assert_eq!(
        alter_column_op("ALTER TABLE t ALTER COLUMN a SET GENERATED ALWAYS"),
        AlterColumnOperation::IdentityOptions {
            options: vec![IdentityColumnOption::SetGenerated(GeneratedAs::Always)],
        }
    );
}

#[test]
fn alter_column_several_identity_options_in_a_row() {
    match alter_column_op(
        "ALTER TABLE t ALTER COLUMN a SET INCREMENT BY 2 SET START WITH 100 RESTART",
    ) {
        AlterColumnOperation::IdentityOptions { options } => {
            assert_eq!(options.len(), 3);
            assert!(matches!(
                options[0],
                IdentityColumnOption::SetSequenceOption(SequenceOptions::IncrementBy(_, true))
            ));
            assert!(matches!(
                options[1],
                IdentityColumnOption::SetSequenceOption(SequenceOptions::StartWith(_, true))
            ));
            assert!(matches!(
                options[2],
                IdentityColumnOption::Restart {
                    with: false,
                    value: None
                }
            ));
        }
        other => panic!("expected identity options, got {other:?}"),
    }
    verified("ALTER TABLE t ALTER COLUMN a SET MINVALUE 1 SET NO MAXVALUE SET CYCLE");
}

#[test]
fn alter_column_add_generated_as_identity() {
    match alter_column_op(
        "ALTER TABLE t ALTER COLUMN a ADD GENERATED BY DEFAULT AS IDENTITY ( START WITH 5 )",
    ) {
        AlterColumnOperation::AddGenerated {
            generated_as,
            sequence_options,
        } => {
            assert_eq!(generated_as, Some(GeneratedAs::ByDefault));
            assert_eq!(sequence_options.map(|o| o.len()), Some(1));
        }
        other => panic!("expected ADD GENERATED, got {other:?}"),
    }
}

#[test]
fn alter_column_set_data_type_with_collation_and_using() {
    match alter_column_op("ALTER TABLE t ALTER COLUMN id SET DATA TYPE INT COLLATE \"C\"") {
        AlterColumnOperation::SetDataTypeCollate {
            collation,
            using,
            had_set,
            ..
        } => {
            assert_eq!(collation.to_string(), "\"C\"");
            assert!(using.is_none());
            assert!(had_set);
        }
        other => panic!("expected SET DATA TYPE ... COLLATE, got {other:?}"),
    }
    match alter_column_op("ALTER TABLE t ALTER COLUMN id TYPE TEXT COLLATE \"C\" USING id::TEXT") {
        AlterColumnOperation::SetDataTypeCollate { using, had_set, .. } => {
            assert_eq!(using.map(|u| u.to_string()), Some("id::TEXT".into()));
            assert!(!had_set);
        }
        other => panic!("expected TYPE ... COLLATE, got {other:?}"),
    }
    // Without COLLATE the existing representation is kept.
    assert!(matches!(
        alter_column_op("ALTER TABLE t ALTER COLUMN id SET DATA TYPE INT"),
        AlterColumnOperation::SetDataType { .. }
    ));
}

// =============================================================================
// ALTER TABLE — constraints
// =============================================================================

#[test]
fn alter_table_add_not_null_constraint() {
    match &alter_table_operations("ALTER TABLE t ADD CONSTRAINT c NOT NULL a")[0] {
        AlterTableOperation::AddConstraint {
            constraint: TableConstraint::NotNull(not_null),
            not_valid,
        } => {
            assert_eq!(
                not_null.name.as_ref().map(|n| n.value.clone()),
                Some("c".into())
            );
            assert_eq!(not_null.column.value, "a");
            assert!(!not_null.no_inherit);
            assert!(!not_valid);
        }
        other => panic!("expected ADD CONSTRAINT ... NOT NULL, got {other:?}"),
    }
    match &alter_table_operations("ALTER TABLE t ADD CONSTRAINT c NOT NULL a NO INHERIT")[0] {
        AlterTableOperation::AddConstraint {
            constraint: TableConstraint::NotNull(not_null),
            ..
        } => assert!(not_null.no_inherit),
        other => panic!("expected NO INHERIT, got {other:?}"),
    }
    match &alter_table_operations("ALTER TABLE t ADD CONSTRAINT c NOT NULL a NOT VALID")[0] {
        AlterTableOperation::AddConstraint { not_valid, .. } => assert!(not_valid),
        other => panic!("expected NOT VALID, got {other:?}"),
    }
}

#[test]
fn alter_table_add_check_constraint_with_no_inherit() {
    match &alter_table_operations("ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) NO INHERIT")[0] {
        AlterTableOperation::AddConstraint {
            constraint: TableConstraint::Check(check),
            not_valid,
        } => {
            assert!(check.no_inherit);
            assert!(!not_valid);
        }
        other => panic!("expected ADD CHECK, got {other:?}"),
    }
    match &alter_table_operations(
        "ALTER TABLE t ADD CONSTRAINT c CHECK (a = 2) NO INHERIT NOT VALID",
    )[0]
    {
        AlterTableOperation::AddConstraint {
            constraint: TableConstraint::Check(check),
            not_valid,
        } => {
            assert!(check.no_inherit);
            assert!(not_valid);
        }
        other => panic!("expected ADD CHECK, got {other:?}"),
    }
}

#[test]
fn alter_table_add_constraint_using_existing_index() {
    match &alter_table_operations("ALTER TABLE t ADD PRIMARY KEY USING INDEX i")[0] {
        AlterTableOperation::AddConstraint {
            constraint: TableConstraint::PrimaryKey(pk),
            ..
        } => {
            let details = pk.index_details.as_ref().expect("index details");
            assert_eq!(
                details.using_index.as_ref().map(|i| i.value.clone()),
                Some("i".into())
            );
            assert!(pk.columns.is_empty());
        }
        other => panic!("expected ADD PRIMARY KEY USING INDEX, got {other:?}"),
    }
    verified("ALTER TABLE t ADD CONSTRAINT c UNIQUE USING INDEX i");
    verified(
        "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY USING INDEX i DEFERRABLE INITIALLY DEFERRED",
    );
}

#[test]
fn alter_table_add_constraint_with_include_and_index_options() {
    match &alter_table_operations("ALTER TABLE t ADD UNIQUE (c1, c2) INCLUDE (c3, c4)")[0] {
        AlterTableOperation::AddConstraint {
            constraint: TableConstraint::Unique(unique),
            ..
        } => {
            let details = unique.index_details.as_ref().expect("index details");
            assert_eq!(details.include.len(), 2);
        }
        other => panic!("expected ADD UNIQUE, got {other:?}"),
    }
    verified("ALTER TABLE t ADD PRIMARY KEY (c1, c2) INCLUDE (c3, c4)");
    verified("ALTER TABLE t ADD UNIQUE (a) WITH (fillfactor = 70) USING INDEX TABLESPACE fast");
}

#[test]
fn alter_table_add_column_with_named_check_and_enforcement() {
    let operations = alter_table_operations(
        "ALTER TABLE t ADD COLUMN y INT CONSTRAINT c CHECK (y > 0) NOT ENFORCED",
    );
    match &operations[0] {
        AlterTableOperation::AddColumn { column_def, .. } => {
            assert_eq!(column_def.options.len(), 2);
            assert_eq!(
                column_def.options[0].name.as_ref().map(|n| n.value.clone()),
                Some("c".into())
            );
            assert!(matches!(
                column_def.options[0].option,
                ColumnOption::Check(_)
            ));
            assert_eq!(
                column_def.options[1].option,
                ColumnOption::ConstraintAttribute(ConstraintAttribute::NotEnforced)
            );
        }
        other => panic!("expected ADD COLUMN, got {other:?}"),
    }
    // PostgreSQL parses each constraint attribute as its own qualifier, so a
    // contradictory pair is still syntactically valid.
    verified("ALTER TABLE t ADD COLUMN z INT CHECK (x > 0) NOT ENFORCED ENFORCED");
}

// =============================================================================
// CREATE TABLE
// =============================================================================

#[test]
fn create_table_like_clause_positions() {
    let create = create_table("CREATE TABLE copy (LIKE src, c INT)");
    assert_eq!(create.like_elements.len(), 1);
    assert_eq!(create.like_elements[0].after_columns, 0);
    assert_eq!(create.columns.len(), 1);

    let create = create_table("CREATE TABLE copy (x TEXT, LIKE src INCLUDING CONSTRAINTS, y TEXT)");
    assert_eq!(create.like_elements[0].after_columns, 1);
    assert_eq!(
        create.like_elements[0].source.options,
        vec![CreateTableLikeOption::IncludingConstraints]
    );

    let create = create_table(
        "CREATE TABLE copy (LIKE src1 INCLUDING STORAGE, LIKE src2 INCLUDING STORAGE)",
    );
    assert_eq!(create.like_elements.len(), 2);

    verified("CREATE TABLE copy (LIKE src, LIKE src)");
    verified("CREATE TABLE child (LIKE src, b INT) INHERITS (parent)");
}

#[test]
fn create_table_like_option_kinds() {
    // A sole `LIKE` element keeps the pre-existing parenthesized representation.
    for kind in [
        TableLikeOptionKind::Comments,
        TableLikeOptionKind::Compression,
        TableLikeOptionKind::Generated,
        TableLikeOptionKind::Identity,
        TableLikeOptionKind::Indexes,
        TableLikeOptionKind::Statistics,
        TableLikeOptionKind::Storage,
        TableLikeOptionKind::All,
    ] {
        let create = create_table(&format!("CREATE TABLE copy (LIKE src INCLUDING {kind})"));
        assert_eq!(
            sole_like(&create).options,
            vec![CreateTableLikeOption::Including(kind)]
        );
    }
    let create = create_table("CREATE TABLE copy (LIKE src INCLUDING ALL EXCLUDING INDEXES)");
    assert_eq!(
        sole_like(&create).options,
        vec![
            CreateTableLikeOption::Including(TableLikeOptionKind::All),
            CreateTableLikeOption::Excluding(TableLikeOptionKind::Indexes),
        ]
    );
    verified("CREATE TABLE copy (LIKE src INCLUDING DEFAULTS INCLUDING GENERATED)");
}

#[test]
fn create_table_not_null_constraint_forms() {
    let create = create_table("CREATE TABLE t (a INT, b INT, CONSTRAINT c NOT NULL a)");
    match &create.constraints[0] {
        TableConstraint::NotNull(not_null) => {
            assert_eq!(not_null.column.value, "a");
            assert!(!not_null.no_inherit);
        }
        other => panic!("expected NOT NULL constraint, got {other:?}"),
    }
    let create = create_table("CREATE TABLE t (a INT, CONSTRAINT c NOT NULL a NO INHERIT)");
    match &create.constraints[0] {
        TableConstraint::NotNull(not_null) => assert!(not_null.no_inherit),
        other => panic!("expected NOT NULL constraint, got {other:?}"),
    }
    let create = create_table("CREATE TABLE t (f1 INT CONSTRAINT c NOT NULL NO INHERIT)");
    assert_eq!(
        create.columns[0].options[0].option,
        ColumnOption::NotNullNoInherit
    );
    verified("CREATE TABLE t (f1 INT NOT NULL NO INHERIT, f2 INT)");
}

#[test]
fn create_table_check_constraint_no_inherit() {
    let create = create_table("CREATE TABLE t (f1 INT, CHECK (f1 < 10) NO INHERIT)");
    match &create.constraints[0] {
        TableConstraint::Check(check) => assert!(check.no_inherit),
        other => panic!("expected CHECK constraint, got {other:?}"),
    }
    let create = create_table("CREATE TABLE t (t INT CHECK (t > 0) NO INHERIT)");
    match &create.columns[0].options[0].option {
        ColumnOption::Check(check) => assert!(check.no_inherit),
        other => panic!("expected a column CHECK, got {other:?}"),
    }
}

#[test]
fn create_table_unique_and_primary_key_include() {
    let create = create_table(
        "CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, UNIQUE (c1, c2) INCLUDE (c3, c4))",
    );
    match &create.constraints[0] {
        TableConstraint::Unique(unique) => {
            let details = unique.index_details.as_ref().expect("index details");
            assert_eq!(details.include.len(), 2);
        }
        other => panic!("expected UNIQUE constraint, got {other:?}"),
    }
    verified("CREATE TABLE t (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c2) INCLUDE (c3))");
    verified(
        "CREATE TABLE t (c1 INT, c2 INT, c3 INT, CONSTRAINT covering UNIQUE (c1, c2) INCLUDE (c3))",
    );
}

#[test]
fn create_table_unique_without_overlaps() {
    let create = create_table(
        "CREATE TABLE t (id int4range, valid_at daterange, CONSTRAINT c UNIQUE (id, valid_at WITHOUT OVERLAPS))",
    );
    match &create.constraints[0] {
        TableConstraint::Unique(unique) => {
            assert_eq!(
                unique
                    .period_without_overlaps
                    .as_ref()
                    .map(|i| i.value.clone()),
                Some("valid_at".into())
            );
            assert_eq!(unique.columns.len(), 1);
        }
        other => panic!("expected UNIQUE constraint, got {other:?}"),
    }
    verified(
        "CREATE TABLE t (id int4range, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))",
    );
}

#[test]
fn create_table_exclude_constraint_characteristics() {
    let create = create_table(
        "CREATE TABLE t (f1 INT, f2 INT, CONSTRAINT c EXCLUDE (f1 WITH =) INITIALLY DEFERRED)",
    );
    match &create.constraints[0] {
        TableConstraint::Exclude(exclude) => {
            let characteristics = exclude.characteristics.as_ref().expect("characteristics");
            assert!(characteristics.initially.is_some());
        }
        other => panic!("expected EXCLUDE constraint, got {other:?}"),
    }
    verified("CREATE TABLE t (a INT, EXCLUDE USING gist (a WITH =) WHERE (a > 0))");
    verified(
        "CREATE TABLE t (a INT, b INT, EXCLUDE (a WITH =) INCLUDE (b) WITH (fillfactor = 70) USING INDEX TABLESPACE fast DEFERRABLE)",
    );
}

#[test]
fn create_table_foreign_key_on_delete_column_list() {
    let create = create_table(
        "CREATE TABLE c (tid INT, fk_id INT, FOREIGN KEY (tid, fk_id) REFERENCES p ON DELETE SET NULL (fk_id))",
    );
    match &create.constraints[0] {
        TableConstraint::ForeignKey(fk) => {
            assert_eq!(fk.on_delete, Some(ReferentialAction::SetNull));
            assert_eq!(fk.on_delete_columns.len(), 1);
            assert_eq!(fk.on_delete_columns[0].value, "fk_id");
        }
        other => panic!("expected FOREIGN KEY constraint, got {other:?}"),
    }
    verified("CREATE TABLE c (a INT, FOREIGN KEY (a) REFERENCES p ON DELETE SET DEFAULT (a))");
}

#[test]
fn create_table_column_storage_and_compression() {
    let create = create_table("CREATE TABLE t (a TEXT, c TEXT STORAGE plain)");
    assert_eq!(
        create.columns[1].options[0].option,
        ColumnOption::Storage(UserDefinedTypeStorage::Plain)
    );
    let create = create_table("CREATE TABLE t (a TEXT COMPRESSION lz4 NOT NULL)");
    assert_eq!(
        create.columns[0].options[0].option,
        ColumnOption::Compression(ColumnCompression::Method(Ident::new("lz4")))
    );
    verified("CREATE TABLE t (a TEXT STORAGE main COMPRESSION pglz)");
}

#[test]
fn create_table_generated_column_defaults_to_no_storage_keyword() {
    let create =
        create_table("CREATE TABLE t (a INT PRIMARY KEY, b INT GENERATED ALWAYS AS (a * 2))");
    match &create.columns[1].options[0].option {
        ColumnOption::Generated {
            generation_expr,
            generation_expr_mode,
            ..
        } => {
            assert!(generation_expr.is_some());
            assert!(generation_expr_mode.is_none());
        }
        other => panic!("expected a generated column, got {other:?}"),
    }
    verified("CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a) STORED)");
    verified("CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a) VIRTUAL)");
}

#[test]
fn create_table_relation_options_without_values() {
    let create = create_table("CREATE TABLE t () WITH (oids)");
    match &create.table_options {
        CreateTableOptions::With(options) => match &options[0] {
            SqlOption::Reloption(option) => {
                assert_eq!(option.name.to_string(), "oids");
                assert!(option.value.is_none());
            }
            other => panic!("expected a valueless relation option, got {other:?}"),
        },
        other => panic!("expected WITH options, got {other:?}"),
    }
    assert!(create_table("CREATE TABLE t (a INT) WITHOUT OIDS").without_oids);
    verified("CREATE TABLE t (a INT) WITH (fillfactor = 70, toast.autovacuum_enabled = off)");
}

#[test]
fn create_table_as_query_forms() {
    let create = create_table("CREATE TABLE t (z) AS SELECT a FROM src");
    assert_eq!(create.column_aliases.len(), 1);
    assert_eq!(create.column_aliases[0].value, "z");
    assert!(create.query.is_some());

    assert_eq!(
        create_table("CREATE TABLE t AS SELECT a FROM src WITH DATA").with_data,
        Some(CreateTableWithData::WithData)
    );
    assert_eq!(
        create_table("CREATE TABLE t AS SELECT a FROM src WITH NO DATA").with_data,
        Some(CreateTableWithData::WithNoData)
    );
    parses_to(
        "CREATE TEMP TABLE t (col) ON COMMIT DELETE ROWS AS SELECT 1",
        "CREATE TEMPORARY TABLE t (col) ON COMMIT DELETE ROWS AS SELECT 1",
    );
    parses_to(
        "CREATE TEMP TABLE t (col) ON COMMIT DROP AS SELECT 1",
        "CREATE TEMPORARY TABLE t (col) ON COMMIT DROP AS SELECT 1",
    );
}

#[test]
fn create_table_as_execute() {
    let create = create_table("CREATE TABLE t AS EXECUTE p");
    let execute = create.execute.expect("EXECUTE");
    assert_eq!(execute.name.value, "p");
    assert!(execute.parameters.is_empty());

    let create = create_table("CREATE TABLE t AS EXECUTE p(1, 'x') WITH DATA");
    let execute = create.execute.expect("EXECUTE");
    assert_eq!(execute.parameters.len(), 2);
    assert_eq!(create.with_data, Some(CreateTableWithData::WithData));
}

#[test]
fn create_unlogged_table_and_sequence() {
    assert!(create_table("CREATE UNLOGGED TABLE t (a INT PRIMARY KEY)").unlogged);
    verified("CREATE UNLOGGED TABLE public.t (a INT PRIMARY KEY)");
    verified("CREATE UNLOGGED TABLE pg_temp.t (a INT PRIMARY KEY)");
    match verified("CREATE UNLOGGED SEQUENCE s") {
        Statement::CreateSequence { unlogged, .. } => assert!(unlogged),
        other => panic!("expected CREATE SEQUENCE, got {other:?}"),
    }
}

#[test]
fn create_typed_table_with_element_list() {
    let statement = verified(
        "CREATE TABLE persons OF person_type (id WITH OPTIONS PRIMARY KEY, UNIQUE (name))",
    );
    match statement {
        Statement::CreateTypedTable(create) => {
            assert_eq!(create.of_type.to_string(), "person_type");
            assert_eq!(create.elements.len(), 2);
            match &create.elements[0] {
                TypedTableElement::Column(column) => {
                    assert_eq!(column.name.value, "id");
                    assert!(column.with_options);
                    assert_eq!(column.options.len(), 1);
                }
                other => panic!("expected a typed-table column, got {other:?}"),
            }
            assert!(matches!(
                create.elements[1],
                TypedTableElement::Constraint(_)
            ));
        }
        other => panic!("expected CREATE TABLE ... OF, got {other:?}"),
    }
    verified(
        "CREATE TABLE persons OF person_type (PRIMARY KEY (id), name WITH OPTIONS DEFAULT '')",
    );
    verified("CREATE TABLE persons OF person_type");
    verified("CREATE UNLOGGED TABLE persons OF person_type (a WITH OPTIONS NOT NULL) PARTITION BY RANGE (a)");
    verified("CREATE TABLE persons OF person_type (PRIMARY KEY (id)) WITH (fillfactor = 70) ON COMMIT DROP TABLESPACE fast");
}

// =============================================================================
// CREATE / ALTER FOREIGN TABLE and foreign-data options
// =============================================================================

#[test]
fn create_foreign_table_inherits_and_column_options() {
    match verified("CREATE FOREIGN TABLE child () INHERITS (parent) SERVER s") {
        Statement::CreateForeignTable(create) => {
            assert_eq!(create.inherits.len(), 1);
            assert!(create.columns.is_empty());
        }
        other => panic!("expected CREATE FOREIGN TABLE, got {other:?}"),
    }
    match verified(
        "CREATE FOREIGN TABLE t (c1 INTEGER OPTIONS (column_name 'a1') NOT NULL, c2 TEXT) SERVER s",
    ) {
        Statement::CreateForeignTable(create) => {
            assert_eq!(create.columns[0].options.len(), 2);
            match &create.columns[0].options[0].option {
                ColumnOption::GenericOptions(options) => {
                    assert_eq!(options.len(), 1);
                    assert!(matches!(options[0], SqlMedOptionAction::Implicit { .. }));
                }
                other => panic!("expected column OPTIONS, got {other:?}"),
            }
        }
        other => panic!("expected CREATE FOREIGN TABLE, got {other:?}"),
    }
}

#[test]
fn alter_foreign_table_shares_the_alter_table_actions() {
    match verified("ALTER FOREIGN TABLE t INHERIT parent") {
        Statement::AlterForeignTable(alter) => assert!(matches!(
            alter.operations[0],
            AlterForeignTableOperation::TableOperation(AlterTableOperation::Inherit { .. })
        )),
        other => panic!("expected ALTER FOREIGN TABLE, got {other:?}"),
    }
    verified("ALTER FOREIGN TABLE t NO INHERIT parent");
    verified("ALTER FOREIGN TABLE t ENABLE TRIGGER ALL");
    verified("ALTER FOREIGN TABLE t DISABLE TRIGGER ALL");
    verified("ALTER FOREIGN TABLE t ENABLE TRIGGER USER");
    verified("ALTER FOREIGN TABLE t ADD CONSTRAINT c CHECK (c1 > 0) NOT VALID");
    verified("ALTER FOREIGN TABLE t DROP CONSTRAINT c");
    verified("ALTER FOREIGN TABLE t DROP CONSTRAINT IF EXISTS c");
    verified("ALTER FOREIGN TABLE t VALIDATE CONSTRAINT c");
    verified("ALTER FOREIGN TABLE ONLY t ALTER COLUMN a OPTIONS (SET x 'y')");
    verified("ALTER FOREIGN TABLE t ALTER COLUMN c1 SET STATISTICS 10");
    parses_to(
        "ALTER FOREIGN TABLE t RENAME c1 TO cc1",
        "ALTER FOREIGN TABLE t RENAME COLUMN c1 TO cc1",
    );
    parses_to(
        "ALTER FOREIGN TABLE t ALTER COLUMN c2 TYPE VARCHAR(10)",
        "ALTER FOREIGN TABLE t ALTER COLUMN c2 SET DATA TYPE VARCHAR(10)",
    );
}

#[test]
fn alter_foreign_table_keeps_its_own_operation_shapes() {
    match verified("ALTER FOREIGN TABLE t SET SCHEMA other") {
        Statement::AlterForeignTable(alter) => assert!(matches!(
            alter.operations[0],
            AlterForeignTableOperation::SetSchema(_)
        )),
        other => panic!("expected ALTER FOREIGN TABLE, got {other:?}"),
    }
    match verified("ALTER FOREIGN TABLE t RENAME TO u") {
        Statement::AlterForeignTable(alter) => assert!(matches!(
            alter.operations[0],
            AlterForeignTableOperation::RenameTo(_)
        )),
        other => panic!("expected ALTER FOREIGN TABLE, got {other:?}"),
    }
    match verified("ALTER FOREIGN TABLE t OWNER TO r") {
        Statement::AlterForeignTable(alter) => assert!(matches!(
            alter.operations[0],
            AlterForeignTableOperation::OwnerTo(_)
        )),
        other => panic!("expected ALTER FOREIGN TABLE, got {other:?}"),
    }
    verified("ALTER FOREIGN TABLE IF EXISTS t ADD COLUMN a INT");
    verified("ALTER FOREIGN TABLE t DROP COLUMN a");
}

#[test]
fn foreign_data_options_accept_an_implicit_add() {
    match verified("ALTER FOREIGN DATA WRAPPER w OPTIONS (a '1', b '2')") {
        Statement::AlterForeignDataWrapper(alter) => match &alter.operations[0] {
            AlterForeignDataWrapperOperation::Options(options) => {
                assert_eq!(options.len(), 2);
                assert!(options
                    .iter()
                    .all(|o| matches!(o, SqlMedOptionAction::Implicit { .. })));
            }
            other => panic!("expected OPTIONS, got {other:?}"),
        },
        other => panic!("expected ALTER FOREIGN DATA WRAPPER, got {other:?}"),
    }
    verified(
        "ALTER FOREIGN DATA WRAPPER w HANDLER h VALIDATOR v OPTIONS (a '1', SET b '2', DROP c)",
    );
    verified("ALTER FOREIGN DATA WRAPPER w NO HANDLER NO VALIDATOR");
    verified("ALTER FOREIGN DATA WRAPPER w RENAME TO x");
    verified("ALTER SERVER s OPTIONS (connect_timeout '30', SET dbname 'db1', DROP host)");
    verified("ALTER SERVER s VERSION '1.0' OPTIONS (servername 'sv')");
    verified("ALTER SERVER s OWNER TO r");
    verified("ALTER SERVER s RENAME TO t");
}

// =============================================================================
// CREATE DOMAIN
// =============================================================================

#[test]
fn create_domain_without_the_as_keyword() {
    parses_to("CREATE DOMAIN d int4", "CREATE DOMAIN d AS INT4");
    parses_to(
        "CREATE DOMAIN d numeric(8,2)",
        "CREATE DOMAIN d AS NUMERIC(8,2)",
    );
    parses_to(
        "CREATE DOMAIN d base_domain",
        "CREATE DOMAIN d AS base_domain",
    );
}

#[test]
fn create_domain_constraints_use_the_column_qualifier_grammar() {
    match verified("CREATE DOMAIN d AS INT4 CONSTRAINT cc GENERATED BY DEFAULT AS IDENTITY") {
        Statement::CreateDomain(domain) => {
            assert_eq!(domain.domain_constraints.len(), 1);
            assert_eq!(
                domain.domain_constraints[0]
                    .name
                    .as_ref()
                    .map(|n| n.value.clone()),
                Some("cc".into())
            );
            assert!(matches!(
                domain.domain_constraints[0].option,
                ColumnOption::Generated { .. }
            ));
            // Only NOT NULL and CHECK are meaningful for a domain, so nothing
            // reaches the semantic fields.
            assert!(!domain.not_null);
            assert!(domain.constraints.is_empty());
        }
        other => panic!("expected CREATE DOMAIN, got {other:?}"),
    }

    match verified("CREATE DOMAIN d AS INT4 CONSTRAINT cc CHECK (value > 1) DEFERRABLE") {
        Statement::CreateDomain(domain) => {
            assert_eq!(domain.domain_constraints.len(), 2);
            assert_eq!(
                domain.domain_constraints[1].option,
                ColumnOption::ConstraintAttribute(ConstraintAttribute::Deferrable)
            );
            assert_eq!(domain.constraints.len(), 1);
        }
        other => panic!("expected CREATE DOMAIN, got {other:?}"),
    }

    match verified("CREATE DOMAIN d AS INT4 NOT NULL NO INHERIT") {
        Statement::CreateDomain(domain) => {
            assert_eq!(
                domain.domain_constraints[0].option,
                ColumnOption::NotNullNoInherit
            );
            assert!(domain.not_null);
        }
        other => panic!("expected CREATE DOMAIN, got {other:?}"),
    }

    verified("CREATE DOMAIN d AS VARCHAR(5) COLLATE \"C\" DEFAULT 'x' CHECK (value <> '')");
    verified("CREATE DOMAIN d AS INT NULL");
}

// =============================================================================
// CREATE INDEX / SCHEMA / TRIGGER / VIEW
// =============================================================================

#[test]
fn create_index_on_only() {
    match verified("CREATE INDEX i ON ONLY t(a)") {
        Statement::CreateIndex(index) => {
            assert!(index.only);
            assert!(index.tablespace.is_none());
        }
        other => panic!("expected CREATE INDEX, got {other:?}"),
    }
    match verified("CREATE INDEX i ON t(a) TABLESPACE fast") {
        Statement::CreateIndex(index) => {
            assert!(!index.only);
            assert_eq!(index.tablespace.map(|t| t.value), Some("fast".into()));
        }
        other => panic!("expected CREATE INDEX, got {other:?}"),
    }
    verified("CREATE INDEX i ON ONLY t USING BTREE (a) INCLUDE (b) NULLS NOT DISTINCT WITH (fillfactor = 70) TABLESPACE fast WHERE a > 0");
}

#[test]
fn create_schema_with_elements() {
    match verified("CREATE SCHEMA s AUTHORIZATION owner CREATE TABLE tab (id INT)") {
        Statement::CreateSchema {
            schema_elements, ..
        } => {
            assert_eq!(schema_elements.len(), 1);
            assert!(matches!(
                schema_elements[0].statement,
                Statement::CreateTable(_)
            ));
        }
        other => panic!("expected CREATE SCHEMA, got {other:?}"),
    }
    match verified(
        "CREATE SCHEMA s CREATE TABLE tab (id INT) CREATE VIEW vw AS SELECT id FROM tab CREATE SEQUENCE seq",
    ) {
        Statement::CreateSchema {
            schema_elements, ..
        } => assert_eq!(schema_elements.len(), 3),
        other => panic!("expected CREATE SCHEMA, got {other:?}"),
    }
    parses_to(
        "CREATE SCHEMA s AUTHORIZATION CURRENT_ROLE CREATE TABLE tab (id INT)",
        "CREATE SCHEMA s AUTHORIZATION current_role CREATE TABLE tab (id INT)",
    );
    verified("CREATE SCHEMA s CREATE TABLE tab (id INT) CREATE INDEX tab_id_idx ON tab(id)");
    verified("CREATE SCHEMA s CREATE TABLE s.tab (id INT)");
    parses_to(
        "CREATE SCHEMA IF NOT EXISTS s AUTHORIZATION SESSION_USER",
        "CREATE SCHEMA IF NOT EXISTS s AUTHORIZATION session_user",
    );
    match verified("CREATE SCHEMA s CREATE TABLE tab (id INT) GRANT SELECT ON tab TO alice") {
        Statement::CreateSchema {
            schema_elements, ..
        } => {
            assert_eq!(schema_elements.len(), 2);
            assert!(matches!(
                schema_elements[1].statement,
                Statement::Grant { .. }
            ));
        }
        other => panic!("expected CREATE SCHEMA, got {other:?}"),
    }
}

#[test]
fn create_trigger_with_literal_arguments() {
    match verified(
        "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f('first', 'second')",
    ) {
        Statement::CreateTrigger(trigger) => {
            let exec_body = trigger.exec_body.expect("EXECUTE body");
            let args = exec_body.args.expect("literal arguments");
            assert_eq!(args.len(), 2);
            assert_eq!(args[0].to_string(), "'first'");
            assert!(exec_body.func_desc.args.is_none());
        }
        other => panic!("expected CREATE TRIGGER, got {other:?}"),
    }
    verified(
        "CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE f('first', 'second')",
    );
    verified("CREATE TRIGGER tg BEFORE INSERT ON t FOR EACH ROW EXECUTE PROCEDURE f('WS')");
    verified("CREATE TRIGGER tg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f(1, 'x', y)");
    // The no-argument spelling is unchanged.
    match verified("CREATE TRIGGER tg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()") {
        Statement::CreateTrigger(trigger) => {
            let exec_body = trigger.exec_body.expect("EXECUTE body");
            assert!(exec_body.args.is_none());
            assert_eq!(exec_body.func_desc.args.map(|a| a.len()), Some(0));
        }
        other => panic!("expected CREATE TRIGGER, got {other:?}"),
    }
}

#[test]
fn create_view_with_check_option() {
    match verified("CREATE VIEW v AS SELECT a FROM t WHERE a < 10 WITH CHECK OPTION") {
        Statement::CreateView(view) => {
            assert_eq!(view.check_option, Some(ViewCheckOption::Unqualified));
            assert!(!view.temporary);
        }
        other => panic!("expected CREATE VIEW, got {other:?}"),
    }
    match verified("CREATE VIEW v AS SELECT a FROM t WITH CASCADED CHECK OPTION") {
        Statement::CreateView(view) => {
            assert_eq!(view.check_option, Some(ViewCheckOption::Cascaded))
        }
        other => panic!("expected CREATE VIEW, got {other:?}"),
    }
    match verified("CREATE VIEW v AS SELECT a FROM t WITH LOCAL CHECK OPTION") {
        Statement::CreateView(view) => assert_eq!(view.check_option, Some(ViewCheckOption::Local)),
        other => panic!("expected CREATE VIEW, got {other:?}"),
    }
    match parses_to(
        "CREATE OR REPLACE TEMP VIEW v (a, b) AS SELECT 1, 2 WITH LOCAL CHECK OPTION",
        "CREATE OR REPLACE TEMPORARY VIEW v (a, b) AS SELECT 1, 2 WITH LOCAL CHECK OPTION",
    ) {
        Statement::CreateView(view) => assert!(view.temporary),
        other => panic!("expected CREATE VIEW, got {other:?}"),
    }
}

#[test]
fn select_from_rows_from() {
    match verified(
        "CREATE VIEW v AS SELECT * FROM ROWS FROM (generate_series(1, 2), generate_series(5, 6)) AS z (a, b)",
    ) {
        Statement::CreateView(view) => {
            let select = match &*view.query.body {
                SetExpr::Select(select) => select,
                other => panic!("expected a SELECT, got {other:?}"),
            };
            match &select.from[0].relation {
                TableFactor::RowsFrom {
                    lateral,
                    rows_from,
                    functions,
                    with_ordinality,
                    alias,
                } => {
                    assert!(!lateral);
                    assert!(rows_from);
                    assert_eq!(functions.len(), 2);
                    assert!(!with_ordinality);
                    assert_eq!(alias.as_ref().map(|a| a.columns.len()), Some(2));
                }
                other => panic!("expected ROWS FROM, got {other:?}"),
            }
        }
        other => panic!("expected CREATE VIEW, got {other:?}"),
    }
    verified(
        "SELECT * FROM ROWS FROM (a(1) AS (x INT, y TEXT), b()) WITH ORDINALITY AS z (p, q, r)",
    );
    verified("SELECT * FROM LATERAL ROWS FROM (a(1))");
}
