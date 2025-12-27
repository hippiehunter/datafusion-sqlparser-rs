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

//! Tests for CREATE TRIGGER statement compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createtrigger.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{
    Statement, TriggerEvent, TriggerExecBodyType, TriggerObject, TriggerObjectKind, TriggerPeriod,
};

// =============================================================================
// Basic Trigger Syntax Tests
// =============================================================================

#[test]
fn test_basic_before_insert_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER check_insert BEFORE INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION check_account_insert()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "check_insert");
            assert_eq!(trigger.period, Some(TriggerPeriod::Before));
            assert_eq!(trigger.events.len(), 1);
            assert!(matches!(trigger.events[0], TriggerEvent::Insert));
            assert_eq!(trigger.table_name.to_string(), "accounts");
            assert_eq!(
                trigger.trigger_object,
                Some(TriggerObjectKind::ForEach(TriggerObject::Row))
            );
            assert!(trigger.exec_body.is_some());
            let exec_body = trigger.exec_body.as_ref().unwrap();
            assert_eq!(exec_body.exec_type, TriggerExecBodyType::Function);
            assert_eq!(exec_body.func_desc.name.to_string(), "check_account_insert");
        }
    );
}

#[test]
fn test_basic_after_update_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER check_update AFTER UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION check_account_update()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "check_update");
            assert_eq!(trigger.period, Some(TriggerPeriod::After));
            assert_eq!(trigger.events.len(), 1);
            assert!(matches!(trigger.events[0], TriggerEvent::Update(_)));
        }
    );
}

#[test]
fn test_basic_after_delete_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER check_delete AFTER DELETE ON accounts FOR EACH ROW EXECUTE FUNCTION check_account_delete()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "check_delete");
            assert_eq!(trigger.period, Some(TriggerPeriod::After));
            assert_eq!(trigger.events.len(), 1);
            assert!(matches!(trigger.events[0], TriggerEvent::Delete));
        }
    );
}

#[test]
fn test_instead_of_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    // INSTEAD OF triggers are typically used on views
    pg_test!(
        "CREATE TRIGGER view_insert INSTEAD OF INSERT ON accounts_view FOR EACH ROW EXECUTE FUNCTION handle_view_insert()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "view_insert");
            assert_eq!(trigger.period, Some(TriggerPeriod::InsteadOf));
            assert_eq!(trigger.events.len(), 1);
            assert!(matches!(trigger.events[0], TriggerEvent::Insert));
        }
    );
}

#[test]
fn test_truncate_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER check_truncate BEFORE TRUNCATE ON accounts FOR EACH STATEMENT EXECUTE FUNCTION check_account_truncate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "check_truncate");
            assert_eq!(trigger.period, Some(TriggerPeriod::Before));
            assert_eq!(trigger.events.len(), 1);
            assert!(matches!(trigger.events[0], TriggerEvent::Truncate));
        }
    );
}

// =============================================================================
// Trigger Timing Tests (FOR EACH ROW vs FOR EACH STATEMENT)
// =============================================================================

#[test]
fn test_for_each_row() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER row_trigger BEFORE INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION func()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(
                trigger.trigger_object,
                Some(TriggerObjectKind::ForEach(TriggerObject::Row))
            );
        }
    );
}

#[test]
fn test_for_each_statement() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER stmt_trigger BEFORE INSERT ON accounts FOR EACH STATEMENT EXECUTE FUNCTION func()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(
                trigger.trigger_object,
                Some(TriggerObjectKind::ForEach(TriggerObject::Statement))
            );
        }
    );
}

// =============================================================================
// Multiple Events Tests
// =============================================================================

#[test]
fn test_multiple_events_insert_update() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER multi_event BEFORE INSERT OR UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION func()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.events.len(), 2);
            assert!(matches!(trigger.events[0], TriggerEvent::Insert));
            assert!(matches!(trigger.events[1], TriggerEvent::Update(_)));
        }
    );
}

#[test]
fn test_multiple_events_insert_update_delete() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER all_dml AFTER INSERT OR UPDATE OR DELETE ON accounts FOR EACH ROW EXECUTE FUNCTION audit_log()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.events.len(), 3);
            assert!(matches!(trigger.events[0], TriggerEvent::Insert));
            assert!(matches!(trigger.events[1], TriggerEvent::Update(_)));
            assert!(matches!(trigger.events[2], TriggerEvent::Delete));
        }
    );
}

// =============================================================================
// UPDATE OF column_name Tests
// =============================================================================

#[test]
fn test_update_of_single_column() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER balance_update BEFORE UPDATE OF balance ON accounts FOR EACH ROW EXECUTE FUNCTION check_balance()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.events.len(), 1);
            match &trigger.events[0] {
                TriggerEvent::Update(cols) => {
                    assert_eq!(cols.len(), 1);
                    assert_eq!(cols[0].to_string(), "balance");
                }
                _ => panic!("Expected UPDATE event"),
            }
        }
    );
}

#[test]
fn test_update_of_multiple_columns() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER multi_col_update BEFORE UPDATE OF balance, status ON accounts FOR EACH ROW EXECUTE FUNCTION validate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.events.len(), 1);
            match &trigger.events[0] {
                TriggerEvent::Update(cols) => {
                    assert_eq!(cols.len(), 2);
                    assert_eq!(cols[0].to_string(), "balance");
                    assert_eq!(cols[1].to_string(), "status");
                }
                _ => panic!("Expected UPDATE event"),
            }
        }
    );
}

// =============================================================================
// WHEN Condition Tests
// =============================================================================

#[test]
fn test_when_condition_simple() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER check_large AFTER UPDATE ON accounts FOR EACH ROW WHEN (NEW.balance > 10000) EXECUTE FUNCTION check_account_update()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.condition.is_some());
        }
    );
}

#[test]
fn test_when_condition_complex() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER complex_when BEFORE UPDATE ON accounts FOR EACH ROW WHEN (OLD.balance <> NEW.balance AND NEW.balance > 0) EXECUTE FUNCTION validate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.condition.is_some());
        }
    );
}

#[test]
fn test_when_condition_with_old_new() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER check_status AFTER UPDATE ON accounts FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION log_status_change()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.condition.is_some());
        }
    );
}

// =============================================================================
// REFERENCING Clause Tests
// =============================================================================

#[test]
fn test_referencing_new_table() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER ref_new AFTER INSERT ON accounts REFERENCING NEW TABLE AS new_accounts FOR EACH STATEMENT EXECUTE FUNCTION process_new()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.referencing.len(), 1);
            assert_eq!(
                trigger.referencing[0]
                    .transition_relation_name
                    .to_string(),
                "new_accounts"
            );
        }
    );
}

#[test]
fn test_referencing_old_table() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER ref_old AFTER DELETE ON accounts REFERENCING OLD TABLE AS old_accounts FOR EACH STATEMENT EXECUTE FUNCTION archive_deleted()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.referencing.len(), 1);
            assert_eq!(
                trigger.referencing[0]
                    .transition_relation_name
                    .to_string(),
                "old_accounts"
            );
        }
    );
}

#[test]
fn test_referencing_both_old_and_new() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER ref_both AFTER UPDATE ON accounts REFERENCING OLD TABLE AS old_accounts NEW TABLE AS new_accounts FOR EACH STATEMENT EXECUTE FUNCTION compare_changes()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.referencing.len(), 2);
        }
    );
}

// =============================================================================
// EXECUTE FUNCTION vs EXECUTE PROCEDURE Tests
// =============================================================================

#[test]
fn test_execute_function() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER func_trigger BEFORE INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION my_func()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.exec_body.is_some());
            let exec_body = trigger.exec_body.as_ref().unwrap();
            assert_eq!(exec_body.exec_type, TriggerExecBodyType::Function);
        }
    );
}

#[test]
fn test_execute_procedure() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    // EXECUTE PROCEDURE is supported as an alias for EXECUTE FUNCTION
    pg_test!(
        "CREATE TRIGGER proc_trigger BEFORE INSERT ON accounts FOR EACH ROW EXECUTE PROCEDURE my_proc()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.exec_body.is_some());
            let exec_body = trigger.exec_body.as_ref().unwrap();
            assert_eq!(exec_body.exec_type, TriggerExecBodyType::Procedure);
        }
    );
}

#[test]
fn test_execute_function_with_args() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    // TODO: Function arguments in EXECUTE FUNCTION not yet fully supported
    // The parser currently expects a function call signature but doesn't handle string literals as args
    pg_expect_parse_error!(
        "CREATE TRIGGER func_with_args BEFORE INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION process_account('arg1', 'arg2')",
        "Expected"
    );
}

// =============================================================================
// CONSTRAINT Trigger Tests
// =============================================================================

#[test]
fn test_constraint_trigger_basic() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE CONSTRAINT TRIGGER check_constraint AFTER INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION validate_constraint()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.is_constraint);
            assert_eq!(trigger.name.to_string(), "check_constraint");
        }
    );
}

#[test]
fn test_constraint_trigger_deferrable() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE CONSTRAINT TRIGGER deferred_check AFTER INSERT ON accounts DEFERRABLE FOR EACH ROW EXECUTE FUNCTION validate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.is_constraint);
            assert!(trigger.characteristics.is_some());
            let chars = trigger.characteristics.as_ref().unwrap();
            assert_eq!(chars.deferrable, Some(true));
        }
    );
}

#[test]
fn test_constraint_trigger_initially_deferred() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE CONSTRAINT TRIGGER init_deferred AFTER INSERT ON accounts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION validate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.is_constraint);
            assert!(trigger.characteristics.is_some());
            let chars = trigger.characteristics.as_ref().unwrap();
            assert_eq!(chars.deferrable, Some(true));
            assert!(chars.initially.is_some());
        }
    );
}

#[test]
fn test_constraint_trigger_initially_immediate() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE CONSTRAINT TRIGGER init_immediate AFTER INSERT ON accounts DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION validate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.is_constraint);
            assert!(trigger.characteristics.is_some());
        }
    );
}

#[test]
fn test_constraint_trigger_not_deferrable() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE CONSTRAINT TRIGGER not_defer AFTER INSERT ON accounts NOT DEFERRABLE FOR EACH ROW EXECUTE FUNCTION validate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.is_constraint);
            assert!(trigger.characteristics.is_some());
            let chars = trigger.characteristics.as_ref().unwrap();
            assert_eq!(chars.deferrable, Some(false));
        }
    );
}

// =============================================================================
// CREATE OR REPLACE TRIGGER Tests
// =============================================================================

#[test]
fn test_or_replace_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE OR REPLACE TRIGGER replace_trigger BEFORE INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION func()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.or_replace);
            assert_eq!(trigger.name.to_string(), "replace_trigger");
        }
    );
}

// =============================================================================
// Schema-Qualified Names Tests
// =============================================================================

#[test]
fn test_schema_qualified_trigger_name() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER myschema.my_trigger BEFORE INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION func()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "myschema.my_trigger");
        }
    );
}

#[test]
fn test_schema_qualified_table_name() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER my_trigger BEFORE INSERT ON myschema.accounts FOR EACH ROW EXECUTE FUNCTION func()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.table_name.to_string(), "myschema.accounts");
        }
    );
}

#[test]
fn test_schema_qualified_function_name() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER my_trigger BEFORE INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION myschema.check_account()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            let exec_body = trigger.exec_body.as_ref().unwrap();
            assert_eq!(exec_body.func_desc.name.to_string(), "myschema.check_account");
        }
    );
}

#[test]
fn test_fully_qualified_names() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    pg_test!(
        "CREATE TRIGGER schema1.my_trigger BEFORE INSERT ON schema2.accounts FOR EACH ROW EXECUTE FUNCTION schema3.validate()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "schema1.my_trigger");
            assert_eq!(trigger.table_name.to_string(), "schema2.accounts");
            let exec_body = trigger.exec_body.as_ref().unwrap();
            assert_eq!(exec_body.func_desc.name.to_string(), "schema3.validate");
        }
    );
}

// =============================================================================
// Complex Real-World Examples
// =============================================================================

#[test]
fn test_audit_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    // A typical audit trigger that logs all changes
    pg_test!(
        "CREATE TRIGGER audit_changes AFTER INSERT OR UPDATE OR DELETE ON accounts FOR EACH ROW EXECUTE FUNCTION audit_log()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "audit_changes");
            assert_eq!(trigger.period, Some(TriggerPeriod::After));
            assert_eq!(trigger.events.len(), 3);
        }
    );
}

#[test]
fn test_timestamp_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    // Auto-update timestamp trigger
    pg_test!(
        "CREATE TRIGGER update_timestamp BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION update_modified_column()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "update_timestamp");
            assert_eq!(trigger.period, Some(TriggerPeriod::Before));
        }
    );
}

#[test]
fn test_validation_trigger_with_when() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    // Conditional validation trigger
    pg_test!(
        "CREATE TRIGGER validate_balance BEFORE UPDATE OF balance ON accounts FOR EACH ROW WHEN (NEW.balance < 0) EXECUTE FUNCTION raise_negative_balance_error()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert_eq!(trigger.name.to_string(), "validate_balance");
            assert!(trigger.condition.is_some());
            match &trigger.events[0] {
                TriggerEvent::Update(cols) => {
                    assert_eq!(cols.len(), 1);
                    assert_eq!(cols[0].to_string(), "balance");
                }
                _ => panic!("Expected UPDATE event"),
            }
        }
    );
}

#[test]
fn test_referential_integrity_trigger() {
    // https://www.postgresql.org/docs/current/sql-createtrigger.html
    // Custom referential integrity check
    pg_test!(
        "CREATE CONSTRAINT TRIGGER check_foreign_key AFTER INSERT OR UPDATE ON orders DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_customer_exists()",
        |stmt: Statement| {
            let trigger = extract_create_trigger(&stmt);
            assert!(trigger.is_constraint);
            assert_eq!(trigger.events.len(), 2);
            assert!(trigger.characteristics.is_some());
        }
    );
}
