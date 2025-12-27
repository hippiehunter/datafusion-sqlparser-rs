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

//! Tests for CREATE EVENT TRIGGER statement compatibility
//!
//! Event triggers fire on DDL events rather than table modifications.
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createeventtrigger.html>

use crate::postgres_compat::common::*;

// =============================================================================
// Basic Event Trigger Syntax Tests
// =============================================================================

#[test]
fn test_event_trigger_ddl_command_start() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // CREATE EVENT TRIGGER name ON ddl_command_start EXECUTE FUNCTION function_name()
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER log_ddl_start ON ddl_command_start EXECUTE FUNCTION log_ddl_commands()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_ddl_command_end() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Fires after DDL command completes
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER log_ddl_end ON ddl_command_end EXECUTE FUNCTION log_ddl_commands()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_sql_drop() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Fires just before DROP command execution
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER prevent_drops ON sql_drop EXECUTE FUNCTION prevent_object_drops()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_table_rewrite() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Fires before table rewrite
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER audit_rewrites ON table_rewrite EXECUTE FUNCTION audit_table_rewrites()",
        "Expected"
    );
}

// =============================================================================
// Event Trigger with WHEN Clause Tests
// =============================================================================

#[test]
fn test_event_trigger_when_tag() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Filter by specific command tags
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER log_create_table ON ddl_command_end WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION log_create_table()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_when_multiple_tags() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER log_table_ddl ON ddl_command_end WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE') EXECUTE FUNCTION log_table_commands()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_when_tag_create_index() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER log_index_creation ON ddl_command_end WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION log_index_commands()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_when_tag_drop() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER prevent_table_drops ON sql_drop WHEN TAG IN ('DROP TABLE') EXECUTE FUNCTION prevent_drops()",
        "Expected"
    );
}

// =============================================================================
// Event Trigger Options Tests
// =============================================================================

#[test]
fn test_event_trigger_or_replace() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // CREATE OR REPLACE EVENT TRIGGER
    pg_expect_parse_error!(
        "CREATE OR REPLACE EVENT TRIGGER my_event_trigger ON ddl_command_start EXECUTE FUNCTION my_func()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_disable() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Event triggers can be disabled with ALTER EVENT TRIGGER
    pg_expect_parse_error!("ALTER EVENT TRIGGER my_event_trigger DISABLE", "Expected");
}

#[test]
fn test_event_trigger_enable() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!("ALTER EVENT TRIGGER my_event_trigger ENABLE", "Expected");
}

#[test]
fn test_event_trigger_enable_replica() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!(
        "ALTER EVENT TRIGGER my_event_trigger ENABLE REPLICA",
        "Expected"
    );
}

#[test]
fn test_event_trigger_enable_always() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!(
        "ALTER EVENT TRIGGER my_event_trigger ENABLE ALWAYS",
        "Expected"
    );
}

// =============================================================================
// Event Trigger Rename Tests
// =============================================================================

#[test]
fn test_event_trigger_rename() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!(
        "ALTER EVENT TRIGGER my_event_trigger RENAME TO new_event_trigger",
        "Expected"
    );
}

// =============================================================================
// Event Trigger Ownership Tests
// =============================================================================

#[test]
fn test_event_trigger_owner_to() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    pg_expect_parse_error!(
        "ALTER EVENT TRIGGER my_event_trigger OWNER TO new_owner",
        "Expected"
    );
}

// =============================================================================
// DROP EVENT TRIGGER Tests
// =============================================================================

#[test]
fn test_drop_event_trigger() {
    // https://www.postgresql.org/docs/current/sql-dropeventtrigger.html
    pg_expect_parse_error!("DROP EVENT TRIGGER my_event_trigger", "Expected");
}

#[test]
fn test_drop_event_trigger_if_exists() {
    // https://www.postgresql.org/docs/current/sql-dropeventtrigger.html
    pg_expect_parse_error!("DROP EVENT TRIGGER IF EXISTS my_event_trigger", "Expected");
}

#[test]
fn test_drop_event_trigger_cascade() {
    // https://www.postgresql.org/docs/current/sql-dropeventtrigger.html
    pg_expect_parse_error!("DROP EVENT TRIGGER my_event_trigger CASCADE", "Expected");
}

#[test]
fn test_drop_event_trigger_restrict() {
    // https://www.postgresql.org/docs/current/sql-dropeventtrigger.html
    pg_expect_parse_error!("DROP EVENT TRIGGER my_event_trigger RESTRICT", "Expected");
}

// =============================================================================
// Real-World Event Trigger Examples
// =============================================================================

#[test]
fn test_event_trigger_audit_all_ddl() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // A common use case: audit all DDL commands
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER audit_all_ddl ON ddl_command_end EXECUTE FUNCTION audit_ddl_commands()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_prevent_table_drops() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Prevent accidental table drops in production
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER prevent_table_drops ON sql_drop WHEN TAG IN ('DROP TABLE') EXECUTE FUNCTION raise_exception()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_log_schema_changes() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Log all schema-related DDL commands
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER log_schema_changes ON ddl_command_end WHEN TAG IN ('CREATE SCHEMA', 'ALTER SCHEMA', 'DROP SCHEMA') EXECUTE FUNCTION log_schema_ddl()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_track_extensions() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Track extension installations
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER track_extensions ON ddl_command_end WHEN TAG IN ('CREATE EXTENSION', 'DROP EXTENSION') EXECUTE FUNCTION track_extension_changes()",
        "Expected"
    );
}

#[test]
fn test_event_trigger_comprehensive_ddl_filter() {
    // https://www.postgresql.org/docs/current/sql-createeventtrigger.html
    // Complex real-world example with multiple DDL commands
    pg_expect_parse_error!(
        "CREATE EVENT TRIGGER comprehensive_ddl_log ON ddl_command_end WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE', 'CREATE INDEX', 'DROP INDEX', 'CREATE VIEW', 'DROP VIEW', 'CREATE FUNCTION', 'DROP FUNCTION') EXECUTE FUNCTION log_important_ddl()",
        "Expected"
    );
}
