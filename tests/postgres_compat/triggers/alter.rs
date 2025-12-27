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

//! Tests for ALTER TRIGGER statement compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-altertrigger.html>

use crate::postgres_compat::common::*;

// =============================================================================
// ALTER TRIGGER RENAME TO Tests
// =============================================================================

#[test]
fn test_alter_trigger_rename_basic() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    // ALTER TRIGGER name ON table_name RENAME TO new_name
    pg_expect_parse_error!(
        "ALTER TRIGGER check_insert ON accounts RENAME TO validate_insert",
        "Expected"
    );
}

#[test]
fn test_alter_trigger_rename_schema_qualified() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    pg_expect_parse_error!(
        "ALTER TRIGGER my_trigger ON myschema.accounts RENAME TO new_trigger",
        "Expected"
    );
}

#[test]
fn test_alter_trigger_rename_if_exists() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    // PostgreSQL doesn't support IF EXISTS for ALTER TRIGGER, but testing for completeness
    pg_expect_parse_error!(
        "ALTER TRIGGER IF EXISTS my_trigger ON accounts RENAME TO new_trigger",
        "Expected"
    );
}

// =============================================================================
// ALTER TRIGGER DEPENDS ON EXTENSION Tests
// =============================================================================

#[test]
fn test_alter_trigger_depends_on_extension() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    // ALTER TRIGGER name ON table_name DEPENDS ON EXTENSION extension_name
    pg_expect_parse_error!(
        "ALTER TRIGGER my_trigger ON accounts DEPENDS ON EXTENSION my_extension",
        "Expected"
    );
}

#[test]
fn test_alter_trigger_depends_on_extension_schema_qualified() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    pg_expect_parse_error!(
        "ALTER TRIGGER my_trigger ON myschema.accounts DEPENDS ON EXTENSION postgis",
        "Expected"
    );
}

// =============================================================================
// Complex ALTER TRIGGER Examples
// =============================================================================

#[test]
fn test_alter_trigger_rename_complex_names() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    pg_expect_parse_error!(
        "ALTER TRIGGER \"CamelCaseTrigger\" ON public.\"MixedCaseTable\" RENAME TO \"NewTriggerName\"",
        "Expected"
    );
}

#[test]
fn test_alter_trigger_multiple_operations() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    // Note: PostgreSQL doesn't support multiple operations in one ALTER TRIGGER
    // This test documents that limitation
    pg_expect_parse_error!(
        "ALTER TRIGGER my_trigger ON accounts RENAME TO new_trigger, DEPENDS ON EXTENSION my_ext",
        "Expected"
    );
}
