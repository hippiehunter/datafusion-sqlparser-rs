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

//! Tests for ALTER PROCEDURE compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-alterprocedure.html>
//!
//! ## Feature Status
//!
//! ALTER PROCEDURE is NOT implemented in the AST. All tests use `pg_expect_parse_error!`
//! and are expected to fail until the feature is implemented.

use crate::postgres_compat::common::*;

// =============================================================================
// ALTER PROCEDURE ... RENAME TO
// =============================================================================

#[test]
fn test_alter_procedure_rename_basic() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Rename procedure without arguments
    pg_expect_parse_error!("ALTER PROCEDURE my_proc RENAME TO new_proc");
}

#[test]
fn test_alter_procedure_rename_with_args() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Rename procedure with argument signature
    pg_expect_parse_error!("ALTER PROCEDURE update_record(INTEGER, TEXT) RENAME TO modify_record");
}

#[test]
fn test_alter_procedure_rename_qualified() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Rename procedure with schema-qualified name
    pg_expect_parse_error!("ALTER PROCEDURE myschema.myproc(TEXT) RENAME TO newproc");
}

#[test]
fn test_alter_procedure_rename_no_args() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Rename procedure with empty parameter list
    pg_expect_parse_error!("ALTER PROCEDURE cleanup_temp() RENAME TO cleanup_temporary");
}

// =============================================================================
// ALTER PROCEDURE ... OWNER TO
// =============================================================================

#[test]
fn test_alter_procedure_owner_basic() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Change procedure owner
    pg_expect_parse_error!("ALTER PROCEDURE my_proc OWNER TO new_owner");
}

#[test]
fn test_alter_procedure_owner_with_args() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Change owner with argument signature
    pg_expect_parse_error!("ALTER PROCEDURE process_data(NUMERIC, TEXT) OWNER TO admin");
}

#[test]
fn test_alter_procedure_owner_current_user() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Set owner to CURRENT_USER
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() OWNER TO CURRENT_USER");
}

#[test]
fn test_alter_procedure_owner_current_role() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Set owner to CURRENT_ROLE
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() OWNER TO CURRENT_ROLE");
}

#[test]
fn test_alter_procedure_owner_session_user() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Set owner to SESSION_USER
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() OWNER TO SESSION_USER");
}

// =============================================================================
// ALTER PROCEDURE ... SET SCHEMA
// =============================================================================

#[test]
fn test_alter_procedure_set_schema() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Move procedure to different schema
    pg_expect_parse_error!("ALTER PROCEDURE my_proc SET SCHEMA new_schema");
}

#[test]
fn test_alter_procedure_set_schema_with_args() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Move procedure with argument signature to different schema
    pg_expect_parse_error!("ALTER PROCEDURE archive_data(DATE) SET SCHEMA archive");
}

#[test]
fn test_alter_procedure_set_schema_qualified() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Move schema-qualified procedure to different schema
    pg_expect_parse_error!("ALTER PROCEDURE old_schema.my_proc() SET SCHEMA new_schema");
}

// =============================================================================
// ALTER PROCEDURE ... SECURITY
// =============================================================================

#[test]
fn test_alter_procedure_security_definer() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // SECURITY DEFINER: procedure executes with privileges of owner
    pg_expect_parse_error!("ALTER PROCEDURE privileged_operation() SECURITY DEFINER");
}

#[test]
fn test_alter_procedure_security_invoker() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // SECURITY INVOKER: procedure executes with privileges of caller (default)
    pg_expect_parse_error!("ALTER PROCEDURE public_operation() SECURITY INVOKER");
}

// =============================================================================
// ALTER PROCEDURE ... SET/RESET
// =============================================================================

#[test]
fn test_alter_procedure_set_config() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // SET configuration parameter for procedure execution
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() SET search_path TO myschema, public");
}

#[test]
fn test_alter_procedure_set_config_equals() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // SET with = syntax
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() SET work_mem = '256MB'");
}

#[test]
fn test_alter_procedure_set_config_from_current() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // SET FROM CURRENT: set parameter to current session value
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() SET timezone FROM CURRENT");
}

#[test]
fn test_alter_procedure_reset_config() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // RESET configuration parameter
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() RESET search_path");
}

#[test]
fn test_alter_procedure_reset_all() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // RESET ALL: reset all procedure-specific configuration
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() RESET ALL");
}

// =============================================================================
// ALTER PROCEDURE ... DEPENDS ON EXTENSION
// =============================================================================

#[test]
fn test_alter_procedure_depends_on_extension() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Mark procedure as dependent on extension
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() DEPENDS ON EXTENSION my_extension");
}

#[test]
fn test_alter_procedure_no_depends_on_extension() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Remove extension dependency
    pg_expect_parse_error!("ALTER PROCEDURE my_proc() NO DEPENDS ON EXTENSION my_extension");
}

// =============================================================================
// Complex combinations
// =============================================================================

#[test]
fn test_alter_procedure_multiple_options() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    // Multiple options can be specified in one statement
    pg_expect_parse_error!(
        "ALTER PROCEDURE process_records(INTEGER) SECURITY DEFINER SET work_mem = '512MB'"
    );
}
