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

//! Tests for SECURITY LABEL statements
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-security-label.html>
//!
//! SECURITY LABEL is used for integration with Mandatory Access Control (MAC)
//! systems like SELinux. It supports 20 different object types.
//!
//! Currently, this statement is not implemented in the parser. These tests
//! document the expected syntax and will guide implementation.

use crate::postgres_compat::common::*;

// =============================================================================
// SECURITY LABEL Tests - All Currently Unimplemented
// =============================================================================
// All tests in this file expect parsing to fail. When SECURITY LABEL support
// is added, these should be converted to pg_test! with proper AST validation.

#[test]
fn test_security_label_on_table() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add Statement::SecurityLabel variant with SecurityLabelObject::Table
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON TABLE mytable IS 'system_u:object_r:sepgsql_table_t:s0'"
    );
}

#[test]
fn test_security_label_on_column() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Column variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON COLUMN mytable.mycolumn IS 'system_u:object_r:sepgsql_table_t:s0'"
    );
}

#[test]
fn test_security_label_on_schema() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Schema variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON SCHEMA myschema IS 'system_u:object_r:sepgsql_schema_t:s0'"
    );
}

#[test]
fn test_security_label_on_database() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Database variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON DATABASE mydb IS 'system_u:object_r:sepgsql_db_t:s0'"
    );
}

#[test]
fn test_security_label_on_function() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Function variant with signature
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON FUNCTION myfunc(integer) IS 'system_u:object_r:sepgsql_proc_exec_t:s0'"
    );
}

#[test]
fn test_security_label_on_procedure() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Procedure variant with signature
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON PROCEDURE myproc(integer) IS 'system_u:object_r:sepgsql_proc_exec_t:s0'"
    );
}

#[test]
fn test_security_label_on_routine() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Routine variant with signature
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON ROUTINE myroutine(text) IS 'system_u:object_r:sepgsql_proc_exec_t:s0'"
    );
}

#[test]
fn test_security_label_on_view() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::View variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON VIEW myview IS 'system_u:object_r:sepgsql_view_t:s0'"
    );
}

#[test]
fn test_security_label_on_materialized_view() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::MaterializedView variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON MATERIALIZED VIEW mymatview IS 'system_u:object_r:sepgsql_table_t:s0'"
    );
}

#[test]
fn test_security_label_on_sequence() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Sequence variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON SEQUENCE myseq IS 'system_u:object_r:sepgsql_seq_t:s0'"
    );
}

#[test]
fn test_security_label_on_domain() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Domain variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON DOMAIN email_address IS 'system_u:object_r:sepgsql_type_t:s0'"
    );
}

#[test]
fn test_security_label_on_type() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Type variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON TYPE custom_type IS 'system_u:object_r:sepgsql_type_t:s0'"
    );
}

#[test]
fn test_security_label_on_aggregate() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Aggregate variant with signature
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON AGGREGATE my_avg(integer) IS 'system_u:object_r:sepgsql_proc_exec_t:s0'"
    );
}

#[test]
fn test_security_label_on_language() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Language variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON LANGUAGE plpgsql IS 'system_u:object_r:sepgsql_lang_t:s0'"
    );
}

#[test]
fn test_security_label_on_foreign_table() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::ForeignTable variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON FOREIGN TABLE foreign_emp IS 'system_u:object_r:sepgsql_table_t:s0'"
    );
}

#[test]
fn test_security_label_on_event_trigger() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::EventTrigger variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON EVENT TRIGGER ddl_trigger IS 'system_u:object_r:sepgsql_proc_exec_t:s0'"
    );
}

#[test]
fn test_security_label_on_role() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Role variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON ROLE admin IS 'system_u:system_r:sysadm_t:s0'"
    );
}

#[test]
fn test_security_label_on_tablespace() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Tablespace variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON TABLESPACE pg_default IS 'system_u:object_r:sepgsql_sysobj_t:s0'"
    );
}

#[test]
fn test_security_label_on_publication() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Publication variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON PUBLICATION mypub IS 'system_u:object_r:sepgsql_sysobj_t:s0'"
    );
}

#[test]
fn test_security_label_on_subscription() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::Subscription variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON SUBSCRIPTION mysub IS 'system_u:object_r:sepgsql_sysobj_t:s0'"
    );
}

#[test]
fn test_security_label_on_large_object() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // TODO: Add SecurityLabelObject::LargeObject variant
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON LARGE OBJECT 12345 IS 'system_u:object_r:sepgsql_blob_t:s0'"
    );
}

#[test]
fn test_security_label_without_provider() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // Provider name is optional if exactly one provider is loaded
    // TODO: Support optional FOR provider clause
    pg_expect_parse_error!(
        "SECURITY LABEL ON TABLE mytable IS 'system_u:object_r:sepgsql_table_t:s0'"
    );
}

#[test]
fn test_security_label_with_null() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // NULL removes the security label
    // TODO: Support NULL label value
    pg_expect_parse_error!("SECURITY LABEL FOR selinux ON TABLE mytable IS NULL");
}

#[test]
fn test_security_label_with_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // Object names can be schema-qualified
    // TODO: Ensure qualified names work with SECURITY LABEL
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON TABLE public.employees IS 'system_u:object_r:sepgsql_table_t:s0'"
    );
}

#[test]
fn test_security_label_function_with_multiple_args() {
    // https://www.postgresql.org/docs/current/sql-security-label.html
    // Functions can have complex signatures
    // TODO: Support function signatures in SECURITY LABEL
    pg_expect_parse_error!(
        "SECURITY LABEL FOR selinux ON FUNCTION add(IN a integer, IN b integer) IS 'system_u:object_r:sepgsql_proc_exec_t:s0'"
    );
}
