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

//! Tests for COMMENT ON statements
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-comment.html>
//!
//! PostgreSQL supports COMMENT ON for 43 different object types. This file tests
//! the currently implemented subset and documents what still needs to be added.

use crate::postgres_compat::common::*;
use sqlparser::ast::{CommentObject, Statement};

// =============================================================================
// Basic COMMENT ON Tests - Currently Supported Object Types
// =============================================================================

#[test]
fn test_comment_on_column() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON COLUMN my_table.my_column IS 'Employee ID number'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    if_exists,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::Column);
                    assert_eq!(object_name.to_string(), "my_table.my_column");
                    assert_eq!(comment, Some("Employee ID number".to_string()));
                    assert!(!if_exists);
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_on_table() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON TABLE my_table IS 'This is my table'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    if_exists,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::Table);
                    assert_eq!(object_name.to_string(), "my_table");
                    assert_eq!(comment, Some("This is my table".to_string()));
                    assert!(!if_exists);
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_on_schema() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON SCHEMA my_schema IS 'Application schema'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::Schema);
                    assert_eq!(object_name.to_string(), "my_schema");
                    assert_eq!(comment, Some("Application schema".to_string()));
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_on_database() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON DATABASE my_db IS 'Production database'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::Database);
                    assert_eq!(object_name.to_string(), "my_db");
                    assert_eq!(comment, Some("Production database".to_string()));
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_on_extension() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON EXTENSION hstore IS 'data type for storing sets of (key, value) pairs'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::Extension);
                    assert_eq!(object_name.to_string(), "hstore");
                    assert_eq!(
                        comment,
                        Some("data type for storing sets of (key, value) pairs".to_string())
                    );
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_on_role() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON ROLE admin IS 'Administrator role'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::Role);
                    assert_eq!(object_name.to_string(), "admin");
                    assert_eq!(comment, Some("Administrator role".to_string()));
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_on_user() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON USER john_doe IS 'Engineering department'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::User);
                    assert_eq!(object_name.to_string(), "john_doe");
                    assert_eq!(comment, Some("Engineering department".to_string()));
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_with_null() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // NULL comment removes the existing comment
    pg_test!("COMMENT ON TABLE my_table IS NULL", |stmt: Statement| {
        match stmt {
            Statement::Comment {
                object_type,
                object_name,
                comment,
                ..
            } => {
                assert_eq!(object_type, CommentObject::Table);
                assert_eq!(object_name.to_string(), "my_table");
                assert_eq!(comment, None);
            }
            _ => panic!("Expected Statement::Comment"),
        }
    });
}

#[test]
fn test_comment_with_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    pg_test!(
        "COMMENT ON TABLE public.employees IS 'Company employees'",
        |stmt: Statement| {
            match stmt {
                Statement::Comment {
                    object_type,
                    object_name,
                    comment,
                    ..
                } => {
                    assert_eq!(object_type, CommentObject::Table);
                    assert_eq!(object_name.to_string(), "public.employees");
                    assert_eq!(comment, Some("Company employees".to_string()));
                }
                _ => panic!("Expected Statement::Comment"),
            }
        }
    );
}

#[test]
fn test_comment_with_escaped_quotes() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // SQL uses doubled single quotes ('') to escape quotes in string literals.
    // The parser normalizes these to single quotes in the AST.
    // NOTE: There's a Display bug where the quotes aren't re-escaped on output,
    // so we can't test round-trip. We just verify the AST is correct.
    let sql = "COMMENT ON TABLE my_table IS 'This comment has ''quotes'' in it'";
    let stmts = try_parse_pg(sql).expect("Should parse successfully");
    let stmt = &stmts[0];

    match stmt {
        Statement::Comment {
            object_type,
            comment,
            ..
        } => {
            assert_eq!(*object_type, CommentObject::Table);
            assert_eq!(
                comment,
                &Some("This comment has 'quotes' in it".to_string())
            );
        }
        _ => panic!("Expected Statement::Comment"),
    }
}

// =============================================================================
// COMMENT ON - Object Types Not Yet Implemented
// =============================================================================
// The following tests document PostgreSQL COMMENT ON object types that are
// not yet supported by the parser. When support is added, these tests should
// be converted from pg_expect_parse_error! to pg_test! with AST validation.

#[test]
fn test_comment_on_function() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Function variant
    pg_expect_parse_error!("COMMENT ON FUNCTION add(integer, integer) IS 'Adds two integers'");
}

#[test]
fn test_comment_on_procedure() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Procedure variant
    pg_expect_parse_error!("COMMENT ON PROCEDURE insert_data(integer, text) IS 'Inserts data'");
}

#[test]
fn test_comment_on_trigger() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Trigger variant with ON clause
    pg_expect_parse_error!(
        "COMMENT ON TRIGGER emp_stamp ON emp IS 'Enforce employee modification stamp'"
    );
}

#[test]
fn test_comment_on_index() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Index variant
    pg_expect_parse_error!("COMMENT ON INDEX idx_emp_id IS 'Employee ID index'");
}

#[test]
fn test_comment_on_view() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::View variant
    pg_expect_parse_error!("COMMENT ON VIEW emp_view IS 'Employee view'");
}

#[test]
fn test_comment_on_materialized_view() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::MaterializedView variant
    pg_expect_parse_error!(
        "COMMENT ON MATERIALIZED VIEW summary_view IS 'Materialized summary data'"
    );
}

#[test]
fn test_comment_on_sequence() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Sequence variant
    pg_expect_parse_error!("COMMENT ON SEQUENCE emp_id_seq IS 'Employee ID sequence'");
}

#[test]
fn test_comment_on_type() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Type variant
    pg_expect_parse_error!("COMMENT ON TYPE custom_type IS 'Custom composite type'");
}

#[test]
fn test_comment_on_domain() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Domain variant
    pg_expect_parse_error!("COMMENT ON DOMAIN email_address IS 'Email address type'");
}

#[test]
fn test_comment_on_constraint() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Constraint variant with ON clause
    pg_expect_parse_error!(
        "COMMENT ON CONSTRAINT emp_id_pk ON employees IS 'Primary key constraint'"
    );
}

#[test]
fn test_comment_on_constraint_on_domain() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add support for CONSTRAINT on DOMAIN
    pg_expect_parse_error!(
        "COMMENT ON CONSTRAINT email_check ON DOMAIN email_address IS 'Email format check'"
    );
}

#[test]
fn test_comment_on_rule() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Rule variant with ON clause
    pg_expect_parse_error!("COMMENT ON RULE emp_audit_rule ON employees IS 'Audit trail rule'");
}

#[test]
fn test_comment_on_policy() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Policy variant with ON clause
    pg_expect_parse_error!(
        "COMMENT ON POLICY emp_policy ON employees IS 'Row-level security policy'"
    );
}

#[test]
fn test_comment_on_aggregate() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Aggregate variant with signature
    pg_expect_parse_error!("COMMENT ON AGGREGATE my_avg(integer) IS 'Custom average function'");
}

#[test]
fn test_comment_on_operator() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Operator variant with signature
    pg_expect_parse_error!("COMMENT ON OPERATOR @@ (text, text) IS 'Text search match operator'");
}

#[test]
fn test_comment_on_cast() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Cast variant
    pg_expect_parse_error!("COMMENT ON CAST (integer AS text) IS 'Integer to text cast'");
}

#[test]
fn test_comment_on_language() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Language variant
    pg_expect_parse_error!("COMMENT ON LANGUAGE plpgsql IS 'PL/pgSQL procedural language'");
}

#[test]
fn test_comment_on_event_trigger() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::EventTrigger variant
    pg_expect_parse_error!("COMMENT ON EVENT TRIGGER ddl_trigger IS 'DDL change tracking trigger'");
}

#[test]
fn test_comment_on_foreign_table() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::ForeignTable variant
    pg_expect_parse_error!("COMMENT ON FOREIGN TABLE foreign_emp IS 'External employee data'");
}

#[test]
fn test_comment_on_tablespace() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Tablespace variant
    pg_expect_parse_error!("COMMENT ON TABLESPACE fast_storage IS 'SSD tablespace'");
}

#[test]
fn test_comment_on_text_search_configuration() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::TextSearchConfiguration variant
    pg_expect_parse_error!("COMMENT ON TEXT SEARCH CONFIGURATION english IS 'English text search'");
}

#[test]
fn test_comment_on_collation() {
    // https://www.postgresql.org/docs/current/sql-comment.html
    // TODO: Add CommentObject::Collation variant
    pg_expect_parse_error!("COMMENT ON COLLATION my_collation IS 'Custom collation'");
}
