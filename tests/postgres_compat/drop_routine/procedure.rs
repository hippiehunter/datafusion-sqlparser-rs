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

//! Tests for DROP PROCEDURE compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-dropprocedure.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{DropBehavior, FunctionDesc, Statement};

// =============================================================================
// Helper functions
// =============================================================================

#[allow(dead_code)]
struct DropProcedureExtract<'a> {
    if_exists: bool,
    proc_desc: &'a Vec<FunctionDesc>,
    drop_behavior: &'a Option<DropBehavior>,
}

fn extract_drop_procedure(stmt: &Statement) -> DropProcedureExtract<'_> {
    match stmt {
        Statement::DropProcedure {
            if_exists,
            proc_desc,
            drop_behavior,
            ..
        } => DropProcedureExtract {
            if_exists: *if_exists,
            proc_desc,
            drop_behavior,
        },
        other => panic!(
            "Expected Statement::DropProcedure, got: {:?}",
            std::mem::discriminant(other)
        ),
    }
}

// =============================================================================
// Basic DROP PROCEDURE
// =============================================================================

#[test]
fn test_drop_procedure_basic() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure without arguments
    pg_test!("DROP PROCEDURE my_proc", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert!(!dp.if_exists);
        assert_eq!(dp.proc_desc.len(), 1);
        assert_eq!(dp.proc_desc[0].name.to_string(), "my_proc");
        assert!(dp.proc_desc[0].args.is_none());
        assert!(dp.drop_behavior.is_none());
    });
}

#[test]
fn test_drop_procedure_with_parens() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with empty parentheses
    pg_test!("DROP PROCEDURE my_proc()", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert_eq!(dp.proc_desc.len(), 1);
        assert_eq!(dp.proc_desc[0].name.to_string(), "my_proc");
    });
}

#[test]
fn test_drop_procedure_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with schema-qualified name
    pg_test!("DROP PROCEDURE myschema.myproc", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert_eq!(dp.proc_desc[0].name.to_string(), "myschema.myproc");
        assert_eq!(dp.proc_desc[0].name.0.len(), 2);
    });
}

#[test]
fn test_drop_procedure_double_qualified() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with database.schema.procedure name
    pg_test!("DROP PROCEDURE mydb.myschema.myproc", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert_eq!(dp.proc_desc[0].name.to_string(), "mydb.myschema.myproc");
        assert_eq!(dp.proc_desc[0].name.0.len(), 3);
    });
}

// =============================================================================
// DROP PROCEDURE with argument types
// =============================================================================

#[test]
fn test_drop_procedure_single_arg() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with single argument type
    pg_test!(
        "DROP PROCEDURE update_record(INTEGER)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn test_drop_procedure_multiple_args() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with multiple argument types
    pg_test!(
        "DROP PROCEDURE insert_data(INTEGER, TEXT, TIMESTAMP)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

#[test]
fn test_drop_procedure_complex_arg_types() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with complex argument types
    pg_test!(
        "DROP PROCEDURE process_data(VARCHAR(100), NUMERIC(10,2), BOOLEAN)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

#[test]
fn test_drop_procedure_in_parameter() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with explicit IN parameter
    pg_test!(
        "DROP PROCEDURE insert_record(IN id INTEGER, IN name TEXT)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

#[test]
fn test_drop_procedure_out_parameter() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with OUT parameter
    pg_test!(
        "DROP PROCEDURE get_stats(IN id INTEGER, OUT total INTEGER, OUT average NUMERIC)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

#[test]
fn test_drop_procedure_inout_parameter() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with INOUT parameter
    pg_test!(
        "DROP PROCEDURE increment_counter(INOUT counter INTEGER)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn test_drop_procedure_mixed_parameters() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with mixed parameter modes
    pg_test!(
        "DROP PROCEDURE process(IN x INTEGER, OUT y INTEGER, INOUT z TEXT)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

// =============================================================================
// DROP PROCEDURE IF EXISTS
// =============================================================================

#[test]
fn test_drop_procedure_if_exists() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // IF EXISTS prevents error if procedure doesn't exist
    pg_test!("DROP PROCEDURE IF EXISTS my_proc", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert!(dp.if_exists, "Expected if_exists=true");
        assert_eq!(dp.proc_desc.len(), 1);
    });
}

#[test]
fn test_drop_procedure_if_exists_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // IF EXISTS with argument signature
    pg_test!(
        "DROP PROCEDURE IF EXISTS update_stats(INTEGER, TEXT)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert!(dp.if_exists);
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

#[test]
fn test_drop_procedure_if_exists_qualified() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // IF EXISTS with schema-qualified name
    pg_test!(
        "DROP PROCEDURE IF EXISTS myschema.myproc(INTEGER)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert!(dp.if_exists);
            assert_eq!(dp.proc_desc[0].name.to_string(), "myschema.myproc");
        }
    );
}

// =============================================================================
// DROP PROCEDURE with CASCADE/RESTRICT
// =============================================================================

#[test]
fn test_drop_procedure_cascade() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // CASCADE automatically drops dependent objects
    pg_test!("DROP PROCEDURE my_proc CASCADE", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert_eq!(*dp.drop_behavior, Some(DropBehavior::Cascade));
    });
}

#[test]
fn test_drop_procedure_cascade_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // CASCADE with argument signature
    pg_test!(
        "DROP PROCEDURE cleanup_data(DATE, DATE) CASCADE",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert_eq!(*dp.drop_behavior, Some(DropBehavior::Cascade));
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

#[test]
fn test_drop_procedure_restrict() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // RESTRICT refuses to drop if dependent objects exist (default)
    pg_test!("DROP PROCEDURE my_proc RESTRICT", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert_eq!(*dp.drop_behavior, Some(DropBehavior::Restrict));
    });
}

#[test]
fn test_drop_procedure_restrict_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // RESTRICT with argument signature
    pg_test!(
        "DROP PROCEDURE validate_data(TEXT) RESTRICT",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert_eq!(*dp.drop_behavior, Some(DropBehavior::Restrict));
        }
    );
}

#[test]
fn test_drop_procedure_if_exists_cascade() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Combine IF EXISTS with CASCADE
    pg_test!(
        "DROP PROCEDURE IF EXISTS my_proc CASCADE",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert!(dp.if_exists);
            assert_eq!(*dp.drop_behavior, Some(DropBehavior::Cascade));
        }
    );
}

#[test]
fn test_drop_procedure_if_exists_restrict() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Combine IF EXISTS with RESTRICT
    pg_test!(
        "DROP PROCEDURE IF EXISTS my_proc RESTRICT",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert!(dp.if_exists);
            assert_eq!(*dp.drop_behavior, Some(DropBehavior::Restrict));
        }
    );
}

// =============================================================================
// DROP multiple procedures
// =============================================================================

#[test]
fn test_drop_multiple_procedures() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop multiple procedures in one statement
    pg_test!("DROP PROCEDURE proc1, proc2, proc3", |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert_eq!(dp.proc_desc.len(), 3);
        assert_eq!(dp.proc_desc[0].name.to_string(), "proc1");
        assert_eq!(dp.proc_desc[1].name.to_string(), "proc2");
        assert_eq!(dp.proc_desc[2].name.to_string(), "proc3");
    });
}

#[test]
fn test_drop_multiple_procedures_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop multiple procedures with different signatures
    pg_test!(
        "DROP PROCEDURE insert_data(INTEGER, TEXT), update_data(INTEGER), delete_data(INTEGER)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert_eq!(dp.proc_desc.len(), 3);
            assert_eq!(dp.proc_desc[0].name.to_string(), "insert_data");
            assert_eq!(dp.proc_desc[1].name.to_string(), "update_data");
            assert_eq!(dp.proc_desc[2].name.to_string(), "delete_data");
        }
    );
}

#[test]
fn test_drop_multiple_procedures_mixed() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop multiple procedures with mixed signatures
    pg_test!(
        "DROP PROCEDURE proc1(), proc2(INTEGER), proc3",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert_eq!(dp.proc_desc.len(), 3);
        }
    );
}

#[test]
fn test_drop_multiple_procedures_if_exists_cascade() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop multiple procedures with IF EXISTS and CASCADE
    pg_test!(
        "DROP PROCEDURE IF EXISTS proc1, proc2 CASCADE",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert!(dp.if_exists);
            assert_eq!(dp.proc_desc.len(), 2);
            assert_eq!(*dp.drop_behavior, Some(DropBehavior::Cascade));
        }
    );
}

// =============================================================================
// Edge cases and special scenarios
// =============================================================================

#[test]
fn test_drop_procedure_quoted_identifier() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop procedure with quoted identifier (case-sensitive)
    pg_test!(r#"DROP PROCEDURE "MyProcedure"()"#, |stmt: Statement| {
        let dp = extract_drop_procedure(&stmt);
        assert_eq!(dp.proc_desc[0].name.to_string(), r#""MyProcedure""#);
    });
}

#[test]
fn test_drop_procedure_schema_qualified_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Drop schema-qualified procedure with complex argument signature
    pg_test!(
        "DROP PROCEDURE myschema.complex_proc(INTEGER, VARCHAR(50), TIMESTAMP WITH TIME ZONE)",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert_eq!(dp.proc_desc[0].name.to_string(), "myschema.complex_proc");
            let args = dp.proc_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

#[test]
fn test_drop_procedure_comprehensive() {
    // https://www.postgresql.org/docs/current/sql-dropprocedure.html
    // Comprehensive test with all options
    pg_test!(
        "DROP PROCEDURE IF EXISTS myschema.proc1(INTEGER, TEXT), myschema.proc2() CASCADE",
        |stmt: Statement| {
            let dp = extract_drop_procedure(&stmt);
            assert!(dp.if_exists);
            assert_eq!(dp.proc_desc.len(), 2);
            assert_eq!(*dp.drop_behavior, Some(DropBehavior::Cascade));
        }
    );
}
