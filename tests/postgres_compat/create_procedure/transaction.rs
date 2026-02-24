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

//! Tests for CREATE PROCEDURE transaction control
//!
//! PostgreSQL 11+ allows procedures to manage transactions with COMMIT and ROLLBACK.
//! Functions cannot do this, which is a key distinction between procedures and functions.
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createprocedure.html>
//! Reference: <https://www.postgresql.org/docs/current/xproc.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::Statement;

// ============================================================================
// COMMIT Statement Tests (PostgreSQL 11+, LIKELY FAIL)
// ============================================================================

#[test]
fn test_create_procedure_with_commit() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can execute COMMIT to commit the current transaction
    // This is a key feature that distinguishes procedures from functions
    pg_test!(
        "CREATE PROCEDURE proc_commit() AS BEGIN INSERT INTO tbl VALUES (1); COMMIT; INSERT INTO tbl VALUES (2); END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "proc_commit");
            // TODO: Verify body contains COMMIT statement AST
        }
    );
}

#[test]
fn test_create_procedure_multiple_commits() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can execute multiple COMMITs
    pg_test!(
        r#"CREATE PROCEDURE multi_commit() AS $$
BEGIN
    INSERT INTO tbl VALUES (1);
    COMMIT;
    INSERT INTO tbl VALUES (2);
    COMMIT;
    INSERT INTO tbl VALUES (3);
    COMMIT;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "multi_commit");
            // TODO: Verify body contains multiple COMMIT statements
        }
    );
}

#[test]
fn test_create_procedure_commit_work() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // COMMIT WORK is an alternative syntax
    pg_test!(
        "CREATE PROCEDURE commit_work() AS BEGIN INSERT INTO tbl VALUES (1); COMMIT WORK; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "commit_work");
            // TODO: Verify COMMIT WORK is parsed correctly
        }
    );
}

#[test]
fn test_create_procedure_commit_and() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // COMMIT AND CHAIN starts a new transaction
    pg_test!(
        "CREATE PROCEDURE commit_chain() AS BEGIN INSERT INTO tbl VALUES (1); COMMIT AND CHAIN; INSERT INTO tbl VALUES (2); END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "commit_chain");
            // TODO: Verify COMMIT AND CHAIN is parsed correctly
        }
    );
}

// ============================================================================
// ROLLBACK Statement Tests (PostgreSQL 11+, LIKELY FAIL)
// ============================================================================

#[test]
fn test_create_procedure_with_rollback() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can execute ROLLBACK to abort the current transaction
    pg_test!(
        "CREATE PROCEDURE proc_rollback() AS BEGIN INSERT INTO tbl VALUES (1); ROLLBACK; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "proc_rollback");
            // TODO: Verify body contains ROLLBACK statement AST
        }
    );
}

#[test]
fn test_create_procedure_conditional_rollback() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Typical pattern: conditional rollback based on error handling
    pg_test!(
        r#"CREATE PROCEDURE conditional_rollback(x INTEGER) AS $$
BEGIN
    INSERT INTO tbl VALUES (x);
    IF x < 0 THEN
        ROLLBACK;
    ELSE
        COMMIT;
    END IF;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "conditional_rollback");
            // TODO: Verify IF statement with ROLLBACK/COMMIT branches
        }
    );
}

#[test]
fn test_create_procedure_rollback_work() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // ROLLBACK WORK is an alternative syntax
    pg_test!(
        "CREATE PROCEDURE rollback_work() AS BEGIN INSERT INTO tbl VALUES (1); ROLLBACK WORK; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "rollback_work");
            // TODO: Verify ROLLBACK WORK is parsed correctly
        }
    );
}

#[test]
fn test_create_procedure_rollback_and_chain() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // ROLLBACK AND CHAIN aborts current transaction and starts a new one
    pg_test!(
        "CREATE PROCEDURE rollback_chain() AS BEGIN ROLLBACK AND CHAIN; INSERT INTO tbl VALUES (1); END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "rollback_chain");
            // TODO: Verify ROLLBACK AND CHAIN is parsed correctly
        }
    );
}

// ============================================================================
// Exception Handling with Transaction Control
// ============================================================================

#[test]
fn test_create_procedure_exception_with_rollback() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can use EXCEPTION blocks with transaction control
    pg_test!(
        r#"CREATE PROCEDURE exception_rollback() AS $$
BEGIN
    BEGIN
        INSERT INTO tbl VALUES (1);
        COMMIT;
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK;
        RAISE NOTICE 'Error occurred, rolled back';
    END;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "exception_rollback");
            // TODO: Verify EXCEPTION block with ROLLBACK
        }
    );
}

// ============================================================================
// SAVEPOINT Operations (LIKELY FAIL)
// ============================================================================

#[test]
fn test_create_procedure_savepoint() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can create SAVEPOINTs for partial rollback
    pg_test!(
        r#"CREATE PROCEDURE with_savepoint() AS $$
BEGIN
    INSERT INTO tbl VALUES (1);
    SAVEPOINT sp1;
    INSERT INTO tbl VALUES (2);
    ROLLBACK TO sp1;
    COMMIT;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "with_savepoint");
            // TODO: Verify SAVEPOINT and ROLLBACK TO statements
        }
    );
}

#[test]
fn test_create_procedure_release_savepoint() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // RELEASE SAVEPOINT destroys a savepoint
    pg_test!(
        r#"CREATE PROCEDURE release_savepoint() AS $$
BEGIN
    SAVEPOINT sp1;
    INSERT INTO tbl VALUES (1);
    RELEASE SAVEPOINT sp1;
    COMMIT;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "release_savepoint");
            // TODO: Verify RELEASE SAVEPOINT statement
        }
    );
}

#[test]
fn test_create_procedure_multiple_savepoints() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Procedures can have multiple nested savepoints
    pg_test!(
        r#"CREATE PROCEDURE nested_savepoints() AS $$
BEGIN
    INSERT INTO tbl VALUES (1);
    SAVEPOINT sp1;
    INSERT INTO tbl VALUES (2);
    SAVEPOINT sp2;
    INSERT INTO tbl VALUES (3);
    ROLLBACK TO sp2;
    ROLLBACK TO sp1;
    COMMIT;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "nested_savepoints");
            // TODO: Verify multiple SAVEPOINTs and ROLLBACKs
        }
    );
}

// ============================================================================
// Transaction Control with Loops
// ============================================================================

#[test]
fn test_create_procedure_commit_in_loop() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Common pattern: COMMIT in a loop for batch processing
    pg_expect_parse_error!(
        r#"CREATE PROCEDURE batch_insert(n INTEGER) AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..n LOOP
        INSERT INTO tbl VALUES (i);
        IF i % 100 = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    COMMIT;
END
$$"#
    );
}

#[test]
fn test_create_procedure_rollback_in_loop() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Pattern: try/rollback in a loop
    pg_test!(
        r#"CREATE PROCEDURE try_insert(vals INTEGER[]) AS $$
DECLARE
    v INTEGER;
BEGIN
    FOREACH v IN ARRAY vals LOOP
        BEGIN
            INSERT INTO tbl VALUES (v);
            COMMIT;
        EXCEPTION WHEN OTHERS THEN
            ROLLBACK;
        END;
    END LOOP;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "try_insert");
            // TODO: Verify FOREACH loop with COMMIT/ROLLBACK
        }
    );
}

// ============================================================================
// Restrictions and Invalid Cases
// ============================================================================

#[test]
fn test_create_procedure_commit_in_subtransaction_invalid() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // COMMIT is not allowed in a subtransaction (EXCEPTION block creates one)
    // This should parse but would fail at runtime
    pg_test!(
        r#"CREATE PROCEDURE invalid_commit() AS $$
BEGIN
    BEGIN
        INSERT INTO tbl VALUES (1);
        COMMIT;  -- Invalid: can't COMMIT in EXCEPTION block's subtransaction
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "invalid_commit");
            // This parses but would fail at runtime in PostgreSQL
        }
    );
}

// ============================================================================
// Mixed Transaction Control
// ============================================================================

#[test]
fn test_create_procedure_mixed_transaction_commands() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Complex procedure with various transaction commands
    pg_test!(
        r#"CREATE PROCEDURE complex_transaction() AS $$
BEGIN
    INSERT INTO log VALUES ('start');
    COMMIT;

    BEGIN
        INSERT INTO data VALUES (1);
        SAVEPOINT sp1;
        UPDATE data SET val = 2;
        ROLLBACK TO sp1;
        COMMIT;
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO errors VALUES (SQLERRM);
        COMMIT;
    END;

    INSERT INTO log VALUES ('end');
    COMMIT;
END
$$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "complex_transaction");
            // TODO: Verify all transaction control statements are parsed
        }
    );
}

// ============================================================================
// Transaction Control Keywords Case Insensitivity
// ============================================================================

#[test]
fn test_create_procedure_commit_case_insensitive() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // SQL keywords are case-insensitive
    pg_test!(
        "CREATE PROCEDURE commit_case() AS BEGIN commit; Commit; COMMIT; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "commit_case");
            // TODO: Verify multiple COMMIT statements regardless of case
        }
    );
}

#[test]
fn test_create_procedure_rollback_case_insensitive() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // SQL keywords are case-insensitive
    pg_test!(
        "CREATE PROCEDURE rollback_case() AS BEGIN rollback; Rollback; ROLLBACK; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.name.to_string(), "rollback_case");
            // TODO: Verify multiple ROLLBACK statements regardless of case
        }
    );
}
