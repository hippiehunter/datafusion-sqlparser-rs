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

//! Tests for CREATE OPERATOR syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createoperator.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::Statement;

// ============================================================================
// Basic CREATE OPERATOR Syntax
// ============================================================================

#[test]
fn test_create_operator_basic() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Basic binary operator with procedure
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_schema_qualified() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Operator with schema qualification
    pg_test!(
        "CREATE OPERATOR myschema.@@ (PROCEDURE = text_match, LEFTARG = TEXT, RIGHTARG = TEXT)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_function_keyword() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // FUNCTION is a synonym for PROCEDURE
    pg_test!(
        "CREATE OPERATOR @@ (FUNCTION = text_match, LEFTARG = TEXT, RIGHTARG = TEXT)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

// ============================================================================
// Unary Operators
// ============================================================================

#[test]
fn test_create_operator_left_unary() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Left unary operator (prefix)
    pg_test!(
        "CREATE OPERATOR ! (PROCEDURE = bigint_factorial, RIGHTARG = BIGINT)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_right_unary() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Right unary operator (postfix)
    pg_test!(
        "CREATE OPERATOR ! (PROCEDURE = bigint_factorial, LEFTARG = BIGINT)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

// ============================================================================
// Commutator and Negator
// ============================================================================

#[test]
fn test_create_operator_commutator() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // COMMUTATOR for commutative operators (a OP b = b OP a)
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, COMMUTATOR = =)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_negator() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // NEGATOR for logical negation (a OP b = NOT (a NEG b))
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, NEGATOR = <>)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_commutator_and_negator() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Both COMMUTATOR and NEGATOR
    pg_test!(
        "CREATE OPERATOR < (PROCEDURE = int4lt, LEFTARG = INT4, RIGHTARG = INT4, COMMUTATOR = >, NEGATOR = >=)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_commutator_qualified() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Schema-qualified commutator operator
    pg_test!(
        "CREATE OPERATOR myschema.= (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, COMMUTATOR = OPERATOR(myschema.=))",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

// ============================================================================
// Index Optimization (RESTRICT and JOIN)
// ============================================================================

#[test]
fn test_create_operator_restrict() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // RESTRICT selectivity estimation function
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, RESTRICT = eqsel)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_join() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // JOIN selectivity estimation function
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, JOIN = eqjoinsel)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_restrict_and_join() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Both RESTRICT and JOIN estimation functions
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, RESTRICT = eqsel, JOIN = eqjoinsel)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

// ============================================================================
// Operator Properties
// ============================================================================

#[test]
fn test_create_operator_hashes() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // HASHES indicates operator can support hash join
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, HASHES)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_merges() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // MERGES indicates operator can support merge join
    pg_test!(
        "CREATE OPERATOR < (PROCEDURE = int4lt, LEFTARG = INT4, RIGHTARG = INT4, MERGES)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_hashes_and_merges() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Both HASHES and MERGES
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, COMMUTATOR = =, HASHES, MERGES)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

// ============================================================================
// Complex Operator Examples
// ============================================================================

#[test]
fn test_create_operator_equals_full() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Complete equality operator definition
    pg_test!(
        "CREATE OPERATOR = (PROCEDURE = int4eq, LEFTARG = INT4, RIGHTARG = INT4, COMMUTATOR = =, NEGATOR = <>, RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_less_than_full() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Complete less-than operator definition
    pg_test!(
        "CREATE OPERATOR < (PROCEDURE = int4lt, LEFTARG = INT4, RIGHTARG = INT4, COMMUTATOR = >, NEGATOR = >=, RESTRICT = scalarltsel, JOIN = scalarltjoinsel)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_text_search() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Text search operator example
    pg_test!(
        "CREATE OPERATOR @@ (PROCEDURE = ts_match_vq, LEFTARG = TSVECTOR, RIGHTARG = TSQUERY, COMMUTATOR = @@, RESTRICT = contsel, JOIN = contjoinsel)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_geometric() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Geometric operator example
    pg_test!(
        "CREATE OPERATOR <-> (PROCEDURE = point_distance, LEFTARG = POINT, RIGHTARG = POINT, COMMUTATOR = <->)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}

#[test]
fn test_create_operator_array_contains() {
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    // Array containment operator
    pg_test!(
        "CREATE OPERATOR @> (PROCEDURE = array_contains, LEFTARG = ANYARRAY, RIGHTARG = ANYARRAY, COMMUTATOR = <@, RESTRICT = arraycontsel, JOIN = arraycontjoinsel)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperator(_)));
        }
    );
}
