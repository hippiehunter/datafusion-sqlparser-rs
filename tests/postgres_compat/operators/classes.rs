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

//! Tests for CREATE OPERATOR CLASS and CREATE OPERATOR FAMILY syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createopclass.html>
//! Reference: <https://www.postgresql.org/docs/current/sql-createopfamily.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::Statement;

// ============================================================================
// CREATE OPERATOR FAMILY
// ============================================================================

#[test]
fn test_create_operator_family_basic() {
    // https://www.postgresql.org/docs/current/sql-createopfamily.html
    // Basic operator family for an index method
    pg_test!(
        "CREATE OPERATOR FAMILY integer_ops USING btree",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorFamily(_)));
        }
    );
}

#[test]
fn test_create_operator_family_schema_qualified() {
    // https://www.postgresql.org/docs/current/sql-createopfamily.html
    // Schema-qualified operator family
    pg_test!(
        "CREATE OPERATOR FAMILY myschema.text_ops USING hash",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorFamily(_)));
        }
    );
}

#[test]
fn test_create_operator_family_gist() {
    // https://www.postgresql.org/docs/current/sql-createopfamily.html
    // GiST index operator family
    pg_test!(
        "CREATE OPERATOR FAMILY gist_geometry_ops USING gist",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorFamily(_)));
        }
    );
}

#[test]
fn test_create_operator_family_gin() {
    // https://www.postgresql.org/docs/current/sql-createopfamily.html
    // GIN index operator family
    pg_test!(
        "CREATE OPERATOR FAMILY gin_trgm_ops USING gin",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorFamily(_)));
        }
    );
}

#[test]
fn test_create_operator_family_spgist() {
    // https://www.postgresql.org/docs/current/sql-createopfamily.html
    // SP-GiST index operator family
    pg_test!(
        "CREATE OPERATOR FAMILY quad_point_ops USING spgist",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorFamily(_)));
        }
    );
}

#[test]
fn test_create_operator_family_brin() {
    // https://www.postgresql.org/docs/current/sql-createopfamily.html
    // BRIN index operator family
    pg_test!(
        "CREATE OPERATOR FAMILY brin_minmax_ops USING brin",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorFamily(_)));
        }
    );
}

// ============================================================================
// CREATE OPERATOR CLASS - Basic
// ============================================================================

#[test]
fn test_create_operator_class_basic() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Basic operator class
    pg_test!(
        "CREATE OPERATOR CLASS int4_ops
            FOR TYPE int4 USING btree AS
            OPERATOR 1 <,
            OPERATOR 2 <=,
            OPERATOR 3 =,
            OPERATOR 4 >=,
            OPERATOR 5 >,
            FUNCTION 1 btint4cmp(int4, int4)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

#[test]
fn test_create_operator_class_default() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // DEFAULT operator class for a type
    pg_test!(
        "CREATE OPERATOR CLASS text_ops
            DEFAULT FOR TYPE text USING btree AS
            OPERATOR 1 <,
            OPERATOR 2 <=,
            OPERATOR 3 =,
            OPERATOR 4 >=,
            OPERATOR 5 >,
            FUNCTION 1 bttextcmp(text, text)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

#[test]
fn test_create_operator_class_family() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Operator class with FAMILY
    pg_test!(
        "CREATE OPERATOR CLASS int4_ops
            FOR TYPE int4 USING btree FAMILY integer_ops AS
            OPERATOR 1 <,
            OPERATOR 2 <=,
            OPERATOR 3 =,
            OPERATOR 4 >=,
            OPERATOR 5 >,
            FUNCTION 1 btint4cmp(int4, int4)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

// ============================================================================
// CREATE OPERATOR CLASS - Operators
// ============================================================================

#[test]
fn test_create_operator_class_operator_for_order_by() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // OPERATOR with FOR ORDER BY (for distance operators)
    pg_test!(
        "CREATE OPERATOR CLASS gist_point_ops
            FOR TYPE point USING gist AS
            OPERATOR 1 <<,
            OPERATOR 15 <-> FOR ORDER BY float_ops",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

#[test]
fn test_create_operator_class_operator_qualified() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Operator with schema qualification
    pg_test!(
        "CREATE OPERATOR CLASS int4_ops
            FOR TYPE int4 USING btree AS
            OPERATOR 1 pg_catalog.<(int4, int4),
            OPERATOR 3 OPERATOR(pg_catalog.=)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

#[test]
fn test_create_operator_class_operator_recheck() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // OPERATOR with RECHECK (for lossy index types)
    pg_test!(
        "CREATE OPERATOR CLASS gist_int4_ops
            FOR TYPE int4 USING gist AS
            OPERATOR 1 <,
            OPERATOR 3 = RECHECK",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

// ============================================================================
// CREATE OPERATOR CLASS - Functions
// ============================================================================

#[test]
fn test_create_operator_class_function_support() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Support function (not operator function)
    pg_test!(
        "CREATE OPERATOR CLASS array_ops
            FOR TYPE anyarray USING gin AS
            OPERATOR 1 &&,
            OPERATOR 2 @>,
            OPERATOR 3 <@,
            OPERATOR 4 =,
            FUNCTION 1 btarraycmp(anyarray, anyarray),
            FUNCTION 2 ginarrayextract(anyarray, internal),
            FUNCTION 3 ginqueryarrayextract(anyarray, internal, int2, internal, internal)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

#[test]
fn test_create_operator_class_function_for_order_by() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Support function FOR ORDER BY
    pg_test!(
        "CREATE OPERATOR CLASS gist_point_ops
            FOR TYPE point USING gist AS
            OPERATOR 15 <-> FOR ORDER BY float_ops,
            FUNCTION 8 (point, point) point_distance(point, point) FOR ORDER BY float_ops",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

// ============================================================================
// CREATE OPERATOR CLASS - Storage
// ============================================================================

#[test]
fn test_create_operator_class_storage() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // STORAGE clause for index tuple storage type
    pg_test!(
        "CREATE OPERATOR CLASS gist_int4_ops
            FOR TYPE int4 USING gist AS
            OPERATOR 1 <,
            OPERATOR 2 <=,
            OPERATOR 3 =,
            OPERATOR 4 >=,
            OPERATOR 5 >,
            FUNCTION 1 gist_int4_consistent(internal, int4, int2, oid, internal),
            FUNCTION 2 gist_int4_union(internal, internal),
            STORAGE int4",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

// ============================================================================
// CREATE OPERATOR CLASS - Hash
// ============================================================================

#[test]
fn test_create_operator_class_hash() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Hash index operator class
    pg_test!(
        "CREATE OPERATOR CLASS int4_ops
            FOR TYPE int4 USING hash AS
            OPERATOR 1 =,
            FUNCTION 1 hashint4(int4)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

#[test]
fn test_create_operator_class_hash_extended() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Hash index with extended hash function (PostgreSQL 10+)
    pg_test!(
        "CREATE OPERATOR CLASS text_ops
            FOR TYPE text USING hash AS
            OPERATOR 1 =,
            FUNCTION 1 hashtext(text),
            FUNCTION 2 hashtextextended(text, int8)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

// ============================================================================
// CREATE OPERATOR CLASS - GiST
// ============================================================================

#[test]
fn test_create_operator_class_gist_complete() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Complete GiST operator class
    pg_test!(
        "CREATE OPERATOR CLASS gist_box_ops
            DEFAULT FOR TYPE box USING gist AS
            OPERATOR 1 << ,
            OPERATOR 2 &< ,
            OPERATOR 3 &&,
            OPERATOR 4 &>,
            OPERATOR 5 >>,
            OPERATOR 6 ~=,
            OPERATOR 7 ~,
            OPERATOR 8 @,
            FUNCTION 1 gist_box_consistent(internal, box, int4),
            FUNCTION 2 gist_box_union(internal, internal),
            FUNCTION 3 gist_box_compress(internal),
            FUNCTION 4 gist_box_decompress(internal),
            FUNCTION 5 gist_box_penalty(internal, internal, internal),
            FUNCTION 6 gist_box_picksplit(internal, internal),
            FUNCTION 7 gist_box_same(box, box, internal)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

// ============================================================================
// CREATE OPERATOR CLASS - Complex Examples
// ============================================================================

#[test]
fn test_create_operator_class_array_gin() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // GIN operator class for arrays
    pg_test!(
        "CREATE OPERATOR CLASS gin_array_ops
            DEFAULT FOR TYPE anyarray USING gin AS
            OPERATOR 1 &&,
            OPERATOR 2 @>,
            OPERATOR 3 <@,
            OPERATOR 4 =,
            FUNCTION 1 btarraycmp(anyarray, anyarray),
            FUNCTION 2 ginarrayextract(anyarray, internal, internal),
            FUNCTION 3 ginqueryarrayextract(anyarray, internal, int2, internal, internal, internal, internal),
            FUNCTION 4 ginarrayconsistent(internal, int2, anyarray, int4, internal, internal, internal, internal)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}

#[test]
fn test_create_operator_class_schema_qualified() {
    // https://www.postgresql.org/docs/current/sql-createopclass.html
    // Schema-qualified operator class
    pg_test!(
        "CREATE OPERATOR CLASS myschema.int4_ops
            FOR TYPE int4 USING btree AS
            OPERATOR 1 <,
            FUNCTION 1 btint4cmp(int4, int4)",
        |stmt: Statement| {
            assert!(matches!(stmt, Statement::CreateOperatorClass(_)));
        }
    );
}
