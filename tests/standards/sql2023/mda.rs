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

//! SQL/MDA (Multi-Dimensional Arrays) Standards Compliance Tests
//!
//! SQL/MDA (ISO/IEC 9075-15:2023) defines multi-dimensional array support in SQL.
//! This module tests parsing support for MDARRAY types and expressions.
//!
//! ## Feature Coverage
//!
//! ### MDARRAY Type Definitions
//! - MDA-T01: 1D MDARRAY type (`INTEGER MDARRAY[x]`)
//! - MDA-T02: 2D MDARRAY type (`FLOAT MDARRAY[x, y]`)
//! - MDA-T03: 3D MDARRAY type (`INTEGER MDARRAY[x, y, z]`)
//! - MDA-T04: MDARRAY with spatiotemporal dimensions (`INTEGER MDARRAY[time, lat, lon]`)
//!
//! ### MDARRAY Constructor Expressions
//! - MDA-E01: Simple 1D MDARRAY constructor (`MDARRAY[x(0:2)] [0, 1, 2]`)
//! - MDA-E02: 2D MDARRAY constructor (`MDARRAY[x(1:2), y(1:2)] [1, 2, 5, 6]`)
//! - MDA-E03: MDARRAY with open upper bound (`MDARRAY[x(0:*)] [1, 2, 3]`)
//! - MDA-E04: MDARRAY with open lower bound (`MDARRAY[x(*:10)] [1, 2, 3]`)
//! - MDA-E05: MDARRAY without bounds (`MDARRAY[x, y] [1, 2, 3, 4]`)
//! - MDA-E06: Empty MDARRAY constructor
//!
//! ### MDARRAY in DDL Statements
//! - MDA-D01: CREATE TABLE with MDARRAY column
//! - MDA-D02: CREATE TABLE with multi-dimensional MDARRAY
//!
//! ### MDARRAY in DML Statements
//! - MDA-M01: INSERT with MDARRAY value
//! - MDA-M02: SELECT with MDARRAY constructor
//!
//! Reference: ISO/IEC 9075-15:2023 (SQL/MDA)
//! See: <https://www.iso.org/standard/84807.html>

use crate::standards::common::{try_parse, verified_standard_stmt};
use sqlparser::ast::{DataType, Expr, MdArray, SelectItem, Statement};

// ==================== MDA-T: MDARRAY Type Definitions ====================

#[test]
fn mda_t01_1d_mdarray_type() {
    // SQL/MDA: 1D MDARRAY type declaration
    let stmt = verified_standard_stmt("CREATE TABLE arrays (a INTEGER MDARRAY[x])");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            let col = &ct.columns[0];
            assert_eq!(col.name.value, "a");
            match &col.data_type {
                DataType::MdArray(def) => {
                    match def.element_type.as_ref() {
                        DataType::Int(None) | DataType::Integer(None) => {}
                        other => panic!("Expected INTEGER type, got {:?}", other),
                    }
                    assert_eq!(def.dimensions.len(), 1);
                    assert_eq!(def.dimensions[0].value, "x");
                }
                _ => panic!("Expected MdArray type"),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn mda_t02_2d_mdarray_type() {
    // SQL/MDA: 2D MDARRAY type declaration
    let stmt = verified_standard_stmt("CREATE TABLE matrices (m FLOAT MDARRAY[x, y])");
    match stmt {
        Statement::CreateTable(ct) => {
            let col = &ct.columns[0];
            match &col.data_type {
                DataType::MdArray(def) => {
                    assert_eq!(def.dimensions.len(), 2);
                    assert_eq!(def.dimensions[0].value, "x");
                    assert_eq!(def.dimensions[1].value, "y");
                }
                _ => panic!("Expected MdArray type"),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn mda_t03_3d_mdarray_type() {
    // SQL/MDA: 3D MDARRAY type declaration
    let stmt = verified_standard_stmt("CREATE TABLE cubes (c INTEGER MDARRAY[x, y, z])");
    match stmt {
        Statement::CreateTable(ct) => {
            let col = &ct.columns[0];
            match &col.data_type {
                DataType::MdArray(def) => {
                    assert_eq!(def.dimensions.len(), 3);
                    assert_eq!(def.dimensions[0].value, "x");
                    assert_eq!(def.dimensions[1].value, "y");
                    assert_eq!(def.dimensions[2].value, "z");
                }
                _ => panic!("Expected MdArray type"),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn mda_t04_spatiotemporal_mdarray_type() {
    // SQL/MDA: MDARRAY with spatiotemporal dimensions (common in scientific data)
    let stmt =
        verified_standard_stmt("CREATE TABLE geodata (raster INTEGER MDARRAY[time, lat, lon])");
    match stmt {
        Statement::CreateTable(ct) => {
            let col = &ct.columns[0];
            match &col.data_type {
                DataType::MdArray(def) => {
                    assert_eq!(def.dimensions.len(), 3);
                    assert_eq!(def.dimensions[0].value, "time");
                    assert_eq!(def.dimensions[1].value, "lat");
                    assert_eq!(def.dimensions[2].value, "lon");
                }
                _ => panic!("Expected MdArray type"),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn mda_t05_mdarray_with_double_type() {
    // SQL/MDA: MDARRAY with DOUBLE PRECISION element type
    let stmt = verified_standard_stmt(
        "CREATE TABLE measurements (data DOUBLE PRECISION MDARRAY[sensor, timestamp])",
    );
    match stmt {
        Statement::CreateTable(ct) => {
            let col = &ct.columns[0];
            match &col.data_type {
                DataType::MdArray(def) => {
                    assert!(matches!(*def.element_type, DataType::DoublePrecision));
                    assert_eq!(def.dimensions.len(), 2);
                }
                _ => panic!("Expected MdArray type"),
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

// ==================== MDA-E: MDARRAY Constructor Expressions ====================

#[test]
fn mda_e01_simple_1d_constructor() {
    // SQL/MDA: Simple 1D MDARRAY constructor with bounds
    let stmt = verified_standard_stmt("SELECT MDARRAY[x(0:2)] [0, 1, 2]");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, values })) => {
                        // Check dimension
                        assert_eq!(dimensions.len(), 1);
                        assert_eq!(dimensions[0].name.value, "x");
                        // Check bounds
                        assert!(dimensions[0].lower_bound.is_some());
                        assert!(dimensions[0].upper_bound.is_some());
                        // Check values
                        assert_eq!(values.len(), 3);
                    }
                    other => panic!("Expected MdArray expression, got {:?}", other),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn mda_e02_2d_constructor() {
    // SQL/MDA: 2D MDARRAY constructor
    let stmt = verified_standard_stmt("SELECT MDARRAY[x(1:2), y(1:2)] [1, 2, 5, 6]");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, values })) => {
                        assert_eq!(dimensions.len(), 2);
                        assert_eq!(dimensions[0].name.value, "x");
                        assert_eq!(dimensions[1].name.value, "y");
                        assert_eq!(values.len(), 4);
                    }
                    _ => panic!("Expected MdArray expression"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn mda_e03_open_upper_bound() {
    // SQL/MDA: MDARRAY with open upper bound (wildcard)
    let stmt = verified_standard_stmt("SELECT MDARRAY[x(0:*)] [1, 2, 3]");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, .. })) => {
                        assert_eq!(dimensions.len(), 1);
                        assert!(dimensions[0].lower_bound.is_some());
                        assert!(dimensions[0].upper_bound.is_none()); // Open upper bound
                    }
                    _ => panic!("Expected MdArray expression"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn mda_e04_open_lower_bound() {
    // SQL/MDA: MDARRAY with open lower bound (wildcard)
    let stmt = verified_standard_stmt("SELECT MDARRAY[x(*:10)] [1, 2, 3]");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, .. })) => {
                        assert_eq!(dimensions.len(), 1);
                        assert!(dimensions[0].lower_bound.is_none()); // Open lower bound
                        assert!(dimensions[0].upper_bound.is_some());
                    }
                    _ => panic!("Expected MdArray expression"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn mda_e05_dimensions_without_bounds() {
    // SQL/MDA: MDARRAY with named dimensions but no explicit bounds
    let stmt = verified_standard_stmt("SELECT MDARRAY[x, y] [1, 2, 3, 4]");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, values })) => {
                        assert_eq!(dimensions.len(), 2);
                        assert_eq!(dimensions[0].name.value, "x");
                        assert_eq!(dimensions[1].name.value, "y");
                        // Both bounds should be None when not specified
                        assert!(dimensions[0].lower_bound.is_none());
                        assert!(dimensions[0].upper_bound.is_none());
                        assert!(dimensions[1].lower_bound.is_none());
                        assert!(dimensions[1].upper_bound.is_none());
                        assert_eq!(values.len(), 4);
                    }
                    _ => panic!("Expected MdArray expression"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn mda_e06_empty_constructor() {
    // SQL/MDA: Empty MDARRAY constructor
    let stmt = verified_standard_stmt("SELECT MDARRAY[x(0:0)] []");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, values })) => {
                        assert_eq!(dimensions.len(), 1);
                        assert_eq!(values.len(), 0);
                    }
                    _ => panic!("Expected MdArray expression"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn mda_e07_3d_constructor() {
    // SQL/MDA: 3D MDARRAY constructor
    let stmt =
        verified_standard_stmt("SELECT MDARRAY[x(0:1), y(0:1), z(0:1)] [1, 2, 3, 4, 5, 6, 7, 8]");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, values })) => {
                        assert_eq!(dimensions.len(), 3);
                        assert_eq!(values.len(), 8);
                    }
                    _ => panic!("Expected MdArray expression"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn mda_e08_expression_bounds() {
    // SQL/MDA: MDARRAY with expression-based bounds
    let stmt = verified_standard_stmt("SELECT MDARRAY[x(1:10)] [1, 2, 3]");
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::MdArray(MdArray { dimensions, .. })) => {
                        assert_eq!(dimensions.len(), 1);
                        // Verify bounds are numeric literals
                        match &dimensions[0].lower_bound {
                            Some(Expr::Value(v)) => {
                                assert!(v.to_string().contains('1'));
                            }
                            _ => panic!("Expected numeric lower bound"),
                        }
                    }
                    _ => panic!("Expected MdArray expression"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

// ==================== MDA-D: MDARRAY in DDL Statements ====================

#[test]
fn mda_d01_create_table_with_mdarray() {
    // SQL/MDA: CREATE TABLE with MDARRAY column
    verified_standard_stmt("CREATE TABLE sensor_data (id INTEGER, readings FLOAT MDARRAY[time])");
}

#[test]
fn mda_d02_create_table_multidimensional() {
    // SQL/MDA: CREATE TABLE with multi-dimensional MDARRAY
    verified_standard_stmt(
        "CREATE TABLE images (id INTEGER, pixels INTEGER MDARRAY[row, col, channel])",
    );
}

#[test]
fn mda_d03_create_table_multiple_mdarray_columns() {
    // SQL/MDA: CREATE TABLE with multiple MDARRAY columns
    verified_standard_stmt(
        "CREATE TABLE data (id INTEGER, x FLOAT MDARRAY[i], y FLOAT MDARRAY[j])",
    );
}

// ==================== MDA-M: MDARRAY in DML Statements ====================

#[test]
fn mda_m01_insert_with_mdarray() {
    // SQL/MDA: INSERT statement with MDARRAY constructor value
    verified_standard_stmt("INSERT INTO arrays VALUES (1, MDARRAY[x(0:2)] [10, 20, 30])");
}

#[test]
fn mda_m02_select_with_mdarray() {
    // SQL/MDA: SELECT with MDARRAY constructor in column list
    verified_standard_stmt("SELECT MDARRAY[x(0:2)] [1, 2, 3] AS arr");
}

#[test]
fn mda_m03_mdarray_in_expression() {
    // SQL/MDA: MDARRAY used in an expression context
    verified_standard_stmt("SELECT id, MDARRAY[x(1:3)] [a, b, c] FROM data");
}

// ==================== Round-trip Tests ====================

#[test]
fn mda_roundtrip_type() {
    // Verify MDARRAY type round-trips correctly
    let sql = "CREATE TABLE t (a INTEGER MDARRAY[x, y])";
    let stmt = verified_standard_stmt(sql);
    let reparsed = stmt.to_string();
    assert_eq!(sql, reparsed);
}

#[test]
fn mda_roundtrip_constructor_with_bounds() {
    // Verify MDARRAY constructor with bounds round-trips correctly
    let sql = "SELECT MDARRAY[x(0:2)] [1, 2, 3]";
    let stmt = verified_standard_stmt(sql);
    let reparsed = stmt.to_string();
    assert_eq!(sql, reparsed);
}

#[test]
fn mda_roundtrip_constructor_without_bounds() {
    // Verify MDARRAY constructor without bounds round-trips correctly
    let sql = "SELECT MDARRAY[x, y] [1, 2, 3, 4]";
    let stmt = verified_standard_stmt(sql);
    let reparsed = stmt.to_string();
    assert_eq!(sql, reparsed);
}

#[test]
fn mda_roundtrip_open_bounds() {
    // Verify MDARRAY with open bounds round-trips correctly
    let sql_upper = "SELECT MDARRAY[x(0:*)] [1, 2, 3]";
    let stmt_upper = verified_standard_stmt(sql_upper);
    assert_eq!(sql_upper, stmt_upper.to_string());

    let sql_lower = "SELECT MDARRAY[x(*:10)] [1, 2, 3]";
    let stmt_lower = verified_standard_stmt(sql_lower);
    assert_eq!(sql_lower, stmt_lower.to_string());
}

// ==================== Error Cases ====================

#[test]
fn mda_error_missing_dimension_brackets() {
    // MDARRAY keyword without dimension brackets should fail
    let result = try_parse("SELECT MDARRAY [1, 2, 3]");
    assert!(result.is_err());
}

#[test]
fn mda_fallback_to_identifier_without_value_brackets() {
    // When MDARRAY syntax is incomplete (missing value brackets), it falls back
    // to being parsed as an identifier with subscript access
    // This is consistent with SQL keyword fallback behavior
    let result = try_parse("SELECT MDARRAY[x(0:2)]");
    assert!(result.is_ok());
    // It parses as identifier MDARRAY with subscript [x(0:2)]
    match result {
        Ok(stmts) => {
            assert_eq!(stmts.len(), 1);
            // The expression is a CompoundFieldAccess with MDARRAY as identifier
        }
        Err(_) => panic!("Expected successful parse with identifier fallback"),
    }
}

// ==================== Integration Tests ====================

#[test]
fn mda_integration_subquery_with_mdarray() {
    // SQL/MDA: MDARRAY in a subquery
    verified_standard_stmt("SELECT * FROM (SELECT MDARRAY[x(0:2)] [1, 2, 3] AS arr) AS sub");
}

#[test]
fn mda_integration_cte_with_mdarray() {
    // SQL/MDA: MDARRAY in a CTE
    verified_standard_stmt(
        "WITH arr_data AS (SELECT MDARRAY[dim(0:2)] [1, 2, 3] AS arr) SELECT * FROM arr_data",
    );
}
