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

//! SQL:2023 JSON Improvements Tests
//!
//! JSON enhancements introduced in SQL:2023.
//!
//! ## Feature Coverage
//!
//! - T860-T864: JSON simplified accessor (dot notation)
//!   - T860: Simple field access (t.data.field)
//!   - T861: Array subscript access (t.data.items[0])
//!   - T862: Array wildcard (t.data.items[*]) - NOT YET IMPLEMENTED
//!   - T863: Combined access patterns
//!   - T864: JSON accessor in WHERE/ORDER BY
//!
//! - T865-T878: JSON item methods
//!   - T865: .bigint()
//!   - T866: .boolean()
//!   - T867: .date()
//!   - T868: .decimal() without precision
//!   - T869: .decimal(precision, scale)
//!   - T870: .integer()
//!   - T871: .number()
//!   - T872: .string()
//!   - T873: .time()
//!   - T874: .time_tz()
//!   - T875: .time() with precision (not tested)
//!   - T876: .timestamp()
//!   - T877: .timestamp_tz()
//!   - T878: .timestamp() with precision (not tested)
//!
//! - T879-T882: JSON comparison
//!   - T879: JSON equality/inequality
//!   - T880: JSON ordering (< > BETWEEN)
//!   - T881: JSON in GROUP BY
//!   - T882: JSON distinctness (DISTINCT, ORDER BY)

use crate::standards::common::{one_statement_parses_to_std, verified_standard_stmt};
use sqlparser::ast::{AccessExpr, Expr, SelectItem, Statement, Subscript};

// ==================== T860-T864: JSON Simplified Accessor ====================

#[test]
fn t860_01_json_dot_notation_simple() {
    // SQL:2023 T860: JSON simplified accessor - simple field access
    let stmt = one_statement_parses_to_std(
        "SELECT t.data.name FROM users t",
        "SELECT t.data.name FROM users AS t",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                assert_eq!(select.projection.len(), 1);
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                        assert_eq!(idents.len(), 3);
                        assert_eq!(idents[0].value, "t");
                        assert_eq!(idents[1].value, "data");
                        assert_eq!(idents[2].value, "name");
                    }
                    _ => panic!("Expected CompoundIdentifier"),
                }
            } else {
                panic!("Expected Select");
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t860_02_json_dot_notation_nested() {
    // SQL:2023 T860: JSON simplified accessor - nested field access
    let stmt = one_statement_parses_to_std(
        "SELECT t.profile.address.city FROM users t",
        "SELECT t.profile.address.city FROM users AS t",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                        assert_eq!(idents.len(), 4);
                        assert_eq!(idents[0].value, "t");
                        assert_eq!(idents[1].value, "profile");
                        assert_eq!(idents[2].value, "address");
                        assert_eq!(idents[3].value, "city");
                    }
                    _ => panic!("Expected CompoundIdentifier"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t860_03_json_dot_notation_deep_nesting() {
    // SQL:2023 T860: JSON simplified accessor - deeply nested fields
    let stmt = one_statement_parses_to_std(
        "SELECT doc.level1.level2.level3.level4.value FROM documents doc",
        "SELECT doc.level1.level2.level3.level4.value FROM documents AS doc",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                        assert_eq!(idents.len(), 6);
                        assert_eq!(idents[0].value, "doc");
                        assert_eq!(idents[5].value, "value");
                    }
                    _ => panic!("Expected CompoundIdentifier"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t861_01_json_array_subscript_basic() {
    // SQL:2023 T861: JSON array subscript access
    let stmt = one_statement_parses_to_std(
        "SELECT t.data.items[0] FROM orders t",
        "SELECT t.data.items[0] FROM orders AS t",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::CompoundFieldAccess { root, access_chain }) => {
                        // Root should be identifier "t"
                        assert!(matches!(**root, Expr::Identifier(_)));
                        // Should have 3 access elements: .data, .items, [0]
                        assert_eq!(access_chain.len(), 3);
                        // Last should be a subscript
                        assert!(matches!(
                            access_chain[2],
                            AccessExpr::Subscript(Subscript::Index { .. })
                        ));
                    }
                    _ => panic!(
                        "Expected CompoundFieldAccess, got {:?}",
                        &select.projection[0]
                    ),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t861_02_json_array_subscript_nested() {
    // SQL:2023 T861: JSON array subscript with nested access
    let stmt = one_statement_parses_to_std(
        "SELECT t.data.items[0].name FROM orders t",
        "SELECT t.data.items[0].name FROM orders AS t",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::CompoundFieldAccess { access_chain, .. }) => {
                        // Should have 4 access elements: .data, .items, [0], .name
                        assert_eq!(access_chain.len(), 4);
                        // Third should be subscript, fourth should be dot
                        assert!(matches!(
                            access_chain[2],
                            AccessExpr::Subscript(Subscript::Index { .. })
                        ));
                        assert!(matches!(access_chain[3], AccessExpr::Dot(_)));
                    }
                    _ => panic!("Expected CompoundFieldAccess"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t861_03_json_array_multiple_subscripts() {
    // SQL:2023 T861: JSON multiple array subscripts
    let stmt = one_statement_parses_to_std(
        "SELECT t.matrix[0][1] FROM data t",
        "SELECT t.matrix[0][1] FROM data AS t",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::CompoundFieldAccess { access_chain, .. }) => {
                        // Should have 3 access elements: .matrix, [0], [1]
                        assert_eq!(access_chain.len(), 3);
                        // Last two should be subscripts
                        assert!(matches!(
                            access_chain[1],
                            AccessExpr::Subscript(Subscript::Index { .. })
                        ));
                        assert!(matches!(
                            access_chain[2],
                            AccessExpr::Subscript(Subscript::Index { .. })
                        ));
                    }
                    _ => panic!("Expected CompoundFieldAccess"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t862_01_json_array_wildcard() {
    // SQL:2023 T862: JSON array wildcard accessor
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT t.data.items[*].price FROM products t");
}

#[test]
fn t862_02_json_array_wildcard_nested() {
    // SQL:2023 T862: JSON array wildcard with nested fields
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT doc.orders[*].items[*].sku FROM documents doc");
}

#[test]
fn t863_01_json_combined_access() {
    // SQL:2023 T863: Combined dot notation and array subscript
    let stmt = one_statement_parses_to_std(
        "SELECT t.data.list[0].field.nested[1] FROM complex t",
        "SELECT t.data.list[0].field.nested[1] FROM complex AS t",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::CompoundFieldAccess { access_chain, .. }) => {
                        // Should have complex chain: .data, .list, [0], .field, .nested, [1]
                        assert_eq!(access_chain.len(), 6);
                        // Check pattern: dot, dot, subscript, dot, dot, subscript
                        assert!(matches!(access_chain[0], AccessExpr::Dot(_)));
                        assert!(matches!(access_chain[1], AccessExpr::Dot(_)));
                        assert!(matches!(access_chain[2], AccessExpr::Subscript(_)));
                        assert!(matches!(access_chain[3], AccessExpr::Dot(_)));
                        assert!(matches!(access_chain[4], AccessExpr::Dot(_)));
                        assert!(matches!(access_chain[5], AccessExpr::Subscript(_)));
                    }
                    _ => panic!("Expected CompoundFieldAccess"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t864_01_json_accessor_in_where() {
    // SQL:2023 T864: JSON accessor in WHERE clause
    one_statement_parses_to_std(
        "SELECT * FROM users t WHERE t.data.age > 18",
        "SELECT * FROM users AS t WHERE t.data.age > 18",
    );
}

#[test]
fn t864_02_json_accessor_in_order_by() {
    // SQL:2023 T864: JSON accessor in ORDER BY clause
    one_statement_parses_to_std(
        "SELECT * FROM users t ORDER BY t.data.created_at DESC",
        "SELECT * FROM users AS t ORDER BY t.data.created_at DESC",
    );
}

// ==================== T865-T878: JSON Item Methods ====================

#[test]
fn t865_01_json_bigint_method() {
    // SQL:2023 T865: JSON .bigint() item method
    let stmt = one_statement_parses_to_std(
        "SELECT t.data.id.bigint() FROM records t",
        "SELECT t.data.id.bigint() FROM records AS t",
    );
    match stmt {
        Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                match &select.projection[0] {
                    SelectItem::UnnamedExpr(Expr::Function(func)) => {
                        // Function name should be compound: t.data.id.bigint
                        assert_eq!(func.name.0.len(), 4);
                        if let sqlparser::ast::ObjectNamePart::Identifier(ident) = &func.name.0[3] {
                            assert_eq!(ident.value, "bigint");
                        } else {
                            panic!("Expected Identifier");
                        }
                    }
                    _ => panic!("Expected Function"),
                }
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn t866_01_json_boolean_method() {
    // SQL:2023 T866: JSON .boolean() item method
    one_statement_parses_to_std(
        "SELECT t.data.is_active.boolean() FROM users t",
        "SELECT t.data.is_active.boolean() FROM users AS t",
    );
}

#[test]
fn t867_01_json_date_method() {
    // SQL:2023 T867: JSON .date() item method
    one_statement_parses_to_std(
        "SELECT t.data.birth_date.date() FROM users t",
        "SELECT t.data.birth_date.date() FROM users AS t",
    );
}

#[test]
fn t868_01_json_decimal_method() {
    // SQL:2023 T868: JSON .decimal() item method
    one_statement_parses_to_std(
        "SELECT t.data.price.decimal() FROM products t",
        "SELECT t.data.price.decimal() FROM products AS t",
    );
}

#[test]
fn t869_01_json_decimal_method_precision() {
    // SQL:2023 T869: JSON .decimal() with precision and scale
    // Note: T868 is .decimal() without params, T869 adds precision/scale support
    one_statement_parses_to_std(
        "SELECT t.data.amount.decimal(10, 2) FROM transactions t",
        "SELECT t.data.amount.decimal(10, 2) FROM transactions AS t",
    );
}

#[test]
fn t870_01_json_integer_method() {
    // SQL:2023 T870: JSON .integer() item method
    one_statement_parses_to_std(
        "SELECT t.data.count.integer() FROM stats t",
        "SELECT t.data.count.integer() FROM stats AS t",
    );
}

#[test]
fn t871_01_json_number_method() {
    // SQL:2023 T871: JSON .number() item method (DOUBLE PRECISION)
    one_statement_parses_to_std(
        "SELECT t.data.value.number() FROM measurements t",
        "SELECT t.data.value.number() FROM measurements AS t",
    );
}

#[test]
fn t872_01_json_string_method() {
    // SQL:2023 T872: JSON .string() item method
    one_statement_parses_to_std(
        "SELECT t.data.name.string() FROM entities t",
        "SELECT t.data.name.string() FROM entities AS t",
    );
}

#[test]
fn t873_01_json_time_method() {
    // SQL:2023 T873: JSON .time() item method
    one_statement_parses_to_std(
        "SELECT t.data.start_time.time() FROM events t",
        "SELECT t.data.start_time.time() FROM events AS t",
    );
}

#[test]
fn t874_01_json_time_tz_method() {
    // SQL:2023 T874: JSON .time_tz() item method
    one_statement_parses_to_std(
        "SELECT t.data.scheduled_time.time_tz() FROM appointments t",
        "SELECT t.data.scheduled_time.time_tz() FROM appointments AS t",
    );
}

#[test]
fn t876_01_json_timestamp_method() {
    // SQL:2023 T876: JSON .timestamp() item method
    // Note: T875 is .time() with precision (skipped as likely not widely implemented)
    one_statement_parses_to_std(
        "SELECT t.data.created_at.timestamp() FROM records t",
        "SELECT t.data.created_at.timestamp() FROM records AS t",
    );
}

#[test]
fn t877_01_json_timestamp_tz_method() {
    // SQL:2023 T877: JSON .timestamp_tz() item method
    one_statement_parses_to_std(
        "SELECT t.data.modified_at.timestamp_tz() FROM logs t",
        "SELECT t.data.modified_at.timestamp_tz() FROM logs AS t",
    );
}

#[test]
fn t878_01_json_method_chaining() {
    // SQL:2023 T878: Chaining JSON accessors with item methods
    // Note: T878 is .timestamp() with precision in the standard, but we test
    // method chaining here as a general integration test
    one_statement_parses_to_std(
        "SELECT t.data.nested.value.integer() FROM complex t",
        "SELECT t.data.nested.value.integer() FROM complex AS t",
    );
}

#[test]
fn json_method_with_array_integration() {
    // Integration test: JSON item methods with array access
    one_statement_parses_to_std(
        "SELECT t.data.items[0].price.decimal(10, 2) FROM orders t",
        "SELECT t.data.items[0].price.decimal(10, 2) FROM orders AS t",
    );
}

#[test]
fn json_multiple_methods_integration() {
    // Integration test: Multiple JSON item methods in query
    one_statement_parses_to_std(
        "SELECT t.data.id.integer(), t.data.name.string(), t.data.active.boolean() FROM records t",
        "SELECT t.data.id.integer(), t.data.name.string(), t.data.active.boolean() FROM records AS t",
    );
}

// ==================== T879-T882: JSON Comparison ====================

#[test]
fn t879_01_json_equality() {
    // SQL:2023 T879: JSON equality comparison
    verified_standard_stmt(
        "SELECT * FROM documents WHERE json_col = CAST('{\"key\": \"value\"}' AS JSON)",
    );
}

#[test]
fn t879_02_json_inequality() {
    // SQL:2023 T879: JSON inequality comparison
    verified_standard_stmt(
        "SELECT * FROM documents WHERE json_col <> CAST('{\"key\": \"other\"}' AS JSON)",
    );
}

#[test]
fn t879_03_json_equality_null_handling() {
    // SQL:2023 T879: JSON equality with NULL handling
    verified_standard_stmt(
        "SELECT * FROM documents WHERE json_col IS NULL OR json_col = CAST('{}' AS JSON)",
    );
}

#[test]
fn t880_01_json_ordering_less_than() {
    // SQL:2023 T880: JSON ordering - less than
    verified_standard_stmt(
        "SELECT * FROM documents WHERE json_col < CAST('{\"key\": 100}' AS JSON)",
    );
}

#[test]
fn t880_02_json_ordering_greater_than() {
    // SQL:2023 T880: JSON ordering - greater than
    verified_standard_stmt("SELECT * FROM documents WHERE json_col > CAST('{\"key\": 0}' AS JSON)");
}

#[test]
fn t880_03_json_ordering_between() {
    // SQL:2023 T880: JSON ordering with BETWEEN
    verified_standard_stmt(
        "SELECT * FROM documents WHERE json_col BETWEEN CAST('{\"a\": 1}' AS JSON) AND CAST('{\"z\": 99}' AS JSON)",
    );
}

#[test]
fn t881_01_json_in_group_by() {
    // SQL:2023 T881: JSON in GROUP BY clause
    verified_standard_stmt("SELECT json_col, COUNT(*) FROM documents GROUP BY json_col");
}

#[test]
fn t881_02_json_group_by_complex() {
    // SQL:2023 T881: JSON grouping with multiple columns
    verified_standard_stmt(
        "SELECT category, json_data, COUNT(*) FROM items GROUP BY category, json_data",
    );
}

#[test]
fn t882_01_json_in_distinct() {
    // SQL:2023 T882: JSON with DISTINCT
    verified_standard_stmt("SELECT DISTINCT json_col FROM documents");
}

#[test]
fn t882_02_json_distinct_multiple() {
    // SQL:2023 T882: Multiple JSON columns with DISTINCT
    verified_standard_stmt("SELECT DISTINCT json_col1, json_col2 FROM multi_json");
}

#[test]
fn t882_03_json_in_order_by_comparison() {
    // SQL:2023 T882: JSON in ORDER BY for comparison
    verified_standard_stmt("SELECT * FROM documents ORDER BY json_col ASC");
}

#[test]
fn t882_04_json_comparison_in_join() {
    // SQL:2023 T882: JSON comparison in JOIN condition
    verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.json_col = t2.json_col");
}

#[test]
fn t882_05_json_comparison_in_having() {
    // SQL:2023 T882: JSON in HAVING clause
    verified_standard_stmt(
        "SELECT json_col, COUNT(*) FROM documents GROUP BY json_col HAVING json_col > CAST('{}' AS JSON)",
    );
}
