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

//! SQL:2023 New Functions Tests
//!
//! New scalar and aggregate functions introduced in SQL:2023.
//!
//! ## Feature Coverage
//!
//! - T054: GREATEST and LEAST functions
//!   - GREATEST(1, 2, 3) returns 3
//!   - LEAST(1, 2, 3) returns 1
//!
//! - T055: String padding functions
//!   - LPAD(string, length, pad_string)
//!   - RPAD(string, length, pad_string)
//!
//! - T056: Multi-character TRIM functions
//!   - LTRIM(string, characters)
//!   - RTRIM(string, characters)
//!   - BTRIM(string, characters)
//!
//! - T081: VARCHAR without length
//!   - CREATE TABLE t (a VARCHAR) -- length is implementation-defined
//!
//! - T133: Enhanced cycle mark values (boolean type)
//!   - CYCLE id SET is_cycle USING path
//!
//! - T626: ANY_VALUE aggregate function
//!   - Returns arbitrary non-null value from a group
//!
//! - F292: UNIQUE null treatment
//!   - UNIQUE NULLS DISTINCT
//!   - UNIQUE NULLS NOT DISTINCT

use crate::standards::common::{one_statement_parses_to_std, verified_standard_stmt};
use crate::verified_with_ast;
use sqlparser::ast::{
    ColumnDef, CreateTable, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, NullsDistinctOption, ObjectName, Query, Select,
    SelectItem, SetExpr, Statement, TableConstraint, UniqueConstraint,
};

/// Helper function to extract a function call from a SELECT statement
fn extract_function_from_select(stmt: Statement) -> Function {
    if let Statement::Query(q) = stmt {
        if let SetExpr::Select(sel) = *q.body {
            if let Some(SelectItem::UnnamedExpr(Expr::Function(func))) = sel.projection.first() {
                return func.clone();
            }
        }
    }
    panic!("Expected SELECT with function call");
}

/// Helper function to get function arguments as expressions
fn get_function_args(func: &Function) -> Vec<&Expr> {
    if let FunctionArguments::List(FunctionArgumentList { args, .. }) = &func.args {
        args.iter()
            .filter_map(|arg| {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                    Some(expr)
                } else {
                    None
                }
            })
            .collect()
    } else {
        vec![]
    }
}

// ==================== T054: GREATEST and LEAST ====================

#[test]
fn t054_01_greatest_basic() {
    // SQL:2023 T054: GREATEST function - basic usage
    verified_with_ast!("SELECT GREATEST(a, b)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "GREATEST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT GREATEST(a, b, c)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "GREATEST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT GREATEST(1, 2, 3, 4, 5)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "GREATEST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 5);
    });
}

#[test]
fn t054_02_greatest_expressions() {
    // SQL:2023 T054: GREATEST with complex expressions
    verified_with_ast!(
        "SELECT GREATEST(price * quantity, min_total) FROM orders",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "GREATEST");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 2);
            // First arg should be a binary operation
            assert!(matches!(args[0], Expr::BinaryOp { .. }));
        }
    );

    verified_with_ast!("SELECT GREATEST(a + b, c * d, e - f)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "GREATEST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
        // All args should be binary operations
        assert!(matches!(args[0], Expr::BinaryOp { .. }));
        assert!(matches!(args[1], Expr::BinaryOp { .. }));
        assert!(matches!(args[2], Expr::BinaryOp { .. }));
    });

    verified_with_ast!(
        "SELECT GREATEST(COALESCE(a, 0), COALESCE(b, 0))",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "GREATEST");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 2);
            // Both args should be function calls (COALESCE)
            assert!(matches!(args[0], Expr::Function(_)));
            assert!(matches!(args[1], Expr::Function(_)));
        }
    );
}

#[test]
fn t054_03_least_basic() {
    // SQL:2023 T054: LEAST function - basic usage
    verified_with_ast!("SELECT LEAST(a, b)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT LEAST(a, b, c)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT LEAST(1, 2, 3, 4, 5)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 5);
    });
}

#[test]
fn t054_04_least_expressions() {
    // SQL:2023 T054: LEAST with complex expressions
    verified_with_ast!(
        "SELECT LEAST(price, max_price) FROM products",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "LEAST");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 2);
        }
    );

    verified_with_ast!("SELECT LEAST(a + b, c * d, e - f)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT LEAST(LENGTH(name), 50)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
        // First arg should be a function call
        assert!(matches!(args[0], Expr::Function(_)));
    });
}

#[test]
fn t054_05_greatest_least_in_where() {
    // SQL:2023 T054: GREATEST/LEAST in WHERE clause
    verified_with_ast!(
        "SELECT * FROM t WHERE GREATEST(a, b, c) > 10",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    if let Some(Expr::BinaryOp { left, .. }) = sel.selection {
                        if let Expr::Function(func) = *left {
                            assert_eq!(func.name.to_string(), "GREATEST");
                            let args = get_function_args(&func);
                            assert_eq!(args.len(), 3);
                        } else {
                            panic!("Expected GREATEST function in WHERE");
                        }
                    }
                }
            }
        }
    );

    verified_with_ast!(
        "SELECT * FROM t WHERE LEAST(x, y) < threshold",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    if let Some(Expr::BinaryOp { left, .. }) = sel.selection {
                        if let Expr::Function(func) = *left {
                            assert_eq!(func.name.to_string(), "LEAST");
                            let args = get_function_args(&func);
                            assert_eq!(args.len(), 2);
                        }
                    }
                }
            }
        }
    );

    verified_standard_stmt("SELECT * FROM t WHERE value BETWEEN LEAST(a, b) AND GREATEST(a, b)");
}

#[test]
fn t054_06_greatest_with_null() {
    // SQL:2023 T054: GREATEST with NULL handling
    // Per SQL standard, GREATEST returns NULL if any argument is NULL
    // (This differs from MIN aggregate which ignores NULLs)
    verified_with_ast!("SELECT GREATEST(a, NULL, b)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "GREATEST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT GREATEST(1, 2, NULL)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "GREATEST");
    });

    verified_with_ast!("SELECT GREATEST(NULL, NULL)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "GREATEST");
    });
}

#[test]
fn t054_07_least_with_null() {
    // SQL:2023 T054: LEAST with NULL handling
    // Per SQL standard, LEAST returns NULL if any argument is NULL
    verified_with_ast!("SELECT LEAST(a, NULL, b)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT LEAST(1, NULL, 3)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
    });

    verified_with_ast!("SELECT LEAST(NULL, NULL)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LEAST");
    });
}

#[test]
fn t054_08_greatest_least_coalesce_pattern() {
    // Common pattern: COALESCE with GREATEST/LEAST for NULL-safe behavior
    verified_standard_stmt("SELECT COALESCE(GREATEST(a, b, c), 0) FROM t");
    verified_standard_stmt("SELECT COALESCE(LEAST(a, b, c), 0) FROM t");
}

// ==================== T055: String Padding Functions ====================

#[test]
fn t055_01_lpad_two_args() {
    // SQL:2023 T055: LPAD with two arguments (default space padding)
    verified_with_ast!("SELECT LPAD(name, 10)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT LPAD(code, 5) FROM products", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });
}

#[test]
fn t055_02_lpad_three_args() {
    // SQL:2023 T055: LPAD with three arguments (custom padding)
    verified_with_ast!("SELECT LPAD(name, 10, ' ')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT LPAD(name, 10, '0')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT LPAD(col, 20, '**')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!(
        "SELECT LPAD(CAST(id AS VARCHAR), 8, '0') FROM users",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "LPAD");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 3);
            // First arg should be a CAST
            assert!(matches!(args[0], Expr::Cast { .. }));
        }
    );
}

#[test]
fn t055_03_rpad_two_args() {
    // SQL:2023 T055: RPAD with two arguments (default space padding)
    verified_with_ast!("SELECT RPAD(name, 10)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT RPAD(code, 5) FROM products", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });
}

#[test]
fn t055_04_rpad_three_args() {
    // SQL:2023 T055: RPAD with three arguments (custom padding)
    verified_with_ast!("SELECT RPAD(name, 10, ' ')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT RPAD(name, 10, '0')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!("SELECT RPAD(col, 20, '**')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RPAD");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 3);
    });

    verified_with_ast!(
        "SELECT RPAD(description, 100, '.') FROM items",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "RPAD");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 3);
        }
    );
}

// ==================== T056: Multi-Character TRIM Functions ====================
//
// SQL:2023 T056 enhances TRIM to support multi-character trim specifications
// and adds LTRIM, RTRIM, BTRIM as standard functions.

#[test]
fn t056_01_trim_multi_char_leading() {
    // SQL:2023 T056: Standard TRIM with multi-character trim spec
    // TRIM is parsed as Expr::Trim, not a function
    verified_with_ast!("SELECT TRIM(LEADING 'abc' FROM name)", |stmt: Statement| {
        if let Statement::Query(q) = stmt {
            if let SetExpr::Select(sel) = *q.body {
                if let Some(SelectItem::UnnamedExpr(Expr::Trim {
                    trim_where,
                    trim_what,
                    ..
                })) = sel.projection.first()
                {
                    assert!(matches!(
                        trim_where,
                        Some(sqlparser::ast::TrimWhereField::Leading)
                    ));
                    assert!(trim_what.is_some(), "Expected multi-char trim spec");
                } else {
                    panic!("Expected TRIM expression");
                }
            }
        }
    });

    verified_with_ast!(
        "SELECT TRIM(LEADING '0x' FROM hex_value)",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    if let Some(SelectItem::UnnamedExpr(Expr::Trim {
                        trim_where,
                        trim_what,
                        ..
                    })) = sel.projection.first()
                    {
                        assert!(matches!(
                            trim_where,
                            Some(sqlparser::ast::TrimWhereField::Leading)
                        ));
                        assert!(trim_what.is_some());
                    } else {
                        panic!("Expected TRIM expression");
                    }
                }
            }
        }
    );
}

#[test]
fn t056_02_trim_multi_char_trailing() {
    // SQL:2023 T056: Standard TRIM TRAILING with multi-character spec
    verified_with_ast!(
        "SELECT TRIM(TRAILING 'xyz' FROM name)",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    if let Some(SelectItem::UnnamedExpr(Expr::Trim {
                        trim_where,
                        trim_what,
                        ..
                    })) = sel.projection.first()
                    {
                        assert!(matches!(
                            trim_where,
                            Some(sqlparser::ast::TrimWhereField::Trailing)
                        ));
                        assert!(trim_what.is_some());
                    } else {
                        panic!("Expected TRIM expression");
                    }
                }
            }
        }
    );
}

#[test]
fn t056_03_trim_multi_char_both() {
    // SQL:2023 T056: Standard TRIM BOTH with multi-character spec
    verified_with_ast!(
        "SELECT TRIM(BOTH '[]' FROM bracketed_value)",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    if let Some(SelectItem::UnnamedExpr(Expr::Trim {
                        trim_where,
                        trim_what,
                        ..
                    })) = sel.projection.first()
                    {
                        assert!(matches!(
                            trim_where,
                            Some(sqlparser::ast::TrimWhereField::Both)
                        ));
                        assert!(trim_what.is_some());
                    } else {
                        panic!("Expected TRIM expression");
                    }
                }
            }
        }
    );
}

// SQL:2023 T056 added LTRIM/RTRIM/BTRIM as standard functions.

#[test]
fn t056_04_ltrim_one_arg() {
    // LTRIM convenience function (equivalent to TRIM(LEADING ...))
    verified_with_ast!("SELECT LTRIM(name)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 1);
    });

    verified_with_ast!(
        "SELECT LTRIM(description) FROM products",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "LTRIM");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn t056_05_ltrim_two_args() {
    // LTRIM with two arguments (custom character set)
    verified_with_ast!("SELECT LTRIM(name, ' ')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT LTRIM(name, ' \\t')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT LTRIM(path, '/')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT LTRIM(code, '0')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "LTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });
}

#[test]
fn t056_06_rtrim_one_arg() {
    // RTRIM convenience function (equivalent to TRIM(TRAILING ...))
    verified_with_ast!("SELECT RTRIM(name)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 1);
    });

    verified_with_ast!(
        "SELECT RTRIM(description) FROM products",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "RTRIM");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn t056_07_rtrim_two_args() {
    // RTRIM with two arguments (custom character set)
    verified_with_ast!("SELECT RTRIM(name, ' ')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT RTRIM(name, ' \\t')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT RTRIM(path, '/')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT RTRIM(code, '.')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "RTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });
}

#[test]
fn t056_08_btrim_one_arg() {
    // BTRIM convenience function (equivalent to TRIM(BOTH ...))
    verified_with_ast!("SELECT BTRIM(name)", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "BTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 1);
    });

    verified_with_ast!(
        "SELECT BTRIM(description) FROM products",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "BTRIM");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn t056_09_btrim_two_args() {
    // BTRIM with two arguments (custom character set)
    verified_with_ast!("SELECT BTRIM(name, ' ')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "BTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT BTRIM(name, ' \\t')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "BTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT BTRIM(value, '\"')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "BTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });

    verified_with_ast!("SELECT BTRIM(text, '[]')", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "BTRIM");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 2);
    });
}

// ==================== T081: VARCHAR Without Length ====================

#[test]
fn t081_01_varchar_no_length() {
    // SQL:2023 T081: VARCHAR without length specification
    verified_with_ast!("CREATE TABLE t (a VARCHAR)", |stmt: Statement| {
        if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].name.to_string(), "a");
            // VARCHAR without length should be represented as Varchar(None) or similar
            assert!(matches!(
                columns[0].data_type,
                DataType::Varchar(_) | DataType::String(_)
            ));
        } else {
            panic!("Expected CreateTable statement");
        }
    });

    verified_with_ast!(
        "CREATE TABLE users (name VARCHAR, email VARCHAR)",
        |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name.to_string(), "name");
                assert_eq!(columns[1].name.to_string(), "email");
                assert!(matches!(
                    columns[0].data_type,
                    DataType::Varchar(_) | DataType::String(_)
                ));
                assert!(matches!(
                    columns[1].data_type,
                    DataType::Varchar(_) | DataType::String(_)
                ));
            } else {
                panic!("Expected CreateTable statement");
            }
        }
    );
}

#[test]
fn t081_02_varchar_no_length_multiple_columns() {
    // SQL:2023 T081: Multiple VARCHAR columns without length
    verified_with_ast!(
        "CREATE TABLE products (name VARCHAR, description VARCHAR, category VARCHAR)",
        |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 3);
                for col in &columns {
                    assert!(matches!(
                        col.data_type,
                        DataType::Varchar(_) | DataType::String(_)
                    ));
                }
            } else {
                panic!("Expected CreateTable statement");
            }
        }
    );
}

#[test]
fn t081_03_varchar_mixed_with_length() {
    // SQL:2023 T081: Mixed VARCHAR with and without length
    verified_with_ast!(
        "CREATE TABLE t (short_name VARCHAR(50), long_description VARCHAR)",
        |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name.to_string(), "short_name");
                assert_eq!(columns[1].name.to_string(), "long_description");
                // Both should be VARCHAR types
                assert!(matches!(
                    columns[0].data_type,
                    DataType::Varchar(_) | DataType::String(_)
                ));
                assert!(matches!(
                    columns[1].data_type,
                    DataType::Varchar(_) | DataType::String(_)
                ));
            } else {
                panic!("Expected CreateTable statement");
            }
        }
    );

    verified_with_ast!(
        "CREATE TABLE users (username VARCHAR(32), bio VARCHAR, email VARCHAR(255))",
        |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 3);
                for col in &columns {
                    assert!(matches!(
                        col.data_type,
                        DataType::Varchar(_) | DataType::String(_)
                    ));
                }
            } else {
                panic!("Expected CreateTable statement");
            }
        }
    );
}

// ==================== T133: Enhanced Cycle Mark Values ====================

#[test]
fn t133_01_cycle_basic() {
    // SQL:2023 T133: CYCLE clause with SET and USING - NOT YET IMPLEMENTED
    // Note: CYCLE is from SQL:2016, T133 in SQL:2023 refines it to allow boolean type
    verified_standard_stmt("WITH RECURSIVE cte AS (SELECT id, parent_id FROM t UNION ALL SELECT t.id, t.parent_id FROM t JOIN cte ON t.parent_id = cte.id) CYCLE id SET is_cycle TO true DEFAULT false USING path SELECT * FROM cte");
}

#[test]
fn t133_02_cycle_boolean_values() {
    // SQL:2023 T133: Enhanced cycle marks with boolean type - NOT YET IMPLEMENTED
    one_statement_parses_to_std(
        "WITH RECURSIVE hierarchy AS (SELECT id, parent_id FROM employees UNION ALL SELECT e.id, e.parent_id FROM employees e JOIN hierarchy h ON e.parent_id = h.id) CYCLE id SET has_cycle TO '1' DEFAULT '0' USING traversal_path SELECT * FROM hierarchy",
        "WITH RECURSIVE hierarchy AS (SELECT id, parent_id FROM employees UNION ALL SELECT e.id, e.parent_id FROM employees AS e JOIN hierarchy AS h ON e.parent_id = h.id) CYCLE id SET has_cycle TO '1' DEFAULT '0' USING traversal_path SELECT * FROM hierarchy"
    );
}

#[test]
fn t133_03_cycle_complex() {
    // SQL:2023 T133: CYCLE with multiple columns - NOT YET IMPLEMENTED
    one_statement_parses_to_std(
        "WITH RECURSIVE graph AS (SELECT node1, node2 FROM edges UNION ALL SELECT e.node1, e.node2 FROM edges e JOIN graph g ON e.node1 = g.node2) CYCLE node1, node2 SET is_cyclic TO 'Y' DEFAULT 'N' USING path_array SELECT * FROM graph",
        "WITH RECURSIVE graph AS (SELECT node1, node2 FROM edges UNION ALL SELECT e.node1, e.node2 FROM edges AS e JOIN graph AS g ON e.node1 = g.node2) CYCLE node1, node2 SET is_cyclic TO 'Y' DEFAULT 'N' USING path_array SELECT * FROM graph"
    );
}

// ==================== T626: ANY_VALUE Aggregate Function ====================

#[test]
fn t626_01_any_value_basic() {
    // SQL:2023 T626: ANY_VALUE aggregate function
    verified_with_ast!("SELECT ANY_VALUE(name) FROM users", |stmt: Statement| {
        let func = extract_function_from_select(stmt);
        assert_eq!(func.name.to_string(), "ANY_VALUE");
        let args = get_function_args(&func);
        assert_eq!(args.len(), 1);
    });

    verified_with_ast!(
        "SELECT ANY_VALUE(price) FROM products",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "ANY_VALUE");
            let args = get_function_args(&func);
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn t626_02_any_value_group_by() {
    // SQL:2023 T626: ANY_VALUE in GROUP BY queries
    verified_with_ast!(
        "SELECT category, ANY_VALUE(name), COUNT(*) FROM products GROUP BY category",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    assert_eq!(sel.projection.len(), 3);
                    // Second item should be ANY_VALUE
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &sel.projection[1] {
                        assert_eq!(func.name.to_string(), "ANY_VALUE");
                    }
                    // Third item should be COUNT
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &sel.projection[2] {
                        assert_eq!(func.name.to_string(), "COUNT");
                    }
                }
            }
        }
    );

    verified_with_ast!(
        "SELECT department, ANY_VALUE(description) FROM employees GROUP BY department",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    assert_eq!(sel.projection.len(), 2);
                    // Second item should be ANY_VALUE
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &sel.projection[1] {
                        assert_eq!(func.name.to_string(), "ANY_VALUE");
                    }
                }
            }
        }
    );
}

#[test]
fn t626_03_any_value_complex() {
    // SQL:2023 T626: ANY_VALUE with expressions and multiple uses
    verified_with_ast!(
        "SELECT region, ANY_VALUE(city), ANY_VALUE(country), COUNT(*) FROM locations GROUP BY region",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    assert_eq!(sel.projection.len(), 4);
                    // Second and third items should be ANY_VALUE
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &sel.projection[1] {
                        assert_eq!(func.name.to_string(), "ANY_VALUE");
                    }
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &sel.projection[2] {
                        assert_eq!(func.name.to_string(), "ANY_VALUE");
                    }
                }
            }
        }
    );

    verified_with_ast!(
        "SELECT ANY_VALUE(DISTINCT status) FROM orders",
        |stmt: Statement| {
            let func = extract_function_from_select(stmt);
            assert_eq!(func.name.to_string(), "ANY_VALUE");
            // Verify DISTINCT is present in the args
            if let FunctionArguments::List(list) = &func.args {
                assert!(list.duplicate_treatment.is_some());
            }
        }
    );
}

#[test]
fn t626_04_any_value_with_having() {
    // SQL:2023 T626: ANY_VALUE with HAVING clause
    verified_with_ast!(
        "SELECT department, ANY_VALUE(name) FROM employees GROUP BY department HAVING COUNT(*) > 5",
        |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let SetExpr::Select(sel) = *q.body {
                    assert_eq!(sel.projection.len(), 2);
                    // Second item should be ANY_VALUE
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &sel.projection[1] {
                        assert_eq!(func.name.to_string(), "ANY_VALUE");
                    }
                    // Verify HAVING clause exists
                    assert!(sel.having.is_some());
                }
            }
        }
    );
}

// ==================== F292: UNIQUE Null Treatment ====================
//
// SQL:2023 F292 adds NULLS DISTINCT / NULLS NOT DISTINCT to UNIQUE constraints.
//
// Standard SQL:2023 syntax: UNIQUE (columns) NULLS [NOT] DISTINCT
//
// ## Conformance Status: NOT YET IMPLEMENTED
//
// The standard syntax is not supported. These tests document the gap.

#[test]
fn f292_01_unique_nulls_distinct() {
    // SQL:2023 F292: UNIQUE constraint with NULLS DISTINCT
    // Standard syntax: UNIQUE (column) NULLS DISTINCT
    verified_standard_stmt("CREATE TABLE t (a INT, UNIQUE (a) NULLS DISTINCT)");
}

#[test]
fn f292_02_unique_nulls_not_distinct() {
    // SQL:2023 F292: UNIQUE constraint with NULLS NOT DISTINCT
    // Standard syntax: UNIQUE (column) NULLS NOT DISTINCT
    verified_standard_stmt("CREATE TABLE t (a INT, UNIQUE (a) NULLS NOT DISTINCT)");
}

#[test]
fn f292_03_unique_nulls_distinct_multi_column() {
    // SQL:2023 F292: Multi-column UNIQUE with NULLS DISTINCT
    // Standard syntax: UNIQUE (a, b) NULLS DISTINCT
    verified_standard_stmt("CREATE TABLE t (a INT, b INT, UNIQUE (a, b) NULLS DISTINCT)");
}

#[test]
fn f292_04_unique_nulls_not_distinct_inline() {
    // SQL:2023 F292: Inline column UNIQUE with NULLS NOT DISTINCT
    // Standard syntax: column_name type UNIQUE NULLS NOT DISTINCT
    verified_standard_stmt("CREATE TABLE t (email VARCHAR UNIQUE NULLS NOT DISTINCT)");
}

#[test]
fn f292_05_unique_nulls_in_alter_table() {
    // SQL:2023 F292: ALTER TABLE ADD CONSTRAINT with NULLS treatment
    // Standard syntax: ADD CONSTRAINT name UNIQUE (column) NULLS NOT DISTINCT
    verified_standard_stmt(
        "ALTER TABLE users ADD CONSTRAINT uk_email UNIQUE (email) NULLS NOT DISTINCT",
    );
}
