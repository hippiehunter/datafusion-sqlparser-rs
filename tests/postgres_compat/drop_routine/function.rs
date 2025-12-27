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

//! Tests for DROP FUNCTION compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-dropfunction.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{DropBehavior, Statement};

// =============================================================================
// Helper functions
// =============================================================================

fn extract_drop_function(stmt: &Statement) -> &sqlparser::ast::DropFunction {
    match stmt {
        Statement::DropFunction(df) => df,
        other => panic!(
            "Expected Statement::DropFunction, got: {:?}",
            std::mem::discriminant(other)
        ),
    }
}

// =============================================================================
// Basic DROP FUNCTION
// =============================================================================

#[test]
fn test_drop_function_basic() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function without arguments (only works if function name is unique)
    pg_test!("DROP FUNCTION my_func", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert!(!df.if_exists);
        assert_eq!(df.func_desc.len(), 1);
        assert_eq!(df.func_desc[0].name.to_string(), "my_func");
        assert!(df.func_desc[0].args.is_none());
        assert!(df.drop_behavior.is_none());
    });
}

#[test]
fn test_drop_function_with_parens() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with empty parentheses
    pg_test!("DROP FUNCTION my_func()", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert_eq!(df.func_desc.len(), 1);
        assert_eq!(df.func_desc[0].name.to_string(), "my_func");
        // Empty args list should be represented as Some(vec![])
        assert!(
            df.func_desc[0]
                .args
                .as_ref()
                .map(|a| a.is_empty())
                .unwrap_or(false)
                || df.func_desc[0].args.is_none()
        );
    });
}

#[test]
fn test_drop_function_qualified_name() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with schema-qualified name
    pg_test!("DROP FUNCTION myschema.myfunc", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert_eq!(df.func_desc[0].name.to_string(), "myschema.myfunc");
        assert_eq!(df.func_desc[0].name.0.len(), 2);
    });
}

#[test]
fn test_drop_function_double_qualified() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with database.schema.function name
    pg_test!("DROP FUNCTION mydb.myschema.myfunc", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert_eq!(df.func_desc[0].name.to_string(), "mydb.myschema.myfunc");
        assert_eq!(df.func_desc[0].name.0.len(), 3);
    });
}

// =============================================================================
// DROP FUNCTION with argument types
// =============================================================================

#[test]
fn test_drop_function_single_arg() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with single argument type
    pg_test!("DROP FUNCTION square(INTEGER)", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        let args = df.func_desc[0].args.as_ref().unwrap();
        assert_eq!(args.len(), 1);
    });
}

#[test]
fn test_drop_function_multiple_args() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with multiple argument types
    pg_test!("DROP FUNCTION add(INTEGER, INTEGER)", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        let args = df.func_desc[0].args.as_ref().unwrap();
        assert_eq!(args.len(), 2);
    });
}

#[test]
fn test_drop_function_complex_arg_types() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with complex argument types
    pg_test!(
        "DROP FUNCTION process(VARCHAR(100), NUMERIC(10,2), TIMESTAMP)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

#[test]
fn test_drop_function_array_arg() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with array argument
    pg_test!("DROP FUNCTION array_sum(INTEGER[])", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        let args = df.func_desc[0].args.as_ref().unwrap();
        assert_eq!(args.len(), 1);
    });
}

#[test]
fn test_drop_function_variadic_arg() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with VARIADIC argument
    pg_test!(
        "DROP FUNCTION concat_all(VARIADIC TEXT[])",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn test_drop_function_out_parameter() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with OUT parameter
    pg_test!(
        "DROP FUNCTION get_values(IN x INTEGER, OUT y INTEGER)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

#[test]
fn test_drop_function_inout_parameter() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with INOUT parameter
    pg_test!(
        "DROP FUNCTION increment(INOUT counter INTEGER)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 1);
        }
    );
}

#[test]
fn test_drop_function_default_parameter() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function - default values are not included in signature
    pg_test!(
        "DROP FUNCTION add_default(INTEGER, INTEGER)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

// =============================================================================
// DROP FUNCTION IF EXISTS
// =============================================================================

#[test]
fn test_drop_function_if_exists() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // IF EXISTS prevents error if function doesn't exist
    pg_test!("DROP FUNCTION IF EXISTS my_func", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert!(df.if_exists, "Expected if_exists=true");
        assert_eq!(df.func_desc.len(), 1);
    });
}

#[test]
fn test_drop_function_if_exists_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // IF EXISTS with argument signature
    pg_test!(
        "DROP FUNCTION IF EXISTS calculate(NUMERIC, NUMERIC)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert!(df.if_exists);
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

#[test]
fn test_drop_function_if_exists_qualified() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // IF EXISTS with schema-qualified name
    pg_test!(
        "DROP FUNCTION IF EXISTS myschema.myfunc(TEXT)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert!(df.if_exists);
            assert_eq!(df.func_desc[0].name.to_string(), "myschema.myfunc");
        }
    );
}

// =============================================================================
// DROP FUNCTION with CASCADE/RESTRICT
// =============================================================================

#[test]
fn test_drop_function_cascade() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // CASCADE automatically drops dependent objects
    pg_test!("DROP FUNCTION my_func CASCADE", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert_eq!(df.drop_behavior, Some(DropBehavior::Cascade));
    });
}

#[test]
fn test_drop_function_cascade_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // CASCADE with argument signature
    pg_test!(
        "DROP FUNCTION calculate(INTEGER, INTEGER) CASCADE",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert_eq!(df.drop_behavior, Some(DropBehavior::Cascade));
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 2);
        }
    );
}

#[test]
fn test_drop_function_restrict() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // RESTRICT refuses to drop if dependent objects exist (default)
    pg_test!("DROP FUNCTION my_func RESTRICT", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert_eq!(df.drop_behavior, Some(DropBehavior::Restrict));
    });
}

#[test]
fn test_drop_function_restrict_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // RESTRICT with argument signature
    pg_test!(
        "DROP FUNCTION validate(TEXT) RESTRICT",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert_eq!(df.drop_behavior, Some(DropBehavior::Restrict));
        }
    );
}

#[test]
fn test_drop_function_if_exists_cascade() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Combine IF EXISTS with CASCADE
    pg_test!(
        "DROP FUNCTION IF EXISTS my_func CASCADE",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert!(df.if_exists);
            assert_eq!(df.drop_behavior, Some(DropBehavior::Cascade));
        }
    );
}

#[test]
fn test_drop_function_if_exists_restrict() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Combine IF EXISTS with RESTRICT
    pg_test!(
        "DROP FUNCTION IF EXISTS my_func RESTRICT",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert!(df.if_exists);
            assert_eq!(df.drop_behavior, Some(DropBehavior::Restrict));
        }
    );
}

// =============================================================================
// DROP multiple functions
// =============================================================================

#[test]
fn test_drop_multiple_functions() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop multiple functions in one statement
    pg_test!("DROP FUNCTION func1, func2, func3", |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert_eq!(df.func_desc.len(), 3);
        assert_eq!(df.func_desc[0].name.to_string(), "func1");
        assert_eq!(df.func_desc[1].name.to_string(), "func2");
        assert_eq!(df.func_desc[2].name.to_string(), "func3");
    });
}

#[test]
fn test_drop_multiple_functions_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop multiple functions with different signatures
    pg_test!(
        "DROP FUNCTION add(INTEGER, INTEGER), multiply(NUMERIC, NUMERIC), concat(TEXT, TEXT)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert_eq!(df.func_desc.len(), 3);
            assert_eq!(df.func_desc[0].name.to_string(), "add");
            assert_eq!(df.func_desc[1].name.to_string(), "multiply");
            assert_eq!(df.func_desc[2].name.to_string(), "concat");
        }
    );
}

#[test]
fn test_drop_multiple_functions_mixed() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop multiple functions with mixed signatures (with and without args)
    pg_test!(
        "DROP FUNCTION func1(), func2(INTEGER), func3",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert_eq!(df.func_desc.len(), 3);
        }
    );
}

#[test]
fn test_drop_multiple_functions_if_exists_cascade() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop multiple functions with IF EXISTS and CASCADE
    pg_test!(
        "DROP FUNCTION IF EXISTS func1, func2 CASCADE",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert!(df.if_exists);
            assert_eq!(df.func_desc.len(), 2);
            assert_eq!(df.drop_behavior, Some(DropBehavior::Cascade));
        }
    );
}

// =============================================================================
// Edge cases and special scenarios
// =============================================================================

#[test]
fn test_drop_function_quoted_identifier() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop function with quoted identifier (case-sensitive)
    pg_test!(r#"DROP FUNCTION "MyFunction"()"#, |stmt: Statement| {
        let df = extract_drop_function(&stmt);
        assert_eq!(df.func_desc[0].name.to_string(), r#""MyFunction""#);
    });
}

#[test]
fn test_drop_function_schema_qualified_with_args() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Drop schema-qualified function with complex argument signature
    pg_test!(
        "DROP FUNCTION myschema.complex_func(INTEGER, VARCHAR(50), TIMESTAMP WITH TIME ZONE)",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert_eq!(df.func_desc[0].name.to_string(), "myschema.complex_func");
            let args = df.func_desc[0].args.as_ref().unwrap();
            assert_eq!(args.len(), 3);
        }
    );
}

#[test]
fn test_drop_function_comprehensive() {
    // https://www.postgresql.org/docs/current/sql-dropfunction.html
    // Comprehensive test with all options
    pg_test!(
        "DROP FUNCTION IF EXISTS myschema.func1(INTEGER, TEXT), myschema.func2() CASCADE",
        |stmt: Statement| {
            let df = extract_drop_function(&stmt);
            assert!(df.if_exists);
            assert_eq!(df.func_desc.len(), 2);
            assert_eq!(df.drop_behavior, Some(DropBehavior::Cascade));
        }
    );
}
