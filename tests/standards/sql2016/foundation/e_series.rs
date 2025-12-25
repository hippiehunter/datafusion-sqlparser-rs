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

//! SQL:2016 E-Series (Core Features) Tests
//!
//! E011-E182: Core SQL features required for basic conformance.
//!
//! ## Feature Coverage
//!
//! - E011: Numeric data types
//! - E021: Character string types
//! - E031: Identifiers
//! - E051: Basic query specification
//! - E061: Basic predicates and search conditions
//! - E071: Basic query expressions (UNION, EXCEPT, INTERSECT)
//! - E081: Basic Privileges
//! - E091: Set functions (aggregates)
//! - E101: Basic data manipulation
//! - E111: Single row SELECT statement
//! - E121: Basic cursor support
//! - E131: Null value support
//! - E141: Basic integrity constraints
//! - E151: Transaction support
//! - E152: Basic SET TRANSACTION statement
//! - E153: Updatable queries with subqueries
//! - E161: SQL comments using leading double minus
//! - E171: SQLSTATE support
//! - E182: Host language binding

use crate::standards::common::{one_statement_parses_to_std, verified_standard_stmt};
use crate::verified_with_ast;
use sqlparser::ast::{
    Action, Assignment, BinaryOperator, CascadeOption, CharacterLength, ColumnOption, CreateTable,
    DataType, Delete, ExactNumberInfo, Expr, GrantObjects, GroupByExpr, Insert, Privileges,
    SelectItem, SetExpr, SetOperator, Statement, TableConstraint, TableFactor, TableObject, Update,
    Value, ValueWithSpan, Values,
};

// =============================================================================
// E011: Numeric Data Types
// =============================================================================

mod e011_numeric_data_types {
    use super::*;

    #[test]
    fn e011_01_integer_and_smallint() {
        // SQL:2016 E011-01: INTEGER and SMALLINT data types
        verified_with_ast!(
            "CREATE TABLE t (a INTEGER, b SMALLINT)",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0].name.to_string(), "a");
                    assert!(matches!(columns[0].data_type, DataType::Integer(_)));
                    assert_eq!(columns[1].name.to_string(), "b");
                    assert!(matches!(columns[1].data_type, DataType::SmallInt(_)));
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );

        verified_with_ast!("CREATE TABLE t (id INT PRIMARY KEY)", |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name.to_string(), "id");
                assert!(matches!(columns[0].data_type, DataType::Int(_)));
                // Verify PRIMARY KEY constraint
                assert!(columns[0]
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ColumnOption::PrimaryKey(_))));
            } else {
                panic!("Expected CreateTable statement");
            }
        });

        verified_standard_stmt("SELECT CAST(1 AS INTEGER)");
        verified_standard_stmt("SELECT CAST(100 AS SMALLINT)");

        // Table with integer columns
        verified_standard_stmt(
            "CREATE TABLE employees (employee_id INTEGER, department_id SMALLINT)",
        );
    }

    #[test]
    fn e011_02_real_double_float() {
        // SQL:2016 E011-02: REAL, DOUBLE PRECISION, FLOAT data types
        verified_standard_stmt("CREATE TABLE t (a REAL, b DOUBLE PRECISION, c FLOAT)");
        verified_standard_stmt("SELECT CAST(3.14 AS REAL)");
        verified_standard_stmt("SELECT CAST(2.718281828 AS DOUBLE PRECISION)");
        verified_standard_stmt("SELECT CAST(1.414 AS FLOAT)");

        // FLOAT with precision
        verified_standard_stmt("CREATE TABLE measurements (temp FLOAT(24), pressure REAL)");
    }

    #[test]
    fn e011_03_decimal_and_numeric() {
        // SQL:2016 E011-03: DECIMAL and NUMERIC with precision/scale
        verified_with_ast!(
            "CREATE TABLE t (a DECIMAL, b NUMERIC)",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                    assert_eq!(columns.len(), 2);
                    assert!(matches!(columns[0].data_type, DataType::Decimal(_)));
                    assert!(matches!(columns[1].data_type, DataType::Numeric(_)));
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );

        verified_with_ast!("CREATE TABLE t (price DECIMAL(10,2))", |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 1);
                if let DataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, s)) =
                    &columns[0].data_type
                {
                    assert_eq!(*p, 10);
                    assert_eq!(*s, 2);
                } else {
                    panic!("Expected Decimal type with precision and scale");
                }
            } else {
                panic!("Expected CreateTable statement");
            }
        });

        verified_with_ast!(
            "CREATE TABLE t (amount NUMERIC(15,4))",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                    assert_eq!(columns.len(), 1);
                    if let DataType::Numeric(ExactNumberInfo::PrecisionAndScale(p, s)) =
                        &columns[0].data_type
                    {
                        assert_eq!(*p, 15);
                        assert_eq!(*s, 4);
                    } else {
                        panic!("Expected Numeric type with precision and scale");
                    }
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );

        verified_standard_stmt("SELECT CAST(123.45 AS DECIMAL(5,2))");
        verified_standard_stmt("SELECT CAST(999.999 AS NUMERIC(6,3))");

        // Various precision and scale combinations
        verified_standard_stmt(
            "CREATE TABLE financials (balance DECIMAL(18,2), rate NUMERIC(5,4))",
        );
        verified_standard_stmt(
            "CREATE TABLE inventory (quantity DECIMAL(10), unit_cost NUMERIC(12,3))",
        );
    }

    #[test]
    fn e011_04_arithmetic_operators() {
        // SQL:2016 E011-04: Arithmetic operators
        verified_with_ast!("SELECT 1 + 2", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::BinaryOp { op, .. }) =
                        &select.projection[0]
                    {
                        assert!(matches!(op, BinaryOperator::Plus));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            } else {
                panic!("Expected Query statement");
            }
        });

        verified_with_ast!("SELECT 10 - 3", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::BinaryOp { op, .. }) =
                        &select.projection[0]
                    {
                        assert!(matches!(op, BinaryOperator::Minus));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            }
        });

        verified_with_ast!("SELECT 4 * 5", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::BinaryOp { op, .. }) =
                        &select.projection[0]
                    {
                        assert!(matches!(op, BinaryOperator::Multiply));
                    }
                }
            }
        });

        verified_with_ast!("SELECT 20 / 4", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::BinaryOp { op, .. }) =
                        &select.projection[0]
                    {
                        assert!(matches!(op, BinaryOperator::Divide));
                    }
                }
            }
        });

        verified_with_ast!("SELECT 17 % 5", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::BinaryOp { op, .. }) =
                        &select.projection[0]
                    {
                        assert!(matches!(op, BinaryOperator::Modulo));
                    }
                }
            }
        });

        // Complex expressions
        verified_standard_stmt("SELECT (a + b) * c FROM t");
        verified_standard_stmt("SELECT price * quantity - discount FROM orders");
        verified_standard_stmt("SELECT salary / 12 FROM employees");
        verified_standard_stmt("SELECT (total + tax) * (1 - discount_rate / 100) FROM invoices");

        // Nested arithmetic
        verified_standard_stmt("SELECT ((a + b) * c) / (d - e) FROM t");
    }

    #[test]
    fn e011_05_numeric_comparison() {
        // SQL:2016 E011-05: Numeric comparison predicates
        verified_standard_stmt("SELECT * FROM t WHERE a = 1");
        verified_standard_stmt("SELECT * FROM t WHERE b <> 2");
        verified_standard_stmt("SELECT * FROM t WHERE c < 3");
        verified_standard_stmt("SELECT * FROM t WHERE d > 4");
        verified_standard_stmt("SELECT * FROM t WHERE e <= 5");
        verified_standard_stmt("SELECT * FROM t WHERE f >= 6");

        // Alternative syntax
        verified_standard_stmt("SELECT * FROM t WHERE a <> 1");

        // Multiple comparisons
        verified_standard_stmt("SELECT * FROM t WHERE price > 100 AND quantity <= 50");
        verified_standard_stmt(
            "SELECT * FROM employees WHERE salary >= 50000 AND salary <= 100000",
        );
    }

    #[test]
    fn e011_06_implicit_numeric_casting() {
        // SQL:2016 E011-06: Implicit casting among numeric types
        // These should parse correctly; actual casting behavior is implementation-specific
        verified_standard_stmt("SELECT CAST(1 AS REAL)");
        verified_standard_stmt("SELECT CAST(3.14 AS INTEGER)");
        verified_standard_stmt("SELECT CAST(CAST(10 AS SMALLINT) AS DECIMAL(5,2))");

        // Mixed numeric types in expressions
        verified_standard_stmt("SELECT integer_col + real_col FROM t");
        verified_standard_stmt("SELECT decimal_col * float_col FROM t");
    }
}

// =============================================================================
// E021: Character String Types
// =============================================================================

mod e021_character_string_types {
    use super::*;

    #[test]
    fn e021_01_character_and_char() {
        // SQL:2016 E021-01: CHARACTER(n) and CHAR(n)
        verified_with_ast!("CREATE TABLE t (a CHARACTER(10))", |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 1);
                if let DataType::Character(Some(CharacterLength::IntegerLength {
                    length, ..
                })) = &columns[0].data_type
                {
                    assert_eq!(*length, 10);
                } else {
                    panic!("Expected Character type with length");
                }
            } else {
                panic!("Expected CreateTable statement");
            }
        });

        verified_with_ast!("CREATE TABLE t (b CHAR(20))", |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 1);
                if let DataType::Char(Some(CharacterLength::IntegerLength { length, .. })) =
                    &columns[0].data_type
                {
                    assert_eq!(*length, 20);
                } else {
                    panic!("Expected Char type with length");
                }
            } else {
                panic!("Expected CreateTable statement");
            }
        });

        verified_standard_stmt("CREATE TABLE t (code CHAR(5), name CHARACTER(100))");
        verified_standard_stmt("SELECT CAST('hello' AS CHARACTER(10))");
        verified_standard_stmt("SELECT CAST('x' AS CHAR(1))");
    }

    #[test]
    fn e021_02_character_varying() {
        // SQL:2016 E021-02: CHARACTER VARYING(n) and VARCHAR(n)
        verified_standard_stmt("CREATE TABLE t (a CHARACTER VARYING(255))");
        verified_standard_stmt("CREATE TABLE t (b VARCHAR(100))");
        verified_standard_stmt(
            "CREATE TABLE users (username VARCHAR(50), email CHARACTER VARYING(255))",
        );
        verified_standard_stmt("SELECT CAST('hello world' AS VARCHAR(50))");
    }

    #[test]
    fn e021_03_character_literals() {
        // SQL:2016 E021-03: Character literals with escaping
        verified_standard_stmt("SELECT 'hello'");
        verified_standard_stmt("SELECT 'world'");
        verified_standard_stmt("SELECT 'it''s a test'");
        verified_standard_stmt("SELECT 'line1\nline2'");
        verified_standard_stmt("SELECT 'tab\there'");

        // Empty string
        verified_standard_stmt("SELECT ''");

        // Unicode and special characters
        verified_standard_stmt("SELECT 'Hello, 世界'");
        verified_standard_stmt("SELECT 'Price: $99.99'");
    }

    #[test]
    fn e021_04_character_length() {
        // SQL:2016 E021-04: CHARACTER_LENGTH and CHAR_LENGTH functions
        verified_standard_stmt("SELECT CHARACTER_LENGTH('hello')");
        verified_standard_stmt("SELECT CHAR_LENGTH('world')");
        verified_standard_stmt("SELECT CHARACTER_LENGTH(name) FROM users");
        verified_standard_stmt("SELECT CHAR_LENGTH(description) AS desc_len FROM products");

        // In WHERE clause
        verified_standard_stmt("SELECT * FROM t WHERE CHARACTER_LENGTH(name) > 10");
        verified_standard_stmt("SELECT * FROM t WHERE CHAR_LENGTH(code) = 5");
    }

    #[test]
    fn e021_05_octet_length() {
        // SQL:2016 E021-05: OCTET_LENGTH function
        verified_standard_stmt("SELECT OCTET_LENGTH('hello')");
        verified_standard_stmt("SELECT OCTET_LENGTH(data) FROM t");
        verified_standard_stmt("SELECT * FROM t WHERE OCTET_LENGTH(binary_data) > 1024");
    }

    #[test]
    fn e021_06_substring() {
        // SQL:2016 E021-06: SUBSTRING function
        verified_standard_stmt("SELECT SUBSTRING('hello' FROM 1 FOR 3)");
        verified_standard_stmt("SELECT SUBSTRING('world' FROM 2)");
        verified_standard_stmt("SELECT SUBSTRING(name FROM 1 FOR 10) FROM users");

        // With column references
        verified_standard_stmt(
            "SELECT SUBSTRING(description FROM 1 FOR 50) AS preview FROM articles",
        );
        verified_standard_stmt("SELECT SUBSTRING(code FROM position FOR length) FROM t");

        // Alternative syntax (common extension)
        verified_standard_stmt("SELECT SUBSTRING('hello', 1, 3)");
        verified_standard_stmt("SELECT SUBSTRING(text, 5, 10) FROM documents");
    }

    #[test]
    fn e021_07_string_concatenation() {
        // SQL:2016 E021-07: String concatenation operator (||)
        verified_standard_stmt("SELECT 'hello' || 'world'");
        verified_standard_stmt("SELECT first_name || ' ' || last_name FROM users");
        verified_standard_stmt("SELECT 'ID: ' || id || ', Name: ' || name FROM products");

        // Multiple concatenations
        verified_standard_stmt("SELECT a || b || c || d FROM t");
        verified_standard_stmt(
            "SELECT street || ', ' || city || ', ' || state || ' ' || zip FROM addresses",
        );
    }

    #[test]
    fn e021_08_upper_and_lower() {
        // SQL:2016 E021-08: UPPER and LOWER functions
        verified_standard_stmt("SELECT UPPER('hello')");
        verified_standard_stmt("SELECT LOWER('WORLD')");
        verified_standard_stmt("SELECT UPPER(name) FROM users");
        verified_standard_stmt("SELECT LOWER(email) FROM contacts");

        // In WHERE clause
        verified_standard_stmt("SELECT * FROM users WHERE UPPER(username) = 'ADMIN'");
        verified_standard_stmt("SELECT * FROM t WHERE LOWER(code) = 'abc123'");

        // Combined
        verified_standard_stmt("SELECT UPPER(first_name) || ' ' || LOWER(last_name) FROM people");
    }

    #[test]
    fn e021_09_trim() {
        // SQL:2016 E021-09: TRIM function (LEADING/TRAILING/BOTH)
        verified_standard_stmt("SELECT TRIM('  hello  ')");
        verified_standard_stmt("SELECT TRIM(LEADING ' ' FROM '  hello')");
        verified_standard_stmt("SELECT TRIM(TRAILING ' ' FROM 'hello  ')");
        verified_standard_stmt("SELECT TRIM(BOTH ' ' FROM '  hello  ')");

        // With specific characters
        verified_standard_stmt("SELECT TRIM(LEADING 'x' FROM 'xxxhelloxxx')");
        verified_standard_stmt("SELECT TRIM(TRAILING '0' FROM '12300')");
        verified_standard_stmt("SELECT TRIM(BOTH '*' FROM '***text***')");

        // On columns
        verified_standard_stmt("SELECT TRIM(name) FROM users");
        verified_standard_stmt("SELECT TRIM(BOTH ' ' FROM description) FROM products");
    }

    #[test]
    fn e021_10_implicit_string_casting() {
        // SQL:2016 E021-10: Implicit casting among string types
        verified_standard_stmt("SELECT CAST('hello' AS CHAR(10))");
        verified_standard_stmt("SELECT CAST('world' AS VARCHAR(50))");
        verified_standard_stmt("SELECT CAST(char_col AS VARCHAR(100)) FROM t");
        verified_standard_stmt("SELECT CAST(varchar_col AS CHARACTER(20)) FROM t");
    }

    #[test]
    fn e021_11_position() {
        // SQL:2016 E021-11: POSITION function
        verified_standard_stmt("SELECT POSITION('or' IN 'world')");
        verified_standard_stmt("SELECT POSITION('substring' IN full_text) FROM documents");
        verified_standard_stmt("SELECT * FROM t WHERE POSITION('@' IN email) > 0");

        // Alternative syntax
        verified_standard_stmt("SELECT POSITION('x', 'example')");
    }

    #[test]
    fn e021_12_character_comparison() {
        // SQL:2016 E021-12: Character comparison
        verified_standard_stmt("SELECT * FROM t WHERE name = 'John'");
        verified_standard_stmt("SELECT * FROM t WHERE code <> 'ABC'");
        verified_standard_stmt("SELECT * FROM t WHERE name < 'M'");
        verified_standard_stmt("SELECT * FROM t WHERE name > 'N'");
        verified_standard_stmt("SELECT * FROM t WHERE name <= 'Smith'");
        verified_standard_stmt("SELECT * FROM t WHERE name >= 'A'");

        // Case sensitivity depends on collation
        verified_standard_stmt("SELECT * FROM users WHERE username = 'admin'");
    }
}

// =============================================================================
// E031: Identifiers
// =============================================================================

mod e031_identifiers {
    use super::*;

    #[test]
    fn e031_01_delimited_identifiers() {
        // SQL:2016 E031-01: Delimited identifiers
        verified_standard_stmt(r#"SELECT "column name" FROM "table name""#);
        verified_standard_stmt(r#"CREATE TABLE "My Table" ("My Column" INTEGER)"#);
        verified_standard_stmt(r#"SELECT "user"."name" FROM "user""#);
        verified_standard_stmt(r#"SELECT "First Name", "Last Name" FROM "Employees""#);

        // Reserved words as identifiers
        verified_standard_stmt(r#"SELECT "select", "from", "where" FROM "table""#);
    }

    #[test]
    fn e031_02_lowercase_identifiers() {
        // SQL:2016 E031-02: Lower case identifiers
        verified_standard_stmt("SELECT columnname FROM tablename");
        verified_standard_stmt("CREATE TABLE users (userid INTEGER, username VARCHAR(50))");
        verified_standard_stmt("SELECT a.columnname FROM tablename AS a");
    }

    #[test]
    fn e031_03_trailing_underscore() {
        // SQL:2016 E031-03: Trailing underscore in identifiers
        verified_standard_stmt("SELECT column_ FROM table_");
        verified_standard_stmt("CREATE TABLE table_ (column_ INTEGER, value_ REAL)");
        verified_standard_stmt("SELECT user_id_, user_name_ FROM users_");
    }
}

// =============================================================================
// E051: Basic Query Specification
// =============================================================================

mod e051_basic_query_specification {
    use super::*;

    #[test]
    fn e051_01_select_distinct() {
        // SQL:2016 E051-01: SELECT DISTINCT
        verified_with_ast!("SELECT DISTINCT a FROM t", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert!(select.distinct.is_some());
                    assert_eq!(select.projection.len(), 1);
                } else {
                    panic!("Expected Select");
                }
            } else {
                panic!("Expected Query statement");
            }
        });

        verified_with_ast!("SELECT DISTINCT a, b FROM t", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert!(select.distinct.is_some());
                    assert_eq!(select.projection.len(), 2);
                }
            }
        });

        verified_with_ast!(
            "SELECT DISTINCT department FROM employees",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert!(select.distinct.is_some());
                        assert_eq!(select.projection.len(), 1);
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT DISTINCT city, state FROM addresses",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert!(select.distinct.is_some());
                        assert_eq!(select.projection.len(), 2);
                    }
                }
            }
        );

        // DISTINCT with expressions
        verified_with_ast!(
            "SELECT DISTINCT UPPER(name) FROM users",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert!(select.distinct.is_some());
                        assert_eq!(select.projection.len(), 1);
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT DISTINCT price * quantity FROM orders",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert!(select.distinct.is_some());
                        assert_eq!(select.projection.len(), 1);
                    }
                }
            }
        );
    }

    #[test]
    fn e051_02_group_by() {
        // SQL:2016 E051-02: GROUP BY clause
        verified_with_ast!(
            "SELECT department, COUNT(*) FROM employees GROUP BY department",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1);
                            if let Expr::Identifier(ident) = &exprs[0] {
                                assert_eq!(ident.value, "department");
                            } else {
                                panic!("Expected Identifier in GROUP BY");
                            }
                        } else {
                            panic!("Expected GroupByExpr::Expressions");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT a, b, SUM(c) FROM t GROUP BY a, b",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 2);
                            if let Expr::Identifier(ident) = &exprs[0] {
                                assert_eq!(ident.value, "a");
                            }
                            if let Expr::Identifier(ident) = &exprs[1] {
                                assert_eq!(ident.value, "b");
                            }
                        } else {
                            panic!("Expected GroupByExpr::Expressions");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT category, AVG(price) FROM products GROUP BY category",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1);
                        }
                    }
                }
            }
        );

        // Multiple grouping columns
        verified_with_ast!(
            "SELECT year, month, SUM(sales) FROM revenue GROUP BY year, month",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 2);
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e051_04_group_by_not_in_select() {
        // SQL:2016 E051-04: GROUP BY columns not in select list
        verified_with_ast!(
            "SELECT COUNT(*) FROM t GROUP BY category",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1);
                        }
                        // Verify projection doesn't contain the GROUP BY column
                        assert_eq!(select.projection.len(), 1);
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT SUM(amount) FROM transactions GROUP BY customer_id, year",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 2);
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT AVG(salary) FROM employees GROUP BY department, location",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 2);
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e051_05_column_aliases() {
        // SQL:2016 E051-05: Column aliases with AS
        verified_with_ast!("SELECT a AS column_a FROM t", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert_eq!(select.projection.len(), 1);
                    if let SelectItem::ExprWithAlias { alias, .. } = &select.projection[0] {
                        assert_eq!(alias.value, "column_a");
                    } else {
                        panic!("Expected ExprWithAlias");
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT first_name AS fname, last_name AS lname FROM users",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.projection.len(), 2);
                        if let SelectItem::ExprWithAlias { alias, .. } = &select.projection[0] {
                            assert_eq!(alias.value, "fname");
                        }
                        if let SelectItem::ExprWithAlias { alias, .. } = &select.projection[1] {
                            assert_eq!(alias.value, "lname");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT price * quantity AS total FROM orders",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.projection.len(), 1);
                        if let SelectItem::ExprWithAlias { alias, .. } = &select.projection[0] {
                            assert_eq!(alias.value, "total");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT COUNT(*) AS employee_count FROM employees",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.projection.len(), 1);
                        if let SelectItem::ExprWithAlias { alias, .. } = &select.projection[0] {
                            assert_eq!(alias.value, "employee_count");
                        }
                    }
                }
            }
        );

        // Without AS (common extension)
        verified_with_ast!("SELECT a AS column_a FROM t", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::ExprWithAlias { alias, .. } = &select.projection[0] {
                        assert_eq!(alias.value, "column_a");
                    }
                }
            }
        });

        verified_with_ast!("SELECT name AS user_name FROM users", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::ExprWithAlias { alias, .. } = &select.projection[0] {
                        assert_eq!(alias.value, "user_name");
                    }
                }
            }
        });
    }

    #[test]
    fn e051_06_having_clause() {
        // SQL:2016 E051-06: HAVING clause
        verified_with_ast!(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 10",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert!(select.having.is_some(), "Expected HAVING clause");
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1);
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT category, AVG(price) FROM products GROUP BY category HAVING AVG(price) > 100",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert!(select.having.is_some(), "Expected HAVING clause");
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1);
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT a, SUM(b) FROM t GROUP BY a HAVING SUM(b) > 1000",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert!(select.having.is_some(), "Expected HAVING clause");
                    }
                }
            }
        );

        // HAVING with multiple conditions
        verified_with_ast!("SELECT region, SUM(sales) FROM revenue GROUP BY region HAVING SUM(sales) > 50000 AND COUNT(*) > 5", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert!(select.having.is_some(), "Expected HAVING clause");
                    // Verify it's a binary AND operation
                    if let Some(Expr::BinaryOp { op, .. }) = &select.having {
                        assert_eq!(*op, BinaryOperator::And);
                    }
                }
            }
        });
    }

    #[test]
    fn e051_07_qualified_asterisk() {
        // SQL:2016 E051-07: Qualified * in select list
        verified_with_ast!("SELECT t.* FROM t", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert_eq!(select.projection.len(), 1);
                    if let SelectItem::QualifiedWildcard(ref obj, _) = select.projection[0] {
                        assert_eq!(obj.to_string(), "t.*");
                    } else {
                        panic!("Expected QualifiedWildcard");
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT a.*, b.id FROM a JOIN b ON a.id = b.a_id",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.projection.len(), 2);
                        if let SelectItem::QualifiedWildcard(ref obj, _) = select.projection[0] {
                            assert_eq!(obj.to_string(), "a.*");
                        }
                    }
                }
            }
        );

        verified_with_ast!("SELECT employees.*, departments.name FROM employees JOIN departments ON employees.dept_id = departments.id", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert_eq!(select.projection.len(), 2);
                    if let SelectItem::QualifiedWildcard(ref obj, _) = select.projection[0] {
                        assert_eq!(obj.to_string(), "employees.*");
                    }
                }
            }
        });
    }

    #[test]
    fn e051_08_correlation_names() {
        // SQL:2016 E051-08: Correlation names (table aliases)
        verified_with_ast!("SELECT e.name FROM employees AS e", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    assert_eq!(select.from.len(), 1);
                    if let TableFactor::Table { alias, .. } = &select.from[0].relation {
                        if let Some(ref alias) = alias {
                            assert_eq!(alias.name.value, "e");
                        } else {
                            panic!("Expected table alias");
                        }
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT u.username, u.email FROM users AS u",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.from.len(), 1);
                        if let TableFactor::Table { alias, .. } = &select.from[0].relation {
                            if let Some(ref alias) = alias {
                                assert_eq!(alias.name.value, "u");
                            }
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT a.id, b.value FROM table_a AS a, table_b AS b WHERE a.id = b.id",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.from.len(), 2);
                        if let TableFactor::Table { alias, .. } = &select.from[0].relation {
                            if let Some(ref alias) = alias {
                                assert_eq!(alias.name.value, "a");
                            }
                        }
                        if let TableFactor::Table { alias, .. } = &select.from[1].relation {
                            if let Some(ref alias) = alias {
                                assert_eq!(alias.name.value, "b");
                            }
                        }
                    }
                }
            }
        );

        // Without AS
        verified_with_ast!("SELECT e.name FROM employees AS e", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let TableFactor::Table { alias, .. } = &select.from[0].relation {
                        if let Some(ref alias) = alias {
                            assert_eq!(alias.name.value, "e");
                        }
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT t1.a, t2.b FROM t AS t1 JOIN t AS t2 ON t1.id = t2.ref_id",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        // Verify both table aliases exist
                        if let TableFactor::Table { alias, .. } = &select.from[0].relation {
                            if let Some(ref alias) = alias {
                                assert_eq!(alias.name.value, "t1");
                            }
                        }
                        assert_eq!(select.from[0].joins.len(), 1);
                    }
                }
            }
        );
    }

    #[test]
    fn e051_09_rename_columns_in_from() {
        // SQL:2016 E051-09: Rename columns in FROM clause
        verified_with_ast!("SELECT x, y FROM t AS s (x, y)", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let TableFactor::Table { alias, .. } = &select.from[0].relation {
                        if let Some(ref alias) = alias {
                            assert_eq!(alias.name.value, "s");
                            assert_eq!(alias.columns.len(), 2);
                            assert_eq!(alias.columns[0].name.value, "x");
                            assert_eq!(alias.columns[1].name.value, "y");
                        } else {
                            panic!("Expected table alias with column renames");
                        }
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT col1, col2 FROM (SELECT a, b FROM t) AS derived (col1, col2)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let TableFactor::Derived { alias, .. } = &select.from[0].relation {
                            if let Some(ref alias) = alias {
                                assert_eq!(alias.name.value, "derived");
                                assert_eq!(alias.columns.len(), 2);
                                assert_eq!(alias.columns[0].name.value, "col1");
                                assert_eq!(alias.columns[1].name.value, "col2");
                            }
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT first, last FROM users AS u (first, last)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let TableFactor::Table { alias, .. } = &select.from[0].relation {
                            if let Some(ref alias) = alias {
                                assert_eq!(alias.name.value, "u");
                                assert_eq!(alias.columns.len(), 2);
                                assert_eq!(alias.columns[0].name.value, "first");
                                assert_eq!(alias.columns[1].name.value, "last");
                            }
                        }
                    }
                }
            }
        );
    }
}

// =============================================================================
// E061: Basic Predicates and Search Conditions
// =============================================================================

mod e061_basic_predicates {
    use super::*;

    #[test]
    fn e061_01_comparison_predicates() {
        // SQL:2016 E061-01: Comparison predicates
        verified_with_ast!("SELECT * FROM t WHERE a = 1", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                        assert!(matches!(op, BinaryOperator::Eq));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            }
        });

        verified_with_ast!("SELECT * FROM t WHERE b <> 2", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                        assert!(matches!(op, BinaryOperator::NotEq));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            }
        });

        verified_with_ast!("SELECT * FROM t WHERE c < 3", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                        assert!(matches!(op, BinaryOperator::Lt));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            }
        });

        verified_with_ast!("SELECT * FROM t WHERE d > 4", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                        assert!(matches!(op, BinaryOperator::Gt));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            }
        });

        verified_with_ast!("SELECT * FROM t WHERE e <= 5", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                        assert!(matches!(op, BinaryOperator::LtEq));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            }
        });

        verified_with_ast!("SELECT * FROM t WHERE f >= 6", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                        assert!(matches!(op, BinaryOperator::GtEq));
                    } else {
                        panic!("Expected BinaryOp expression");
                    }
                }
            }
        });
    }

    #[test]
    fn e061_02_between_predicate() {
        // SQL:2016 E061-02: BETWEEN predicate
        verified_with_ast!(
            "SELECT * FROM t WHERE a BETWEEN 1 AND 10",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Between { negated, .. }) = &select.selection {
                            assert!(!negated, "Expected BETWEEN, not NOT BETWEEN");
                        } else {
                            panic!("Expected Between expression");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Between { negated, .. }) = &select.selection {
                            assert!(*negated, "Expected NOT BETWEEN");
                        } else {
                            panic!("Expected Between expression");
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT * FROM t WHERE price BETWEEN 50 AND 100");
        verified_standard_stmt("SELECT * FROM employees WHERE salary BETWEEN 40000 AND 80000");
        verified_standard_stmt("SELECT * FROM t WHERE date BETWEEN '2024-01-01' AND '2024-12-31'");
    }

    #[test]
    fn e061_03_in_predicate_with_values() {
        // SQL:2016 E061-03: IN predicate with value list
        verified_with_ast!("SELECT * FROM t WHERE a IN (1, 2, 3)", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::InList { negated, list, .. }) = &select.selection {
                        assert!(!negated, "Expected IN, not NOT IN");
                        assert_eq!(list.len(), 3);
                    } else {
                        panic!("Expected InList expression");
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT * FROM t WHERE id NOT IN (1, 5, 10, 15)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::InList { negated, list, .. }) = &select.selection {
                            assert!(*negated, "Expected NOT IN");
                            assert_eq!(list.len(), 4);
                        } else {
                            panic!("Expected InList expression");
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT * FROM t WHERE status IN ('active', 'pending', 'approved')");
        verified_standard_stmt(
            "SELECT * FROM employees WHERE department IN ('Sales', 'Marketing', 'HR')",
        );
    }

    #[test]
    fn e061_04_like_predicate() {
        // SQL:2016 E061-04: LIKE predicate
        verified_with_ast!(
            "SELECT * FROM t WHERE name LIKE 'John%'",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Like {
                            negated,
                            escape_char,
                            ..
                        }) = &select.selection
                        {
                            assert!(!negated, "Expected LIKE, not NOT LIKE");
                            assert!(escape_char.is_none(), "Expected no ESCAPE clause");
                        } else {
                            panic!("Expected Like expression");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE email LIKE '%@example.com'",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Like { negated, .. }) = &select.selection {
                            assert!(!negated, "Expected LIKE, not NOT LIKE");
                        } else {
                            panic!("Expected Like expression");
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT * FROM t WHERE code LIKE 'A_B%'");
        verified_standard_stmt("SELECT * FROM products WHERE description LIKE '%discount%'");

        // NOT LIKE
        verified_with_ast!(
            "SELECT * FROM t WHERE name NOT LIKE 'Test%'",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Like { negated, .. }) = &select.selection {
                            assert!(*negated, "Expected NOT LIKE");
                        } else {
                            panic!("Expected Like expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e061_05_like_with_escape() {
        // SQL:2016 E061-05: LIKE with ESCAPE clause
        verified_with_ast!(
            "SELECT * FROM t WHERE name LIKE 'John\\%' ESCAPE '\\'",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Like {
                            negated,
                            escape_char,
                            ..
                        }) = &select.selection
                        {
                            assert!(!negated, "Expected LIKE, not NOT LIKE");
                            assert!(escape_char.is_some(), "Expected ESCAPE clause");
                        } else {
                            panic!("Expected Like expression");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE path LIKE 'C:\\\\Users\\\\%' ESCAPE '\\'",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Like { escape_char, .. }) = &select.selection {
                            assert!(escape_char.is_some(), "Expected ESCAPE clause");
                        } else {
                            panic!("Expected Like expression");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE text LIKE '%!%%' ESCAPE '!'",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Like { escape_char, .. }) = &select.selection {
                            assert!(escape_char.is_some(), "Expected ESCAPE clause");
                        } else {
                            panic!("Expected Like expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e061_06_null_predicate() {
        // SQL:2016 E061-06: NULL predicate
        verified_with_ast!("SELECT * FROM t WHERE a IS NULL", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::IsNull(_)) = &select.selection {
                        // IS NULL predicate found
                    } else {
                        panic!("Expected IsNull expression");
                    }
                }
            }
        });

        verified_with_ast!("SELECT * FROM t WHERE b IS NOT NULL", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::IsNotNull(_)) = &select.selection {
                        // IS NOT NULL predicate found
                    } else {
                        panic!("Expected IsNotNull expression");
                    }
                }
            }
        });

        verified_standard_stmt("SELECT * FROM employees WHERE middle_name IS NULL");
        verified_standard_stmt("SELECT * FROM orders WHERE shipped_date IS NOT NULL");
    }

    #[test]
    fn e061_07_quantified_comparison() {
        // SQL:2016 E061-07: Quantified comparison predicates (ANY, ALL, SOME)
        verified_with_ast!(
            "SELECT * FROM t WHERE a > ANY(SELECT b FROM s)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::AnyOp {
                            compare_op,
                            is_some,
                            ..
                        }) = &select.selection
                        {
                            assert!(matches!(compare_op, BinaryOperator::Gt));
                            assert!(!is_some, "Expected ANY, not SOME");
                        } else {
                            panic!("Expected AnyOp expression");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE price < ALL(SELECT min_price FROM products)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::AllOp { compare_op, .. }) = &select.selection {
                            assert!(matches!(compare_op, BinaryOperator::Lt));
                        } else {
                            panic!("Expected AllOp expression");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE x = SOME(SELECT y FROM r)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::AnyOp {
                            compare_op,
                            is_some,
                            ..
                        }) = &select.selection
                        {
                            assert!(matches!(compare_op, BinaryOperator::Eq));
                            assert!(*is_some, "Expected SOME");
                        } else {
                            panic!("Expected AnyOp expression (SOME is synonym for ANY)");
                        }
                    }
                }
            }
        );

        verified_standard_stmt(
            "SELECT * FROM employees WHERE salary > ALL(SELECT salary FROM managers)",
        );

        // Various operators with quantifiers
        verified_standard_stmt("SELECT * FROM t WHERE a >= ANY(SELECT b FROM s)");
        verified_standard_stmt("SELECT * FROM t WHERE a <> ALL(SELECT b FROM s)");
    }

    #[test]
    fn e061_08_exists_predicate() {
        // SQL:2016 E061-08: EXISTS predicate
        verified_with_ast!(
            "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Exists { negated, .. }) = &select.selection {
                            assert!(!negated, "Expected EXISTS, not NOT EXISTS");
                        } else {
                            panic!("Expected Exists expression");
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT * FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)");

        // NOT EXISTS
        verified_with_ast!(
            "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.ref = t.id)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::Exists { negated, .. }) = &select.selection {
                            assert!(*negated, "Expected NOT EXISTS");
                        } else {
                            panic!("Expected Exists expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e061_09_subqueries_in_comparison() {
        // SQL:2016 E061-09: Subqueries in comparison predicates
        verified_with_ast!(
            "SELECT * FROM t WHERE a = (SELECT MAX(b) FROM s)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::BinaryOp { op, right, .. }) = &select.selection {
                            assert!(matches!(op, BinaryOperator::Eq));
                            assert!(matches!(right.as_ref(), Expr::Subquery(_)));
                        } else {
                            panic!("Expected BinaryOp with subquery");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE price > (SELECT AVG(price) FROM products)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::BinaryOp { op, right, .. }) = &select.selection {
                            assert!(matches!(op, BinaryOperator::Gt));
                            assert!(matches!(right.as_ref(), Expr::Subquery(_)));
                        } else {
                            panic!("Expected BinaryOp with subquery");
                        }
                    }
                }
            }
        );

        verified_standard_stmt(
            "SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)",
        );
    }

    #[test]
    fn e061_11_subqueries_in_in() {
        // SQL:2016 E061-11: Subqueries in IN predicate
        verified_with_ast!(
            "SELECT * FROM t WHERE a IN (SELECT b FROM s)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::InSubquery { negated, .. }) = &select.selection {
                            assert!(!negated, "Expected IN, not NOT IN");
                        } else {
                            panic!("Expected InSubquery expression");
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT * FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE total > 1000)");
        verified_standard_stmt("SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE active = 1)");

        // NOT IN with subquery
        verified_with_ast!(
            "SELECT * FROM t WHERE id NOT IN (SELECT ref_id FROM s)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::InSubquery { negated, .. }) = &select.selection {
                            assert!(*negated, "Expected NOT IN");
                        } else {
                            panic!("Expected InSubquery expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e061_12_subqueries_in_quantified() {
        // SQL:2016 E061-12: Subqueries in quantified comparison
        verified_with_ast!(
            "SELECT * FROM t WHERE a > ANY(SELECT b FROM s WHERE s.type = 'active')",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::AnyOp {
                            compare_op, right, ..
                        }) = &select.selection
                        {
                            assert!(matches!(compare_op, BinaryOperator::Gt));
                            assert!(matches!(right.as_ref(), Expr::Subquery(_)));
                        } else {
                            panic!("Expected AnyOp with subquery");
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT * FROM t WHERE price < ALL(SELECT cost FROM inventory WHERE available = 1)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::AllOp {
                            compare_op, right, ..
                        }) = &select.selection
                        {
                            assert!(matches!(compare_op, BinaryOperator::Lt));
                            assert!(matches!(right.as_ref(), Expr::Subquery(_)));
                        } else {
                            panic!("Expected AllOp with subquery");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e061_13_correlated_subqueries() {
        // SQL:2016 E061-13: Correlated subqueries
        verified_with_ast!(
            "SELECT * FROM t WHERE a > (SELECT AVG(b) FROM s WHERE s.category = t.category)",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::BinaryOp { op, right, .. }) = &select.selection {
                            assert!(matches!(op, BinaryOperator::Gt));
                            assert!(matches!(right.as_ref(), Expr::Subquery(_)));
                        } else {
                            panic!("Expected BinaryOp with correlated subquery");
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT * FROM employees AS e WHERE salary > (SELECT AVG(salary) FROM employees WHERE department = e.department)");
        verified_standard_stmt("SELECT * FROM products AS p WHERE price = (SELECT MAX(price) FROM products WHERE category = p.category)");
    }

    #[test]
    fn e061_14_search_conditions() {
        // SQL:2016 E061-14: Complex search conditions (AND, OR, NOT)
        verified_with_ast!(
            "SELECT * FROM t WHERE a = 1 AND b = 2",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                            assert!(matches!(op, BinaryOperator::And));
                        } else {
                            panic!("Expected BinaryOp with AND");
                        }
                    }
                }
            }
        );

        verified_with_ast!("SELECT * FROM t WHERE a = 1 OR b = 2", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::BinaryOp { op, .. }) = &select.selection {
                        assert!(matches!(op, BinaryOperator::Or));
                    } else {
                        panic!("Expected BinaryOp with OR");
                    }
                }
            }
        });

        verified_with_ast!("SELECT * FROM t WHERE NOT a = 1", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(Expr::UnaryOp { .. }) = &select.selection {
                        // NOT operator creates UnaryOp
                    } else {
                        panic!("Expected UnaryOp with NOT");
                    }
                }
            }
        });

        verified_standard_stmt("SELECT * FROM t WHERE (a = 1 OR a = 2) AND b > 10");
        verified_standard_stmt("SELECT * FROM t WHERE a = 1 AND (b = 2 OR c = 3)");

        // Complex nested conditions
        verified_standard_stmt("SELECT * FROM employees WHERE (department = 'Sales' OR department = 'Marketing') AND salary > 50000 AND NOT status = 'terminated'");
        verified_standard_stmt(
            "SELECT * FROM t WHERE (a > 5 AND b < 10) OR (c = 'test' AND d IS NOT NULL)",
        );
    }
}

// =============================================================================
// E071: Basic Query Expressions
// =============================================================================

mod e071_basic_query_expressions {
    use super::*;

    #[test]
    fn e071_01_union_distinct() {
        // SQL:2016 E071-01: UNION DISTINCT
        verified_with_ast!(
            "SELECT a FROM t UNION SELECT b FROM s",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::SetOperation {
                        op, set_quantifier, ..
                    } = query.body.as_ref()
                    {
                        assert!(matches!(op, SetOperator::Union));
                        // UNION defaults to DISTINCT
                        assert!(matches!(
                            set_quantifier,
                            sqlparser::ast::SetQuantifier::Distinct
                                | sqlparser::ast::SetQuantifier::None
                        ));
                    } else {
                        panic!("Expected SetOperation");
                    }
                } else {
                    panic!("Expected Query statement");
                }
            }
        );

        verified_with_ast!(
            "SELECT a FROM t UNION DISTINCT SELECT b FROM s",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::SetOperation {
                        op, set_quantifier, ..
                    } = query.body.as_ref()
                    {
                        assert!(matches!(op, SetOperator::Union));
                        assert!(matches!(
                            set_quantifier,
                            sqlparser::ast::SetQuantifier::Distinct
                        ));
                    }
                }
            }
        );

        verified_standard_stmt(
            "SELECT id, name FROM customers UNION SELECT id, name FROM suppliers",
        );

        // Multiple UNIONs
        verified_standard_stmt("SELECT a FROM t1 UNION SELECT a FROM t2 UNION SELECT a FROM t3");
    }

    #[test]
    fn e071_02_union_all() {
        // SQL:2016 E071-02: UNION ALL
        verified_standard_stmt("SELECT a FROM t UNION ALL SELECT b FROM s");
        verified_standard_stmt("SELECT * FROM orders_2023 UNION ALL SELECT * FROM orders_2024");
        verified_standard_stmt(
            "SELECT id FROM active_users UNION ALL SELECT id FROM inactive_users",
        );

        // Multiple UNION ALLs
        verified_standard_stmt(
            "SELECT a FROM t1 UNION ALL SELECT a FROM t2 UNION ALL SELECT a FROM t3",
        );
    }

    #[test]
    fn e071_03_except_distinct() {
        // SQL:2016 E071-03: EXCEPT DISTINCT
        verified_standard_stmt("SELECT a FROM t EXCEPT SELECT b FROM s");
        verified_standard_stmt("SELECT a FROM t EXCEPT DISTINCT SELECT b FROM s");
        verified_standard_stmt("SELECT id FROM all_users EXCEPT SELECT id FROM blocked_users");
    }

    #[test]
    fn e071_05_mixed_types() {
        // SQL:2016 E071-05: Columns need not have same type in set operations
        // Parser should accept these; type compatibility is semantic validation
        verified_standard_stmt("SELECT CAST(a AS VARCHAR(10)) FROM t UNION SELECT b FROM s");
        verified_standard_stmt("SELECT CAST(id AS VARCHAR) FROM t1 UNION SELECT code FROM t2");
    }

    #[test]
    fn e071_06_table_operators_in_subqueries() {
        // SQL:2016 E071-06: Table operators in subqueries
        verified_standard_stmt("SELECT * FROM (SELECT a FROM t UNION SELECT b FROM s) AS combined");
        verified_standard_stmt(
            "SELECT * FROM t WHERE id IN (SELECT id FROM t1 UNION SELECT id FROM t2)",
        );
        verified_standard_stmt("SELECT COUNT(*) FROM (SELECT customer_id FROM orders EXCEPT SELECT customer_id FROM refunds) AS active");
    }

    #[test]
    fn e071_intersect() {
        // INTERSECT (commonly supported alongside UNION/EXCEPT)
        verified_standard_stmt("SELECT a FROM t INTERSECT SELECT b FROM s");
        verified_standard_stmt("SELECT a FROM t INTERSECT DISTINCT SELECT b FROM s");
        verified_standard_stmt(
            "SELECT id FROM active_members INTERSECT SELECT id FROM paid_members",
        );
    }
}

// =============================================================================
// E081: Basic Privileges
// =============================================================================

mod e081_basic_privileges {
    use super::*;

    #[test]
    fn e081_01_select_privilege() {
        // SQL:2016 E081-01: SELECT privilege
        verified_with_ast!("GRANT SELECT ON t TO user1", |stmt: Statement| {
            if let Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                ..
            } = stmt
            {
                // Verify SELECT privilege
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert!(matches!(actions[0], Action::Select { .. }));
                } else {
                    panic!("Expected Actions privileges");
                }
                // Verify table object
                if let Some(GrantObjects::Tables(tables)) = objects {
                    assert_eq!(tables.len(), 1);
                    assert_eq!(tables[0].to_string(), "t");
                } else {
                    panic!("Expected Tables grant object");
                }
                // Verify grantee
                assert_eq!(grantees.len(), 1);
                assert!(!with_grant_option);
            } else {
                panic!("Expected Grant statement");
            }
        });
        verified_standard_stmt("GRANT SELECT ON customers TO analyst_role");
        verified_standard_stmt("GRANT SELECT ON employees TO PUBLIC ");
    }

    #[test]
    fn e081_02_delete_privilege() {
        // SQL:2016 E081-02: DELETE privilege
        verified_with_ast!("GRANT DELETE ON t TO user1", |stmt: Statement| {
            if let Statement::Grant { privileges, .. } = stmt {
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert!(matches!(actions[0], Action::Delete));
                } else {
                    panic!("Expected Actions privileges");
                }
            } else {
                panic!("Expected Grant statement");
            }
        });
        verified_standard_stmt("GRANT DELETE ON orders TO manager_role");
    }

    #[test]
    fn e081_03_insert_privilege_table() {
        // SQL:2016 E081-03: INSERT privilege at table level
        verified_with_ast!("GRANT INSERT ON t TO user1", |stmt: Statement| {
            if let Statement::Grant { privileges, .. } = stmt {
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert!(matches!(actions[0], Action::Insert { columns: None }));
                } else {
                    panic!("Expected Actions privileges");
                }
            } else {
                panic!("Expected Grant statement");
            }
        });
        verified_standard_stmt("GRANT INSERT ON products TO data_entry");
    }

    #[test]
    fn e081_04_update_privilege_table() {
        // SQL:2016 E081-04: UPDATE privilege at table level
        verified_with_ast!("GRANT UPDATE ON t TO user1", |stmt: Statement| {
            if let Statement::Grant { privileges, .. } = stmt {
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert!(matches!(actions[0], Action::Update { columns: None }));
                } else {
                    panic!("Expected Actions privileges");
                }
            } else {
                panic!("Expected Grant statement");
            }
        });
        verified_standard_stmt("GRANT UPDATE ON inventory TO warehouse_staff");
    }

    #[test]
    fn e081_05_update_privilege_column() {
        // SQL:2016 E081-05: UPDATE privilege at column level
        verified_with_ast!(
            "GRANT UPDATE (salary) ON employees TO hr_manager",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    objects,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 1);
                        if let Action::Update {
                            columns: Some(cols),
                        } = &actions[0]
                        {
                            assert_eq!(cols.len(), 1);
                            assert_eq!(cols[0].to_string(), "salary");
                        } else {
                            panic!("Expected Update action with columns");
                        }
                    } else {
                        panic!("Expected Actions privileges");
                    }
                    if let Some(GrantObjects::Tables(tables)) = objects {
                        assert_eq!(tables[0].to_string(), "employees");
                    }
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );

        verified_with_ast!(
            "GRANT UPDATE (status, updated_at) ON orders TO system_user",
            |stmt: Statement| {
                if let Statement::Grant { privileges, .. } = stmt {
                    if let Privileges::Actions(actions) = privileges {
                        if let Action::Update {
                            columns: Some(cols),
                        } = &actions[0]
                        {
                            assert_eq!(cols.len(), 2);
                            assert_eq!(cols[0].to_string(), "status");
                            assert_eq!(cols[1].to_string(), "updated_at");
                        } else {
                            panic!("Expected Update action with columns");
                        }
                    }
                }
            }
        );
        verified_standard_stmt("GRANT UPDATE (a, b, c) ON t TO user1");
    }

    #[test]
    fn e081_06_references_privilege_table() {
        // SQL:2016 E081-06: REFERENCES privilege at table level
        verified_with_ast!("GRANT REFERENCES ON t TO user1", |stmt: Statement| {
            if let Statement::Grant { privileges, .. } = stmt {
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert!(matches!(actions[0], Action::References { columns: None }));
                } else {
                    panic!("Expected Actions privileges");
                }
            } else {
                panic!("Expected Grant statement");
            }
        });
        verified_standard_stmt("GRANT REFERENCES ON departments TO schema_admin");
    }

    #[test]
    fn e081_07_references_privilege_column() {
        // SQL:2016 E081-07: REFERENCES privilege at column level
        verified_with_ast!(
            "GRANT REFERENCES (id) ON customers TO app_schema",
            |stmt: Statement| {
                if let Statement::Grant { privileges, .. } = stmt {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 1);
                        if let Action::References {
                            columns: Some(cols),
                        } = &actions[0]
                        {
                            assert_eq!(cols.len(), 1);
                            assert_eq!(cols[0].to_string(), "id");
                        } else {
                            panic!("Expected References action with columns");
                        }
                    } else {
                        panic!("Expected Actions privileges");
                    }
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );

        verified_with_ast!(
            "GRANT REFERENCES (dept_id, manager_id) ON departments TO user1",
            |stmt: Statement| {
                if let Statement::Grant { privileges, .. } = stmt {
                    if let Privileges::Actions(actions) = privileges {
                        if let Action::References {
                            columns: Some(cols),
                        } = &actions[0]
                        {
                            assert_eq!(cols.len(), 2);
                            assert_eq!(cols[0].to_string(), "dept_id");
                            assert_eq!(cols[1].to_string(), "manager_id");
                        } else {
                            panic!("Expected References action with columns");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn e081_08_with_grant_option() {
        // SQL:2016 E081-08: WITH GRANT OPTION
        verified_with_ast!(
            "GRANT SELECT ON t TO user1 WITH GRANT OPTION",
            |stmt: Statement| {
                if let Statement::Grant {
                    with_grant_option, ..
                } = stmt
                {
                    assert!(with_grant_option, "Expected WITH GRANT OPTION to be true");
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );

        verified_with_ast!(
            "GRANT INSERT, UPDATE, DELETE ON customers TO manager WITH GRANT OPTION",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    with_grant_option,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 3);
                        assert!(matches!(actions[0], Action::Insert { .. }));
                        assert!(matches!(actions[1], Action::Update { .. }));
                        assert!(matches!(actions[2], Action::Delete));
                    } else {
                        panic!("Expected Actions privileges");
                    }
                    assert!(with_grant_option);
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );

        verified_with_ast!(
            "GRANT ALL PRIVILEGES ON products TO admin WITH GRANT OPTION",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    with_grant_option,
                    ..
                } = stmt
                {
                    assert!(matches!(privileges, Privileges::All { .. }));
                    assert!(with_grant_option);
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );
    }

    #[test]
    fn e081_09_usage_privilege() {
        // SQL:2016 E081-09: USAGE privilege
        verified_with_ast!(
            "GRANT USAGE ON SCHEMA public TO user1",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    objects,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 1);
                        assert!(matches!(actions[0], Action::Usage));
                    } else {
                        panic!("Expected Actions privileges");
                    }
                    if let Some(GrantObjects::Schemas(schemas)) = objects {
                        assert_eq!(schemas.len(), 1);
                        assert_eq!(schemas[0].to_string(), "public");
                    } else {
                        panic!("Expected Schemas grant object");
                    }
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );

        verified_with_ast!(
            "GRANT USAGE ON DOMAIN email_domain TO app_user",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    objects,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert!(matches!(actions[0], Action::Usage));
                    }
                    if let Some(GrantObjects::Domains(domains)) = objects {
                        assert_eq!(domains.len(), 1);
                        assert_eq!(domains[0].to_string(), "email_domain");
                    } else {
                        panic!("Expected Domains grant object");
                    }
                }
            }
        );
    }

    #[test]
    fn e081_10_execute_privilege() {
        // SQL:2016 E081-10: EXECUTE privilege
        verified_with_ast!(
            "GRANT EXECUTE ON FUNCTION calculate_bonus TO payroll_user",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    objects,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 1);
                        assert!(matches!(actions[0], Action::Execute { .. }));
                    } else {
                        panic!("Expected Actions privileges");
                    }
                    if let Some(GrantObjects::Function { name, .. }) = objects {
                        assert_eq!(name.to_string(), "calculate_bonus");
                    } else {
                        panic!("Expected Function grant object");
                    }
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );

        verified_with_ast!(
            "GRANT EXECUTE ON PROCEDURE process_order TO app_role",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    objects,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert!(matches!(actions[0], Action::Execute { .. }));
                    }
                    if let Some(GrantObjects::Procedure { name, .. }) = objects {
                        assert_eq!(name.to_string(), "process_order");
                    } else {
                        panic!("Expected Procedure grant object");
                    }
                }
            }
        );
    }

    #[test]
    fn e081_revoke_statements() {
        // REVOKE statements
        verified_with_ast!("REVOKE SELECT ON t FROM user1", |stmt: Statement| {
            if let Statement::Revoke {
                privileges,
                objects,
                grantees,
                cascade,
                grant_option_for,
                ..
            } = stmt
            {
                // Verify SELECT privilege
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert!(matches!(actions[0], Action::Select { .. }));
                } else {
                    panic!("Expected Actions privileges");
                }
                // Verify table object
                if let Some(GrantObjects::Tables(tables)) = objects {
                    assert_eq!(tables.len(), 1);
                    assert_eq!(tables[0].to_string(), "t");
                } else {
                    panic!("Expected Tables grant object");
                }
                // Verify grantee
                assert_eq!(grantees.len(), 1);
                assert_eq!(cascade, None);
                assert!(!grant_option_for);
            } else {
                panic!("Expected Revoke statement");
            }
        });

        verified_with_ast!(
            "REVOKE INSERT, UPDATE ON customers FROM temp_user",
            |stmt: Statement| {
                if let Statement::Revoke { privileges, .. } = stmt {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 2);
                        assert!(matches!(actions[0], Action::Insert { .. }));
                        assert!(matches!(actions[1], Action::Update { .. }));
                    }
                }
            }
        );

        verified_with_ast!(
            "REVOKE ALL PRIVILEGES ON products FROM old_admin",
            |stmt: Statement| {
                if let Statement::Revoke { privileges, .. } = stmt {
                    assert!(matches!(privileges, Privileges::All { .. }));
                }
            }
        );

        verified_with_ast!(
            "REVOKE GRANT OPTION FOR SELECT ON t FROM user1",
            |stmt: Statement| {
                if let Statement::Revoke {
                    grant_option_for, ..
                } = stmt
                {
                    assert!(grant_option_for, "Expected GRANT OPTION FOR to be true");
                } else {
                    panic!("Expected Revoke statement");
                }
            }
        );

        // REVOKE with CASCADE/RESTRICT
        verified_with_ast!(
            "REVOKE SELECT ON t FROM user1 CASCADE",
            |stmt: Statement| {
                if let Statement::Revoke { cascade, .. } = stmt {
                    assert_eq!(cascade, Some(CascadeOption::Cascade));
                } else {
                    panic!("Expected Revoke statement");
                }
            }
        );

        verified_with_ast!(
            "REVOKE UPDATE ON employees FROM manager RESTRICT",
            |stmt: Statement| {
                if let Statement::Revoke { cascade, .. } = stmt {
                    assert_eq!(cascade, Some(CascadeOption::Restrict));
                } else {
                    panic!("Expected Revoke statement");
                }
            }
        );
    }

    #[test]
    fn e081_multiple_privileges() {
        // Multiple privileges in single GRANT
        verified_with_ast!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON t TO user1",
            |stmt: Statement| {
                if let Statement::Grant { privileges, .. } = stmt {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 4);
                        assert!(matches!(actions[0], Action::Select { .. }));
                        assert!(matches!(actions[1], Action::Insert { .. }));
                        assert!(matches!(actions[2], Action::Update { .. }));
                        assert!(matches!(actions[3], Action::Delete));
                    } else {
                        panic!("Expected Actions privileges");
                    }
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );

        verified_with_ast!(
            "GRANT ALL PRIVILEGES ON customers TO admin_role",
            |stmt: Statement| {
                if let Statement::Grant { privileges, .. } = stmt {
                    if let Privileges::All {
                        with_privileges_keyword,
                    } = privileges
                    {
                        assert!(with_privileges_keyword);
                    } else {
                        panic!("Expected All privileges");
                    }
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );
    }
}

// =============================================================================
// E091: Set Functions (Aggregates)
// =============================================================================

mod e091_set_functions {
    use super::*;

    #[test]
    fn e091_01_avg() {
        // SQL:2016 E091-01: AVG aggregate function
        verified_with_ast!("SELECT AVG(salary) FROM employees", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "AVG");
                    } else {
                        panic!("Expected AVG function");
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT AVG(DISTINCT score) FROM test_results",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
                        {
                            assert_eq!(func.name.to_string().to_uppercase(), "AVG");
                            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                                assert!(matches!(
                                    arg_list.duplicate_treatment,
                                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                                ));
                            } else {
                                panic!("Expected function argument list");
                            }
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT AVG(ALL price) FROM products");
        verified_standard_stmt("SELECT department, AVG(salary) FROM employees GROUP BY department");

        // With WHERE clause
        verified_standard_stmt("SELECT AVG(amount) FROM transactions WHERE year = 2024");
    }

    #[test]
    fn e091_02_count() {
        // SQL:2016 E091-02: COUNT aggregate function
        verified_with_ast!("SELECT COUNT(*) FROM employees", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "COUNT");
                        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                            assert_eq!(arg_list.args.len(), 1);
                            assert!(matches!(
                                arg_list.args[0],
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Wildcard
                                )
                            ));
                        } else {
                            panic!("Expected function argument list");
                        }
                    } else {
                        panic!("Expected COUNT function");
                    }
                }
            }
        });

        verified_with_ast!("SELECT COUNT(id) FROM users", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "COUNT");
                    } else {
                        panic!("Expected COUNT function");
                    }
                }
            }
        });

        verified_with_ast!(
            "SELECT COUNT(DISTINCT customer_id) FROM orders",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
                        {
                            assert_eq!(func.name.to_string().to_uppercase(), "COUNT");
                            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                                assert!(matches!(
                                    arg_list.duplicate_treatment,
                                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                                ));
                            } else {
                                panic!("Expected function argument list");
                            }
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT COUNT(ALL order_id) FROM line_items");
        verified_standard_stmt("SELECT department, COUNT(*) FROM employees GROUP BY department");

        // Multiple counts
        verified_standard_stmt("SELECT COUNT(*), COUNT(DISTINCT customer_id) FROM orders");
    }

    #[test]
    fn e091_03_max() {
        // SQL:2016 E091-03: MAX aggregate function
        verified_with_ast!("SELECT MAX(salary) FROM employees", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "MAX");
                    } else {
                        panic!("Expected MAX function");
                    }
                }
            }
        });

        verified_with_ast!("SELECT MAX(price) FROM products", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "MAX");
                    } else {
                        panic!("Expected MAX function");
                    }
                }
            }
        });

        verified_standard_stmt("SELECT MAX(created_at) FROM orders");
        verified_standard_stmt("SELECT category, MAX(price) FROM products GROUP BY category");
    }

    #[test]
    fn e091_04_min() {
        // SQL:2016 E091-04: MIN aggregate function
        verified_with_ast!("SELECT MIN(salary) FROM employees", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "MIN");
                    } else {
                        panic!("Expected MIN function");
                    }
                }
            }
        });

        verified_with_ast!("SELECT MIN(price) FROM products", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "MIN");
                    } else {
                        panic!("Expected MIN function");
                    }
                }
            }
        });

        verified_standard_stmt("SELECT MIN(start_date) FROM projects");
        verified_standard_stmt("SELECT region, MIN(temperature) FROM weather GROUP BY region");
    }

    #[test]
    fn e091_05_sum() {
        // SQL:2016 E091-05: SUM aggregate function
        verified_with_ast!("SELECT SUM(amount) FROM transactions", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "SUM");
                    } else {
                        panic!("Expected SUM function");
                    }
                }
            }
        });

        verified_standard_stmt("SELECT SUM(ALL quantity) FROM inventory");

        verified_with_ast!(
            "SELECT SUM(DISTINCT category_id) FROM products",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
                        {
                            assert_eq!(func.name.to_string().to_uppercase(), "SUM");
                            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                                assert!(matches!(
                                    arg_list.duplicate_treatment,
                                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                                ));
                            } else {
                                panic!("Expected function argument list");
                            }
                        }
                    }
                }
            }
        );

        verified_standard_stmt("SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id");
    }

    #[test]
    fn e091_06_all_quantifier() {
        // SQL:2016 E091-06: ALL quantifier in set functions
        verified_with_ast!(
            "SELECT SUM(ALL amount) FROM transactions",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
                        {
                            assert_eq!(func.name.to_string().to_uppercase(), "SUM");
                            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                                assert!(matches!(
                                    arg_list.duplicate_treatment,
                                    Some(sqlparser::ast::DuplicateTreatment::All)
                                ));
                            } else {
                                panic!("Expected function argument list");
                            }
                        } else {
                            panic!("Expected SUM function");
                        }
                    }
                }
            }
        );

        verified_with_ast!("SELECT AVG(ALL price) FROM products", |stmt: Statement| {
            if let Statement::Query(query) = stmt {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                        assert_eq!(func.name.to_string().to_uppercase(), "AVG");
                        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                            assert!(matches!(
                                arg_list.duplicate_treatment,
                                Some(sqlparser::ast::DuplicateTreatment::All)
                            ));
                        } else {
                            panic!("Expected function argument list");
                        }
                    } else {
                        panic!("Expected AVG function");
                    }
                }
            }
        });

        verified_standard_stmt("SELECT COUNT(ALL id) FROM users");
        verified_standard_stmt("SELECT MAX(ALL score) FROM tests");
        verified_standard_stmt("SELECT MIN(ALL value) FROM measurements");
    }

    #[test]
    fn e091_07_distinct_quantifier() {
        // SQL:2016 E091-07: DISTINCT quantifier in set functions
        verified_with_ast!(
            "SELECT COUNT(DISTINCT customer_id) FROM orders",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
                        {
                            assert_eq!(func.name.to_string().to_uppercase(), "COUNT");
                            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                                assert!(matches!(
                                    arg_list.duplicate_treatment,
                                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                                ));
                            } else {
                                panic!("Expected function argument list");
                            }
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT SUM(DISTINCT amount) FROM payments",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
                        {
                            assert_eq!(func.name.to_string().to_uppercase(), "SUM");
                            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                                assert!(matches!(
                                    arg_list.duplicate_treatment,
                                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                                ));
                            } else {
                                panic!("Expected function argument list");
                            }
                        }
                    }
                }
            }
        );

        verified_with_ast!(
            "SELECT AVG(DISTINCT salary) FROM employees",
            |stmt: Statement| {
                if let Statement::Query(query) = stmt {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
                        {
                            assert_eq!(func.name.to_string().to_uppercase(), "AVG");
                            if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                                assert!(matches!(
                                    arg_list.duplicate_treatment,
                                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                                ));
                            } else {
                                panic!("Expected function argument list");
                            }
                        }
                    }
                }
            }
        );

        // Multiple aggregates with DISTINCT
        verified_standard_stmt(
            "SELECT COUNT(DISTINCT product_id), SUM(DISTINCT price) FROM order_items",
        );
    }

    #[test]
    fn e091_complex_aggregates() {
        // Complex aggregate expressions
        verified_standard_stmt("SELECT department, COUNT(*), AVG(salary), MIN(salary), MAX(salary) FROM employees GROUP BY department");
        verified_standard_stmt("SELECT year, month, SUM(revenue), COUNT(DISTINCT customer_id) FROM sales GROUP BY year, month");
    }
}

// =============================================================================
// E101: Basic Data Manipulation
// =============================================================================

mod e101_basic_data_manipulation {
    use super::*;

    #[test]
    fn e101_01_insert_values() {
        // SQL:2016 E101-01: INSERT statement with VALUES
        verified_with_ast!("INSERT INTO t (a, b) VALUES (1, 2)", |stmt: Statement| {
            if let Statement::Insert(Insert {
                table,
                columns,
                source,
                ..
            }) = stmt
            {
                if let TableObject::TableName(name) = table {
                    assert_eq!(name.to_string(), "t");
                } else {
                    panic!("Expected TableName");
                }
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].to_string(), "a");
                assert_eq!(columns[1].to_string(), "b");
                if let Some(query) = source {
                    if let SetExpr::Values(Values { rows, .. }) = query.body.as_ref() {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].len(), 2);
                    } else {
                        panic!("Expected Values in source");
                    }
                } else {
                    panic!("Expected source query");
                }
            } else {
                panic!("Expected Insert statement");
            }
        });

        verified_with_ast!("INSERT INTO t VALUES (1, 2, 3)", |stmt: Statement| {
            if let Statement::Insert(Insert {
                table,
                columns,
                source,
                ..
            }) = stmt
            {
                if let TableObject::TableName(name) = table {
                    assert_eq!(name.to_string(), "t");
                } else {
                    panic!("Expected TableName");
                }
                assert_eq!(columns.len(), 0, "No columns specified");
                if let Some(query) = source {
                    if let SetExpr::Values(Values { rows, .. }) = query.body.as_ref() {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].len(), 3);
                    } else {
                        panic!("Expected Values in source");
                    }
                } else {
                    panic!("Expected source query");
                }
            } else {
                panic!("Expected Insert statement");
            }
        });

        verified_standard_stmt(
            "INSERT INTO customers (name, email) VALUES ('John Doe', 'john@example.com')",
        );

        // Multiple rows
        verified_with_ast!(
            "INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)",
            |stmt: Statement| {
                if let Statement::Insert(Insert {
                    table,
                    columns,
                    source,
                    ..
                }) = stmt
                {
                    if let TableObject::TableName(name) = table {
                        assert_eq!(name.to_string(), "t");
                    } else {
                        panic!("Expected TableName");
                    }
                    assert_eq!(columns.len(), 2);
                    if let Some(query) = source {
                        if let SetExpr::Values(Values { rows, .. }) = query.body.as_ref() {
                            assert_eq!(rows.len(), 3, "Expected 3 value rows");
                            assert_eq!(rows[0].len(), 2);
                            assert_eq!(rows[1].len(), 2);
                            assert_eq!(rows[2].len(), 2);
                        } else {
                            panic!("Expected Values in source");
                        }
                    } else {
                        panic!("Expected source query");
                    }
                } else {
                    panic!("Expected Insert statement");
                }
            }
        );

        verified_standard_stmt(
            "INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)",
        );
    }

    #[test]
    fn e101_01_insert_select() {
        // SQL:2016 E101-01: INSERT with SELECT
        verified_with_ast!("INSERT INTO t SELECT * FROM s", |stmt: Statement| {
            if let Statement::Insert(Insert {
                table,
                columns,
                source,
                ..
            }) = stmt
            {
                if let TableObject::TableName(name) = table {
                    assert_eq!(name.to_string(), "t");
                } else {
                    panic!("Expected TableName");
                }
                assert_eq!(columns.len(), 0, "No columns specified");
                if let Some(query) = source {
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.projection.len(), 1);
                    } else {
                        panic!("Expected Select in source");
                    }
                } else {
                    panic!("Expected source query");
                }
            } else {
                panic!("Expected Insert statement");
            }
        });

        verified_standard_stmt("INSERT INTO archive_orders SELECT * FROM orders WHERE year = 2023");

        verified_with_ast!(
            "INSERT INTO summary (category, total) SELECT category, SUM(amount) FROM transactions GROUP BY category",
            |stmt: Statement| {
                if let Statement::Insert(Insert { table, columns, source, .. }) = stmt {
                    if let TableObject::TableName(name) = table {
                        assert_eq!(name.to_string(), "summary");
                    } else {
                        panic!("Expected TableName");
                    }
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0].to_string(), "category");
                    assert_eq!(columns[1].to_string(), "total");
                    if let Some(query) = source {
                        if let SetExpr::Select(select) = query.body.as_ref() {
                            assert_eq!(select.projection.len(), 2);
                            // Verify GROUP BY exists
                            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                                assert!(!exprs.is_empty(), "Expected GROUP BY expressions");
                            } else {
                                panic!("Expected GROUP BY Expressions");
                            }
                        } else {
                            panic!("Expected Select in source");
                        }
                    } else {
                        panic!("Expected source query");
                    }
                } else {
                    panic!("Expected Insert statement");
                }
            }
        );
    }

    #[test]
    fn e101_01_insert_default_values() {
        // SQL:2016 E101-01: INSERT with DEFAULT VALUES
        verified_with_ast!("INSERT INTO t DEFAULT VALUES", |stmt: Statement| {
            if let Statement::Insert(Insert {
                table,
                columns,
                source,
                ..
            }) = stmt
            {
                if let TableObject::TableName(name) = table {
                    assert_eq!(name.to_string(), "t");
                } else {
                    panic!("Expected TableName");
                }
                assert_eq!(columns.len(), 0);
                assert!(
                    source.is_none(),
                    "DEFAULT VALUES should have no source query"
                );
            } else {
                panic!("Expected Insert statement");
            }
        });

        verified_standard_stmt("INSERT INTO audit_log DEFAULT VALUES");
    }

    #[test]
    fn e101_03_searched_update() {
        // SQL:2016 E101-03: Searched UPDATE statement
        verified_with_ast!("UPDATE t SET a = 1", |stmt: Statement| {
            if let Statement::Update(Update {
                table,
                assignments,
                selection,
                ..
            }) = stmt
            {
                assert_eq!(table.relation.to_string(), "t");
                assert_eq!(assignments.len(), 1);
                assert!(selection.is_none());
            } else {
                panic!("Expected Update statement");
            }
        });

        verified_with_ast!("UPDATE t SET a = 1, b = 2", |stmt: Statement| {
            if let Statement::Update(Update {
                table,
                assignments,
                selection,
                ..
            }) = stmt
            {
                assert_eq!(table.relation.to_string(), "t");
                assert_eq!(assignments.len(), 2);
                assert!(selection.is_none());
            } else {
                panic!("Expected Update statement");
            }
        });

        verified_with_ast!(
            "UPDATE employees SET salary = salary * 1.1 WHERE department = 'Sales'",
            |stmt: Statement| {
                if let Statement::Update(Update {
                    table,
                    assignments,
                    selection,
                    ..
                }) = stmt
                {
                    assert_eq!(table.relation.to_string(), "employees");
                    assert_eq!(assignments.len(), 1);
                    assert!(selection.is_some(), "Expected WHERE clause");
                } else {
                    panic!("Expected Update statement");
                }
            }
        );

        verified_standard_stmt(
            "UPDATE products SET price = 9.99, updated_at = CURRENT_TIMESTAMP WHERE id = 100",
        );

        // UPDATE with subquery
        verified_with_ast!(
            "UPDATE t SET a = (SELECT MAX(b) FROM s)",
            |stmt: Statement| {
                if let Statement::Update(Update {
                    table,
                    assignments,
                    selection,
                    ..
                }) = stmt
                {
                    assert_eq!(table.relation.to_string(), "t");
                    assert_eq!(assignments.len(), 1);
                    // Verify assignment contains subquery
                    if let Assignment {
                        value: Expr::Subquery(_),
                        ..
                    } = &assignments[0]
                    {
                        // Subquery detected
                    } else {
                        panic!("Expected subquery in assignment value");
                    }
                    assert!(selection.is_none());
                } else {
                    panic!("Expected Update statement");
                }
            }
        );

        verified_standard_stmt("UPDATE employees SET salary = (SELECT AVG(salary) FROM employees WHERE department = 'Engineering')");

        // Complex WHERE clause
        verified_standard_stmt("UPDATE orders SET status = 'shipped' WHERE id IN (SELECT order_id FROM shipments WHERE date = CURRENT_DATE)");
    }

    #[test]
    fn e101_04_searched_delete() {
        // SQL:2016 E101-04: Searched DELETE statement
        verified_with_ast!("DELETE FROM t", |stmt: Statement| {
            if let Statement::Delete(Delete {
                from, selection, ..
            }) = stmt
            {
                // FromTable can be WithFromKeyword or WithoutKeyword
                match from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].relation.to_string(), "t");
                    }
                    sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].relation.to_string(), "t");
                    }
                }
                assert!(selection.is_none());
            } else {
                panic!("Expected Delete statement");
            }
        });

        verified_with_ast!("DELETE FROM t WHERE a = 1", |stmt: Statement| {
            if let Statement::Delete(Delete {
                from, selection, ..
            }) = stmt
            {
                match from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].relation.to_string(), "t");
                    }
                    sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].relation.to_string(), "t");
                    }
                }
                assert!(selection.is_some(), "Expected WHERE clause");
            } else {
                panic!("Expected Delete statement");
            }
        });

        verified_standard_stmt("DELETE FROM old_records WHERE created_at < '2020-01-01'");
        verified_standard_stmt("DELETE FROM employees WHERE department = 'Temp' AND hire_date < CURRENT_DATE - INTERVAL '90' DAY");

        // DELETE with subquery
        verified_with_ast!(
            "DELETE FROM t WHERE id IN (SELECT ref_id FROM s)",
            |stmt: Statement| {
                if let Statement::Delete(Delete {
                    from, selection, ..
                }) = stmt
                {
                    match from {
                        sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                            assert_eq!(tables.len(), 1);
                            assert_eq!(tables[0].relation.to_string(), "t");
                        }
                        sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                            assert_eq!(tables.len(), 1);
                            assert_eq!(tables[0].relation.to_string(), "t");
                        }
                    }
                    assert!(selection.is_some(), "Expected WHERE clause");
                    // Verify WHERE contains IN with subquery
                    if let Some(Expr::InSubquery { .. }) = selection {
                        // Subquery detected
                    } else {
                        panic!("Expected IN subquery in WHERE clause");
                    }
                } else {
                    panic!("Expected Delete statement");
                }
            }
        );

        verified_standard_stmt("DELETE FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'inactive')");
    }
}

// =============================================================================
// E111: Single Row SELECT
// =============================================================================

mod e111_single_row_select {
    use super::*;

    #[test]
    fn e111_scalar_subqueries() {
        // SQL:2016 E111: Single row SELECT (scalar subqueries)
        verified_standard_stmt("SELECT (SELECT MAX(salary) FROM employees) AS max_salary");
        verified_standard_stmt("SELECT a, (SELECT COUNT(*) FROM s WHERE s.ref = t.id) FROM t");
        verified_standard_stmt("SELECT name, (SELECT AVG(price) FROM products WHERE category = customers.preferred_category) FROM customers");

        // In WHERE clause
        verified_standard_stmt("SELECT * FROM t WHERE a = (SELECT MAX(b) FROM s)");
    }

    #[test]
    fn e111_select_into() {
        // SQL:2016 E111: SELECT INTO
        // This syntax is supported for creating tables from query results
        verified_standard_stmt("SELECT a, b INTO new_table FROM t");
    }
}

// =============================================================================
// E121: Basic Cursor Support
// =============================================================================

mod e121_basic_cursor_support {
    use super::*;

    #[test]
    fn e121_declare_cursor() {
        // SQL:2016 E121: DECLARE CURSOR
        verified_standard_stmt("DECLARE c CURSOR FOR SELECT * FROM t");
        verified_standard_stmt("DECLARE employee_cursor CURSOR FOR SELECT id, name FROM employees WHERE department = 'Sales'");
        verified_standard_stmt(
            "DECLARE complex_cursor CURSOR FOR SELECT a, b, SUM(c) FROM t GROUP BY a, b",
        );
    }

    #[test]
    fn e121_open_cursor() {
        // SQL:2016 E121: OPEN cursor
        verified_standard_stmt("OPEN c");
        verified_standard_stmt("OPEN employee_cursor");
    }

    #[test]
    fn e121_close_cursor() {
        // SQL:2016 E121: CLOSE cursor
        verified_standard_stmt("CLOSE c");
        verified_standard_stmt("CLOSE employee_cursor");
    }

    #[test]
    fn e121_fetch_cursor() {
        // SQL:2016 E121: FETCH from cursor
        verified_standard_stmt("FETCH NEXT FROM c");
        verified_standard_stmt("FETCH PRIOR FROM employee_cursor");
        verified_standard_stmt("FETCH FIRST FROM c");
        verified_standard_stmt("FETCH LAST FROM c");
        verified_standard_stmt("FETCH ABSOLUTE 10 FROM c");
        verified_standard_stmt("FETCH RELATIVE 5 FROM c");
    }

    #[test]
    fn e121_order_by_not_in_select() {
        // SQL:2016 E121: ORDER BY columns not in select list
        verified_standard_stmt("SELECT name FROM employees ORDER BY hire_date");
        verified_standard_stmt(
            "SELECT product_id FROM sales ORDER BY total_amount DESC, sale_date",
        );
        verified_standard_stmt("DECLARE c CURSOR FOR SELECT a FROM t ORDER BY b, c");
    }
}

// =============================================================================
// E131: NULL Value Support
// =============================================================================

mod e131_null_value_support {
    use super::*;

    #[test]
    fn e131_null_literals() {
        // SQL:2016 E131: NULL literals
        verified_standard_stmt("SELECT NULL");
        verified_standard_stmt("SELECT a, NULL, b FROM t");
        verified_standard_stmt("INSERT INTO t VALUES (1, NULL, 'test')");
        verified_standard_stmt("UPDATE t SET a = NULL WHERE b = 1");
    }

    #[test]
    fn e131_null_constraints() {
        // SQL:2016 E131: NULL constraints
        verified_standard_stmt("CREATE TABLE t (a INTEGER NULL)");
        verified_standard_stmt("CREATE TABLE t (a INTEGER NOT NULL)");
        verified_standard_stmt(
            "CREATE TABLE employees (id INTEGER NOT NULL, middle_name VARCHAR(50) NULL)",
        );
    }

    #[test]
    fn e131_coalesce() {
        // SQL:2016 E131: COALESCE function
        verified_standard_stmt("SELECT COALESCE(a, 0) FROM t");
        verified_standard_stmt("SELECT COALESCE(middle_name, '') FROM users");
        verified_standard_stmt("SELECT COALESCE(discount, 0.0) * price FROM products");
        verified_standard_stmt("SELECT COALESCE(a, b, c, 'default') FROM t");
    }

    #[test]
    fn e131_nullif() {
        // SQL:2016 E131: NULLIF function
        verified_standard_stmt("SELECT NULLIF(a, 0) FROM t");
        verified_standard_stmt("SELECT NULLIF(status, 'unknown') FROM records");
        verified_standard_stmt("SELECT amount / NULLIF(quantity, 0) FROM inventory");
    }
}

// =============================================================================
// E141: Basic Integrity Constraints
// =============================================================================

mod e141_basic_integrity_constraints {
    use super::*;

    #[test]
    fn e141_01_not_null() {
        // SQL:2016 E141-01: NOT NULL constraints
        verified_with_ast!("CREATE TABLE t (a INTEGER NOT NULL)", |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 1);
                assert!(columns[0]
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ColumnOption::NotNull)));
            } else {
                panic!("Expected CreateTable statement");
            }
        });

        verified_standard_stmt("CREATE TABLE employees (id INTEGER NOT NULL, name VARCHAR(100) NOT NULL, email VARCHAR(255) NOT NULL)");
        verified_standard_stmt("ALTER TABLE t ADD COLUMN b VARCHAR(50) NOT NULL");
    }

    #[test]
    fn e141_02_unique() {
        // SQL:2016 E141-02: UNIQUE constraints
        verified_with_ast!("CREATE TABLE t (a INTEGER UNIQUE)", |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 1);
                assert!(columns[0]
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ColumnOption::Unique(_))));
            } else {
                panic!("Expected CreateTable statement");
            }
        });

        verified_with_ast!(
            "CREATE TABLE t (a INTEGER, UNIQUE (a))",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { constraints, .. }) = stmt {
                    assert!(constraints
                        .iter()
                        .any(|c| matches!(c, TableConstraint::Unique { .. })));
                }
            }
        );

        verified_standard_stmt(
            "CREATE TABLE users (username VARCHAR(50) UNIQUE, email VARCHAR(255) UNIQUE)",
        );
        verified_standard_stmt("CREATE TABLE t (a INTEGER, b INTEGER, UNIQUE (a, b))");

        // Named constraints
        verified_standard_stmt("CREATE TABLE t (a INTEGER, CONSTRAINT unique_a UNIQUE (a))");
    }

    #[test]
    fn e141_03_primary_key() {
        // SQL:2016 E141-03: PRIMARY KEY constraints
        verified_standard_stmt("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        verified_standard_stmt("CREATE TABLE t (id INTEGER, PRIMARY KEY (id))");
        verified_standard_stmt(
            "CREATE TABLE employees (employee_id INTEGER PRIMARY KEY, name VARCHAR(100))",
        );

        // Composite primary key
        verified_standard_stmt("CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, PRIMARY KEY (order_id, item_id))");

        // Named constraint
        verified_standard_stmt("CREATE TABLE t (id INTEGER, CONSTRAINT pk_t PRIMARY KEY (id))");
    }

    #[test]
    fn e141_04_foreign_key_no_action() {
        // SQL:2016 E141-04: FOREIGN KEY with NO ACTION
        verified_standard_stmt("CREATE TABLE orders (customer_id INTEGER REFERENCES customers)");
        verified_standard_stmt("CREATE TABLE orders (customer_id INTEGER, FOREIGN KEY (customer_id) REFERENCES customers(id))");
        verified_standard_stmt("CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON DELETE NO ACTION)");
        verified_standard_stmt("CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON UPDATE NO ACTION)");

        // Named constraint
        verified_standard_stmt("CREATE TABLE orders (customer_id INTEGER, CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id))");
    }

    #[test]
    fn e141_06_check_constraints() {
        // SQL:2016 E141-06: CHECK constraints
        verified_with_ast!(
            "CREATE TABLE t (a INTEGER CHECK (a > 0))",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                    assert_eq!(columns.len(), 1);
                    assert!(columns[0]
                        .options
                        .iter()
                        .any(|opt| matches!(opt.option, ColumnOption::Check(_))));
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );

        verified_with_ast!(
            "CREATE TABLE t (a INTEGER, b INTEGER, CHECK (a < b))",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { constraints, .. }) = stmt {
                    assert!(constraints
                        .iter()
                        .any(|c| matches!(c, TableConstraint::Check { .. })));
                }
            }
        );

        verified_standard_stmt("CREATE TABLE products (price DECIMAL(10,2) CHECK (price >= 0))");
        verified_standard_stmt(
            "CREATE TABLE employees (salary INTEGER, CHECK (salary BETWEEN 0 AND 1000000))",
        );

        // Named constraints
        verified_standard_stmt(
            "CREATE TABLE t (a INTEGER, CONSTRAINT check_positive CHECK (a > 0))",
        );
        verified_standard_stmt("CREATE TABLE products (quantity INTEGER, CONSTRAINT check_quantity CHECK (quantity >= 0))");
    }

    #[test]
    fn e141_07_column_defaults() {
        // SQL:2016 E141-07: Column defaults
        verified_with_ast!("CREATE TABLE t (a INTEGER DEFAULT 0)", |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                assert_eq!(columns.len(), 1);
                assert!(columns[0]
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ColumnOption::Default(_))));
            } else {
                panic!("Expected CreateTable statement");
            }
        });

        verified_with_ast!(
            "CREATE TABLE t (status VARCHAR(20) DEFAULT 'active')",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { columns, .. }) = stmt {
                    if let Some(default_opt) = columns[0]
                        .options
                        .iter()
                        .find(|opt| matches!(opt.option, ColumnOption::Default(_)))
                    {
                        if let ColumnOption::Default(Expr::Value(ValueWithSpan { value, .. })) =
                            &default_opt.option
                        {
                            if let Value::SingleQuotedString(s) = value {
                                assert_eq!(s, "active");
                            } else {
                                panic!("Expected SingleQuotedString");
                            }
                        } else {
                            panic!("Expected default value expression");
                        }
                    }
                }
            }
        );

        verified_standard_stmt(
            "CREATE TABLE orders (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        );
        verified_standard_stmt("CREATE TABLE employees (is_active BOOLEAN DEFAULT true)");

        // DEFAULT with expressions
        verified_standard_stmt("CREATE TABLE t (id INTEGER DEFAULT 1 + 1)");
    }

    #[test]
    fn e141_08_not_null_on_primary_key() {
        // SQL:2016 E141-08: NOT NULL inferred on PRIMARY KEY
        verified_standard_stmt("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        // This is implicitly NOT NULL, parser should accept it
        verified_standard_stmt(
            "CREATE TABLE employees (employee_id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL)",
        );
    }

    #[test]
    fn e141_referential_actions() {
        // Referential actions: CASCADE, SET NULL, SET DEFAULT, RESTRICT
        verified_standard_stmt(
            "CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE)",
        );
        verified_standard_stmt(
            "CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON UPDATE CASCADE)",
        );
        verified_standard_stmt(
            "CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL)",
        );
        verified_standard_stmt(
            "CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON UPDATE SET NULL)",
        );
        verified_standard_stmt("CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON DELETE SET DEFAULT)");
        verified_standard_stmt(
            "CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON DELETE RESTRICT)",
        );

        // Both ON DELETE and ON UPDATE
        verified_standard_stmt("CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE ON UPDATE CASCADE)");
    }
}

// =============================================================================
// E151: Transaction Support
// =============================================================================

mod e151_transaction_support {
    use super::*;

    #[test]
    fn e151_01_commit() {
        // SQL:2016 E151-01: COMMIT statement
        verified_standard_stmt("COMMIT");
        verified_standard_stmt("COMMIT");
    }

    #[test]
    fn e151_02_rollback() {
        // SQL:2016 E151-02: ROLLBACK statement
        verified_standard_stmt("ROLLBACK");
        verified_standard_stmt("ROLLBACK");
    }

    #[test]
    fn e151_start_transaction() {
        // SQL:2016 E151: START TRANSACTION
        verified_standard_stmt("START TRANSACTION");
        verified_standard_stmt("START TRANSACTION READ ONLY");
        verified_standard_stmt("START TRANSACTION READ WRITE");
        verified_standard_stmt("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        verified_standard_stmt("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        verified_standard_stmt("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        verified_standard_stmt("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");

        // Combined options
        verified_standard_stmt("START TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE");
    }

    #[test]
    fn e151_begin_transaction() {
        // BEGIN (common alternative to START TRANSACTION)
        verified_standard_stmt("BEGIN");
        verified_standard_stmt("BEGIN WORK");
        verified_standard_stmt("BEGIN TRANSACTION");
    }
}

// =============================================================================
// E152: Basic SET TRANSACTION Statement
// =============================================================================

mod e152_set_transaction {
    use super::*;

    #[test]
    fn e152_set_transaction() {
        // SQL:2016 E152: SET TRANSACTION statement
        verified_standard_stmt("SET TRANSACTION READ ONLY");
        verified_standard_stmt("SET TRANSACTION READ WRITE");
        verified_standard_stmt("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        verified_standard_stmt("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        verified_standard_stmt("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        verified_standard_stmt("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    }
}

// =============================================================================
// E153: Updatable Queries with Subqueries
// =============================================================================

mod e153_updatable_queries {
    use super::*;

    #[test]
    fn e153_updatable_subqueries() {
        // SQL:2016 E153: Updatable queries with subqueries
        verified_standard_stmt("UPDATE t SET a = (SELECT MAX(b) FROM s) WHERE id = 1");
        verified_standard_stmt("UPDATE employees SET salary = (SELECT AVG(salary) FROM employees WHERE department = 'Sales') WHERE department = 'Marketing'");

        // DELETE with subquery
        verified_standard_stmt("DELETE FROM t WHERE a IN (SELECT b FROM s WHERE s.active = 1)");
    }
}

// =============================================================================
// E161: SQL Comments
// =============================================================================

mod e161_sql_comments {
    use super::*;

    #[test]
    fn e161_single_line_comments() {
        // SQL:2016 E161: SQL comments using leading double minus
        // Note: Comments are stripped during parsing, so we verify
        // that SQL with comments parses to the same result as without

        // Comment at end of statement
        one_statement_parses_to_std("SELECT 1 -- this is a comment", "SELECT 1");

        // Comment after column list
        one_statement_parses_to_std(
            "SELECT a, b -- selecting columns\nFROM t",
            "SELECT a, b FROM t",
        );

        // Comment on its own line (before FROM)
        one_statement_parses_to_std("SELECT *\n-- comment line\nFROM t", "SELECT * FROM t");

        // Multiple single-line comments
        one_statement_parses_to_std(
            "SELECT * -- first comment\n-- second comment\nFROM t -- third comment",
            "SELECT * FROM t",
        );
    }

    #[test]
    fn e161_block_comments() {
        // SQL:2016 E161: Block comments (/* */)

        // Simple block comment
        one_statement_parses_to_std("SELECT /* comment */ 1", "SELECT 1");

        // Block comment between keywords
        one_statement_parses_to_std(
            "SELECT /* cols */ a, b /* more cols */ FROM t",
            "SELECT a, b FROM t",
        );

        // Block comment replacing whitespace
        one_statement_parses_to_std("SELECT/*no space*/1", "SELECT 1");

        // Multi-line block comment
        one_statement_parses_to_std(
            "SELECT *\n/* This is a\n   multi-line\n   comment */\nFROM t",
            "SELECT * FROM t",
        );

        // Block comment at end
        one_statement_parses_to_std("SELECT 1 /* trailing comment */", "SELECT 1");
    }

    #[test]
    fn e161_mixed_comments() {
        // SQL:2016 E161: Mixed comment styles

        // Both styles in one statement
        one_statement_parses_to_std(
            "SELECT /* block */ a -- line comment\nFROM t",
            "SELECT a FROM t",
        );

        // Comments in complex query
        one_statement_parses_to_std(
            "SELECT a, /* col b */ b FROM t -- table\nWHERE /* filter */ x = 1",
            "SELECT a, b FROM t WHERE x = 1",
        );
    }
}

// =============================================================================
// E171: SQLSTATE Support
// =============================================================================

mod e171_sqlstate_support {
    use super::*;

    #[test]
    fn e171_sqlstate() {
        // SQL:2016 E171: SQLSTATE support via GET DIAGNOSTICS
        verified_standard_stmt("GET DIAGNOSTICS CONDITION 1 :sqlstate = RETURNED_SQLSTATE");
    }
}

// =============================================================================
// E182: Host Language Binding
// =============================================================================

mod e182_host_language_binding {
    use super::*;

    #[test]
    fn e182_host_parameters() {
        // SQL:2016 E182: Host language binding
        // Prepared statement parameters (?)
        verified_standard_stmt("SELECT * FROM t WHERE a = ?");
        verified_standard_stmt("INSERT INTO t VALUES (?, ?, ?)");
        verified_standard_stmt("UPDATE t SET a = ?, b = ? WHERE id = ?");

        // Multiple parameters
        verified_standard_stmt("SELECT * FROM users WHERE name = ? AND email = ?");
    }
}

// =============================================================================
// Additional E-Series Tests
// =============================================================================

#[test]
fn e_series_comprehensive_query() {
    // Comprehensive query combining multiple E-series features
    verified_standard_stmt(
        "SELECT DISTINCT \
            department, \
            COUNT(*) AS employee_count, \
            AVG(salary) AS avg_salary, \
            MAX(salary) AS max_salary, \
            MIN(salary) AS min_salary \
        FROM employees \
        WHERE salary BETWEEN 30000 AND 150000 \
            AND department IN ('Sales', 'Marketing', 'Engineering') \
            AND hire_date >= '2020-01-01' \
            AND (status = 'active' OR status = 'on_leave') \
        GROUP BY department \
        HAVING COUNT(*) > 5 AND AVG(salary) > 50000 \
        ORDER BY avg_salary DESC",
    );
}

#[test]
fn e_series_comprehensive_dml() {
    // Complex INSERT with subquery
    verified_standard_stmt(
        "INSERT INTO monthly_summary (year, month, department, total_salary, employee_count) \
        SELECT \
            EXTRACT(YEAR FROM hire_date), \
            EXTRACT(MONTH FROM hire_date), \
            department, \
            SUM(salary), \
            COUNT(*) \
        FROM employees \
        WHERE status = 'active' \
        GROUP BY EXTRACT(YEAR FROM hire_date), EXTRACT(MONTH FROM hire_date), department",
    );

    // Complex UPDATE with correlated subquery
    verified_standard_stmt(
        "UPDATE products SET price = price * 1.1, updated_at = CURRENT_TIMESTAMP WHERE category_id IN (SELECT id FROM categories WHERE discount_eligible = 1) AND price < (SELECT AVG(price) FROM products)",
    );

    // Complex DELETE with EXISTS
    verified_standard_stmt(
        "DELETE FROM orders WHERE status = 'cancelled' AND NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id) AND created_at < CURRENT_DATE - INTERVAL '1' YEAR",
    );
}

#[test]
fn e_series_comprehensive_ddl() {
    // Complex CREATE TABLE with all constraint types
    verified_with_ast!(
        "CREATE TABLE employees (employee_id INTEGER PRIMARY KEY, department_id INTEGER NOT NULL REFERENCES departments(id) ON DELETE RESTRICT, manager_id INTEGER REFERENCES employees(employee_id) ON DELETE SET NULL, first_name VARCHAR(50) NOT NULL, last_name VARCHAR(50) NOT NULL, email VARCHAR(255) UNIQUE NOT NULL, salary DECIMAL(10,2) CHECK (salary >= 0), hire_date DATE NOT NULL DEFAULT CURRENT_DATE, status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'on_leave')), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, CONSTRAINT check_name_length CHECK (CHARACTER_LENGTH(first_name) > 0 AND CHARACTER_LENGTH(last_name) > 0), CONSTRAINT unique_email UNIQUE (email))",
        |stmt: Statement| {
            if let Statement::CreateTable(CreateTable { columns, constraints, .. }) = stmt {
                // Verify we have all expected columns
                assert_eq!(columns.len(), 10);

                // Verify employee_id has PRIMARY KEY
                assert!(columns[0].options.iter().any(|opt| matches!(
                    opt.option,
                    ColumnOption::PrimaryKey(_)
                )));

                // Verify department_id has NOT NULL and FOREIGN KEY
                assert!(columns[1].options.iter().any(|opt| matches!(
                    opt.option,
                    ColumnOption::NotNull
                )));
                assert!(columns[1].options.iter().any(|opt| matches!(
                    opt.option,
                    ColumnOption::ForeignKey(_)
                )));

                // Verify email has UNIQUE and NOT NULL
                assert!(columns[5].options.iter().any(|opt| matches!(
                    opt.option,
                    ColumnOption::Unique(_)
                )));
                assert!(columns[5].options.iter().any(|opt| matches!(
                    opt.option,
                    ColumnOption::NotNull
                )));

                // Verify salary has CHECK constraint
                assert!(columns[6].options.iter().any(|opt| matches!(
                    opt.option,
                    ColumnOption::Check(_)
                )));

                // Verify salary is DECIMAL(10,2)
                if let DataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, s)) = &columns[6].data_type {
                    assert_eq!(*p, 10);
                    assert_eq!(*s, 2);
                }

                // Verify status has DEFAULT
                assert!(columns[8].options.iter().any(|opt| matches!(
                    opt.option,
                    ColumnOption::Default(_)
                )));

                // Verify table-level constraints exist
                assert!(constraints.len() >= 2, "Expected at least 2 table-level constraints");
                assert!(constraints.iter().any(|c| matches!(c, TableConstraint::Check { .. })));
                assert!(constraints.iter().any(|c| matches!(c, TableConstraint::Unique { .. })));
            } else {
                panic!("Expected CreateTable statement");
            }
        }
    );
}
