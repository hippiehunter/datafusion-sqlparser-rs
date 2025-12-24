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

//! SQL:2023 Numeric Literal Enhancements Tests
//!
//! Enhanced numeric literal syntax introduced in SQL:2023.
//!
//! ## Feature Coverage
//!
//! - T661: Non-decimal integer literals
//!   - Hexadecimal: 0xFFFF, 0XABCD
//!   - Octal: 0o755, 0O644
//!   - Binary: 0b1010, 0B1111
//!
//! - T662: Underscores in numeric literals
//!   - Integer: 1_000_000
//!   - Decimal: 3.14_15_92
//!   - Hexadecimal: 0xFF_FF_FF
//!
//! ## Implementation Status
//!
//! Currently, the parser handles these features as follows:
//! - Hexadecimal (0xFF): Parsed as HexStringLiteral (X'FF'), not numeric literal
//! - Binary (0b1010): Parsed as 0 with identifier alias 'b1010'
//! - Octal (0o755): Parsed as 0 with identifier alias 'o755'
//! - Underscores (1_000): Parsed as 1 with identifier alias '_000'
//!
//! To fully support SQL:2023 T661/T662, these should be parsed as numeric literals.

use crate::standards::common::{try_parse};
use sqlparser::ast::{Expr, Ident, SelectItem, SetExpr, Statement, Value};

/// Helper to extract the first projection item from a SELECT statement
fn get_first_projection(stmt: &Statement) -> Option<&SelectItem> {
    match stmt {
        Statement::Query(q) => {
            if let SetExpr::Select(select) = q.body.as_ref() {
                return select.projection.first();
            }
            None
        }
        _ => None,
    }
}

/// Helper to extract the first value from a SELECT statement
fn get_first_value(stmt: &Statement) -> Option<&Value> {
    match get_first_projection(stmt)? {
        SelectItem::UnnamedExpr(Expr::Value(val)) => Some(&val.value),
        _ => None,
    }
}

/// Helper to check if a SQL parses and assert on the first value
fn assert_parses_as_value<F>(sql: &str, check: F)
where
    F: FnOnce(&Value),
{
    let stmts = try_parse(sql).expect("Should parse successfully");
    assert_eq!(stmts.len(), 1, "Expected exactly one statement");
    let value = get_first_value(&stmts[0]).expect("Should have a value in SELECT");
    check(value);
}

/// Helper to check if parsing results in a value with an alias (incorrect parsing)
fn assert_parses_with_alias(sql: &str, expected_value: &str, expected_alias: &str) {
    let stmts = try_parse(sql).expect("Should parse successfully");
    assert_eq!(stmts.len(), 1, "Expected exactly one statement");

    match get_first_projection(&stmts[0]) {
        Some(SelectItem::ExprWithAlias { expr: Expr::Value(val), alias }) => {
            match &val.value {
                Value::Number(n, _) => {
                    #[cfg(not(feature = "bigdecimal"))]
                    assert_eq!(n, expected_value);
                    #[cfg(feature = "bigdecimal")]
                    assert_eq!(n.to_string(), expected_value);
                }
                _ => panic!("Expected Number value, got {:?}", val.value),
            }
            assert_eq!(alias.value, expected_alias, "Alias mismatch");
        }
        other => panic!("Expected ExprWithAlias with Number and alias, got {:?}", other),
    }
}

// ==================== T661: Non-Decimal Integer Literals ====================

#[test]
fn t661_01_hexadecimal_lowercase_prefix() {
    // SQL:2023 T661: Hexadecimal literals with lowercase 0x prefix
    // Note: Currently parsed as X'ff' hex string literal, not as numeric literal
    assert_parses_as_value("SELECT 0xff", |value| {
        match value {
            Value::HexStringLiteral(s) => assert_eq!(s, "ff"),
            _ => panic!("Expected HexStringLiteral, got {:?}", value),
        }
    });
}

#[test]
fn t661_02_hexadecimal_uppercase_prefix() {
    // SQL:2023 T661: Hexadecimal literals with uppercase 0X prefix
    // Note: Currently parsed as X'FF' hex string literal, not as numeric literal
    assert_parses_as_value("SELECT 0XFF", |value| {
        match value {
            Value::HexStringLiteral(s) => assert_eq!(s, "FF"),
            _ => panic!("Expected HexStringLiteral, got {:?}", value),
        }
    });
}

#[test]
fn t661_03_hexadecimal_mixed_case() {
    // SQL:2023 T661: Hexadecimal literals with mixed case digits
    // Note: Currently parsed as X'AbCdEf' hex string literal, not as numeric literal
    assert_parses_as_value("SELECT 0xAbCdEf", |value| {
        match value {
            Value::HexStringLiteral(s) => assert_eq!(s, "AbCdEf"),
            _ => panic!("Expected HexStringLiteral, got {:?}", value),
        }
    });
}

#[test]
fn t661_04_hexadecimal_large_value() {
    // SQL:2023 T661: Large hexadecimal values
    // Note: Currently parsed as X'FFFFFFFFFFFFFFFF' hex string literal, not as numeric literal
    assert_parses_as_value("SELECT 0xFFFFFFFFFFFFFFFF", |value| {
        match value {
            Value::HexStringLiteral(s) => assert_eq!(s, "FFFFFFFFFFFFFFFF"),
            _ => panic!("Expected HexStringLiteral, got {:?}", value),
        }
    });
}

#[test]
fn t661_05_hexadecimal_in_expressions() {
    // SQL:2023 T661: Hexadecimal literals in expressions
    // Note: Currently parsed as hex string literals, not numeric literals
    // Just verify it parses without error
    try_parse("SELECT 0xFF + 0x10").expect("Should parse");
}

#[test]
fn t661_06_hexadecimal_in_where_clause() {
    // SQL:2023 T661: Hexadecimal literals in WHERE clause
    // Note: Currently parsed as hex string literals, not numeric literals
    // Just verify it parses without error
    try_parse("SELECT * FROM t WHERE flags = 0x0001").expect("Should parse");
}

#[test]
fn t661_07_binary_lowercase_prefix() {
    // SQL:2023 T661: Binary literals with lowercase 0b prefix
    // Currently parsed as 0 with alias 'b1010', not as a binary numeric literal
    assert_parses_with_alias("SELECT 0b1010", "0", "b1010");
}

#[test]
fn t661_08_binary_uppercase_prefix() {
    // SQL:2023 T661: Binary literals with uppercase 0B prefix
    // Currently parsed as 0 with alias 'B1111', not as a binary numeric literal
    assert_parses_with_alias("SELECT 0B1111", "0", "B1111");
}

#[test]
fn t661_09_binary_long_value() {
    // SQL:2023 T661: Long binary literals
    // Currently parsed as 0 with alias 'b11111111111111111111111111111111'
    assert_parses_with_alias("SELECT 0b11111111111111111111111111111111", "0", "b11111111111111111111111111111111");
}

#[test]
fn t661_10_binary_in_expressions() {
    // SQL:2023 T661: Binary literals in expressions
    // Currently fails to parse due to bitwise OR operator
    verified_standard_stmt("SELECT 0b1000 | 0b0100");
}

#[test]
fn t661_11_binary_in_comparisons() {
    // SQL:2023 T661: Binary literals in comparisons
    // Currently fails to parse due to bitwise AND operator
    verified_standard_stmt("SELECT * FROM t WHERE status & 0b0001 = 0b0001");
}

#[test]
fn t661_12_octal_lowercase_prefix() {
    // SQL:2023 T661: Octal literals with lowercase 0o prefix
    // Currently parsed as 0 with alias 'o755', not as an octal numeric literal
    assert_parses_with_alias("SELECT 0o755", "0", "o755");
}

#[test]
fn t661_13_octal_uppercase_prefix() {
    // SQL:2023 T661: Octal literals with uppercase 0O prefix
    // Currently parsed as 0 with alias 'O644', not as an octal numeric literal
    assert_parses_with_alias("SELECT 0O644", "0", "O644");
}

#[test]
fn t661_14_octal_valid_digits() {
    // SQL:2023 T661: Octal literals with valid digits (0-7)
    // Currently parsed as 0 with alias 'o777', not as an octal numeric literal
    assert_parses_with_alias("SELECT 0o777", "0", "o777");
}

#[test]
fn t661_15_octal_in_expressions() {
    // SQL:2023 T661: Octal literals in expressions
    // Currently fails to parse
    verified_standard_stmt("SELECT 0o100 + 0o010");
}

#[test]
fn t661_16_mixed_radix_expressions() {
    // SQL:2023 T661: Mixed radix literals in single expression
    // Currently fails to parse
    verified_standard_stmt("SELECT 0xFF + 0b1111 + 0o77 + 100");
}

// ==================== T662: Underscores in Numeric Literals ====================

#[test]
fn t662_01_integer_with_underscores() {
    // SQL:2023 T662: Integer literals with underscores as separators
    // Currently parsed as 1 with alias '_000_000', not as 1000000
    assert_parses_with_alias("SELECT 1_000_000", "1", "_000_000");
}

#[test]
fn t662_02_integer_underscores_various_positions() {
    // SQL:2023 T662: Underscores in various positions
    // Currently parsed as 123 with alias '_456_789', not as 123456789
    assert_parses_with_alias("SELECT 123_456_789", "123", "_456_789");
}

#[test]
fn t662_03_decimal_with_underscores() {
    // SQL:2023 T662: Decimal literals with underscores
    // Currently parsed as 3.14 with alias '_15_92', not as 3.141592
    assert_parses_with_alias("SELECT 3.14_15_92", "3.14", "_15_92");
}

#[test]
fn t662_04_decimal_underscores_both_sides() {
    // SQL:2023 T662: Underscores in both integer and fractional parts
    // Currently fails to parse correctly
    verified_standard_stmt("SELECT 1_234.567_89");
}

#[test]
fn t662_05_scientific_notation_with_underscores() {
    // SQL:2023 T662: Scientific notation with underscores
    // Currently fails to parse correctly
    verified_standard_stmt("SELECT 1_234.56e7_8");
}

#[test]
fn t662_06_hexadecimal_with_underscores() {
    // SQL:2023 T662: Hexadecimal literals with underscores
    // Currently parsed as hex string literal with underscores (invalid hex)
    // This should ideally be a numeric literal with value 0xFFFFFF
    verified_standard_stmt("SELECT 0xFF_FF_FF");
}

#[test]
fn t662_07_hexadecimal_underscores_grouping() {
    // SQL:2023 T662: Hexadecimal with byte-wise grouping
    // Currently parsed as hex string literal with underscores (invalid hex)
    verified_standard_stmt("SELECT 0xDE_AD_BE_EF");
}

#[test]
fn t662_08_binary_with_underscores() {
    // SQL:2023 T662: Binary literals with underscores
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT 0b1111_0000_1010_0101");
}

#[test]
fn t662_09_binary_underscores_nibble_grouping() {
    // SQL:2023 T662: Binary with nibble (4-bit) grouping
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT 0b1010_1111");
}

#[test]
fn t662_10_octal_with_underscores() {
    // SQL:2023 T662: Octal literals with underscores
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT 0o7_7_7");
}

#[test]
fn t662_11_underscores_in_large_numbers() {
    // SQL:2023 T662: Underscores for readability in large numbers
    // Currently parsed as 1 with alias '_000_000_000_000', not as 1000000000000
    assert_parses_with_alias("SELECT 1_000_000_000_000", "1", "_000_000_000_000");
}

#[test]
fn t662_12_underscores_in_expressions() {
    // SQL:2023 T662: Numeric literals with underscores in expressions
    // Currently fails to parse correctly
    verified_standard_stmt("SELECT 1_000 + 2_000 * 3.14_15");
}

#[test]
fn t662_13_underscores_in_where_clause() {
    // SQL:2023 T662: Literals with underscores in WHERE clause
    // Currently fails to parse correctly
    verified_standard_stmt("SELECT * FROM t WHERE amount > 100_000");
}

#[test]
fn t662_14_underscores_in_insert() {
    // SQL:2023 T662: Literals with underscores in INSERT statement
    // Currently fails to parse correctly
    verified_standard_stmt("INSERT INTO accounts (balance) VALUES (1_000_000.50)");
}

#[test]
fn t662_15_underscores_mixed_with_hex() {
    // SQL:2023 T662: Combined hexadecimal and underscores
    // Currently fails to parse correctly
    verified_standard_stmt("SELECT 0xFFFF_FFFF + 0xFF");
}

// ==================== Combined T661 + T662: Non-Decimal with Underscores ====================

#[test]
fn t661_t662_01_hex_with_underscores() {
    // SQL:2023 T661+T662: Hexadecimal with underscores
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT 0xAB_CD_EF_01");
}

#[test]
fn t661_t662_02_binary_with_underscores() {
    // SQL:2023 T661+T662: Binary with underscores
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT 0b1111_1111_0000_0000");
}

#[test]
fn t661_t662_03_octal_with_underscores() {
    // SQL:2023 T661+T662: Octal with underscores
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT 0o12_34_56");
}

#[test]
fn t661_t662_04_all_radixes_in_query() {
    // SQL:2023 T661+T662: All numeric literal formats in one query
    // NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT 1_000, 0xFF_FF, 0b1111_0000, 0o7_7_7, 3.14_15");
}

#[test]
fn t661_t662_05_complex_arithmetic() {
    // SQL:2023 T661+T662: Complex arithmetic with various formats
    // Currently fails to parse correctly
    verified_standard_stmt("SELECT (0xFF_00 & 0b1111_1111_0000_0000) | 0o7_7");
}

#[test]
fn t661_t662_06_in_create_table_default() {
    // SQL:2023 T661+T662: Non-decimal literals with underscores in defaults
    // Currently fails to parse correctly
    verified_standard_stmt("CREATE TABLE t (flags INT DEFAULT 0xFF_FF, count BIGINT DEFAULT 1_000_000)");
}
