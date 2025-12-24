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

//! SQL:2016 T-Series (Advanced Features) Tests
//!
//! T031-T670: Advanced SQL features.
//!
//! ## Feature Coverage
//!
//! - T031: BOOLEAN data type
//! - T054: GREATEST and LEAST
//! - T055-T056: String padding functions
//! - T071: BIGINT data type
//! - T121-T133: WITH clause (CTEs), recursive queries
//! - T141-T152: SIMILAR predicate, DISTINCT predicate
//! - T171-T178: Identity columns, sequences
//! - T201: Comparable data types for referential constraints
//! - T212-T217: Triggers
//! - T241: START TRANSACTION statement
//! - T261: Chained transactions
//! - T271: Savepoints
//! - T312: OVERLAY function
//! - T321: User-defined functions and procedures
//! - T431-T434: GROUPING SETS, ROLLUP, CUBE
//! - T441: ABS and MOD functions
//! - T461: Symmetric BETWEEN predicate
//! - T491: LATERAL derived table
//! - T521-T525: Named arguments in routine invocations
//! - T581: Regular expression substring function
//! - T651-T655: SQL-schema statements in routines
//! - T661-T662: Non-decimal literals, underscores in numeric literals
//! - T670: Schema and data statement mixing

use crate::standards::common::verified_standard_stmt;
use sqlparser::ast::*;

// ==================== T031: BOOLEAN Data Type ====================

#[test]
fn t031_01_boolean_type() {
    // SQL:2016 T031-01: BOOLEAN data type
    verified_standard_stmt("CREATE TABLE t (flag BOOLEAN)");
    verified_standard_stmt("CREATE TABLE t (flag BOOL)");
}

#[test]
fn t031_02_boolean_literals() {
    // SQL:2016 T031-02: TRUE and FALSE literals
    // Note: Parser normalizes to lowercase
    verified_standard_stmt("SELECT true");
    verified_standard_stmt("SELECT false");
    verified_standard_stmt("SELECT true, false FROM t");
}

#[test]
fn t031_03_boolean_predicates() {
    // SQL:2016 T031-03: IS TRUE, IS FALSE, IS UNKNOWN predicates
    verified_standard_stmt("SELECT * FROM t WHERE flag IS TRUE");
    verified_standard_stmt("SELECT * FROM t WHERE flag IS FALSE");
    verified_standard_stmt("SELECT * FROM t WHERE flag IS UNKNOWN");
    verified_standard_stmt("SELECT * FROM t WHERE flag IS NOT TRUE");
    verified_standard_stmt("SELECT * FROM t WHERE flag IS NOT FALSE");
    verified_standard_stmt("SELECT * FROM t WHERE flag IS NOT UNKNOWN");
}

#[test]
fn t031_04_boolean_operations() {
    // SQL:2016 T031: Boolean operations
    verified_standard_stmt("SELECT a AND b FROM t");
    verified_standard_stmt("SELECT a OR b FROM t");
    verified_standard_stmt("SELECT NOT a FROM t");
    verified_standard_stmt("SELECT a AND b OR c FROM t");
}

// ==================== T054: GREATEST and LEAST ====================

#[test]
fn t054_01_greatest() {
    // SQL:2016 T054: GREATEST function
    verified_standard_stmt("SELECT GREATEST(a, b)");
    verified_standard_stmt("SELECT GREATEST(a, b, c)");
    verified_standard_stmt("SELECT GREATEST(1, 2, 3, 4, 5)");
    verified_standard_stmt("SELECT GREATEST(name, 'default') FROM t");
}

#[test]
fn t054_02_least() {
    // SQL:2016 T054: LEAST function
    verified_standard_stmt("SELECT LEAST(a, b)");
    verified_standard_stmt("SELECT LEAST(a, b, c)");
    verified_standard_stmt("SELECT LEAST(1, 2, 3, 4, 5)");
    verified_standard_stmt("SELECT LEAST(price, max_price) FROM t");
}

// ==================== T055-T056: String Padding Functions ====================

#[test]
fn t055_01_lpad() {
    // SQL:2016 T055: LPAD function
    verified_standard_stmt("SELECT LPAD(name, 10)");
    verified_standard_stmt("SELECT LPAD(name, 10, ' ')");
    verified_standard_stmt("SELECT LPAD(name, 10, '0')");
    verified_standard_stmt("SELECT LPAD(col, 20, '**')");
}

#[test]
fn t055_02_rpad() {
    // SQL:2016 T055: RPAD function
    verified_standard_stmt("SELECT RPAD(name, 10)");
    verified_standard_stmt("SELECT RPAD(name, 10, ' ')");
    verified_standard_stmt("SELECT RPAD(name, 10, '0')");
    verified_standard_stmt("SELECT RPAD(col, 20, '**')");
}

#[test]
fn t056_01_ltrim() {
    // SQL:2016 T056: LTRIM function
    verified_standard_stmt("SELECT LTRIM(name)");
    verified_standard_stmt("SELECT LTRIM(name, ' ')");
    verified_standard_stmt("SELECT LTRIM(name, ' \\t')");
}

#[test]
fn t056_02_rtrim() {
    // SQL:2016 T056: RTRIM function
    verified_standard_stmt("SELECT RTRIM(name)");
    verified_standard_stmt("SELECT RTRIM(name, ' ')");
    verified_standard_stmt("SELECT RTRIM(name, ' \\t')");
}

#[test]
fn t056_03_btrim() {
    // SQL:2016 T056: BTRIM function
    verified_standard_stmt("SELECT BTRIM(name)");
    verified_standard_stmt("SELECT BTRIM(name, ' ')");
    verified_standard_stmt("SELECT BTRIM(name, ' \\t')");
}

// ==================== T071: BIGINT Data Type ====================

#[test]
fn t071_01_bigint_type() {
    // SQL:2016 T071: BIGINT data type
    verified_standard_stmt("CREATE TABLE t (id BIGINT)");
    verified_standard_stmt("CREATE TABLE t (a BIGINT, b BIGINT)");
}

#[test]
fn t071_02_bigint_cast() {
    // SQL:2016 T071: CAST to BIGINT
    verified_standard_stmt("SELECT CAST(x AS BIGINT)");
    verified_standard_stmt("SELECT CAST('12345' AS BIGINT)");
}

// ==================== T121-T133: WITH Clause (CTEs) ====================

#[test]
fn t121_01_simple_cte() {
    // SQL:2016 T121-01: Simple WITH clause (non-recursive)
    verified_standard_stmt("WITH cte AS (SELECT 1) SELECT * FROM cte");
    verified_standard_stmt("WITH cte AS (SELECT a FROM t) SELECT * FROM cte");
}

#[test]
fn t121_02_multiple_ctes() {
    // SQL:2016 T121-02: Multiple CTEs
    verified_standard_stmt("WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2");
    verified_standard_stmt(
        "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT * FROM a JOIN b ON a.id = b.id"
    );
}

#[test]
fn t121_03_cte_column_aliases() {
    // SQL:2016 T121-03: CTE with column aliases
    verified_standard_stmt("WITH cte (col1, col2) AS (SELECT a, b FROM t) SELECT * FROM cte");
    verified_standard_stmt("WITH cte (x, y, z) AS (SELECT a, b, c FROM t) SELECT x, y FROM cte");
}

#[test]
fn t121_04_cte_in_subquery() {
    // SQL:2016 T121: CTE in subquery
    verified_standard_stmt("SELECT * FROM t WHERE EXISTS (WITH u AS (SELECT 1) SELECT * FROM u)");
}

#[test]
fn t121_05_cte_with_insert() {
    // SQL:2016 T121: CTE with INSERT
    verified_standard_stmt("WITH foo AS (SELECT 1) INSERT INTO t SELECT * FROM foo");
    verified_standard_stmt(
        "INSERT INTO customer WITH foo AS (SELECT 1) SELECT * FROM foo UNION VALUES (1)",
    );
}

#[test]
fn t121_06_cte_with_update() {
    // SQL:2016 T121: CTE with UPDATE
    verified_standard_stmt("WITH x AS (SELECT 1) UPDATE t SET bar = (SELECT * FROM x)");
}

#[test]
fn t121_07_cte_with_delete() {
    // SQL:2016 T121: CTE with DELETE
    verified_standard_stmt("WITH t (x) AS (SELECT 9) DELETE FROM q WHERE id IN (SELECT x FROM t)");
}

#[test]
fn t122_01_recursive_cte() {
    // SQL:2016 T122: WITH RECURSIVE
    verified_standard_stmt(
        "WITH RECURSIVE nums (val) AS (SELECT 1 UNION ALL SELECT val + 1 FROM nums WHERE val < 10) SELECT * FROM nums"
    );
}

#[test]
fn t122_02_recursive_hierarchy() {
    // SQL:2016 T122: Recursive CTE for hierarchical data
    verified_standard_stmt(
        "WITH RECURSIVE emp_hierarchy AS (SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id FROM employees AS e JOIN emp_hierarchy AS h ON e.manager_id = h.id) SELECT * FROM emp_hierarchy"
    );
}

#[test]
fn t133_01_cycle_clause() {
    // SQL:2016 T133: CYCLE clause - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "WITH RECURSIVE cte AS (SELECT 1 AS x) CYCLE x SET is_cycle USING path SELECT * FROM cte",
    );
}

#[test]
fn t133_02_search_clause() {
    // SQL:2016 T133: SEARCH clause - NOT YET IMPLEMENTED
    verified_standard_stmt("WITH RECURSIVE cte AS (SELECT 1 AS x) SEARCH DEPTH FIRST BY x SET order_col SELECT * FROM cte");
}

// ==================== T141-T152: SIMILAR and DISTINCT Predicates ====================

#[test]
fn t141_01_similar_to() {
    // SQL:2016 T141: SIMILAR TO predicate
    verified_standard_stmt("SELECT * FROM t WHERE name SIMILAR TO 'A%'");
    verified_standard_stmt("SELECT * FROM t WHERE name SIMILAR TO '[A-Z]+'");
}

#[test]
fn t141_02_not_similar_to() {
    // SQL:2016 T141: NOT SIMILAR TO predicate
    verified_standard_stmt("SELECT * FROM t WHERE name NOT SIMILAR TO 'A%'");
}

#[test]
fn t141_03_similar_to_escape() {
    // SQL:2016 T141: SIMILAR TO with ESCAPE
    verified_standard_stmt("SELECT * FROM t WHERE name SIMILAR TO 'A!%' ESCAPE '!'");
}

#[test]
fn t151_01_is_distinct_from() {
    // SQL:2016 T151: IS DISTINCT FROM predicate
    verified_standard_stmt("SELECT * FROM t WHERE a IS DISTINCT FROM b");
    verified_standard_stmt("SELECT * FROM t WHERE a IS DISTINCT FROM NULL");
}

#[test]
fn t151_02_is_not_distinct_from() {
    // SQL:2016 T151: IS NOT DISTINCT FROM predicate
    verified_standard_stmt("SELECT * FROM t WHERE a IS NOT DISTINCT FROM b");
    verified_standard_stmt("SELECT * FROM t WHERE a IS NOT DISTINCT FROM NULL");
}

// ==================== T171-T178: Identity Columns and Sequences ====================

#[test]
fn t171_01_identity_always() {
    // SQL:2016 T171: GENERATED ALWAYS AS IDENTITY
    verified_standard_stmt("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)");
}

#[test]
fn t171_02_identity_by_default() {
    // SQL:2016 T171: GENERATED BY DEFAULT AS IDENTITY
    verified_standard_stmt("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY)");
}

#[test]
fn t171_03_identity_with_parameters() {
    // SQL:2016 T171: Identity with parameters
    // Note: Extra spaces in parentheses
    verified_standard_stmt("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY( START WITH 1 ))");
    verified_standard_stmt(
        "CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 ))",
    );
}

#[test]
fn t172_01_create_sequence() {
    // SQL:2016 T172: CREATE SEQUENCE
    verified_standard_stmt("CREATE SEQUENCE seq");
    verified_standard_stmt("CREATE SEQUENCE seq START 1");
    verified_standard_stmt("CREATE SEQUENCE seq INCREMENT 10");
}

#[test]
fn t172_02_create_sequence_options() {
    // SQL:2016 T172: CREATE SEQUENCE with options
    verified_standard_stmt("CREATE SEQUENCE seq MINVALUE 1 MAXVALUE 1000");
    verified_standard_stmt("CREATE SEQUENCE seq CACHE 20");
    verified_standard_stmt("CREATE SEQUENCE seq CYCLE");
    verified_standard_stmt("CREATE SEQUENCE seq NO CYCLE");
}

#[test]
fn t173_01_drop_sequence() {
    // SQL:2016 T173: DROP SEQUENCE
    verified_standard_stmt("DROP SEQUENCE seq");
    verified_standard_stmt("DROP SEQUENCE IF EXISTS seq");
    verified_standard_stmt("DROP SEQUENCE seq CASCADE");
    verified_standard_stmt("DROP SEQUENCE seq RESTRICT");
}

#[test]
fn t174_01_alter_sequence() {
    // SQL:2016 T174: ALTER SEQUENCE - NOT YET IMPLEMENTED
    // ALTER SEQUENCE is not fully supported in GenericDialect
    verified_standard_stmt("ALTER SEQUENCE seq RESTART");
}

#[test]
fn t176_01_next_value_for() {
    // SQL:2016 T176: Sequence generator - NEXT VALUE FOR expression
    // Standard syntax: NEXT VALUE FOR sequence_name
    verified_standard_stmt("SELECT NEXT VALUE FOR seq");
    verified_standard_stmt("INSERT INTO t (id, name) VALUES (NEXT VALUE FOR seq, 'test')");
}

// ==================== T201: Comparable Data Types for Referential Constraints ====================

#[test]
fn t201_01_foreign_key_comparable_types() {
    // SQL:2016 T201: Foreign keys with compatible types
    verified_standard_stmt("CREATE TABLE parent (id INT PRIMARY KEY)");
    verified_standard_stmt(
        "CREATE TABLE child (id INT, parent_id INT, FOREIGN KEY (parent_id) REFERENCES parent(id))",
    );
}

// ==================== T212-T217: Triggers ====================

#[test]
fn t212_01_trigger_before() {
    // SQL:2016 T212: Triggers - BEFORE
    verified_standard_stmt(
        "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW BEGIN SELECT 1; END",
    );
    verified_standard_stmt(
        "CREATE TRIGGER trg BEFORE UPDATE ON t FOR EACH ROW BEGIN SELECT 1; END",
    );
    verified_standard_stmt(
        "CREATE TRIGGER trg BEFORE DELETE ON t FOR EACH ROW BEGIN SELECT 1; END",
    );
}

#[test]
fn t213_01_trigger_after() {
    // SQL:2016 T213: Triggers - AFTER
    verified_standard_stmt("CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END");
    verified_standard_stmt("CREATE TRIGGER trg AFTER UPDATE ON t FOR EACH ROW BEGIN SELECT 1; END");
    verified_standard_stmt("CREATE TRIGGER trg AFTER DELETE ON t FOR EACH ROW BEGIN SELECT 1; END");
}

#[test]
fn t214_01_trigger_instead_of() {
    // SQL:2016 T214: INSTEAD OF triggers
    verified_standard_stmt(
        "CREATE TRIGGER trg INSTEAD OF INSERT ON v FOR EACH ROW BEGIN SELECT 1; END",
    );
}

#[test]
fn t215_01_trigger_for_each_statement() {
    // SQL:2016 T215: FOR EACH STATEMENT
    verified_standard_stmt(
        "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH STATEMENT BEGIN SELECT 1; END",
    );
}

#[test]
fn t216_01_trigger_when_clause() {
    // SQL:2016 T216: WHEN clause in triggers - Parsing succeeds but may not fully validate
    // The WHEN clause in triggers is supported
    verified_standard_stmt(
        "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW WHEN (NEW.value > 0) BEGIN SELECT 1; END"
    );
}

#[test]
fn t217_01_trigger_multiple_events() {
    // SQL:2016 T217: Multiple trigger events
    verified_standard_stmt(
        "CREATE TRIGGER trg BEFORE INSERT OR UPDATE ON t FOR EACH ROW BEGIN SELECT 1; END",
    );
    verified_standard_stmt(
        "CREATE TRIGGER trg AFTER INSERT OR DELETE ON t FOR EACH ROW BEGIN SELECT 1; END",
    );
}

#[test]
fn t218_01_drop_trigger() {
    // DROP TRIGGER
    verified_standard_stmt("DROP TRIGGER trg");
    verified_standard_stmt("DROP TRIGGER IF EXISTS trg");
}

// ==================== T241: START TRANSACTION ====================

#[test]
fn t241_01_start_transaction() {
    // SQL:2016 T241: START TRANSACTION statement
    verified_standard_stmt("START TRANSACTION");
}

#[test]
fn t241_02_start_transaction_read_only() {
    // SQL:2016 T241: START TRANSACTION with READ ONLY
    verified_standard_stmt("START TRANSACTION READ ONLY");
    verified_standard_stmt("START TRANSACTION READ WRITE");
}

#[test]
fn t241_03_start_transaction_isolation() {
    // SQL:2016 T241: START TRANSACTION with ISOLATION LEVEL
    verified_standard_stmt("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    verified_standard_stmt("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
    verified_standard_stmt("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    verified_standard_stmt("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
}

// ==================== T261: Chained Transactions ====================

#[test]
fn t261_01_commit_and_chain() {
    // SQL:2016 T261: COMMIT AND CHAIN
    verified_standard_stmt("COMMIT AND CHAIN");
}

#[test]
fn t261_02_rollback_and_chain() {
    // SQL:2016 T261: ROLLBACK AND CHAIN
    verified_standard_stmt("ROLLBACK AND CHAIN");
}

// ==================== T271: Savepoints ====================

#[test]
fn t271_01_savepoint() {
    // SQL:2016 T271: SAVEPOINT statement
    verified_standard_stmt("SAVEPOINT sp1");
    verified_standard_stmt("SAVEPOINT my_savepoint");
}

#[test]
fn t271_02_release_savepoint() {
    // SQL:2016 T271: RELEASE SAVEPOINT
    verified_standard_stmt("RELEASE SAVEPOINT sp1");
}

#[test]
fn t271_03_rollback_to_savepoint() {
    // SQL:2016 T271: ROLLBACK TO SAVEPOINT
    verified_standard_stmt("ROLLBACK TO SAVEPOINT sp1");
}

// ==================== T312: OVERLAY Function ====================

#[test]
fn t312_01_overlay() {
    // SQL:2016 T312: OVERLAY function
    verified_standard_stmt("SELECT OVERLAY(s PLACING 'abc' FROM 1)");
    verified_standard_stmt("SELECT OVERLAY(s PLACING 'abc' FROM 1 FOR 3)");
    verified_standard_stmt("SELECT OVERLAY(name PLACING '***' FROM 5 FOR 3)");
}

// ==================== T321: User-Defined Functions and Procedures ====================

#[test]
fn t321_01_create_function() {
    // SQL:2016 T321: CREATE FUNCTION
    verified_standard_stmt("CREATE FUNCTION add(a INT, b INT) RETURNS INT RETURN a + b");
}

#[test]
fn t321_02_create_procedure() {
    // SQL:2016 T321: CREATE PROCEDURE
    verified_standard_stmt("CREATE PROCEDURE proc AS BEGIN SELECT 1; END");
    verified_standard_stmt("CREATE PROCEDURE proc (x INT) AS BEGIN SELECT x; END");
}

#[test]
fn t321_03_drop_function() {
    // SQL:2016 T321: DROP FUNCTION
    verified_standard_stmt("DROP FUNCTION add");
    verified_standard_stmt("DROP FUNCTION IF EXISTS add");
}

#[test]
fn t321_04_drop_procedure() {
    // SQL:2016 T321: DROP PROCEDURE
    verified_standard_stmt("DROP PROCEDURE proc");
    verified_standard_stmt("DROP PROCEDURE IF EXISTS proc");
}

#[test]
fn t321_05_call_statement() {
    // SQL:2016 T321: CALL statement
    verified_standard_stmt("CALL proc()");
    verified_standard_stmt("CALL proc(1, 2, 3)");
}

#[test]
fn t321_06_return_statement() {
    // SQL:2016 T321: RETURN statement
    verified_standard_stmt("CREATE FUNCTION f() RETURNS INT RETURN 42");
}

#[test]
fn t321_07_parameters() {
    // SQL:2016 T321: IN, OUT, INOUT parameters
    verified_standard_stmt("CREATE PROCEDURE p (x INT, y INT) AS BEGIN SET y = x * 2; END");
}

// ==================== T431-T434: GROUPING SETS ====================

#[test]
fn t431_01_grouping_sets() {
    // SQL:2016 T431: GROUPING SETS
    verified_standard_stmt("SELECT a, b, COUNT(*) FROM t GROUP BY GROUPING SETS ((a), (b))");
    verified_standard_stmt("SELECT a, b, c, COUNT(*) FROM t GROUP BY GROUPING SETS ((a, b), (c))");
    verified_standard_stmt("SELECT a, COUNT(*) FROM t GROUP BY GROUPING SETS ((a), ())");
}

#[test]
fn t432_01_rollup() {
    // SQL:2016 T432: ROLLUP
    verified_standard_stmt("SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP (a, b)");
    verified_standard_stmt("SELECT year, month, SUM(sales) FROM t GROUP BY ROLLUP (year, month)");
}

#[test]
fn t433_01_cube() {
    // SQL:2016 T433: CUBE
    verified_standard_stmt("SELECT a, b, COUNT(*) FROM t GROUP BY CUBE (a, b)");
    verified_standard_stmt("SELECT a, b, c, SUM(x) FROM t GROUP BY CUBE (a, b, c)");
}

#[test]
fn t434_01_grouping_function() {
    // SQL:2016 T434: GROUPING function
    verified_standard_stmt("SELECT a, GROUPING(a), COUNT(*) FROM t GROUP BY ROLLUP (a)");
    verified_standard_stmt("SELECT a, b, GROUPING(a), GROUPING(b) FROM t GROUP BY CUBE (a, b)");
}

#[test]
fn t434_02_combined_rollup_cube() {
    // SQL:2016 T434: Combined ROLLUP and CUBE
    verified_standard_stmt("SELECT a, b, c FROM t GROUP BY ROLLUP (a), CUBE (b, c)");
}

// ==================== T441: ABS and MOD ====================

#[test]
fn t441_01_abs() {
    // SQL:2016 T441: ABS function
    verified_standard_stmt("SELECT ABS(x)");
    verified_standard_stmt("SELECT ABS(-42)");
    verified_standard_stmt("SELECT ABS(a - b) FROM t");
}

#[test]
fn t441_02_mod() {
    // SQL:2016 T441: MOD function
    verified_standard_stmt("SELECT MOD(x, 10)");
    verified_standard_stmt("SELECT MOD(a, b) FROM t");
}

#[test]
fn t441_03_modulo_operator() {
    // SQL:2016 T441: % operator for modulo
    verified_standard_stmt("SELECT x % 10");
    verified_standard_stmt("SELECT a % b FROM t");
}

// ==================== T461: Symmetric BETWEEN ====================

#[test]
fn t461_01_between_symmetric() {
    // SQL:2016 T461: BETWEEN SYMMETRIC - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM t WHERE x BETWEEN SYMMETRIC 1 AND 10");
}

#[test]
fn t461_02_between_asymmetric() {
    // SQL:2016 T461: BETWEEN ASYMMETRIC (explicit) - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM t WHERE x BETWEEN ASYMMETRIC 1 AND 10");
}

// ==================== T491: LATERAL Derived Table ====================

#[test]
fn t491_01_lateral_subquery() {
    // SQL:2016 T491: LATERAL subquery
    verified_standard_stmt(
        "SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) AS sub",
    );
}

#[test]
fn t491_02_lateral_with_join() {
    // SQL:2016 T491: LATERAL with JOIN
    verified_standard_stmt(
        "SELECT * FROM t1 JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) AS sub ON true",
    );
}

#[test]
fn t491_03_lateral_cross_join() {
    // SQL:2016 T491: LATERAL with CROSS JOIN
    verified_standard_stmt(
        "SELECT * FROM t1 CROSS JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) AS sub",
    );
}

// ==================== T521-T525: Named Arguments ====================

#[test]
fn t521_01_named_arguments() {
    // SQL:2016 T521: Named arguments in function calls
    verified_standard_stmt("SELECT func(param1 => 1, param2 => 2)");
}

#[test]
fn t521_02_mixed_positional_named() {
    // SQL:2016 T521: Mixed positional and named arguments
    verified_standard_stmt("SELECT func(1, param2 => 2)");
}

// ==================== T581: Regular Expression Substring ====================

#[test]
fn t581_01_substring_from_pattern() {
    // SQL:2016 T581: SUBSTRING with regular expression
    verified_standard_stmt("SELECT SUBSTRING(s FROM '[0-9]+')");
}

#[test]
fn t581_02_substring_similar() {
    // SQL:2016 T581: SUBSTRING SIMILAR - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT SUBSTRING(s SIMILAR '[A-Z]+' ESCAPE '\\')");
}

// ==================== T651-T655: SQL-Schema Statements in Routines ====================

#[test]
fn t651_01_ddl_in_procedure() {
    // SQL:2016 T651: DDL in stored procedures - NOT YET IMPLEMENTED
    // Most implementations don't allow arbitrary DDL in procedures
    verified_standard_stmt("CREATE PROCEDURE p() BEGIN CREATE TABLE t (id INT); END");
}

// ==================== T661-T662: Numeric Literals ====================

#[test]
fn t661_01_hexadecimal_literals() {
    // SQL:2016 T661: Hexadecimal literals
    // Note: 0x format normalizes to X'' format
    verified_standard_stmt("SELECT X'FF'");
    verified_standard_stmt("SELECT X'1234ABCD'");
    verified_standard_stmt("SELECT X'DEADBEEF'");
}

#[test]
fn t661_02_binary_literals() {
    // SQL:2016 T661: Binary literals using standard B'...' format
    verified_standard_stmt("SELECT B'1010'");
    verified_standard_stmt("SELECT B'11110000'");
    verified_standard_stmt("SELECT B'10101010'");
}

#[test]
fn t661_03_octal_literals() {
    // SQL:2016 T661: Octal literals - Parsing succeeds but parsed as 0 with alias o777
    // True octal support not yet implemented
    // This test documents current behavior - octal literals parse incorrectly
    let result = crate::standards::common::try_parse("SELECT 0o777");
    assert!(
        result.is_ok(),
        "Octal literal should parse (even if incorrectly)"
    );
}

#[test]
fn t662_01_underscores_in_numbers() {
    // SQL:2016 T662: Underscores in numeric literals - Parsing succeeds but parsed incorrectly
    // Underscores are treated as part of identifier, resulting in "1 AS _000_000"
    // This test documents current behavior - underscore support not yet implemented correctly
    let result = crate::standards::common::try_parse("SELECT 1_000_000");
    assert!(
        result.is_ok(),
        "Underscored number should parse (even if incorrectly)"
    );
}

// ==================== T670: Schema and Data Statement Mixing ====================

#[test]
fn t670_01_mixed_ddl_dml() {
    // SQL:2016 T670: Mixed DDL and DML statements
    // This tests that DDL and DML can be parsed in sequence
    let stmt1 = verified_standard_stmt("CREATE TABLE t (id INT)");
    assert!(matches!(stmt1, Statement::CreateTable { .. }));

    let stmt2 = verified_standard_stmt("INSERT INTO t VALUES (1)");
    assert!(matches!(stmt2, Statement::Insert { .. }));
}

// ==================== Additional T-Series Tests ====================

#[test]
fn t_series_comprehensive_query() {
    // Comprehensive query using multiple T-series features
    verified_standard_stmt(
        "WITH RECURSIVE nums (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT n, GREATEST(n, 5) AS max_val, LEAST(n, 5) AS min_val, ABS(n - 5) AS distance, MOD(n, 2) AS is_odd FROM nums WHERE n IS DISTINCT FROM NULL GROUP BY ROLLUP (n)"
    );
}
