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

//! SQL:2019 Enhancements Tests
//!
//! SQL:2019 (ISO/IEC 9075:2019) is a minor revision to SQL:2016.
//! Most significant features were introduced in SQL:2016, so SQL:2019
//! primarily contains clarifications, refinements, and minor additions.
//!
//! ## Feature Coverage
//!
//! SQL:2019 is primarily a maintenance release with clarifications to SQL:2016.
//! Major features to test are covered in the SQL:2016 test suite.
//! This module contains tests for SQL:2019-specific refinements and any
//! minor additions not already covered in SQL:2016.
//!
//! Note: Many SQL:2019 features are clarifications or minor enhancements
//! to existing SQL:2016 features, so most tests reference SQL:2016 features
//! that were refined in SQL:2019.

use crate::standards::common::{one_statement_parses_to_std, verified_standard_stmt};

// ==================== Basic SQL:2019 Compatibility ====================

#[test]
fn sql2019_01_basic_select() {
    // SQL:2019: Basic SELECT statement (unchanged from SQL:2016)
    verified_standard_stmt("SELECT * FROM users");
    verified_standard_stmt("SELECT id, name FROM customers WHERE active = true");
}

#[test]
fn sql2019_02_joins() {
    // SQL:2019: JOIN operations (unchanged from SQL:2016)
    verified_standard_stmt("SELECT * FROM orders AS o JOIN customers AS c ON o.customer_id = c.id");
    verified_standard_stmt("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id");
    verified_standard_stmt("SELECT * FROM t1 FULL JOIN t2 ON t1.key = t2.key");
}

#[test]
fn sql2019_03_window_functions() {
    // SQL:2019: Window functions (refined from SQL:2016)
    verified_standard_stmt("SELECT id, SUM(amount) OVER (PARTITION BY category) FROM sales");
    verified_standard_stmt("SELECT name, ROW_NUMBER() OVER (ORDER BY created_at DESC) FROM items");
    verified_standard_stmt(
        "SELECT dept, AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date) FROM employees",
    );
}

#[test]
fn sql2019_04_common_table_expressions() {
    // SQL:2019: WITH clause / CTEs (unchanged from SQL:2016)
    verified_standard_stmt("WITH cte AS (SELECT * FROM t) SELECT * FROM cte");
    verified_standard_stmt(
        "WITH sales_summary AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) SELECT * FROM sales_summary WHERE total > 1000"
    );
}

#[test]
fn sql2019_05_recursive_cte() {
    // SQL:2019: Recursive CTEs (unchanged from SQL:2016)
    verified_standard_stmt(
        "WITH RECURSIVE hierarchy AS (SELECT id, parent_id, name FROM nodes WHERE parent_id IS NULL UNION ALL SELECT n.id, n.parent_id, n.name FROM nodes AS n JOIN hierarchy AS h ON n.parent_id = h.id) SELECT * FROM hierarchy"
    );
}

#[test]
fn sql2019_06_grouping_sets() {
    // SQL:2019: GROUPING SETS (unchanged from SQL:2016)
    verified_standard_stmt("SELECT region, category, SUM(sales) FROM data GROUP BY GROUPING SETS ((region), (category), ())");
    verified_standard_stmt(
        "SELECT a, b, COUNT(*) FROM t GROUP BY GROUPING SETS ((a, b), (a), (b), ())",
    );
}

#[test]
fn sql2019_07_rollup() {
    // SQL:2019: ROLLUP (unchanged from SQL:2016)
    verified_standard_stmt(
        "SELECT year, quarter, SUM(revenue) FROM sales GROUP BY ROLLUP (year, quarter)",
    );
    verified_standard_stmt(
        "SELECT country, city, COUNT(*) FROM locations GROUP BY ROLLUP (country, city)",
    );
}

#[test]
fn sql2019_08_cube() {
    // SQL:2019: CUBE (unchanged from SQL:2016)
    verified_standard_stmt(
        "SELECT product, region, SUM(sales) FROM data GROUP BY CUBE (product, region)",
    );
    verified_standard_stmt("SELECT a, b, c, COUNT(*) FROM t GROUP BY CUBE (a, b, c)");
}

#[test]
fn sql2019_09_case_expressions() {
    // SQL:2019: CASE expressions (unchanged from SQL:2016)
    verified_standard_stmt("SELECT CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END FROM persons");
    verified_standard_stmt(
        "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END FROM accounts",
    );
}

#[test]
fn sql2019_10_cast_function() {
    // SQL:2019: CAST function (unchanged from SQL:2016)
    verified_standard_stmt("SELECT CAST(price AS NUMERIC(10,2)) FROM products");
    verified_standard_stmt("SELECT CAST('2023-01-01' AS DATE)");
    verified_standard_stmt("SELECT CAST(id AS VARCHAR(50)) FROM users");
}

// ==================== SQL:2019 Refinements ====================

#[test]
fn sql2019_11_json_features() {
    // SQL:2019: JSON features (refined from SQL:2016, enhanced in SQL:2023)
    // These are SQL:2016 features that work in SQL:2019
    verified_standard_stmt("SELECT JSON_OBJECT('key', 'value')");
    verified_standard_stmt("SELECT JSON_ARRAY(1, 2, 3)");
}

#[test]
fn sql2019_12_polymorphic_table_functions() {
    // SQL:2019: Polymorphic table functions (refined from SQL:2016)
    // Basic table function call syntax
    verified_standard_stmt("SELECT * FROM TABLE(my_function(1, 2))");
}

#[test]
fn sql2019_13_lateral_joins() {
    // SQL:2019: LATERAL joins (unchanged from SQL:2016)
    one_statement_parses_to_std(
        "SELECT * FROM departments AS d, LATERAL (SELECT * FROM employees AS e WHERE e.dept_id = d.id) AS dept_employees",
        "SELECT * FROM departments AS d, LATERAL (SELECT * FROM employees AS e WHERE e.dept_id = d.id) AS dept_employees"
    );
    one_statement_parses_to_std(
        "SELECT * FROM orders AS o LEFT JOIN LATERAL (SELECT * FROM order_items WHERE order_id = o.id FETCH FIRST 1 ROW ONLY) AS first_item ON true",
        "SELECT * FROM orders AS o LEFT JOIN LATERAL (SELECT * FROM order_items WHERE order_id = o.id FETCH FIRST 1 ROWS ONLY) AS first_item ON true"
    );
}

#[test]
fn sql2019_14_temporal_features() {
    // SQL:2019: Temporal data types and operations (refined from SQL:2016)
    verified_standard_stmt("SELECT CURRENT_DATE");
    verified_standard_stmt("SELECT CURRENT_TIME");
    verified_standard_stmt("SELECT CURRENT_TIMESTAMP");
    verified_standard_stmt("SELECT LOCALTIME");
    verified_standard_stmt("SELECT LOCALTIMESTAMP");
}

#[test]
fn sql2019_15_multi_column_distinct() {
    // SQL:2019: DISTINCT with multiple columns (unchanged from SQL:2016)
    verified_standard_stmt("SELECT DISTINCT country, city FROM locations");
    verified_standard_stmt("SELECT DISTINCT a, b, c FROM t");
}

#[test]
fn sql2019_16_fetch_first() {
    // SQL:2019: FETCH FIRST clause (unchanged from SQL:2016)
    verified_standard_stmt("SELECT * FROM products ORDER BY price DESC FETCH FIRST 10 ROWS ONLY");
    verified_standard_stmt(
        "SELECT * FROM users ORDER BY created_at OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY",
    );
}

#[test]
fn sql2019_17_offset_clause() {
    // SQL:2019: OFFSET clause (unchanged from SQL:2016)
    verified_standard_stmt("SELECT * FROM items ORDER BY id OFFSET 100 ROWS");
    verified_standard_stmt(
        "SELECT * FROM logs ORDER BY timestamp DESC OFFSET 50 ROWS FETCH FIRST 25 ROWS ONLY",
    );
}

#[test]
fn sql2019_18_boolean_type() {
    // SQL:2019: BOOLEAN type operations (unchanged from SQL:2016)
    verified_standard_stmt("SELECT * FROM flags WHERE is_active IS TRUE");
    verified_standard_stmt("SELECT * FROM settings WHERE enabled IS FALSE");
    verified_standard_stmt("SELECT * FROM data WHERE flag IS UNKNOWN");
}

#[test]
fn sql2019_19_array_operations() {
    // SQL:2019: ARRAY operations (unchanged from SQL:2016)
    verified_standard_stmt("SELECT ARRAY[1, 2, 3, 4, 5]");
    verified_standard_stmt("SELECT tags[1] FROM articles");
}

#[test]
fn sql2019_20_merge_statement() {
    // SQL:2019: MERGE statement (unchanged from SQL:2016)
    verified_standard_stmt(
        "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET value = source.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)"
    );
}

// ==================== Compliance Notes ====================

#[test]
fn sql2019_compliance_note() {
    // SQL:2019 is primarily a maintenance release.
    // Most features were introduced in SQL:2016 and remain unchanged.
    // SQL:2023 introduces more significant new features (see sql2023 module).

    // This test serves as documentation that SQL:2019 compliance
    // is largely equivalent to SQL:2016 compliance for this parser.
    verified_standard_stmt("SELECT 1");
}
