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

//! SQL:2016 Window Functions (T611-T627) Tests
//!
//! OLAP operations and window function features.
//!
//! ## Feature Coverage
//!
//! - T611: Elementary OLAP operations (ROW_NUMBER, RANK, DENSE_RANK, PARTITION BY)
//! - T612: Advanced OLAP operations (window frames: ROWS, RANGE, GROUPS, EXCLUDE)
//! - T613: Sampling
//! - T614: NTILE function
//! - T615: LEAD and LAG functions
//! - T616: Null treatment for LEAD/LAG
//! - T617: FIRST_VALUE and LAST_VALUE functions
//! - T618: NTH_VALUE function
//! - T619: FROM FIRST / FROM LAST
//! - T620: WINDOW clause: GROUPS option
//! - T621: Enhanced numeric functions (PERCENT_RANK, CUME_DIST)
//! - T622-T624: Trigonometric and logarithmic functions
//! - T625: Frame EXCLUDE clause
//! - T626: ANY_VALUE aggregate
//! - T627: FILTER clause

use crate::standards::common::verified_standard_stmt;
use sqlparser::ast::{
    Expr, Statement, TableSampleKind, TableSampleMethod, Value, ValueWithSpan, WindowFrameBound,
    WindowFrameUnits,
};

// =============================================================================
// T611: Elementary OLAP Operations
// =============================================================================

mod t611_elementary_olap {
    use super::*;

    #[test]
    fn t611_01_row_number() {
        // SQL:2016 T611-01: ROW_NUMBER function
        verified_standard_stmt("SELECT ROW_NUMBER() OVER () FROM t");
        verified_standard_stmt("SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t");
        verified_standard_stmt(
            "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees",
        );
    }

    #[test]
    fn t611_02_rank() {
        // SQL:2016 T611-02: RANK function
        verified_standard_stmt("SELECT RANK() OVER (ORDER BY score) FROM t");
        verified_standard_stmt(
            "SELECT RANK() OVER (PARTITION BY category ORDER BY price DESC) FROM products",
        );
    }

    #[test]
    fn t611_03_dense_rank() {
        // SQL:2016 T611-03: DENSE_RANK function
        verified_standard_stmt("SELECT DENSE_RANK() OVER (ORDER BY score) FROM t");
        verified_standard_stmt(
            "SELECT DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) FROM employees",
        );
    }

    #[test]
    fn t611_04_partition_by() {
        // SQL:2016 T611-04: PARTITION BY clause
        verified_standard_stmt("SELECT ROW_NUMBER() OVER (PARTITION BY dept) FROM t");
        verified_standard_stmt(
            "SELECT RANK() OVER (PARTITION BY category, region ORDER BY sales) FROM t",
        );
        verified_standard_stmt(
            "SELECT SUM(amount) OVER (PARTITION BY customer_id, year) FROM orders",
        );
    }

    #[test]
    fn t611_05_order_by_in_window() {
        // SQL:2016 T611-05: ORDER BY in window specification
        verified_standard_stmt("SELECT ROW_NUMBER() OVER (ORDER BY name) FROM t");
        verified_standard_stmt("SELECT RANK() OVER (ORDER BY score DESC) FROM t");
        verified_standard_stmt("SELECT DENSE_RANK() OVER (ORDER BY a, b DESC, c) FROM t");
    }

    #[test]
    fn t611_06_combined_partition_order() {
        // SQL:2016 T611: Combined PARTITION BY and ORDER BY
        verified_standard_stmt(
            "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hire_date) FROM employees",
        );
        verified_standard_stmt(
            "SELECT RANK() OVER (PARTITION BY year, month ORDER BY sales DESC) FROM sales_data",
        );
    }
}

// =============================================================================
// T612: Advanced OLAP Operations (Window Frames)
// =============================================================================

mod t612_advanced_olap {
    use super::*;

    #[test]
    fn t612_01_rows_frame() {
        // SQL:2016 T612-01: ROWS frame
        verified_standard_stmt("SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING) FROM t");
        verified_standard_stmt("SELECT AVG(x) OVER (ORDER BY y ROWS CURRENT ROW) FROM t");
        verified_standard_stmt("SELECT COUNT(*) OVER (ORDER BY y ROWS UNBOUNDED FOLLOWING) FROM t");
    }

    #[test]
    fn t612_02_rows_between() {
        // SQL:2016 T612-02: ROWS BETWEEN frame
        verified_standard_stmt(
            "SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
        );
        verified_standard_stmt(
            "SELECT AVG(x) OVER (ORDER BY y ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) FROM t",
        );
        verified_standard_stmt(
            "SELECT COUNT(*) OVER (ORDER BY y ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t"
        );
    }

    #[test]
    fn t612_03_range_frame() {
        // SQL:2016 T612-03: RANGE frame
        verified_standard_stmt("SELECT SUM(x) OVER (ORDER BY y RANGE UNBOUNDED PRECEDING) FROM t");
        verified_standard_stmt("SELECT AVG(x) OVER (ORDER BY y RANGE CURRENT ROW) FROM t");
        verified_standard_stmt(
            "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
        );
    }

    #[test]
    fn t612_04_range_offset() {
        // SQL:2016 T612-04: RANGE with offset
        #[allow(unused_imports)]
        use crate::verified_with_ast;
        verified_with_ast!(
            "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM t",
            |stmt: Statement| {
                match stmt {
                    Statement::Query(query) => {
                        if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                            let projection = &select.projection[0];
                            if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                                projection
                            {
                                if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) =
                                    &func.over
                                {
                                    // Verify window frame exists
                                    assert!(spec.window_frame.is_some());
                                    let frame = spec.window_frame.as_ref().unwrap();

                                    // Verify RANGE frame type
                                    assert_eq!(frame.units, WindowFrameUnits::Range);

                                    // Verify start bound: 10 PRECEDING
                                    match &frame.start_bound {
                                        WindowFrameBound::Preceding(Some(expr)) => {
                                            if let Expr::Value(ValueWithSpan {
                                                value: Value::Number(n, _),
                                                ..
                                            }) = &**expr
                                            {
                                                assert_eq!(n, "10");
                                            } else {
                                                panic!("Expected numeric value for PRECEDING");
                                            }
                                        }
                                        _ => panic!("Expected PRECEDING bound with value"),
                                    }

                                    // Verify end bound: 10 FOLLOWING
                                    assert!(frame.end_bound.is_some());
                                    match frame.end_bound.as_ref().unwrap() {
                                        WindowFrameBound::Following(Some(expr)) => {
                                            if let Expr::Value(ValueWithSpan {
                                                value: Value::Number(n, _),
                                                ..
                                            }) = &**expr
                                            {
                                                assert_eq!(n, "10");
                                            } else {
                                                panic!("Expected numeric value for FOLLOWING");
                                            }
                                        }
                                        _ => panic!("Expected FOLLOWING bound with value"),
                                    }
                                } else {
                                    panic!("Expected WindowSpec");
                                }
                            } else {
                                panic!("Expected function in projection");
                            }
                        } else {
                            panic!("Expected Select");
                        }
                    }
                    _ => panic!("Expected Query statement"),
                }
            }
        );
    }

    #[test]
    fn t612_05_groups_frame() {
        // SQL:2016 T612-05: GROUPS frame
        verified_standard_stmt("SELECT SUM(x) OVER (ORDER BY y GROUPS UNBOUNDED PRECEDING) FROM t");
        verified_standard_stmt("SELECT AVG(x) OVER (ORDER BY y GROUPS CURRENT ROW) FROM t");
        verified_standard_stmt(
            "SELECT COUNT(*) OVER (ORDER BY y GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t",
        );
    }

    #[test]
    fn t612_06_groups_between() {
        // SQL:2016 T612-06: GROUPS BETWEEN frame
        verified_standard_stmt(
            "SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
        );
        verified_standard_stmt(
            "SELECT AVG(x) OVER (ORDER BY y GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t"
        );
    }

    #[test]
    fn t612_07_frame_exclude_current_row() {
        // SQL:2016 T612-07: EXCLUDE CURRENT ROW
        verified_standard_stmt(
            "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM t",
        );
        verified_standard_stmt(
            "SELECT AVG(x) OVER (ORDER BY y ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING EXCLUDE CURRENT ROW) FROM t"
        );
    }

    #[test]
    fn t612_08_frame_exclude_group() {
        // SQL:2016 T612-08: EXCLUDE GROUP
        verified_standard_stmt(
            "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE GROUP) FROM t",
        );
    }

    #[test]
    fn t612_09_frame_exclude_ties() {
        // SQL:2016 T612-09: EXCLUDE TIES
        verified_standard_stmt(
            "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE TIES) FROM t",
        );
    }

    #[test]
    fn t612_10_frame_exclude_no_others() {
        // SQL:2016 T612-10: EXCLUDE NO OTHERS
        verified_standard_stmt(
            "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE NO OTHERS) FROM t",
        );
    }

    #[test]
    fn t612_11_complex_frame() {
        // SQL:2016 T612: Complex window frame combinations
        verified_standard_stmt(
            "SELECT SUM(x) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) FROM employees"
        );
        verified_standard_stmt(
            "SELECT AVG(price) OVER (PARTITION BY category ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM products"
        );
    }
}

// =============================================================================
// T613: Sampling
// =============================================================================

mod t613_sampling {
    use super::*;

    #[test]
    fn t613_01_tablesample_system() {
        // SQL:2016 T613-01: TABLESAMPLE SYSTEM
        #[allow(unused_imports)]
        use crate::verified_with_ast;
        verified_with_ast!(
            "SELECT * FROM t TABLESAMPLE SYSTEM (10)",
            |stmt: Statement| {
                match stmt {
                    Statement::Query(query) => {
                        if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                            // Verify we have a FROM clause with table sample
                            let table_with_joins = &select.from[0];
                            if let sqlparser::ast::TableFactor::Table { sample, .. } =
                                &table_with_joins.relation
                            {
                                assert!(sample.is_some());
                                let table_sample = sample.as_ref().unwrap();

                                // Verify we're using TABLESAMPLE SYSTEM
                                if let TableSampleKind::AfterTableAlias(ts) = table_sample {
                                    assert_eq!(ts.name, Some(TableSampleMethod::System));
                                    assert!(ts.quantity.is_some());

                                    // Verify the quantity is 10 and parenthesized
                                    let quantity = ts.quantity.as_ref().unwrap();
                                    assert!(quantity.parenthesized);
                                    if let Expr::Value(ValueWithSpan {
                                        value: Value::Number(n, _),
                                        ..
                                    }) = &quantity.value
                                    {
                                        assert_eq!(n, "10");
                                    } else {
                                        panic!("Expected numeric value for sample quantity");
                                    }
                                } else {
                                    panic!("Expected AfterTableAlias position");
                                }
                            } else {
                                panic!("Expected Table factor");
                            }
                        } else {
                            panic!("Expected Select");
                        }
                    }
                    _ => panic!("Expected Query statement"),
                }
            }
        );
    }

    #[test]
    fn t613_02_tablesample_bernoulli() {
        // SQL:2016 T613-02: TABLESAMPLE BERNOULLI
        #[allow(unused_imports)]
        use crate::verified_with_ast;
        verified_with_ast!(
            "SELECT * FROM t TABLESAMPLE BERNOULLI (5)",
            |stmt: Statement| {
                match stmt {
                    Statement::Query(query) => {
                        if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                            // Verify we have a FROM clause with table sample
                            let table_with_joins = &select.from[0];
                            if let sqlparser::ast::TableFactor::Table { sample, .. } =
                                &table_with_joins.relation
                            {
                                assert!(sample.is_some());
                                let table_sample = sample.as_ref().unwrap();

                                // Verify we're using TABLESAMPLE BERNOULLI
                                if let TableSampleKind::AfterTableAlias(ts) = table_sample {
                                    assert_eq!(ts.name, Some(TableSampleMethod::Bernoulli));
                                    assert!(ts.quantity.is_some());

                                    // Verify the quantity is 5 and parenthesized
                                    let quantity = ts.quantity.as_ref().unwrap();
                                    assert!(quantity.parenthesized);
                                    if let Expr::Value(ValueWithSpan {
                                        value: Value::Number(n, _),
                                        ..
                                    }) = &quantity.value
                                    {
                                        assert_eq!(n, "5");
                                    } else {
                                        panic!("Expected numeric value for sample quantity");
                                    }
                                } else {
                                    panic!("Expected AfterTableAlias position");
                                }
                            } else {
                                panic!("Expected Table factor");
                            }
                        } else {
                            panic!("Expected Select");
                        }
                    }
                    _ => panic!("Expected Query statement"),
                }
            }
        );
    }
}

// =============================================================================
// T614: NTILE Function
// =============================================================================

mod t614_ntile {
    use super::*;

    #[test]
    fn t614_01_ntile_basic() {
        // SQL:2016 T614-01: NTILE function
        verified_standard_stmt("SELECT NTILE(4) OVER (ORDER BY score) FROM t");
        verified_standard_stmt(
            "SELECT NTILE(10) OVER (PARTITION BY category ORDER BY price) FROM products",
        );
    }

    #[test]
    fn t614_02_ntile_with_partition() {
        // SQL:2016 T614-02: NTILE with PARTITION BY
        verified_standard_stmt(
            "SELECT NTILE(5) OVER (PARTITION BY dept ORDER BY salary) FROM employees",
        );
    }

    #[test]
    fn t614_03_ntile_multiple() {
        // SQL:2016 T614: Multiple NTILE functions
        verified_standard_stmt(
            "SELECT NTILE(4) OVER (ORDER BY score) AS quartile, NTILE(10) OVER (ORDER BY score) AS decile FROM t"
        );
    }
}

// =============================================================================
// T615: LEAD and LAG Functions
// =============================================================================

mod t615_lead_lag {
    use super::*;

    #[test]
    fn t615_01_lead_basic() {
        // SQL:2016 T615-01: LEAD function
        verified_standard_stmt("SELECT LEAD(value) OVER (ORDER BY date) FROM t");
        verified_standard_stmt("SELECT LEAD(price, 1) OVER (ORDER BY date) FROM stocks");
    }

    #[test]
    fn t615_02_lead_with_offset() {
        // SQL:2016 T615-02: LEAD with offset
        verified_standard_stmt("SELECT LEAD(value, 2) OVER (ORDER BY date) FROM t");
        verified_standard_stmt(
            "SELECT LEAD(price, 5) OVER (PARTITION BY symbol ORDER BY date) FROM stocks",
        );
    }

    #[test]
    fn t615_03_lead_with_default() {
        // SQL:2016 T615-03: LEAD with default value
        verified_standard_stmt("SELECT LEAD(value, 1, 0) OVER (ORDER BY date) FROM t");
        verified_standard_stmt("SELECT LEAD(price, 1, NULL) OVER (ORDER BY date) FROM stocks");
    }

    #[test]
    fn t615_04_lag_basic() {
        // SQL:2016 T615-04: LAG function
        verified_standard_stmt("SELECT LAG(value) OVER (ORDER BY date) FROM t");
        verified_standard_stmt("SELECT LAG(price, 1) OVER (ORDER BY date) FROM stocks");
    }

    #[test]
    fn t615_05_lag_with_offset() {
        // SQL:2016 T615-05: LAG with offset
        verified_standard_stmt("SELECT LAG(value, 3) OVER (ORDER BY date) FROM t");
        verified_standard_stmt(
            "SELECT LAG(price, 7) OVER (PARTITION BY symbol ORDER BY date) FROM stocks",
        );
    }

    #[test]
    fn t615_06_lag_with_default() {
        // SQL:2016 T615-06: LAG with default value
        verified_standard_stmt("SELECT LAG(value, 1, 0) OVER (ORDER BY date) FROM t");
        verified_standard_stmt("SELECT LAG(price, 1, -1) OVER (ORDER BY date) FROM stocks");
    }

    #[test]
    fn t615_07_lead_lag_combined() {
        // SQL:2016 T615: LEAD and LAG in same query
        verified_standard_stmt(
            "SELECT value, LAG(value) OVER (ORDER BY date) AS prev, LEAD(value) OVER (ORDER BY date) AS next FROM t"
        );
    }
}

// =============================================================================
// T616: Null Treatment for LEAD/LAG
// =============================================================================

mod t616_null_treatment {
    use super::*;

    #[test]
    fn t616_01_respect_nulls() {
        // SQL:2016 T616-01: RESPECT NULLS (default behavior)
        verified_standard_stmt("SELECT LEAD(value) RESPECT NULLS OVER (ORDER BY date) FROM t");
        verified_standard_stmt("SELECT LAG(price) RESPECT NULLS OVER (ORDER BY date) FROM stocks");
    }

    #[test]
    fn t616_02_ignore_nulls() {
        // SQL:2016 T616-02: IGNORE NULLS
        verified_standard_stmt("SELECT LEAD(value) IGNORE NULLS OVER (ORDER BY date) FROM t");
        verified_standard_stmt("SELECT LAG(price) IGNORE NULLS OVER (ORDER BY date) FROM stocks");
    }

    #[test]
    fn t616_03_ignore_nulls_with_offset() {
        // SQL:2016 T616-03: IGNORE NULLS with offset
        verified_standard_stmt("SELECT LEAD(value, 2) IGNORE NULLS OVER (ORDER BY date) FROM t");
        verified_standard_stmt("SELECT LAG(price, 3) IGNORE NULLS OVER (PARTITION BY symbol ORDER BY date) FROM stocks");
    }

    #[test]
    fn t616_04_ignore_nulls_with_default() {
        // SQL:2016 T616-04: IGNORE NULLS with default value
        verified_standard_stmt("SELECT LEAD(value, 1, 0) IGNORE NULLS OVER (ORDER BY date) FROM t");
        verified_standard_stmt(
            "SELECT LAG(price, 1, -1) IGNORE NULLS OVER (ORDER BY date) FROM stocks",
        );
    }
}

// =============================================================================
// T617: FIRST_VALUE and LAST_VALUE
// =============================================================================

mod t617_first_last_value {
    use super::*;

    #[test]
    fn t617_01_first_value() {
        // SQL:2016 T617-01: FIRST_VALUE function
        verified_standard_stmt("SELECT FIRST_VALUE(value) OVER (ORDER BY date) FROM t");
        verified_standard_stmt(
            "SELECT FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY date) FROM stocks",
        );
    }

    #[test]
    fn t617_02_first_value_with_frame() {
        // SQL:2016 T617-02: FIRST_VALUE with window frame
        verified_standard_stmt(
            "SELECT FIRST_VALUE(value) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
        );
        verified_standard_stmt(
            "SELECT FIRST_VALUE(price) OVER (PARTITION BY category ORDER BY date ROWS 5 PRECEDING) FROM products"
        );
    }

    #[test]
    fn t617_03_last_value() {
        // SQL:2016 T617-03: LAST_VALUE function
        verified_standard_stmt("SELECT LAST_VALUE(value) OVER (ORDER BY date) FROM t");
        verified_standard_stmt(
            "SELECT LAST_VALUE(price) OVER (PARTITION BY symbol ORDER BY date) FROM stocks",
        );
    }

    #[test]
    fn t617_04_last_value_with_frame() {
        // SQL:2016 T617-04: LAST_VALUE with window frame
        verified_standard_stmt(
            "SELECT LAST_VALUE(value) OVER (ORDER BY date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t"
        );
        verified_standard_stmt(
            "SELECT LAST_VALUE(price) OVER (PARTITION BY category ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) FROM products"
        );
    }

    #[test]
    fn t617_05_first_last_combined() {
        // SQL:2016 T617: FIRST_VALUE and LAST_VALUE in same query
        verified_standard_stmt(
            "SELECT FIRST_VALUE(value) OVER (ORDER BY date) AS first, LAST_VALUE(value) OVER (ORDER BY date) AS last FROM t"
        );
    }

    #[test]
    fn t617_06_null_treatment() {
        // SQL:2016 T617: Null treatment with FIRST_VALUE/LAST_VALUE
        verified_standard_stmt(
            "SELECT FIRST_VALUE(value) IGNORE NULLS OVER (ORDER BY date) FROM t",
        );
        verified_standard_stmt("SELECT LAST_VALUE(value) IGNORE NULLS OVER (ORDER BY date) FROM t");
        verified_standard_stmt(
            "SELECT FIRST_VALUE(value) RESPECT NULLS OVER (ORDER BY date) FROM t",
        );
    }
}

// =============================================================================
// T618: NTH_VALUE Function
// =============================================================================

mod t618_nth_value {
    use super::*;

    #[test]
    fn t618_01_nth_value_basic() {
        // SQL:2016 T618-01: NTH_VALUE function
        verified_standard_stmt("SELECT NTH_VALUE(value, 1) OVER (ORDER BY date) FROM t");
        verified_standard_stmt(
            "SELECT NTH_VALUE(price, 3) OVER (PARTITION BY category ORDER BY date) FROM products",
        );
    }

    #[test]
    fn t618_02_nth_value_with_frame() {
        // SQL:2016 T618-02: NTH_VALUE with window frame
        verified_standard_stmt(
            "SELECT NTH_VALUE(value, 2) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
        );
        verified_standard_stmt(
            "SELECT NTH_VALUE(price, 5) OVER (PARTITION BY symbol ORDER BY date ROWS 10 PRECEDING) FROM stocks"
        );
    }

    #[test]
    fn t618_03_nth_value_ignore_nulls() {
        // SQL:2016 T618-03: NTH_VALUE with IGNORE NULLS
        verified_standard_stmt(
            "SELECT NTH_VALUE(value, 2) IGNORE NULLS OVER (ORDER BY date) FROM t",
        );
        verified_standard_stmt(
            "SELECT NTH_VALUE(price, 3) RESPECT NULLS OVER (ORDER BY date) FROM stocks",
        );
    }

    #[test]
    fn t618_04_nth_value_various_positions() {
        // SQL:2016 T618: NTH_VALUE with various positions
        verified_standard_stmt(
            "SELECT NTH_VALUE(value, 1) OVER (ORDER BY date) AS first, NTH_VALUE(value, 2) OVER (ORDER BY date) AS second, NTH_VALUE(value, 10) OVER (ORDER BY date) AS tenth FROM t"
        );
    }
}

// =============================================================================
// T619: FROM FIRST / FROM LAST
// =============================================================================

mod t619_from_first_last {
    use super::*;

    #[test]
    fn t619_01_nth_value_from_first() {
        // SQL:2016 T619-01: NTH_VALUE FROM FIRST - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT NTH_VALUE(value, 2) FROM FIRST OVER (ORDER BY date) FROM t");
    }

    #[test]
    fn t619_02_nth_value_from_last() {
        // SQL:2016 T619-02: NTH_VALUE FROM LAST - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT NTH_VALUE(value, 2) FROM LAST OVER (ORDER BY date) FROM t");
    }
}

// =============================================================================
// T620: WINDOW Clause
// =============================================================================

mod t620_window_clause {
    use super::*;

    #[test]
    fn t620_01_named_window_basic() {
        // SQL:2016 T620-01: WINDOW clause with named windows
        verified_standard_stmt("SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY a)");
    }

    #[test]
    fn t620_02_multiple_named_windows() {
        // SQL:2016 T620-02: Multiple named windows
        verified_standard_stmt(
            "SELECT ROW_NUMBER() OVER w1, SUM(x) OVER w2 FROM t WINDOW w1 AS (ORDER BY a), w2 AS (PARTITION BY b)"
        );
    }

    #[test]
    fn t620_03_named_window_with_partition() {
        // SQL:2016 T620-03: Named window with PARTITION BY
        verified_standard_stmt(
            "SELECT SUM(amount) OVER w FROM orders WINDOW w AS (PARTITION BY customer_id ORDER BY order_date)"
        );
    }

    #[test]
    fn t620_04_window_reference() {
        // SQL:2016 T620-04: Window referencing another window
        verified_standard_stmt(
            "SELECT ROW_NUMBER() OVER w2 FROM t WINDOW w1 AS (PARTITION BY a), w2 AS (w1 ORDER BY b)"
        );
    }

    #[test]
    fn t620_05_window_with_groups() {
        // SQL:2016 T620-05: WINDOW clause with GROUPS frame
        verified_standard_stmt(
            "SELECT SUM(x) OVER w FROM t WINDOW w AS (ORDER BY y GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING)"
        );
    }

    #[test]
    fn t620_06_complex_named_windows() {
        // SQL:2016 T620: Complex named window combinations
        verified_standard_stmt(
            "SELECT ROW_NUMBER() OVER w, RANK() OVER (w ORDER BY score DESC) FROM t WINDOW w AS (PARTITION BY category)"
        );
    }
}

// =============================================================================
// T621: Enhanced Numeric Functions
// =============================================================================

mod t621_enhanced_numeric {
    use super::*;

    #[test]
    fn t621_01_percent_rank() {
        // SQL:2016 T621-01: PERCENT_RANK function
        verified_standard_stmt("SELECT PERCENT_RANK() OVER (ORDER BY score) FROM t");
        verified_standard_stmt(
            "SELECT PERCENT_RANK() OVER (PARTITION BY category ORDER BY price) FROM products",
        );
    }

    #[test]
    fn t621_02_cume_dist() {
        // SQL:2016 T621-02: CUME_DIST function
        verified_standard_stmt("SELECT CUME_DIST() OVER (ORDER BY score) FROM t");
        verified_standard_stmt(
            "SELECT CUME_DIST() OVER (PARTITION BY dept ORDER BY salary) FROM employees",
        );
    }

    #[test]
    fn t621_03_combined_ranking() {
        // SQL:2016 T621: Combined ranking functions
        verified_standard_stmt(
            "SELECT RANK() OVER w, DENSE_RANK() OVER w, PERCENT_RANK() OVER w, CUME_DIST() OVER w FROM t WINDOW w AS (ORDER BY score)"
        );
    }
}

// =============================================================================
// T622-T624: Trigonometric and Logarithmic Functions
// =============================================================================

mod t622_t624_math_functions {
    use super::*;

    #[test]
    fn t622_01_trigonometric() {
        // SQL:2016 T622: Trigonometric functions
        verified_standard_stmt("SELECT SIN(x), COS(x), TAN(x) FROM t");
        verified_standard_stmt("SELECT ASIN(x), ACOS(x), ATAN(x) FROM t");
        verified_standard_stmt("SELECT ATAN2(y, x) FROM t");
    }

    #[test]
    fn t623_01_logarithmic() {
        // SQL:2016 T623: Logarithmic functions
        verified_standard_stmt("SELECT LN(x), LOG10(x) FROM t");
        verified_standard_stmt("SELECT LOG(2, x) FROM t");
        verified_standard_stmt("SELECT EXP(x) FROM t");
    }

    #[test]
    fn t624_01_power_sqrt() {
        // SQL:2016 T624: POWER and SQRT functions
        verified_standard_stmt("SELECT POWER(x, 2) FROM t");
        verified_standard_stmt("SELECT SQRT(x) FROM t");
        verified_standard_stmt("SELECT POWER(2, 10), SQRT(144)");
    }
}

// =============================================================================
// T626: ANY_VALUE Aggregate
// =============================================================================

mod t626_any_value {
    use super::*;

    #[test]
    fn t626_01_any_value_basic() {
        // SQL:2016 T626-01: ANY_VALUE aggregate
        verified_standard_stmt("SELECT ANY_VALUE(x) FROM t");
        verified_standard_stmt("SELECT category, ANY_VALUE(name) FROM products GROUP BY category");
    }

    #[test]
    fn t626_02_any_value_window() {
        // SQL:2016 T626-02: ANY_VALUE as window function
        verified_standard_stmt("SELECT ANY_VALUE(x) OVER (PARTITION BY category) FROM t");
    }
}

// =============================================================================
// T627: FILTER Clause
// =============================================================================

mod t627_filter_clause {
    use super::*;

    #[test]
    fn t627_01_filter_with_count() {
        // SQL:2016 T627-01: FILTER clause with COUNT
        verified_standard_stmt("SELECT COUNT(*) FILTER (WHERE x > 0) FROM t");
        verified_standard_stmt("SELECT COUNT(value) FILTER (WHERE status = 'active') FROM t");
    }

    #[test]
    fn t627_02_filter_with_sum() {
        // SQL:2016 T627-02: FILTER clause with SUM
        verified_standard_stmt("SELECT SUM(amount) FILTER (WHERE type = 'sale') FROM transactions");
        verified_standard_stmt("SELECT SUM(price) FILTER (WHERE price > 100) FROM products");
    }

    #[test]
    fn t627_03_filter_with_avg() {
        // SQL:2016 T627-03: FILTER clause with AVG
        verified_standard_stmt("SELECT AVG(score) FILTER (WHERE score IS NOT NULL) FROM tests");
        verified_standard_stmt("SELECT AVG(salary) FILTER (WHERE dept = 'sales') FROM employees");
    }

    #[test]
    fn t627_04_filter_with_max_min() {
        // SQL:2016 T627-04: FILTER clause with MAX and MIN
        verified_standard_stmt("SELECT MAX(value) FILTER (WHERE value > 0) FROM t");
        verified_standard_stmt("SELECT MIN(price) FILTER (WHERE in_stock = true) FROM products");
    }

    #[test]
    fn t627_05_filter_multiple() {
        // SQL:2016 T627-05: Multiple aggregates with different FILTER clauses
        verified_standard_stmt(
            "SELECT SUM(amount) FILTER (WHERE type = 'sale') AS sales, SUM(amount) FILTER (WHERE type = 'refund') AS refunds FROM transactions"
        );
    }

    #[test]
    fn t627_06_filter_with_group_by() {
        // SQL:2016 T627-06: FILTER with GROUP BY
        verified_standard_stmt(
            "SELECT category, COUNT(*) FILTER (WHERE price > 100) FROM products GROUP BY category",
        );
    }

    #[test]
    fn t627_07_filter_with_window() {
        // SQL:2016 T627-07: FILTER clause with window function
        verified_standard_stmt(
            "SELECT SUM(amount) FILTER (WHERE amount > 0) OVER (PARTITION BY customer_id) FROM orders"
        );
    }

    #[test]
    fn t627_08_filter_complex_predicate() {
        // SQL:2016 T627-08: FILTER with complex predicate
        verified_standard_stmt("SELECT COUNT(*) FILTER (WHERE x > 10 AND y < 20 OR z = 0) FROM t");
        verified_standard_stmt(
            "SELECT SUM(amount) FILTER (WHERE date BETWEEN '2023-01-01' AND '2023-12-31' AND status IN ('completed', 'paid')) FROM transactions"
        );
    }
}

// =============================================================================
// Comprehensive Integration Tests
// =============================================================================

mod integration_tests {
    use super::*;

    #[test]
    fn comprehensive_window_query() {
        // Comprehensive query using multiple window function features
        verified_standard_stmt(
            "SELECT dept, name, salary, \
             ROW_NUMBER() OVER w AS rn, \
             RANK() OVER w AS rank, \
             DENSE_RANK() OVER w AS dense_rank, \
             PERCENT_RANK() OVER w AS pct_rank, \
             CUME_DIST() OVER w AS cum_dist, \
             NTILE(4) OVER w AS quartile, \
             LEAD(salary) OVER w AS next_salary, \
             LAG(salary) OVER w AS prev_salary, \
             FIRST_VALUE(salary) OVER w AS dept_min, \
             LAST_VALUE(salary) OVER w AS dept_max, \
             SUM(salary) OVER w AS running_total, \
             AVG(salary) OVER w AS running_avg \
             FROM employees \
             WINDOW w AS (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );
    }

    #[test]
    fn window_with_filter_and_groups() {
        // Window functions with FILTER and GROUPS
        verified_standard_stmt(
            "SELECT category, \
             SUM(amount) FILTER (WHERE amount > 0) OVER (PARTITION BY category ORDER BY date GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS sum_positive, \
             COUNT(*) FILTER (WHERE status = 'active') OVER w AS active_count \
             FROM orders \
             WINDOW w AS (PARTITION BY category ORDER BY date)"
        );
    }

    #[test]
    fn multiple_frames_exclude_clauses() {
        // Multiple window specifications with different EXCLUDE clauses
        verified_standard_stmt(
            "SELECT x, \
             SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) AS sum1, \
             SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE GROUP) AS sum2, \
             SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE TIES) AS sum3, \
             SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING EXCLUDE NO OTHERS) AS sum4 \
             FROM t",
        );
    }

    #[test]
    fn null_treatment_variations() {
        // Various null treatment options
        verified_standard_stmt(
            "SELECT date, value, \
             LEAD(value) IGNORE NULLS OVER (ORDER BY date) AS next_non_null, \
             LAG(value) RESPECT NULLS OVER (ORDER BY date) AS prev_with_null, \
             FIRST_VALUE(value) IGNORE NULLS OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS first_non_null, \
             NTH_VALUE(value, 5) IGNORE NULLS OVER (ORDER BY date) AS fifth_non_null \
             FROM time_series"
        );
    }
}
