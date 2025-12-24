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

//! SQL:2016 Polymorphic Table Function Tests (ISO/IEC 9075-2, Feature B200)
//!
//! Polymorphic Table Functions (PTFs) are table functions whose return type
//! can depend on the input. They allow dynamic schema determination at runtime.

use crate::standards::common::verified_standard_stmt;

// ==================== B200: Polymorphic Table Functions Basic ====================

#[test]
fn b200_01_table_function_basic() {
    // SQL:2016 B200: Basic table function invocation
    verified_standard_stmt("SELECT * FROM TABLE(my_table_function(1, 'test'))");
}

#[test]
fn b200_02_table_function_with_alias() {
    verified_standard_stmt(
        "SELECT t.col1, t.col2 FROM TABLE(generate_series(1, 10)) AS t(col1, col2)",
    );
}

#[test]
fn b200_03_table_function_in_join() {
    verified_standard_stmt(
        "SELECT * FROM orders o JOIN TABLE(get_order_items(o.id)) AS items ON true",
    );
}

#[test]
fn b200_04_table_function_lateral() {
    verified_standard_stmt(
        "SELECT * FROM customers c, LATERAL TABLE(get_customer_orders(c.id)) AS orders",
    );
}

// ==================== PTF with TABLE Parameters ====================

#[test]
fn ptf_table_parameter() {
    // PTF receiving a table as input
    verified_standard_stmt(
        "SELECT * FROM TABLE(pivot_table(TABLE source_data, 'category', 'value'))",
    );
}

#[test]
fn ptf_table_parameter_subquery() {
    verified_standard_stmt(
        "SELECT * FROM TABLE(summarize(TABLE (SELECT * FROM sales WHERE year = 2023)))",
    );
}

#[test]
fn ptf_table_with_pass_through() {
    // Pass-through columns from input table
    verified_standard_stmt(
        "SELECT * FROM TABLE(add_row_numbers(TABLE employees PASS THROUGH COLUMNS))",
    );
}

#[test]
fn ptf_table_partition_by() {
    verified_standard_stmt(
        "SELECT * FROM TABLE(window_aggregate(TABLE sales PARTITION BY region ORDER BY date))",
    );
}

// ==================== PTF Type Categories ====================

#[test]
fn ptf_type_row_semantics() {
    // Row semantics PTF - processes one row at a time
    verified_standard_stmt("SELECT * FROM TABLE(parse_json(TABLE json_documents ROW SEMANTICS))");
}

#[test]
fn ptf_type_set_semantics() {
    // Set semantics PTF - processes entire input set
    verified_standard_stmt(
        "SELECT * FROM TABLE(compute_statistics(TABLE measurements SET SEMANTICS))",
    );
}

// ==================== CREATE FUNCTION for PTF ====================

#[test]
fn create_ptf_basic() {
    verified_standard_stmt(
        "CREATE FUNCTION split_string(input_string VARCHAR, delimiter CHAR) \
         RETURNS TABLE (part VARCHAR, position INT) \
         LANGUAGE SQL",
    );
}

#[test]
fn create_ptf_generic() {
    verified_standard_stmt(
        "CREATE FUNCTION transpose_table(input_table TABLE) \
         RETURNS TABLE \
         POLYMORPHIC \
         LANGUAGE SQL",
    );
}

#[test]
fn create_ptf_with_columns() {
    verified_standard_stmt(
        "CREATE FUNCTION csv_reader(file_path VARCHAR) \
         RETURNS TABLE (line_number INT, line_content VARCHAR) \
         LANGUAGE SQL \
         READS SQL DATA",
    );
}

// ==================== PTF Descriptor Functions ====================

#[test]
fn ptf_descriptor() {
    // DESCRIPTOR for passing column specifications
    verified_standard_stmt(
        "SELECT * FROM TABLE(pivot(TABLE sales, DESCRIPTOR(product_id), DESCRIPTOR(month), 'SUM'))",
    );
}

#[test]
fn ptf_multiple_descriptors() {
    verified_standard_stmt(
        "SELECT * FROM TABLE(unpivot(TABLE wide_data, \
         DESCRIPTOR(measure_name), \
         DESCRIPTOR(measure_value), \
         DESCRIPTOR(jan, feb, mar, apr)))",
    );
}

// ==================== Complex PTF Examples ====================

#[test]
fn ptf_window_function_emulation() {
    verified_standard_stmt(
        "SELECT * FROM TABLE(running_total( \
         TABLE transactions PARTITION BY account_id ORDER BY tx_date, \
         DESCRIPTOR(amount)))",
    );
}

#[test]
fn ptf_json_table_alternative() {
    verified_standard_stmt(
        "SELECT * FROM TABLE(json_extract( \
         TABLE documents, \
         '$.items[*]', \
         COLUMNS(id INT PATH '$.id', name VARCHAR PATH '$.name')))",
    );
}

#[test]
fn ptf_with_scalar_args() {
    verified_standard_stmt("SELECT * FROM TABLE(sample_rows(TABLE large_table, 0.1, 42))");
}

#[test]
fn ptf_chained() {
    verified_standard_stmt(
        "SELECT * FROM TABLE(filter_rows( \
         TABLE(add_derived_columns(TABLE raw_data)), \
         'status = active'))",
    );
}

// ==================== PTF in Subqueries ====================

#[test]
fn ptf_in_with_clause() {
    verified_standard_stmt(
        "WITH expanded AS ( \
           SELECT * FROM TABLE(unnest_array(TABLE arrays_table, DESCRIPTOR(array_col))) \
         ) \
         SELECT * FROM expanded WHERE value > 10",
    );
}

#[test]
fn ptf_in_exists() {
    verified_standard_stmt(
        "SELECT * FROM customers c \
         WHERE EXISTS (SELECT 1 FROM TABLE(get_active_subscriptions(c.id)))",
    );
}

// ==================== DROP PTF ====================

#[test]
fn drop_ptf() {
    verified_standard_stmt("DROP FUNCTION split_string");
}

#[test]
fn drop_ptf_if_exists() {
    verified_standard_stmt("DROP FUNCTION IF EXISTS pivot_table");
}
