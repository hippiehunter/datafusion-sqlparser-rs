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

//! SQL:2016 Temporal Table Tests (ISO/IEC 9075-2, Features T180-T187)

use crate::standards::common::verified_standard_stmt;

// ==================== T180: System-Versioned Tables ====================

#[test]
fn t180_01_system_versioned_table_basic() {
    // SQL:2016 T180: Basic system-versioned table
    verified_standard_stmt(
        "CREATE TABLE t (id INT, name VARCHAR(100), \
         sys_start TIMESTAMP GENERATED ALWAYS AS ROW START, \
         sys_end TIMESTAMP GENERATED ALWAYS AS ROW END, \
         PERIOD FOR SYSTEM_TIME (sys_start, sys_end)) \
         WITH SYSTEM VERSIONING",
    );
}

#[test]
fn t180_02_system_versioned_with_primary_key() {
    verified_standard_stmt(
        "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(100), salary DECIMAL(10,2), \
         sys_start TIMESTAMP(6) GENERATED ALWAYS AS ROW START, \
         sys_end TIMESTAMP(6) GENERATED ALWAYS AS ROW END, \
         PERIOD FOR SYSTEM_TIME (sys_start, sys_end)) \
         WITH SYSTEM VERSIONING",
    );
}

#[test]
fn t180_03_for_system_time_as_of() {
    // SQL:2016 T180: Query historical data at a point in time
    verified_standard_stmt(
        "SELECT * FROM employees FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00'",
    );
}

#[test]
fn t180_04_for_system_time_from_to() {
    verified_standard_stmt(
        "SELECT * FROM employees FOR SYSTEM_TIME FROM TIMESTAMP '2023-01-01' TO TIMESTAMP '2023-12-31'"
    );
}

#[test]
fn t180_05_for_system_time_between() {
    verified_standard_stmt(
        "SELECT * FROM employees FOR SYSTEM_TIME BETWEEN TIMESTAMP '2023-01-01' AND TIMESTAMP '2023-12-31'"
    );
}

#[test]
fn t180_06_for_system_time_contained_in() {
    verified_standard_stmt(
        "SELECT * FROM employees FOR SYSTEM_TIME CONTAINED IN (TIMESTAMP '2023-01-01', TIMESTAMP '2023-12-31')"
    );
}

#[test]
fn t180_07_for_system_time_all() {
    verified_standard_stmt("SELECT * FROM employees FOR SYSTEM_TIME ALL");
}

#[test]
fn t180_08_system_time_in_join() {
    verified_standard_stmt(
        "SELECT e.name, d.dept_name FROM employees FOR SYSTEM_TIME AS OF TIMESTAMP '2023-06-01' e \
         JOIN departments FOR SYSTEM_TIME AS OF TIMESTAMP '2023-06-01' d ON e.dept_id = d.id",
    );
}

#[test]
fn t180_09_system_time_in_subquery() {
    verified_standard_stmt(
        "SELECT * FROM current_employees WHERE id IN \
         (SELECT id FROM employees FOR SYSTEM_TIME AS OF TIMESTAMP '2022-01-01')",
    );
}

// ==================== T181: Application-Time Period Tables ====================

#[test]
fn t181_01_application_time_period() {
    // SQL:2016 T181: Application-time period tables
    verified_standard_stmt(
        "CREATE TABLE contracts (id INT, customer_id INT, \
         valid_from DATE, valid_to DATE, \
         PERIOD FOR valid_period (valid_from, valid_to))",
    );
}

#[test]
fn t181_02_application_time_with_constraints() {
    verified_standard_stmt(
        "CREATE TABLE insurance_policies (policy_id INT PRIMARY KEY, \
         coverage_start DATE NOT NULL, coverage_end DATE NOT NULL, \
         premium DECIMAL(10,2), \
         PERIOD FOR coverage (coverage_start, coverage_end))",
    );
}

#[test]
fn t181_03_for_portion_of() {
    // SQL:2016 T181: Update for portion of period
    verified_standard_stmt(
        "UPDATE contracts FOR PORTION OF valid_period FROM DATE '2023-06-01' TO DATE '2023-12-31' \
         SET premium = 500.00 WHERE id = 1",
    );
}

#[test]
fn t181_04_delete_for_portion_of() {
    verified_standard_stmt(
        "DELETE FROM contracts FOR PORTION OF valid_period FROM DATE '2023-01-01' TO DATE '2023-06-30' \
         WHERE customer_id = 100"
    );
}

// ==================== T182: Application-Time Period Predicates ====================

#[test]
fn t182_01_period_contains() {
    verified_standard_stmt("SELECT * FROM contracts WHERE valid_period CONTAINS DATE '2023-06-15'");
}

#[test]
fn t182_02_period_overlaps() {
    verified_standard_stmt(
        "SELECT * FROM contracts WHERE valid_period OVERLAPS PERIOD (DATE '2023-01-01', DATE '2023-06-30')"
    );
}

#[test]
fn t182_03_period_equals() {
    verified_standard_stmt(
        "SELECT * FROM contracts WHERE valid_period EQUALS PERIOD (DATE '2023-01-01', DATE '2023-12-31')"
    );
}

#[test]
fn t182_04_period_precedes() {
    verified_standard_stmt(
        "SELECT * FROM contracts WHERE valid_period PRECEDES PERIOD (DATE '2024-01-01', DATE '2024-12-31')"
    );
}

#[test]
fn t182_05_period_succeeds() {
    verified_standard_stmt(
        "SELECT * FROM contracts WHERE valid_period SUCCEEDS PERIOD (DATE '2022-01-01', DATE '2022-12-31')"
    );
}

#[test]
fn t182_06_period_immediately_precedes() {
    verified_standard_stmt(
        "SELECT * FROM contracts WHERE valid_period IMMEDIATELY PRECEDES PERIOD (DATE '2024-01-01', DATE '2024-12-31')"
    );
}

#[test]
fn t182_07_period_immediately_succeeds() {
    verified_standard_stmt(
        "SELECT * FROM contracts WHERE valid_period IMMEDIATELY SUCCEEDS PERIOD (DATE '2022-01-01', DATE '2022-12-31')"
    );
}

// ==================== T184: Bitemporal Tables ====================

#[test]
fn t184_01_bitemporal_table() {
    // SQL:2016 T184: Bitemporal tables (both system and application time)
    verified_standard_stmt(
        "CREATE TABLE product_prices (product_id INT, price DECIMAL(10,2), \
         valid_from DATE, valid_to DATE, \
         sys_start TIMESTAMP GENERATED ALWAYS AS ROW START, \
         sys_end TIMESTAMP GENERATED ALWAYS AS ROW END, \
         PERIOD FOR valid_period (valid_from, valid_to), \
         PERIOD FOR SYSTEM_TIME (sys_start, sys_end)) \
         WITH SYSTEM VERSIONING",
    );
}

#[test]
fn t184_02_bitemporal_query() {
    verified_standard_stmt(
        "SELECT * FROM product_prices \
         FOR SYSTEM_TIME AS OF TIMESTAMP '2023-06-01' \
         WHERE valid_period CONTAINS DATE '2023-06-15'",
    );
}

// ==================== T185: Temporal Primary Keys and Foreign Keys ====================

#[test]
fn t185_01_temporal_primary_key() {
    verified_standard_stmt(
        "CREATE TABLE employees (emp_id INT, dept_id INT, \
         valid_from DATE, valid_to DATE, \
         PERIOD FOR employment (valid_from, valid_to), \
         PRIMARY KEY (emp_id, employment WITHOUT OVERLAPS))",
    );
}

#[test]
fn t185_02_temporal_foreign_key() {
    verified_standard_stmt(
        "CREATE TABLE assignments (id INT, emp_id INT, project_id INT, \
         valid_from DATE, valid_to DATE, \
         PERIOD FOR assignment (valid_from, valid_to), \
         FOREIGN KEY (emp_id, PERIOD assignment) REFERENCES employees (emp_id, PERIOD employment))",
    );
}

// ==================== T186: System-Versioned History Tables ====================

#[test]
fn t186_01_with_history_table() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20), \
         sys_start TIMESTAMP GENERATED ALWAYS AS ROW START, \
         sys_end TIMESTAMP GENERATED ALWAYS AS ROW END, \
         PERIOD FOR SYSTEM_TIME (sys_start, sys_end)) \
         WITH SYSTEM VERSIONING WITH HISTORY TABLE order_history",
    );
}

// ==================== T187: Temporal DML ====================

#[test]
fn t187_01_insert_into_temporal() {
    verified_standard_stmt(
        "INSERT INTO contracts (id, customer_id, valid_from, valid_to) \
         VALUES (1, 100, DATE '2023-01-01', DATE '2023-12-31')",
    );
}

#[test]
fn t187_02_update_temporal() {
    verified_standard_stmt("UPDATE contracts SET premium = 600.00 WHERE id = 1");
}

#[test]
fn t187_03_delete_temporal() {
    verified_standard_stmt(
        "DELETE FROM contracts WHERE id = 1 AND valid_period CONTAINS DATE '2023-06-01'",
    );
}
