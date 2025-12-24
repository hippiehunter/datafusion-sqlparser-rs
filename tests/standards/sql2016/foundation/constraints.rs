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

//! SQL:2016 Advanced Constraint Tests (ISO/IEC 9075-2)

use crate::standards::common::verified_standard_stmt;

// ==================== F491: Assertion Constraints ====================

#[test]
fn f491_01_create_assertion_basic() {
    // SQL:2016 F491: Schema-level CHECK constraint
    verified_standard_stmt(
        "CREATE ASSERTION positive_balance CHECK (NOT EXISTS (SELECT * FROM accounts WHERE balance < 0))"
    );
}

#[test]
fn f491_02_create_assertion_with_name() {
    verified_standard_stmt(
        "CREATE ASSERTION inventory_check CHECK \
         (NOT EXISTS (SELECT * FROM products WHERE quantity_on_hand < reorder_level AND NOT discontinued))"
    );
}

#[test]
fn f491_03_create_assertion_cross_table() {
    verified_standard_stmt(
        "CREATE ASSERTION order_customer_exists CHECK \
         (NOT EXISTS (SELECT * FROM orders o WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)))"
    );
}

#[test]
fn f491_04_drop_assertion() {
    verified_standard_stmt("DROP ASSERTION positive_balance");
}

#[test]
fn f491_05_drop_assertion_if_exists() {
    verified_standard_stmt("DROP ASSERTION IF EXISTS positive_balance");
}

// ==================== F701: Referential MATCH Types ====================

#[test]
fn f701_01_match_full() {
    // SQL:2016 F701: MATCH FULL - all columns must be NULL or all must match
    verified_standard_stmt(
        "CREATE TABLE child (id INT, parent_a INT, parent_b INT, \
         FOREIGN KEY (parent_a, parent_b) REFERENCES parent(a, b) MATCH FULL)",
    );
}

#[test]
fn f701_02_match_partial() {
    // SQL:2016 F701: MATCH PARTIAL - non-null columns must match
    verified_standard_stmt(
        "CREATE TABLE child (id INT, parent_a INT, parent_b INT, \
         FOREIGN KEY (parent_a, parent_b) REFERENCES parent(a, b) MATCH PARTIAL)",
    );
}

#[test]
fn f701_03_match_simple() {
    // SQL:2016 F701: MATCH SIMPLE - default, any NULL satisfies constraint
    verified_standard_stmt(
        "CREATE TABLE child (id INT, parent_a INT, parent_b INT, \
         FOREIGN KEY (parent_a, parent_b) REFERENCES parent(a, b) MATCH SIMPLE)",
    );
}

#[test]
fn f701_04_match_with_actions() {
    verified_standard_stmt(
        "CREATE TABLE child (id INT, parent_id INT, \
         FOREIGN KEY (parent_id) REFERENCES parent(id) MATCH FULL ON DELETE CASCADE ON UPDATE SET NULL)"
    );
}

// ==================== F721: Deferrable Constraints ====================

#[test]
fn f721_01_deferrable_primary_key() {
    // SQL:2016 F721: Deferrable constraints
    verified_standard_stmt("CREATE TABLE t (id INT PRIMARY KEY DEFERRABLE)");
}

#[test]
fn f721_02_deferrable_initially_deferred() {
    verified_standard_stmt("CREATE TABLE t (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)");
}

#[test]
fn f721_03_deferrable_initially_immediate() {
    verified_standard_stmt("CREATE TABLE t (id INT PRIMARY KEY DEFERRABLE INITIALLY IMMEDIATE)");
}

#[test]
fn f721_04_not_deferrable() {
    verified_standard_stmt("CREATE TABLE t (id INT PRIMARY KEY NOT DEFERRABLE)");
}

#[test]
fn f721_05_deferrable_unique() {
    verified_standard_stmt(
        "CREATE TABLE t (id INT, email VARCHAR(100) UNIQUE DEFERRABLE INITIALLY DEFERRED)",
    );
}

#[test]
fn f721_06_deferrable_foreign_key() {
    verified_standard_stmt(
        "CREATE TABLE child (id INT, parent_id INT, \
         FOREIGN KEY (parent_id) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
    );
}

#[test]
fn f721_07_deferrable_check() {
    verified_standard_stmt(
        "CREATE TABLE t (id INT, value INT CHECK (value > 0) DEFERRABLE INITIALLY IMMEDIATE)",
    );
}

#[test]
fn f721_08_set_constraints_deferred() {
    verified_standard_stmt("SET CONSTRAINTS ALL DEFERRED");
}

#[test]
fn f721_09_set_constraints_immediate() {
    verified_standard_stmt("SET CONSTRAINTS ALL IMMEDIATE");
}

#[test]
fn f721_10_set_constraints_named() {
    verified_standard_stmt("SET CONSTRAINTS pk_orders, fk_order_customer DEFERRED");
}

#[test]
fn f721_11_set_constraints_single() {
    verified_standard_stmt("SET CONSTRAINTS fk_employee_dept IMMEDIATE");
}

// ==================== F711: Referential Actions ====================

#[test]
fn f711_01_on_delete_cascade() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT REFERENCES customers(id) ON DELETE CASCADE)",
    );
}

#[test]
fn f711_02_on_delete_set_null() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT REFERENCES customers(id) ON DELETE SET NULL)",
    );
}

#[test]
fn f711_03_on_delete_set_default() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT DEFAULT 0 REFERENCES customers(id) ON DELETE SET DEFAULT)"
    );
}

#[test]
fn f711_04_on_delete_restrict() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT REFERENCES customers(id) ON DELETE RESTRICT)",
    );
}

#[test]
fn f711_05_on_delete_no_action() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT REFERENCES customers(id) ON DELETE NO ACTION)"
    );
}

#[test]
fn f711_06_on_update_cascade() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT REFERENCES customers(id) ON UPDATE CASCADE)",
    );
}

#[test]
fn f711_07_combined_actions() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT REFERENCES customers(id) ON DELETE CASCADE ON UPDATE SET NULL)"
    );
}

// ==================== F690: Collation Support ====================

#[test]
fn f690_01_column_collation() {
    verified_standard_stmt("CREATE TABLE t (name VARCHAR(100) COLLATE utf8_general_ci)");
}

#[test]
fn f690_02_comparison_collation() {
    verified_standard_stmt("SELECT * FROM t WHERE name COLLATE utf8_bin = 'Test'");
}

#[test]
fn f690_03_order_by_collation() {
    verified_standard_stmt("SELECT * FROM t ORDER BY name COLLATE utf8_unicode_ci");
}

// ==================== F692: Named Constraints ====================

#[test]
fn f692_01_named_primary_key() {
    verified_standard_stmt("CREATE TABLE t (id INT, CONSTRAINT pk_t PRIMARY KEY (id))");
}

#[test]
fn f692_02_named_foreign_key() {
    verified_standard_stmt(
        "CREATE TABLE orders (id INT, customer_id INT, \
         CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id))",
    );
}

#[test]
fn f692_03_named_unique() {
    verified_standard_stmt(
        "CREATE TABLE t (id INT, email VARCHAR(100), CONSTRAINT uq_email UNIQUE (email))",
    );
}

#[test]
fn f692_04_named_check() {
    verified_standard_stmt(
        "CREATE TABLE t (id INT, value INT, CONSTRAINT chk_positive CHECK (value > 0))",
    );
}

#[test]
fn f692_05_alter_drop_constraint() {
    verified_standard_stmt("ALTER TABLE t DROP CONSTRAINT pk_t");
}

#[test]
fn f692_06_alter_add_constraint() {
    verified_standard_stmt("ALTER TABLE t ADD CONSTRAINT uq_code UNIQUE (code)");
}
