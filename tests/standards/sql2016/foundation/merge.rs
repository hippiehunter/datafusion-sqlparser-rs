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

//! SQL:2016 MERGE Statement Tests (ISO/IEC 9075-2, Features F312-F314)

use crate::standards::common::verified_standard_stmt;

// ==================== F312: MERGE Statement ====================

#[test]
fn f312_01_merge_basic() {
    // SQL:2016 F312: Basic MERGE statement
    verified_standard_stmt(
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.value = s.value \
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
    );
}

#[test]
fn f312_02_merge_update_only() {
    verified_standard_stmt(
        "MERGE INTO inventory i USING shipments s ON i.product_id = s.product_id \
         WHEN MATCHED THEN UPDATE SET i.quantity = i.quantity + s.quantity"
    );
}

#[test]
fn f312_03_merge_insert_only() {
    verified_standard_stmt(
        "MERGE INTO customers c USING new_customers n ON c.email = n.email \
         WHEN NOT MATCHED THEN INSERT (name, email) VALUES (n.name, n.email)"
    );
}

#[test]
fn f312_04_merge_with_subquery() {
    verified_standard_stmt(
        "MERGE INTO target t USING (SELECT * FROM source WHERE active = true) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.value = s.value"
    );
}

#[test]
fn f312_05_merge_multiple_columns() {
    verified_standard_stmt(
        "MERGE INTO employees e USING updates u ON e.id = u.id \
         WHEN MATCHED THEN UPDATE SET e.name = u.name, e.salary = u.salary, e.dept = u.dept \
         WHEN NOT MATCHED THEN INSERT (id, name, salary, dept) VALUES (u.id, u.name, u.salary, u.dept)"
    );
}

// ==================== F313: Enhanced MERGE with DELETE ====================

#[test]
fn f313_01_merge_with_delete() {
    // SQL:2016 F313: MERGE with DELETE action
    verified_standard_stmt(
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED AND s.deleted = true THEN DELETE \
         WHEN MATCHED THEN UPDATE SET t.value = s.value \
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)"
    );
}

#[test]
fn f313_02_merge_delete_only() {
    verified_standard_stmt(
        "MERGE INTO old_records o USING deletions d ON o.id = d.id \
         WHEN MATCHED THEN DELETE"
    );
}

#[test]
fn f313_03_merge_conditional_delete() {
    verified_standard_stmt(
        "MERGE INTO inventory i USING adjustments a ON i.product_id = a.product_id \
         WHEN MATCHED AND i.quantity + a.delta <= 0 THEN DELETE \
         WHEN MATCHED THEN UPDATE SET i.quantity = i.quantity + a.delta"
    );
}

// ==================== F314: MERGE with Conditions ====================

#[test]
fn f314_01_merge_matched_condition() {
    // SQL:2016 F314: MERGE with AND condition on WHEN MATCHED
    verified_standard_stmt(
        "MERGE INTO prices p USING updates u ON p.product_id = u.product_id \
         WHEN MATCHED AND u.price > p.price THEN UPDATE SET p.price = u.price"
    );
}

#[test]
fn f314_02_merge_not_matched_condition() {
    verified_standard_stmt(
        "MERGE INTO customers c USING leads l ON c.email = l.email \
         WHEN NOT MATCHED AND l.score > 50 THEN INSERT (email, name, score) VALUES (l.email, l.name, l.score)"
    );
}

#[test]
fn f314_03_merge_multiple_matched() {
    verified_standard_stmt(
        "MERGE INTO orders o USING updates u ON o.id = u.id \
         WHEN MATCHED AND u.action = 'cancel' THEN DELETE \
         WHEN MATCHED AND u.action = 'update' THEN UPDATE SET o.status = u.new_status \
         WHEN MATCHED THEN UPDATE SET o.modified_at = CURRENT_TIMESTAMP"
    );
}

#[test]
fn f314_04_merge_multiple_not_matched() {
    verified_standard_stmt(
        "MERGE INTO products p USING imports i ON p.sku = i.sku \
         WHEN NOT MATCHED AND i.category = 'electronics' THEN INSERT (sku, name, category, price) VALUES (i.sku, i.name, 'ELEC', i.price) \
         WHEN NOT MATCHED THEN INSERT (sku, name, category, price) VALUES (i.sku, i.name, 'OTHER', i.price)"
    );
}

// ==================== SQL:2023 MERGE Enhancements ====================

#[test]
fn merge_not_matched_by_source() {
    // SQL:2023: NOT MATCHED BY SOURCE clause
    verified_standard_stmt(
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.value = s.value \
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value) \
         WHEN NOT MATCHED BY SOURCE THEN DELETE"
    );
}

#[test]
fn merge_not_matched_by_source_condition() {
    verified_standard_stmt(
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.value = s.value \
         WHEN NOT MATCHED BY SOURCE AND t.created_at < DATE '2020-01-01' THEN DELETE"
    );
}

#[test]
fn merge_not_matched_by_source_update() {
    verified_standard_stmt(
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.value = s.value \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.status = 'orphaned'"
    );
}

#[test]
fn merge_not_matched_by_target() {
    // NOT MATCHED BY TARGET is same as NOT MATCHED
    verified_standard_stmt(
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.value = s.value \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, value) VALUES (s.id, s.value)"
    );
}

// ==================== MERGE with Complex Sources ====================

#[test]
fn merge_with_cte() {
    verified_standard_stmt(
        "WITH source AS (SELECT id, SUM(amount) AS total FROM transactions GROUP BY id) \
         MERGE INTO balances b USING source s ON b.id = s.id \
         WHEN MATCHED THEN UPDATE SET b.balance = b.balance + s.total \
         WHEN NOT MATCHED THEN INSERT (id, balance) VALUES (s.id, s.total)"
    );
}

#[test]
fn merge_with_values() {
    verified_standard_stmt(
        "MERGE INTO settings s USING (VALUES ('theme', 'dark'), ('lang', 'en')) AS v (key, value) ON s.key = v.key \
         WHEN MATCHED THEN UPDATE SET s.value = v.value \
         WHEN NOT MATCHED THEN INSERT (key, value) VALUES (v.key, v.value)"
    );
}

#[test]
fn merge_with_join() {
    verified_standard_stmt(
        "MERGE INTO target t \
         USING (SELECT s.*, r.region FROM source s JOIN regions r ON s.region_id = r.id) AS src \
         ON t.id = src.id \
         WHEN MATCHED THEN UPDATE SET t.region = src.region"
    );
}

// ==================== MERGE Output/Returning ====================

#[test]
fn merge_output() {
    verified_standard_stmt(
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.value = s.value \
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value) \
         OUTPUT $action, inserted.id, deleted.value"
    );
}
