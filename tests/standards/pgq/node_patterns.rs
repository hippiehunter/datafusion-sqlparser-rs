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

//! SQL/PGQ Node Pattern Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn node_pattern_anonymous() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH () COLUMNS (1 AS one))"
    );
}

#[test]
fn node_pattern_variable() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id))"
    );
}

#[test]
fn node_pattern_single_label() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person) COLUMNS (n.name))"
    );
}

#[test]
fn node_pattern_multiple_labels() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person:Employee) COLUMNS (n.name))"
    );
}

#[test]
fn node_pattern_label_disjunction() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person|Company) COLUMNS (n.name))"
    );
}

#[test]
fn node_pattern_label_negation() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:!Inactive) COLUMNS (n.id))"
    );
}

#[test]
fn node_pattern_wildcard_label() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:%) COLUMNS (n.id))"
    );
}

#[test]
fn node_pattern_with_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person WHERE n.age > 18) COLUMNS (n.name))"
    );
}

// ==================== Complex Label Expressions ====================

#[test]
fn label_conjunction() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person&Employee) COLUMNS (n.id))"
    );
}

#[test]
fn label_disjunction_multiple() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person|Company|Organization) COLUMNS (n.id))"
    );
}

#[test]
fn label_negation_combined() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person&!Inactive) COLUMNS (n.id))"
    );
}

#[test]
fn label_complex_expression() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:(Person|Company)&!Deleted) COLUMNS (n.id))"
    );
}

#[test]
fn label_parenthesized() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:(Person&Employee)|Manager) COLUMNS (n.id))"
    );
}

#[test]
fn label_wildcard_with_negation() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:%&!System) COLUMNS (n.id))"
    );
}

// ==================== Property Value Expressions ====================

#[test]
fn property_literal_string() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n {status: 'active'}) COLUMNS (n.id))"
    );
}

#[test]
fn property_literal_number() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n {priority: 1}) COLUMNS (n.id))"
    );
}

#[test]
fn property_literal_boolean() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n {active: true}) COLUMNS (n.id))"
    );
}

#[test]
fn property_multiple() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n {status: 'active', priority: 1}) COLUMNS (n.id))"
    );
}

#[test]
fn property_with_label() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person {age: 30, city: 'NYC'}) COLUMNS (n.name))"
    );
}

#[test]
fn property_null_check() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n {deleted_at: NULL}) COLUMNS (n.id))"
    );
}

#[test]
fn property_nested_access() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.address.city, n.address.zip))"
    );
}

#[test]
fn property_array_access() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.tags[1], n.scores[0]))"
    );
}

#[test]
fn property_expression_in_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person WHERE n.salary * 12 > 100000) COLUMNS (n.name))"
    );
}

#[test]
fn property_function_call() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n WHERE LENGTH(n.name) > 5) COLUMNS (n.name))"
    );
}

#[test]
fn property_case_expression() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (CASE WHEN n.age < 18 THEN 'minor' ELSE 'adult' END AS category))"
    );
}

#[test]
fn property_coalesce() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (COALESCE(n.nickname, n.name) AS display_name))"
    );
}
