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

//! SQL/PGQ GRAPH_TABLE Operator Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn graph_table_basic() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (my_graph MATCH (n) COLUMNS (n.name))");
}

#[test]
fn graph_table_with_alias() {
    verified_standard_stmt(
        "SELECT name FROM GRAPH_TABLE (my_graph MATCH (n) COLUMNS (n.name AS name)) AS g",
    );
}

#[test]
fn graph_table_with_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (my_graph MATCH (n:Person) WHERE n.age > 21 COLUMNS (n.name, n.age))"
    );
}

#[test]
fn graph_table_edge_pattern() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (social MATCH (a)-[e]->(b) COLUMNS (a.name AS from_name, b.name AS to_name))"
    );
}

#[test]
fn graph_table_labeled_edge() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (social MATCH (a:Person)-[e:KNOWS]->(b:Person) COLUMNS (a.name, b.name))"
    );
}

#[test]
fn graph_table_multiple_columns() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id, n.name, n.created_at, n.status))",
    );
}

#[test]
fn graph_table_join_with_regular_table() {
    verified_standard_stmt(
        "SELECT g.name, t.extra FROM GRAPH_TABLE (my_graph MATCH (n) COLUMNS (n.id, n.name)) AS g JOIN other_table AS t ON g.id = t.graph_id"
    );
}

#[test]
fn graph_table_in_subquery() {
    verified_standard_stmt(
        "SELECT * FROM users WHERE id IN (SELECT node_id FROM GRAPH_TABLE (g MATCH (n:Active) COLUMNS (n.id AS node_id)))"
    );
}

#[test]
fn graph_table_complex_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (social MATCH (a)-[e]->(b) WHERE a.age > 18 AND b.age > 18 AND e.since > DATE '2020-01-01' COLUMNS (a.name, b.name, e.since))"
    );
}

#[test]
fn graph_table_aggregation() {
    verified_standard_stmt(
        "SELECT city, COUNT(*) FROM GRAPH_TABLE (social MATCH (p:Person) COLUMNS (p.city)) GROUP BY city"
    );
}
