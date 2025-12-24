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

//! SQL/PGQ Edge Pattern Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn edge_pattern_right_directed() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]->(b) COLUMNS (a.id, b.id))");
}

#[test]
fn edge_pattern_left_directed() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (g MATCH (a)<-[e]-(b) COLUMNS (a.id, b.id))");
}

#[test]
fn edge_pattern_undirected() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]-(b) COLUMNS (a.id, b.id))");
}

#[test]
fn edge_pattern_any_direction() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)<-[e]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn edge_pattern_with_label() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e:KNOWS]->(b) COLUMNS (a.name, b.name))",
    );
}

#[test]
fn edge_pattern_multiple_labels() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e:KNOWS|FOLLOWS]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn edge_pattern_anonymous() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (g MATCH (a)-->(b) COLUMNS (a.id, b.id))");
}

#[test]
fn edge_pattern_with_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e:TRANSFER WHERE e.amount > 1000]->(b) COLUMNS (a.id, e.amount, b.id))"
    );
}

#[test]
fn edge_pattern_chain() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e1]->(b)-[e2]->(c) COLUMNS (a.id, b.id, c.id))",
    );
}

#[test]
fn edge_pattern_long_chain() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[]->(b)-[]->(c)-[]->(d) COLUMNS (a.id, d.id))",
    );
}
