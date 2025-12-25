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

//! SQL/PGQ Quantifier Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn quantifier_zero_or_more() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:KNOWS*]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn quantifier_one_or_more() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:KNOWS+]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn quantifier_optional() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:KNOWS?]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn quantifier_exact() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:KNOWS{3}]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn quantifier_minimum() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:KNOWS{2,}]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn quantifier_maximum() {
    // Note: {,5} syntax is normalized to *..5 (star-range form) in graph patterns
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:KNOWS*..5]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn quantifier_range() {
    // Note: {1,5} syntax is normalized to *1..5 (star-range form) in graph patterns
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:KNOWS*1..5]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn quantifier_on_anonymous_edge() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*1..3]->(b) COLUMNS (a.id, b.id))",
    );
}
