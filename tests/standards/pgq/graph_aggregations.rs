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

//! SQL/PGQ Graph Aggregation Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn aggregation_count() {
    verified_standard_stmt("SELECT COUNT(*) FROM GRAPH_TABLE (g MATCH (n:Person) COLUMNS (n.id))");
}

#[test]
fn aggregation_count_edges() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]->(b) COLUMNS (a.id, COUNT(e) AS edge_count)) GROUP BY a.id"
    );
}

#[test]
fn aggregation_sum() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e:TRANSFER]->(b) COLUMNS (a.id, SUM(e.amount) AS total))"
    );
}

#[test]
fn aggregation_avg() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person) COLUMNS (AVG(n.age) AS avg_age))",
    );
}

#[test]
fn aggregation_group_by() {
    verified_standard_stmt(
        "SELECT city, cnt FROM GRAPH_TABLE (g MATCH (n:Person) COLUMNS (n.city AS city, COUNT(*) AS cnt)) GROUP BY city"
    );
}

#[test]
fn aggregation_having() {
    verified_standard_stmt(
        "SELECT city, cnt FROM GRAPH_TABLE (g MATCH (n:Person) COLUMNS (n.city AS city, COUNT(*) AS cnt)) GROUP BY city HAVING COUNT(*) > 10"
    );
}

#[test]
fn row_limiting_one_row_per_match() {
    // Test with row limiting after patterns (canonical form)
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*]->(b) ONE ROW PER MATCH COLUMNS (a.id, b.id))",
    );
}

#[test]
fn row_limiting_one_row_per_vertex() {
    // Test with row limiting after patterns (canonical form)
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*]->(b) ONE ROW PER VERTEX COLUMNS (a.id, b.id))",
    );
}

#[test]
fn row_limiting_one_row_per_step() {
    // Test with row limiting after patterns (canonical form)
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*]->(b) ONE ROW PER STEP COLUMNS (a.id, b.id))",
    );
}

#[test]
fn row_limiting_before_patterns_parses() {
    use crate::standards::common::one_statement_parses_to_std;
    // Test that row limiting BEFORE patterns also parses correctly
    // (normalizes to canonical form with row limiting after patterns)
    one_statement_parses_to_std(
        "SELECT * FROM GRAPH_TABLE (g MATCH ONE ROW PER MATCH (a)-[*]->(b) COLUMNS (a.id, b.id))",
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*]->(b) ONE ROW PER MATCH COLUMNS (a.id, b.id))",
    );
    one_statement_parses_to_std(
        "SELECT * FROM GRAPH_TABLE (g MATCH ONE ROW PER VERTEX (a)-[*]->(b) COLUMNS (a.id, b.id))",
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*]->(b) ONE ROW PER VERTEX COLUMNS (a.id, b.id))",
    );
    one_statement_parses_to_std(
        "SELECT * FROM GRAPH_TABLE (g MATCH ONE ROW PER STEP (a)-[*]->(b) COLUMNS (a.id, b.id))",
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*]->(b) ONE ROW PER STEP COLUMNS (a.id, b.id))",
    );
}

#[test]
fn row_limiting_with_where_clause() {
    // Test row limiting with WHERE clause (row limiting comes after WHERE)
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a:Person)-[e:KNOWS*]->(b:Person) WHERE a.age > 21 ONE ROW PER MATCH COLUMNS (a.name, b.name))",
    );
}
