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

//! SQL/PGQ Graph Element Subquery Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

// ==================== EXISTS Subqueries ====================

#[test]
fn exists_pattern_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE EXISTS { (n)-[:KNOWS]->() } COLUMNS (n.id))"
    );
}

#[test]
fn not_exists_pattern_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE NOT EXISTS { (n)-[:BLOCKED]->() } COLUMNS (n.id))"
    );
}

#[test]
fn exists_with_path() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE EXISTS { (n)-[*1..5]->(m:Target) } COLUMNS (n.id))"
    );
}

#[test]
fn exists_complex_pattern() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE EXISTS { (n)-[:A]->()-[:B]->()-[:C]->() } COLUMNS (n.id))"
    );
}

#[test]
fn exists_with_condition() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE EXISTS { (n)-[e:OWNS]->(p) WHERE p.value > 1000 } COLUMNS (n.id))"
    );
}

// ==================== COUNT Subqueries ====================

#[test]
fn count_pattern_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE COUNT { (n)-[:FOLLOWS]->() } > 100 COLUMNS (n.id))"
    );
}

#[test]
fn count_in_columns() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n:Person) COLUMNS (n.name, COUNT { (n)-[:KNOWS]->() } AS friend_count))"
    );
}

#[test]
fn count_distinct_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id, COUNT { DISTINCT (n)-[:TAGGED]->(t) RETURN t.category } AS categories))"
    );
}

// ==================== Scalar Subqueries ====================

#[test]
fn scalar_subquery_in_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE n.score > VALUE { (n)-[:BASELINE]->(b) RETURN b.threshold } COLUMNS (n.id))"
    );
}

#[test]
fn scalar_subquery_in_columns() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id, VALUE { (n)-[:MANAGER]->(m) RETURN m.name } AS manager_name))"
    );
}

// ==================== COLLECT Subqueries ====================

#[test]
fn collect_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id, COLLECT { (n)-[:TAGGED]->(t) RETURN t.name } AS tags))"
    );
}

#[test]
fn collect_ordered() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id, COLLECT { (n)-[e:SCORED]->(t) RETURN t.name ORDER BY e.score DESC } AS ranked_items))"
    );
}

#[test]
fn collect_limited() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id, COLLECT { (n)-[:RECENT]->(p) RETURN p.title LIMIT 5 } AS recent_posts))"
    );
}

// ==================== Nested Graph Patterns ====================

#[test]
fn nested_match_in_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g \
         MATCH (a)-[e]->(b) \
         WHERE EXISTS { MATCH (b)-[:VERIFIED]->(:Authority) } \
         COLUMNS (a.id, b.id))"
    );
}

#[test]
fn correlated_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g \
         MATCH (p:Person) \
         WHERE COUNT { (p)-[:PURCHASED]->(i) WHERE i.price > 100 } >= 5 \
         COLUMNS (p.id, p.name))"
    );
}

#[test]
fn multiple_subqueries() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g \
         MATCH (n:User) \
         WHERE EXISTS { (n)-[:PREMIUM]->() } \
           AND COUNT { (n)-[:ORDER]->() } > 10 \
         COLUMNS (n.id))"
    );
}

// ==================== Subqueries with Aggregation ====================

#[test]
fn sum_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (c:Customer) \
         COLUMNS (c.id, SUM { (c)-[:PURCHASED]->(p) RETURN p.price } AS total_spent))"
    );
}

#[test]
fn avg_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (u:User) \
         COLUMNS (u.id, AVG { (u)-[:RATED]->(m) RETURN m.score } AS avg_rating))"
    );
}

#[test]
fn min_max_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (s:Sensor) \
         COLUMNS (s.id, \
                  MIN { (s)-[:READING]->(r) RETURN r.value } AS min_val, \
                  MAX { (s)-[:READING]->(r) RETURN r.value } AS max_val))"
    );
}

// ==================== Path Subqueries ====================

#[test]
fn path_length_subquery() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g \
         MATCH (a), (b) \
         WHERE a <> b \
         COLUMNS (a.id, b.id, VALUE { ANY SHORTEST (a)-[*]->(b) RETURN path_length(p) } AS distance))"
    );
}

#[test]
fn shortest_path_exists() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g \
         MATCH (a:Source), (b:Target) \
         WHERE EXISTS { ANY SHORTEST (a)-[*..10]->(b) } \
         COLUMNS (a.id, b.id))"
    );
}
