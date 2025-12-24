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

//! SQL/PGQ Path Pattern Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn path_pattern_named() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (g MATCH p = (a)-[]->(b) COLUMNS (p))");
}

#[test]
fn path_pattern_multiple() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH p1 = (a)-[]->(b), p2 = (b)-[]->(c) COLUMNS (a.id, c.id))"
    );
}

#[test]
fn path_pattern_alternation() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ((a)-[:KNOWS]->(b) | (a)-[:FOLLOWS]->(b)) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn path_pattern_parenthesized() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ((a)-[e]->(b)) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn path_pattern_complex() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (start:Person)-[:KNOWS*1..5]->(middle)-[:WORKS_AT]->(end:Company) COLUMNS (start.name, end.name))"
    );
}

// ==================== Path Variable Binding ====================

#[test]
fn path_variable_with_quantifier() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH p = (a)-[*1..10]->(b) COLUMNS (path_length(p) AS len))",
    );
}

#[test]
fn path_variable_in_where() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH p = (a)-[*]->(b) WHERE path_length(p) < 5 COLUMNS (a.id, b.id))"
    );
}

#[test]
fn multiple_path_variables() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH p1 = (a)-[*]->(b), p2 = (b)-[*]->(c) \
         WHERE path_length(p1) + path_length(p2) < 10 COLUMNS (a.id, c.id))",
    );
}

// ==================== Pattern Composition ====================

#[test]
fn pattern_sequence() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:A]->(b)-[:B]->(c)-[:C]->(d) COLUMNS (a.id, d.id))"
    );
}

#[test]
fn pattern_mixed_directions() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:X]->(b)<-[:Y]-(c)-[:Z]-(d) COLUMNS (a.id, d.id))",
    );
}

#[test]
fn pattern_repeated_variable() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:E]->(b)-[:E]->(a) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn pattern_diamond() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:L]->(b), (a)-[:R]->(c), (b)-[:M]->(d), (c)-[:M]->(d) COLUMNS (a.id, d.id))"
    );
}

// ==================== Complex Alternation ====================

#[test]
fn alternation_with_quantifiers() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)((-[:A]->){2} | (-[:B]->){3})(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn alternation_nested() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)((-[:A]-> | -[:B]->)(-[:C]-> | -[:D]->))(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn alternation_with_labels() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ((a:Person)-[:WORKS_AT]->(c:Company) | (a:Person)-[:OWNS]->(c:Company)) COLUMNS (a.name, c.name))"
    );
}

// ==================== Grouping and Repetition ====================

#[test]
fn grouped_pattern_quantified() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)((-[:A]->()-[:B]->)){1,3}(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn optional_pattern_segment() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[:START]->(b)(-[:MIDDLE]->())?(c)-[:END]->(d) COLUMNS (a.id, d.id))"
    );
}

#[test]
fn kleene_star_grouped() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)((-[:X]->()-[:Y]->))*-[:Z]->(b) COLUMNS (a.id, b.id))"
    );
}

// ==================== WHERE Inside Patterns ====================

#[test]
fn where_on_node_inline() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a:Person WHERE a.age > 21)-[:KNOWS]->(b WHERE b.verified = true) COLUMNS (a.name, b.name))"
    );
}

#[test]
fn where_on_edge_inline() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e:TRANSFER WHERE e.amount > 10000]->(b) COLUMNS (a.id, e.amount, b.id))"
    );
}

#[test]
fn where_on_path_inline() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e* WHERE ALL(x IN e | x.active = true)]->(b) COLUMNS (a.id, b.id))"
    );
}

// ==================== Anchored Patterns ====================

#[test]
fn pattern_same_start_end() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (g MATCH (a)-[*3..5]->(a) COLUMNS (a.id))");
}

#[test]
fn pattern_fixed_endpoints() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a {id: 1})-[*]->(b {id: 100}) COLUMNS (path_length(p) AS dist))"
    );
}
