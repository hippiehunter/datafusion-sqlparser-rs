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

//! SQL/PGQ Path Finding Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn path_finding_any() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY (a)-[*]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn path_finding_any_shortest() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY SHORTEST (a)-[*]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn path_finding_all_shortest() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ALL SHORTEST (a)-[*]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn path_finding_shortest_k() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH SHORTEST 5 (a)-[*]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn path_finding_shortest_k_paths() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH SHORTEST 3 PATHS (a)-[*]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn path_finding_shortest_k_path_groups() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH SHORTEST 2 PATH GROUPS (a)-[*]->(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn path_finding_all_with_limit() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ALL (a)-[*..10]->(b) COLUMNS (a.id, b.id))",
    );
}

// ==================== COST Expressions for Weighted Path Finding ====================

#[test]
fn cost_basic() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY CHEAPEST (a)-[e*]->(b) COST e.weight COLUMNS (a.id, b.id))"
    );
}

#[test]
fn cost_expression() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY CHEAPEST (a)-[e*]->(b) COST e.distance * e.traffic_factor COLUMNS (a.id, b.id))"
    );
}

#[test]
fn cost_cheapest_k() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH CHEAPEST 5 (a)-[e*]->(b) COST e.weight COLUMNS (a.id, b.id))"
    );
}

#[test]
fn cost_cheapest_k_paths() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH CHEAPEST 3 PATHS (a)-[e*]->(b) COST e.weight COLUMNS (a.id, b.id))"
    );
}

#[test]
fn cost_all_cheapest() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ALL CHEAPEST (a)-[e*]->(b) COST e.distance COLUMNS (a.id, b.id))"
    );
}

#[test]
fn cost_with_default() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY CHEAPEST (a)-[e*]->(b) COST COALESCE(e.weight, 1) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn cost_conditional() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY CHEAPEST (a)-[e*]->(b) COST CASE WHEN e.type = 'highway' THEN e.distance * 0.5 ELSE e.distance END COLUMNS (a.id, b.id))"
    );
}

// ==================== KEEP Clause ====================

#[test]
fn keep_basic() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY SHORTEST (a)-[*]->(b) KEEP CHEAPEST COST e.weight COLUMNS (a.id, b.id))"
    );
}

#[test]
fn keep_shortest() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ALL (a)-[e*]->(b) KEEP SHORTEST COLUMNS (a.id, b.id))",
    );
}

#[test]
fn keep_cheapest() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ALL (a)-[e*]->(b) KEEP CHEAPEST COST e.weight COLUMNS (a.id, b.id))"
    );
}

#[test]
fn keep_first_k() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ALL (a)-[*]->(b) KEEP FIRST 10 COLUMNS (a.id, b.id))",
    );
}

// ==================== Combined Path Finding Options ====================

#[test]
fn shortest_with_mode() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY SHORTEST ACYCLIC (a)-[*]->(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn cheapest_with_mode() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY CHEAPEST SIMPLE (a)-[e*]->(b) COST e.weight COLUMNS (a.id, b.id))"
    );
}

#[test]
fn shortest_with_quantifier() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY SHORTEST (a)-[*1..10]->(b) COLUMNS (a.id, b.id))",
    );
}

#[test]
fn all_paths_bounded() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ALL (a)-[*..5]->(b) COLUMNS (a.id, b.id))",
    );
}
