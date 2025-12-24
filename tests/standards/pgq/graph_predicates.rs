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

//! SQL/PGQ Graph Predicate Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn predicate_is_labeled() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE n IS LABELED Person COLUMNS (n.id))"
    );
}

#[test]
fn predicate_is_not_labeled() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE n IS NOT LABELED Inactive COLUMNS (n.id))"
    );
}

#[test]
fn predicate_is_source_of() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]->(b) WHERE a IS SOURCE OF e COLUMNS (a.id))"
    );
}

#[test]
fn predicate_is_destination_of() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]->(b) WHERE b IS DESTINATION OF e COLUMNS (b.id))"
    );
}

#[test]
fn predicate_exists_match() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->() } COLUMNS (n.id))"
    );
}

#[test]
fn predicate_not_exists_match() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) WHERE NOT EXISTS { MATCH (n)-[:BLOCKED]->() } COLUMNS (n.id))"
    );
}

#[test]
fn predicate_property_comparison() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]->(b) WHERE a.age > b.age COLUMNS (a.name, b.name))"
    );
}

#[test]
fn predicate_same_element() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[]->(b)-[]->(c) WHERE a IS SAME AS c COLUMNS (a.id))"
    );
}
