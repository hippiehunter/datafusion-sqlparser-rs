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

//! SQL/PGQ Path Function Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn path_function_element_id() {
    verified_standard_stmt("SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (element_id(n) AS id))");
}

#[test]
fn path_function_vertices() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH p = (a)-[*]->(b) COLUMNS (vertices(p) AS nodes))",
    );
}

#[test]
fn path_function_edges() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH p = (a)-[*]->(b) COLUMNS (edges(p) AS rels))",
    );
}

#[test]
fn path_function_path_length() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH p = (a)-[*]->(b) COLUMNS (path_length(p) AS len))",
    );
}

#[test]
fn path_function_is_source_of() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]->(b) WHERE is_source_of(a, e) COLUMNS (a.id))",
    );
}

#[test]
fn path_function_is_destination_of() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH (a)-[e]->(b) WHERE is_destination_of(b, e) COLUMNS (b.id))"
    );
}
