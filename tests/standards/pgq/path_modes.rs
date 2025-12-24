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

//! SQL/PGQ Path Mode Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn path_mode_walk() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH WALK (a)-[*]->(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn path_mode_trail() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH TRAIL (a)-[*]->(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn path_mode_acyclic() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ACYCLIC (a)-[*]->(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn path_mode_simple() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH SIMPLE (a)-[*]->(b) COLUMNS (a.id, b.id))"
    );
}

#[test]
fn path_mode_with_shortest() {
    verified_standard_stmt(
        "SELECT * FROM GRAPH_TABLE (g MATCH ANY SHORTEST ACYCLIC (a)-[*]->(b) COLUMNS (a.id, b.id))"
    );
}
