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

//! SQL/PGQ CREATE PROPERTY GRAPH Tests (ISO/IEC 9075-16:2023)

use crate::standards::common::verified_standard_stmt;

#[test]
fn create_property_graph_basic() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH my_graph \
         VERTEX TABLES (person)"
    );
}

#[test]
fn create_property_graph_with_key() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH my_graph \
         VERTEX TABLES (person KEY (id))"
    );
}

#[test]
fn create_property_graph_with_label() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH my_graph \
         VERTEX TABLES (person LABEL Person)"
    );
}

#[test]
fn create_property_graph_with_properties() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH my_graph \
         VERTEX TABLES (person PROPERTIES (name, age))"
    );
}

#[test]
fn create_property_graph_full_vertex() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH my_graph \
         VERTEX TABLES (person KEY (id) LABEL Person PROPERTIES (name, age))"
    );
}

#[test]
fn create_property_graph_multiple_vertices() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH social_network \
         VERTEX TABLES (person KEY (id) LABEL Person, company KEY (id) LABEL Company)"
    );
}

#[test]
fn create_property_graph_with_edges() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH social_network \
         VERTEX TABLES (person KEY (id)) \
         EDGE TABLES (knows SOURCE KEY (person1_id) REFERENCES person DESTINATION KEY (person2_id) REFERENCES person)"
    );
}

#[test]
fn create_property_graph_edge_with_label() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH social_network \
         VERTEX TABLES (person) \
         EDGE TABLES (friendships SOURCE REFERENCES person DESTINATION REFERENCES person LABEL KNOWS)"
    );
}

#[test]
fn create_property_graph_edge_with_properties() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH social_network \
         VERTEX TABLES (person) \
         EDGE TABLES (friendships SOURCE REFERENCES person DESTINATION REFERENCES person PROPERTIES (since, strength))"
    );
}

#[test]
fn create_property_graph_complex() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH financial_network \
         VERTEX TABLES ( \
           accounts KEY (account_id) LABEL Account PROPERTIES (balance, type), \
           customers KEY (customer_id) LABEL Customer PROPERTIES (name, since) \
         ) \
         EDGE TABLES ( \
           transfers SOURCE KEY (from_account) REFERENCES accounts DESTINATION KEY (to_account) REFERENCES accounts LABEL TRANSFER PROPERTIES (amount, date), \
           owns SOURCE REFERENCES customers DESTINATION REFERENCES accounts LABEL OWNS \
         )"
    );
}

#[test]
fn create_or_replace_property_graph() {
    verified_standard_stmt(
        "CREATE OR REPLACE PROPERTY GRAPH my_graph \
         VERTEX TABLES (person)"
    );
}

#[test]
fn create_property_graph_if_not_exists() {
    verified_standard_stmt(
        "CREATE PROPERTY GRAPH IF NOT EXISTS my_graph \
         VERTEX TABLES (person)"
    );
}

#[test]
fn drop_property_graph() {
    verified_standard_stmt("DROP PROPERTY GRAPH my_graph");
}

#[test]
fn drop_property_graph_if_exists() {
    verified_standard_stmt("DROP PROPERTY GRAPH IF EXISTS my_graph");
}

#[test]
fn drop_property_graph_cascade() {
    verified_standard_stmt("DROP PROPERTY GRAPH my_graph CASCADE");
}

#[test]
fn drop_property_graph_restrict() {
    verified_standard_stmt("DROP PROPERTY GRAPH my_graph RESTRICT");
}
