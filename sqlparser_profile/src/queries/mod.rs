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

//! Query collections for profiling
//!
//! This module provides realistic SQL queries for profiling the parser.

pub mod enterprise;
pub mod tpch;

pub use enterprise::ENTERPRISE_DASHBOARD;
pub use tpch::TPCH_QUERIES;

/// Query set selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuerySet {
    Enterprise,
    Tpch,
    All,
}

impl QuerySet {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "enterprise" => Some(Self::Enterprise),
            "tpch" => Some(Self::Tpch),
            "all" => Some(Self::All),
            _ => None,
        }
    }
}

/// Returns all queries for the given query set
pub fn get_queries(set: QuerySet) -> Vec<(&'static str, &'static str)> {
    match set {
        QuerySet::Enterprise => vec![("enterprise_dashboard", ENTERPRISE_DASHBOARD)],
        QuerySet::Tpch => TPCH_QUERIES.to_vec(),
        QuerySet::All => {
            let mut queries = vec![("enterprise_dashboard", ENTERPRISE_DASHBOARD)];
            queries.extend_from_slice(TPCH_QUERIES);
            queries
        }
    }
}
