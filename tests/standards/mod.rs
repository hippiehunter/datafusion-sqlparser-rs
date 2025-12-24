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

//! SQL Standards Compliance Test Suite
//!
//! This module contains tests organized by SQL standard version and feature category.
//! The goal is to systematically track and verify compliance with:
//!
//! - SQL:2016 (ISO/IEC 9075:2016)
//! - SQL:2019 (ISO/IEC 9075:2019)
//! - SQL:2023 (ISO/IEC 9075:2023)
//! - SQL/PGQ (ISO/IEC 9075-16, Property Graph Queries)
//!
//! ## Test Organization
//!
//! Tests are organized hierarchically:
//! 1. By standard version (sql2016/, sql2019/, sql2023/, pgq/)
//! 2. By feature category (foundation/, etc.)
//! 3. By specific feature series (e_series.rs, f_series.rs, etc.)
//!
//! ## Feature ID Convention
//!
//! Tests reference ISO/IEC 9075 feature IDs where applicable:
//! - E-series: Core SQL features (E011-E182)
//! - F-series: Optional features (F021-F869)
//! - T-series: Advanced features (T031-T670)
//! - S-series: Array support (S071-S404)
//! - X-series: XML support (X010-X400)

#[macro_use]
pub mod common;

pub mod pgq;
pub mod sql2016;
pub mod sql2019;
pub mod sql2023;
