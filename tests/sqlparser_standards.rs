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
//! This module contains comprehensive tests organized by SQL standard version
//! and feature category, serving as a living gap analysis for standards compliance.
//!
//! # Coverage
//!
//! - SQL:2016 (ISO/IEC 9075:2016) - Foundation, PSM, XML, Arrays
//! - SQL:2019 (ISO/IEC 9075:2019) - Refinements
//! - SQL:2023 (ISO/IEC 9075:2023) - JSON, new functions, numeric literals
//! - SQL/PGQ (ISO/IEC 9075-16) - Property Graph Queries
//!
//! # Test Patterns
//!
//! - **Implemented features**: Use `verified_standard_stmt()` with AST validation
//! - **Parses but needs AST validation**: Use `verified_roundtrip_only!` with TODO
//! - **Not yet implemented**: Use `expect_not_yet_implemented!` (tests FAIL until implemented)
//!
//! # Running Tests
//!
//! ```bash
//! # Run all standards compliance tests
//! cargo test standards
//!
//! # Run SQL:2016 tests only
//! cargo test standards::sql2016
//!
//! # Run SQL/PGQ tests
//! cargo test standards::pgq
//! ```

#[macro_use]
mod test_utils;

mod standards;
