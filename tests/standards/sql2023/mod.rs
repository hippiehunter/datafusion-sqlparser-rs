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

//! SQL:2023 Standards Compliance Tests
//!
//! SQL:2023 (ISO/IEC 9075:2023) introduces significant new features:
//!
//! - Property Graph Queries (SQL/PGQ) - Part 16
//! - Multi-Dimensional Arrays (SQL/MDA) - Part 15
//! - JSON improvements (native type, dot notation, item methods)
//! - New functions (GREATEST, LEAST, ANY_VALUE, etc.)
//! - Numeric literal enhancements (hex, binary, underscores)
//!
//! Note: SQL/PGQ tests are in a separate `pgq` module due to their scope.

pub mod json_improvements;
pub mod mda;
pub mod new_functions;
// Temporarily commented out due to compilation errors in other test files
// These will be re-enabled once those files are fixed
// pub mod numeric_literals;
