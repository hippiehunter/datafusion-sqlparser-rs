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

//! Tests for PostgreSQL-specific syntax extensions
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-syntax.html>
//!
//! This module contains comprehensive tests for PostgreSQL syntax that differs
//! from standard SQL, organized by feature category:
//!
//! - `data_types` - PostgreSQL-specific data types (SERIAL, TEXT, CIDR, INET, etc.)
//! - `expressions` - ::cast syntax, array subscripts, regex operators
//! - `json` - JSON/JSONB operators and functions
//! - `arrays` - ARRAY constructor, array slicing, array operators
//! - `ranges` - Range types and range operators

mod arrays;
mod data_types;
mod expressions;
mod json;
mod ranges;
