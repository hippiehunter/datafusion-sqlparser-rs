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

//! PostgreSQL Compatibility Test Suite
//!
//! This module contains comprehensive tests for PostgreSQL compatibility,
//! serving as a **living gap analysis** for achieving 100% PostgreSQL support.
//!
//! # Design Philosophy
//!
//! Unlike typical test suites where tests pass for implemented features, this suite:
//!
//! 1. **Tests expect correct behavior** - Each test verifies both parsing AND AST shape
//! 2. **Tests fail until implemented** - Failures document the compatibility gap
//! 3. **Full AST for procedure bodies** - PL/pgSQL must produce real AST nodes, not strings
//!
//! # Coverage
//!
//! - CREATE/ALTER/DROP FUNCTION with all PostgreSQL options
//! - CREATE/ALTER/DROP PROCEDURE with all PostgreSQL options
//! - DO blocks (anonymous code blocks)
//! - PL/pgSQL constructs (RAISE, EXCEPTION, RETURN NEXT/QUERY, cursors, etc.)
//! - CREATE TRIGGER and EVENT TRIGGER
//! - CREATE AGGREGATE
//! - CREATE OPERATOR and operator classes
//! - PostgreSQL-specific syntax (::cast, JSON operators, arrays, ranges, etc.)
//! - COMMENT ON and SECURITY LABEL
//!
//! # Running Tests
//!
//! ```bash
//! # Run all PostgreSQL compatibility tests
//! cargo test postgres_compat
//!
//! # Run specific category
//! cargo test postgres_compat::create_function
//! cargo test postgres_compat::plpgsql
//!
//! # See detailed output for failing tests
//! cargo test postgres_compat -- --nocapture
//! ```
//!
//! # Reference Documentation
//!
//! All tests reference official PostgreSQL documentation:
//! - Functions: <https://www.postgresql.org/docs/current/sql-createfunction.html>
//! - Procedures: <https://www.postgresql.org/docs/current/sql-createprocedure.html>
//! - PL/pgSQL: <https://www.postgresql.org/docs/current/plpgsql.html>
//! - Triggers: <https://www.postgresql.org/docs/current/sql-createtrigger.html>

mod postgres_compat;
