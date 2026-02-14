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

//! Tests for SQL/PSM procedural language constructs compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql.html>
//!
//! # SQL/PSM - SQL Persistent Stored Modules
//!
//! SQL/PSM (ISO/IEC 9075-4) is the SQL standard for procedural extensions. This module
//! tests constructs used by both the SQL standard and PL/pgSQL (PostgreSQL's implementation):
//!
//! - Variables and constants
//! - Control structures (IF, CASE, LOOP, WHILE, FOR)
//! - Exception handling
//! - Cursors
//! - Dynamic SQL execution
//!
//! ## Test Organization
//!
//! This module contains comprehensive tests organized by feature category:
//!
//! - `declarations` - DECLARE blocks, variable types, constants, defaults
//! - `control_flow` - IF, CASE, LOOP, WHILE, FOR, FOREACH, EXIT, CONTINUE
//! - `raise` - RAISE statements (NOTICE, WARNING, EXCEPTION, INFO, LOG, DEBUG)
//! - `exception` - EXCEPTION WHEN handlers, GET DIAGNOSTICS, SQLSTATE
//! - `return_variants` - RETURN, RETURN NEXT, RETURN QUERY, RETURN QUERY EXECUTE
//! - `cursors` - DECLARE CURSOR, OPEN, FETCH, CLOSE, cursor variables
//! - `dynamic_sql` - EXECUTE, EXECUTE...INTO, EXECUTE...USING
//!
//! ## Current Implementation Status
//!
//! **NOT IMPLEMENTED** - Most tests currently expect parse errors.
//!
//! SQL/PSM function bodies are currently stored as raw strings (dollar-quoted literals)
//! without parsing the procedural code inside. Full SQL/PSM support requires:
//!
//! 1. **Full procedural AST** - Parse SQL/PSM blocks into AST nodes
//! 2. **Statement types** - Support all SQL/PSM statement types (not just SQL)
//! 3. **Expression extensions** - Handle procedural-specific expressions
//! 4. **Variable scoping** - Track variable declarations and usage
//!
//! ## Running Tests
//!
//! ```bash
//! # Run all SQL/PSM tests
//! cargo test postgres_compat::sql_psm
//!
//! # Run specific category
//! cargo test postgres_compat::sql_psm::declarations
//! cargo test postgres_compat::sql_psm::control_flow
//! cargo test postgres_compat::sql_psm::raise
//!
//! # See detailed output for failing tests
//! cargo test postgres_compat::sql_psm -- --nocapture
//! ```
//!
//! ## Reference Documentation
//!
//! - PL/pgSQL Structure: <https://www.postgresql.org/docs/current/plpgsql-structure.html>
//! - Declarations: <https://www.postgresql.org/docs/current/plpgsql-declarations.html>
//! - Control Structures: <https://www.postgresql.org/docs/current/plpgsql-control-structures.html>
//! - Cursors: <https://www.postgresql.org/docs/current/plpgsql-cursors.html>
//! - Errors and Messages: <https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html>

mod control_flow;
mod cursors;
mod declarations;
mod dynamic_sql;
mod exception;
mod raise;
mod return_variants;
