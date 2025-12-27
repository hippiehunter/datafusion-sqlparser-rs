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

//! Tests for PL/pgSQL language constructs compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql.html>
//!
//! # PL/pgSQL - PostgreSQL Procedural Language
//!
//! PL/pgSQL is PostgreSQL's native procedural language for writing functions, procedures,
//! and triggers. It extends SQL with:
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
//! PL/pgSQL function bodies are currently stored as raw strings (dollar-quoted literals)
//! without parsing the procedural code inside. True PL/pgSQL support requires:
//!
//! 1. **Full procedural AST** - Parse PL/pgSQL blocks into AST nodes
//! 2. **Statement types** - Support all PL/pgSQL statement types (not just SQL)
//! 3. **Expression extensions** - Handle PL/pgSQL-specific expressions
//! 4. **Variable scoping** - Track variable declarations and usage
//!
//! ## Running Tests
//!
//! ```bash
//! # Run all PL/pgSQL tests
//! cargo test postgres_compat::plpgsql
//!
//! # Run specific category
//! cargo test postgres_compat::plpgsql::declarations
//! cargo test postgres_compat::plpgsql::control_flow
//! cargo test postgres_compat::plpgsql::raise
//!
//! # See detailed output for failing tests
//! cargo test postgres_compat::plpgsql -- --nocapture
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
