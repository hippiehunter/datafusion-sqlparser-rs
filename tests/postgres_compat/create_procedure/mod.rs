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
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! CREATE PROCEDURE PostgreSQL Compatibility Tests
//!
//! This module contains comprehensive tests for CREATE PROCEDURE statement
//! to verify PostgreSQL compatibility and document feature gaps.
//!
//! # PostgreSQL CREATE PROCEDURE Overview
//!
//! CREATE PROCEDURE creates a new procedure. Unlike functions, procedures:
//! - Do NOT have a return value (no RETURNS clause)
//! - CAN manage transactions (COMMIT/ROLLBACK) - PostgreSQL 11+
//! - Are invoked with CALL, not in expressions
//! - Support IN, OUT, and INOUT parameters
//!
//! ## PostgreSQL Versions
//!
//! - PostgreSQL 11: Added CREATE PROCEDURE and transaction control
//! - PostgreSQL 11: Added CREATE OR REPLACE PROCEDURE
//! - PostgreSQL 14: Added BEGIN ATOMIC for SQL-standard syntax
//!
//! ## Key Features Tested
//!
//! ### Basic Syntax (`basic.rs`)
//! - CREATE PROCEDURE name() AS body
//! - CREATE OR REPLACE PROCEDURE (gap: AST has or_alter, not or_replace)
//! - Schema-qualified names
//! - LANGUAGE clause (SQL, plpgsql, C, etc.)
//! - Dollar-quoted bodies ($$ ... $$)
//! - BEGIN ATOMIC (PostgreSQL 14+, likely not supported)
//!
//! ### Parameters (`parameters.rs`)
//! - IN, OUT, INOUT parameter modes
//! - Default values for parameters
//! - Multiple parameters with mixed modes
//! - Various data types (INTEGER, TEXT, TIMESTAMP, arrays, etc.)
//! - VARIADIC parameters (likely not supported)
//!
//! ### Options and Attributes (`options.rs`)
//! - LANGUAGE SQL / plpgsql / C / plpython / etc.
//! - SECURITY DEFINER / SECURITY INVOKER (not in AST, will fail)
//! - SET configuration_parameter (not in AST, will fail)
//! - Combined options
//!
//! ### Transaction Control (`transaction.rs`)
//! - COMMIT within procedure body
//! - ROLLBACK within procedure body
//! - SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO
//! - Transaction control in loops and exception handlers
//! - COMMIT/ROLLBACK AND CHAIN
//!
//! ## Current AST Structure
//!
//! ```rust,ignore
//! Statement::CreateProcedure {
//!     create_token: AttachedToken,
//!     or_alter: bool,                      // Note: or_alter, not or_replace
//!     name: ObjectName,
//!     params: Option<Vec<ProcedureParam>>,
//!     language: Option<Ident>,
//!     has_as: bool,
//!     body: ConditionalStatements,
//! }
//!
//! pub struct ProcedureParam {
//!     pub name: Ident,
//!     pub data_type: DataType,
//!     pub mode: Option<ArgMode>,  // In, Out, InOut
//!     pub default: Option<Expr>,
//! }
//! ```
//!
//! ## Known Gaps
//!
//! The AST is missing several PostgreSQL CREATE PROCEDURE features:
//!
//! 1. **or_replace**: AST has `or_alter` (SQL Server) but not `or_replace` (PostgreSQL)
//! 2. **Security options**: SECURITY DEFINER / SECURITY INVOKER not in AST
//! 3. **SET options**: SET configuration_parameter not in AST
//! 4. **TRANSFORM**: Type transformation for non-SQL languages not in AST
//! 5. **VARIADIC**: Variable-argument parameters likely not supported
//! 6. **BEGIN ATOMIC**: PostgreSQL 14+ SQL-standard syntax not supported
//!
//! ## Test Strategy
//!
//! This is a **living gap analysis** test suite:
//!
//! - Tests that SHOULD work (per PostgreSQL docs) use `pg_test!` macro
//! - Tests that LIKELY FAIL use `pg_expect_parse_error!` macro
//! - All tests document what the correct behavior should be
//! - When features are implemented, failing tests will start passing
//! - Body parsing should produce AST nodes, not raw strings
//!
//! ## Running Tests
//!
//! ```bash
//! # Run all CREATE PROCEDURE tests
//! cargo test postgres_compat::create_procedure
//!
//! # Run specific module
//! cargo test postgres_compat::create_procedure::basic
//! cargo test postgres_compat::create_procedure::parameters
//! cargo test postgres_compat::create_procedure::options
//! cargo test postgres_compat::create_procedure::transaction
//!
//! # Run specific test
//! cargo test postgres_compat::create_procedure::basic::test_create_procedure_minimal
//!
//! # See detailed output
//! cargo test postgres_compat::create_procedure -- --nocapture
//! ```
//!
//! ## References
//!
//! - [CREATE PROCEDURE](https://www.postgresql.org/docs/current/sql-createprocedure.html)
//! - [Transaction Management in Procedures](https://www.postgresql.org/docs/current/xproc.html)
//! - [PL/pgSQL](https://www.postgresql.org/docs/current/plpgsql.html)
//! - [Procedural Languages](https://www.postgresql.org/docs/current/server-programming.html)

pub mod basic;
pub mod options;
pub mod parameters;
pub mod transaction;
