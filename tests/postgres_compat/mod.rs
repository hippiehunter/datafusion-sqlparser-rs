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

//! PostgreSQL Compatibility Test Suite - Module Organization
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
//! 3. **Full AST for procedure bodies** - SQL/PSM must produce real AST nodes, not strings
//!
//! # Test Organization
//!
//! Tests are organized by PostgreSQL feature category:
//!
//! ## Routines (Functions and Procedures)
//!
//! - `create_function` - CREATE FUNCTION with all PostgreSQL options
//!   - Function arguments (IN, OUT, INOUT, VARIADIC)
//!   - Return types (RETURNS, RETURNS TABLE, RETURNS SETOF)
//!   - Function bodies (SQL, SQL/PSM, PL/pgSQL, external languages)
//!   - Function attributes (VOLATILITY, PARALLEL SAFETY, SECURITY, etc.)
//!   - Overloading and polymorphic functions
//!
//! - `create_procedure` - CREATE PROCEDURE with all PostgreSQL options
//!   - Procedure arguments (IN, OUT, INOUT)
//!   - Procedure bodies (SQL, SQL/PSM, PL/pgSQL)
//!   - Transaction control in procedures
//!
//! - `alter_routine` - ALTER FUNCTION and ALTER PROCEDURE
//!   - Renaming functions/procedures
//!   - Changing ownership and schema
//!   - Modifying function attributes
//!   - Extension dependency management
//!
//! - `drop_routine` - DROP FUNCTION and DROP PROCEDURE
//!   - Drop with argument signatures
//!   - Drop with CASCADE/RESTRICT
//!   - Drop multiple routines
//!
//! ## Anonymous Code Blocks
//!
//! - `do_blocks` - DO statement for anonymous code blocks
//!   - DO with SQL/PSM / PL/pgSQL
//!   - DO with other procedural languages
//!   - Variable declarations and control flow
//!
//! ## SQL/PSM Language Constructs
//!
//! - `sql_psm` - SQL/PSM procedural syntax (including PL/pgSQL)
//!   - DECLARE blocks and variable declarations
//!   - Control structures (IF, CASE, LOOP, WHILE, FOR)
//!   - RAISE statements for errors and notices
//!   - EXCEPTION handling blocks
//!   - RETURN, RETURN NEXT, RETURN QUERY
//!   - Cursor operations
//!   - Dynamic SQL (EXECUTE)
//!
//! ## Triggers and Event Triggers
//!
//! - `triggers` - CREATE TRIGGER and CREATE EVENT TRIGGER
//!   - Row-level and statement-level triggers
//!   - BEFORE, AFTER, INSTEAD OF triggers
//!   - Trigger timing (FOR EACH ROW, FOR EACH STATEMENT)
//!   - Trigger conditions (WHEN clause)
//!   - Event triggers (DDL events)
//!   - Trigger functions and NEW/OLD references
//!
//! ## Aggregates and Window Functions
//!
//! - `aggregates` - CREATE AGGREGATE
//!   - State transition functions
//!   - Final functions
//!   - Combine functions (for parallel aggregation)
//!   - Moving-aggregate mode
//!   - Ordered-set aggregates
//!   - Hypothetical-set aggregates
//!
//! ## Operators and Operator Classes
//!
//! - `operators` - CREATE OPERATOR and operator classes
//!   - Binary and unary operators
//!   - Operator precedence
//!   - Commutator and negator operators
//!   - Index optimization (RESTRICT, JOIN selectivity)
//!   - Operator families and operator classes
//!
//! ## PostgreSQL-Specific Syntax
//!
//! - `syntax` - PostgreSQL-specific syntax extensions
//!   - Type casts (::type)
//!   - Array constructors and operations
//!   - Range types and operations
//!   - JSON/JSONB operators (->, ->>, #>, etc.)
//!   - String constants with escape sequences (E'...')
//!   - Dollar-quoted strings ($$...$$)
//!   - Row constructors (ROW(...))
//!   - LATERAL joins
//!   - WITH ORDINALITY
//!
//! ## Metadata and Comments
//!
//! - `metadata` - COMMENT ON and SECURITY LABEL
//!   - COMMENT ON for all object types
//!   - SECURITY LABEL for row-level security
//!   - System catalog queries
//!
//! # Running Tests
//!
//! ```bash
//! # Run all PostgreSQL compatibility tests
//! cargo test postgres_compat
//!
//! # Run specific category
//! cargo test postgres_compat::create_function
//! cargo test postgres_compat::sql_psm
//! cargo test postgres_compat::triggers
//!
//! # See detailed output for failing tests
//! cargo test postgres_compat -- --nocapture
//!
//! # Run a specific test
//! cargo test postgres_compat::create_function::test_basic_function
//! ```
//!
//! # Reference Documentation
//!
//! All tests reference official PostgreSQL documentation:
//! - SQL Commands: <https://www.postgresql.org/docs/current/sql-commands.html>
//! - SQL/PSM / PL/pgSQL: <https://www.postgresql.org/docs/current/plpgsql.html>
//! - Server Programming: <https://www.postgresql.org/docs/current/server-programming.html>
//!
//! # Contributing
//!
//! When adding new tests:
//!
//! 1. Use the `pg_test!` macro from `common.rs` for tests with AST validation
//! 2. Use the `pg_roundtrip_only!` macro for tests that only verify parsing
//! 3. Include a comment with the PostgreSQL documentation URL
//! 4. Add TODO comments for features that need AST validation
//! 5. Organize tests by feature and complexity (basic → advanced)
//!
//! # Example Test
//!
//! ```rust,ignore
//! use crate::postgres_compat::common::*;
//!
//! #[test]
//! fn test_create_function_basic() {
//!     // https://www.postgresql.org/docs/current/sql-createfunction.html
//!     pg_test!(
//!         "CREATE FUNCTION add(a int, b int) RETURNS int AS $$ SELECT a + b $$ LANGUAGE SQL",
//!         |stmt: Statement| {
//!             let cf = extract_create_function(&stmt);
//!             assert_eq!(cf.name.to_string(), "add");
//!             assert_eq!(cf.args.as_ref().unwrap().len(), 2);
//!             assert!(cf.return_type.is_some());
//!         }
//!     );
//! }
//! ```

// Make macros available to all submodules
#[macro_use]
pub mod common;

// Routines (Functions and Procedures)
#[path = "alter_routine/mod.rs"]
pub mod alter_routine;
pub mod create_function;
pub mod create_procedure;
#[path = "drop_routine/mod.rs"]
pub mod drop_routine;

// Anonymous Code Blocks
#[path = "do_blocks/mod.rs"]
pub mod do_blocks;

// SQL/PSM Language Constructs
#[path = "sql_psm/mod.rs"]
pub mod sql_psm;

// Triggers and Event Triggers
pub mod triggers;

// Aggregates and Window Functions
pub mod aggregates;

// Operators and Operator Classes
pub mod operators;

// PostgreSQL-Specific Syntax
pub mod syntax;

// Metadata and Comments
pub mod metadata;
