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

//! Tests for DO blocks (anonymous code blocks) compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-do.html>
//!
//! # PostgreSQL DO Statement
//!
//! The DO statement executes an anonymous code block (procedure) without creating
//! a persistent database object. It's useful for:
//!
//! - One-time administrative tasks
//! - Testing PL/pgSQL code snippets
//! - Database migration scripts
//! - Dynamic operations that don't warrant a permanent function
//!
//! ## Syntax
//!
//! ```sql
//! DO [ LANGUAGE lang_name ] code
//! ```
//!
//! The code is a string literal containing the procedural code to execute.
//! Dollar-quoting ($$) is commonly used for code containing single quotes.
//!
//! ## Key Features
//!
//! 1. **Anonymous execution** - No function/procedure is created
//! 2. **No return value** - DO blocks cannot return values
//! 3. **Language specification** - Defaults to plpgsql if not specified
//! 4. **Full PL/pgSQL support** - DECLARE, control flow, exception handling
//! 5. **Transaction control** - Can include COMMIT/ROLLBACK in procedures
//!
//! ## Common Use Cases
//!
//! ```sql
//! -- Simple notification
//! DO $$ BEGIN RAISE NOTICE 'Hello, World!'; END $$;
//!
//! -- Administrative task
//! DO $$
//! DECLARE
//!     r RECORD;
//! BEGIN
//!     FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public'
//!     LOOP
//!         EXECUTE 'GRANT SELECT ON ' || quote_ident(r.tablename) || ' TO readonly';
//!     END LOOP;
//! END $$;
//!
//! -- Database migration
//! DO LANGUAGE plpgsql $$
//! BEGIN
//!     IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'status_enum') THEN
//!         CREATE TYPE status_enum AS ENUM ('pending', 'active', 'closed');
//!     END IF;
//! END $$;
//! ```
//!
//! ## Test Organization
//!
//! This module contains comprehensive tests organized by feature:
//!
//! - `basic` - Core DO block syntax, language variants, body content
//!
//! ## Current Implementation Status
//!
//! **NOT IMPLEMENTED** - All tests currently expect parse errors.
//!
//! The AST does not have a `Do` or `DoBlock` variant in the Statement enum.
//! When DO block support is added, these tests should be converted from
//! `pg_expect_parse_error!` to `pg_test!` with proper AST validation.
//!
//! ## Implementation Requirements
//!
//! To support DO blocks, the following is needed:
//!
//! 1. **AST Node**: Add `Statement::Do` variant with fields:
//!    - `language: Option<Ident>` - The procedural language (defaults to plpgsql)
//!    - `body: ConditionalStatements` - The code block to execute
//!    - `span: Option<Span>` - Source location information
//!
//! 2. **Parser**: Add `parse_do()` function to handle:
//!    - DO keyword detection
//!    - Optional LANGUAGE clause (before or after body)
//!    - Dollar-quoted string or regular string literal for body
//!    - Full PL/pgSQL statement parsing in body
//!
//! 3. **Display**: Implement `Display` for round-trip SQL generation
//!
//! 4. **Dialect**: Ensure PostgreSqlDialect enables DO parsing

mod basic;
