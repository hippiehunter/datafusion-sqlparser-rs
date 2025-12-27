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

//! Tests for ALTER FUNCTION and ALTER PROCEDURE compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-alterfunction.html>
//! Reference: <https://www.postgresql.org/docs/current/sql-alterprocedure.html>
//!
//! This module contains comprehensive tests for PostgreSQL ALTER FUNCTION and
//! ALTER PROCEDURE syntax, organized by feature category:
//!
//! - `function` - ALTER FUNCTION statements
//! - `procedure` - ALTER PROCEDURE statements
//!
//! ## Living Gap Analysis
//!
//! ALTER FUNCTION/PROCEDURE statements are NOT currently supported in the AST.
//! All tests in this module are expected to FAIL until implementation is complete.
//!
//! When ALTER statements are implemented, these tests should be updated from
//! `pg_expect_parse_error!` to `pg_test!` with full AST validation.

mod function;
mod procedure;
