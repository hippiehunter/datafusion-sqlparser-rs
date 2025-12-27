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

//! Tests for DROP FUNCTION and DROP PROCEDURE compatibility
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-dropfunction.html>
//! Reference: <https://www.postgresql.org/docs/current/sql-dropprocedure.html>
//!
//! This module contains comprehensive tests for PostgreSQL DROP FUNCTION and
//! DROP PROCEDURE syntax, organized by feature category:
//!
//! - `function` - DROP FUNCTION statements
//! - `procedure` - DROP PROCEDURE statements
//!
//! ## AST Support Status
//!
//! DROP FUNCTION and DROP PROCEDURE have basic AST support via:
//! - `Statement::DropFunction(DropFunction)`
//! - `Statement::DropProcedure { ... }`
//!
//! Most tests should PASS as the basic structure exists. Tests verify:
//! - Function/procedure names with argument signatures
//! - IF EXISTS clause
//! - CASCADE/RESTRICT options
//! - Multiple functions/procedures in one statement

mod function;
mod procedure;
