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

//! Executable Oracle grammar corpus exported for downstream capability gates.
//!
//! These records are parser-owned data. Consumers classify the AST produced
//! from each positive fixture and must reject every negative fixture as syntax;
//! copying the IDs into a second hand-maintained ledger is not evidence.

mod negative;
mod obligations;
mod plsql;
mod relational;
mod statements;

pub use negative::{NegativeCase, NEGATIVE_CASES};
pub use obligations::{GrammarObligation, GrammarScope, GRAMMAR_OBLIGATIONS};
pub use plsql::PLSQL_CASES;
pub use relational::RELATIONAL_CASES;
pub use statements::STATEMENT_CASES;

/// One Oracle-positive parser fixture and its owning feature family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleCase {
    pub id: &'static str,
    pub feature: &'static str,
    pub sql: &'static str,
}

/// Iterate the complete positive corpus without allocating or duplicating it.
pub fn positive_cases() -> impl Iterator<Item = &'static OracleCase> {
    RELATIONAL_CASES
        .iter()
        .chain(PLSQL_CASES)
        .chain(STATEMENT_CASES)
}

/// Find a positive fixture by its stable capability ID.
pub fn positive_case(id: &str) -> Option<&'static OracleCase> {
    positive_cases().find(|case| case.id == id)
}
