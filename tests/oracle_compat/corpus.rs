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

use std::collections::BTreeSet;

use sqlparser::dialect::OracleDialect;
use sqlparser::oracle_compat::{
    positive_cases, NEGATIVE_CASES, PLSQL_CASES, RELATIONAL_CASES, STATEMENT_CASES,
};
use sqlparser::parser::Parser;

use super::common::{assert_all_parse, assert_unique_case_ids};

#[test]
fn positive_case_ids_are_unique_and_complete() {
    let cases = positive_cases().copied().collect::<Vec<_>>();
    assert_unique_case_ids(&cases);
}

#[test]
fn oracle_positive_frontier_parses() {
    assert_all_parse(RELATIONAL_CASES);
    assert_all_parse(PLSQL_CASES);
    assert_all_parse(STATEMENT_CASES);
}

#[test]
fn oracle_rejects_invalid_grammar_cases() {
    let failures = NEGATIVE_CASES
        .iter()
        .filter(|case| Parser::parse_sql(&OracleDialect {}, case.sql).is_ok())
        .map(|case| format!("{}: {}", case.id, case.sql))
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "Oracle parser accepted {} invalid cases:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn negative_case_ids_are_unique_and_complete() {
    let ids = NEGATIVE_CASES
        .iter()
        .map(|case| case.id)
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), NEGATIVE_CASES.len());
}
