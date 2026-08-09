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

use sqlparser::dialect::{OracleDialect, PostgreSqlDialect};
use sqlparser::oracle_compat::{
    positive_case, positive_cases, GrammarScope, GRAMMAR_OBLIGATIONS, NEGATIVE_CASES,
};
use sqlparser::parser::Parser;

#[test]
fn grammar_obligations_reference_existing_cases() {
    let positives = positive_cases()
        .map(|case| case.id)
        .collect::<BTreeSet<_>>();
    let negatives = NEGATIVE_CASES
        .iter()
        .map(|case| case.id)
        .collect::<BTreeSet<_>>();
    let mut productions = BTreeSet::new();
    let mut isolation_cases = BTreeSet::new();

    for obligation in GRAMMAR_OBLIGATIONS {
        assert!(
            productions.insert(obligation.production),
            "duplicate grammar obligation: {}",
            obligation.production
        );
        assert!(
            !obligation.positive_cases.is_empty(),
            "{} has no positive cases",
            obligation.production
        );
        assert!(
            !obligation.negative_cases.is_empty(),
            "{} has no negative cases",
            obligation.production
        );
        for case in obligation.positive_cases {
            assert!(
                positives.contains(case),
                "{} references missing positive case {case}",
                obligation.production
            );
        }
        for case in obligation.negative_cases {
            assert!(
                negatives.contains(case),
                "{} references missing negative case {case}",
                obligation.production
            );
        }
        if let GrammarScope::OracleSpecific { isolation_case } = obligation.scope {
            assert!(
                obligation.positive_cases.contains(&isolation_case),
                "{} isolation case {isolation_case} is not one of its positive cases",
                obligation.production
            );
            assert!(
                isolation_cases.insert(isolation_case),
                "isolation case {isolation_case} is reused by multiple grammar obligations"
            );
        }
    }
}

#[test]
fn oracle_specific_grammar_is_rejected_by_postgres() {
    let mut failures = vec![];

    for obligation in GRAMMAR_OBLIGATIONS {
        let GrammarScope::OracleSpecific { isolation_case } = obligation.scope else {
            continue;
        };
        let case = positive_case(isolation_case).unwrap_or_else(|| {
            panic!(
                "{} references missing isolation case {isolation_case}",
                obligation.production
            )
        });

        if let Err(error) = Parser::parse_sql(&OracleDialect {}, case.sql) {
            failures.push(format!(
                "{} ({isolation_case}) failed under Oracle: {error}",
                obligation.production
            ));
            continue;
        }
        if Parser::parse_sql(&PostgreSqlDialect {}, case.sql).is_ok() {
            failures.push(format!(
                "{} ({isolation_case}) was accepted by PostgreSQL:\n  {}",
                obligation.production, case.sql
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} Oracle grammar isolation failures:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
