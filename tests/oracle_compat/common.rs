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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;

use sqlparser::ast::Statement;
use sqlparser::dialect::OracleDialect;
use sqlparser::parser::{Parser, ParserError};

#[derive(Debug, Clone, Copy)]
pub struct OracleCase {
    pub id: &'static str,
    pub feature: &'static str,
    pub sql: &'static str,
}

pub fn parse_oracle(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(&OracleDialect {}, sql)
}

pub fn assert_all_parse(cases: &[OracleCase]) {
    let mut failures = Vec::new();

    for case in cases {
        match parse_oracle(case.sql) {
            Ok(statements) if statements.is_empty() => {
                failures.push(format!(
                    "{} [{}]: parser returned no AST",
                    case.id, case.feature
                ));
            }
            Ok(statements) => {
                let rendered = statements
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ");
                match parse_oracle(&rendered) {
                    Ok(reparsed) if reparsed != statements => failures.push(format!(
                        "{} [{}]: formatting changed the AST\n  rendered: {rendered}",
                        case.id, case.feature
                    )),
                    Ok(_) => {}
                    Err(error) => {
                        failures.push(format!(
                            "{} [{}]: AST does not reparse: {error}\n  rendered: {rendered}",
                            case.id, case.feature
                        ));
                    }
                }
            }
            Err(error) => failures.push(format!(
                "{} [{}]: {error}\n  SQL: {}",
                case.id,
                case.feature,
                case.sql.replace('\n', " ")
            )),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} Oracle compatibility cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures
            .iter()
            .take(cases.len())
            .cloned()
            .collect::<Vec<_>>()
            .join("\n\n")
    );
}

pub fn assert_unique_case_ids(cases: &[OracleCase]) {
    let mut seen = BTreeSet::new();
    let duplicates = cases
        .iter()
        .filter_map(|case| (!seen.insert(case.id)).then_some(case.id))
        .collect::<Vec<_>>();
    assert!(
        duplicates.is_empty(),
        "duplicate Oracle case IDs: {duplicates:?}"
    );
}

pub fn covered_features(cases: &[OracleCase]) -> BTreeSet<&'static str> {
    cases.iter().map(|case| case.feature).collect()
}
