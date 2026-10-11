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

use sqlparser::ast::{SetExpr, Statement, TableFactor};

use super::common::parse_oracle;

/// The tables of the first FROM item's join list, each as the alias it was
/// given and whether it carries a sampling clause.
fn tables(sql: &str) -> Vec<(Option<String>, bool)> {
    let statements = parse_oracle(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    let [Statement::Query(query)] = statements.as_slice() else {
        panic!("{sql}: not one query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("{sql}: not a plain select");
    };
    select
        .from
        .iter()
        .flat_map(|item| {
            std::iter::once(&item.relation).chain(item.joins.iter().map(|join| &join.relation))
        })
        .map(|relation| {
            let TableFactor::Table { alias, sample, .. } = relation else {
                panic!("{sql}: not a table");
            };
            (
                alias.as_ref().map(|alias| alias.name.value.clone()),
                sample.is_some(),
            )
        })
        .collect()
}

#[test]
fn sample_names_a_table_unless_a_sampling_clause_follows() {
    assert_eq!(
        tables("SELECT 1 FROM t SAMPLE WHERE SAMPLE.id = 1"),
        [(Some("SAMPLE".to_string()), false)]
    );
    assert_eq!(
        tables("SELECT 1 FROM a, t SAMPLE, b WHERE SAMPLE.id = b.id"),
        [
            (None, false),
            (Some("SAMPLE".to_string()), false),
            (None, false)
        ]
    );
    assert_eq!(
        tables("SELECT 1 FROM a JOIN t SAMPLE ON SAMPLE.id = a.id"),
        [(None, false), (Some("SAMPLE".to_string()), false)]
    );
    assert_eq!(tables("SELECT 1 FROM t SAMPLE (10)"), [(None, true)]);
    assert_eq!(tables("SELECT 1 FROM t SAMPLE BLOCK (10)"), [(None, true)]);
}
