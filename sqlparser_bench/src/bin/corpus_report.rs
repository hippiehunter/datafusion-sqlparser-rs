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

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser_bench::postgres_corpus::{postgres_corpus, Family, Tier};

fn main() {
    let corpus = postgres_corpus();
    let dialect = PostgreSqlDialect {};
    let mut failures = 0;

    println!("PostgreSQL benchmark corpus");
    println!("  queries:   {}", corpus.len());
    println!(
        "  SQL bytes: {}",
        corpus.iter().map(|case| case.sql.len()).sum::<usize>()
    );
    println!(
        "  sentinels: {}",
        corpus.iter().filter(|case| case.sentinel).count()
    );
    println!();
    println!("By family:");
    for family in Family::ALL {
        let cases = corpus.iter().filter(|case| case.family == family);
        let count = cases.clone().count();
        let bytes = cases.map(|case| case.sql.len()).sum::<usize>();
        println!(
            "  {:<18} {:>3} queries  {:>7} bytes",
            family.as_str(),
            count,
            bytes
        );
    }
    println!();
    println!("By tier:");
    for tier in Tier::ALL {
        let cases = corpus.iter().filter(|case| case.tier == tier);
        let count = cases.clone().count();
        let bytes = cases.map(|case| case.sql.len()).sum::<usize>();
        println!(
            "  {:<18} {:>3} queries  {:>7} bytes",
            tier.as_str(),
            count,
            bytes
        );
    }

    for case in &corpus {
        if let Err(error) = Parser::parse_sql(&dialect, &case.sql) {
            failures += 1;
            eprintln!("\nFAILED {}: {error}\n{}", case.id, case.sql);
        }
    }
    if failures > 0 {
        panic!("{failures} corpus queries did not parse");
    }
    println!("\nAll queries parse with PostgreSqlDialect.");
}
