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

//! SQL Parser Profiling Binary
//!
//! A standalone binary for profiling sqlparser memory allocations and CPU usage.
//!
//! # Usage
//!
//! ## DHAT heap profiling
//! ```bash
//! cargo build --release --features dhat-heap
//! ./target/release/sqlparser_profile --single --query all
//! ```
//!
//! ## Flamegraph
//! ```bash
//! cargo flamegraph --release -- --iterations 1000 --mode parse
//! ```
//!
//! ## Samply
//! ```bash
//! cargo build --release
//! samply record ./target/release/sqlparser_profile --iterations 1000
//! ```

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use clap::Parser as ClapParser;
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

mod queries;

use queries::{get_queries, QuerySet};

#[derive(ClapParser)]
#[command(name = "sqlparser_profile")]
#[command(about = "Profile sqlparser memory and CPU usage")]
struct Args {
    /// Number of iterations (for CPU profiling)
    #[arg(short, long, default_value = "100")]
    iterations: usize,

    /// Mode: "parse", "tokenize", or "both"
    #[arg(short, long, default_value = "parse")]
    mode: String,

    /// Query set: "enterprise", "tpch", or "all"
    #[arg(short, long, default_value = "all")]
    query: String,

    /// Dialect: "generic", "postgres", or "mysql"
    #[arg(short, long, default_value = "generic")]
    dialect: String,

    /// Single iteration mode (for DHAT/heap profiling)
    #[arg(long)]
    single: bool,

    /// Print timing information
    #[arg(long)]
    timing: bool,
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let args = Args::parse();

    let dialect: Box<dyn Dialect> = match args.dialect.to_lowercase().as_str() {
        "postgres" | "postgresql" => Box::new(PostgreSqlDialect {}),
        "mysql" => Box::new(MySqlDialect {}),
        _ => Box::new(GenericDialect {}),
    };

    let query_set = QuerySet::from_str(&args.query).unwrap_or_else(|| {
        eprintln!(
            "Unknown query set '{}', using 'all'. Options: enterprise, tpch, all",
            args.query
        );
        QuerySet::All
    });

    let queries = get_queries(query_set);
    let iterations = if args.single { 1 } else { args.iterations };

    eprintln!("Profiling {} queries, {} iterations", queries.len(), iterations);
    eprintln!("Mode: {}, Dialect: {}", args.mode, args.dialect);

    let start = std::time::Instant::now();

    for i in 0..iterations {
        if args.timing && i > 0 && i % 100 == 0 {
            eprintln!("  Iteration {}/{}", i, iterations);
        }

        for (name, sql) in &queries {
            match args.mode.as_str() {
                "tokenize" => {
                    profile_tokenize(&*dialect, sql, *name);
                }
                "parse" => {
                    profile_parse(&*dialect, sql, *name);
                }
                "both" => {
                    profile_tokenize(&*dialect, sql, *name);
                    profile_parse(&*dialect, sql, *name);
                }
                _ => {
                    eprintln!("Unknown mode '{}', using 'parse'", args.mode);
                    profile_parse(&*dialect, sql, *name);
                }
            }
        }
    }

    let elapsed = start.elapsed();

    if args.timing {
        eprintln!("\nCompleted {} iterations in {:?}", iterations, elapsed);
        eprintln!(
            "Average per iteration: {:?}",
            elapsed / iterations as u32
        );
        let total_queries = queries.len() * iterations;
        eprintln!(
            "Total queries parsed: {}, avg per query: {:?}",
            total_queries,
            elapsed / total_queries as u32
        );
    }

    eprintln!("Done.");
}

/// Profile tokenization - kept as separate function for clear flamegraph boundaries
#[inline(never)]
fn profile_tokenize(dialect: &dyn Dialect, sql: &str, _name: &str) {
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let _tokens = tokenizer.tokenize().expect("tokenization failed");
}

/// Profile parsing - kept as separate function for clear flamegraph boundaries
#[inline(never)]
fn profile_parse(dialect: &dyn Dialect, sql: &str, _name: &str) {
    let _ast = Parser::parse_sql(dialect, sql).expect("parsing failed");
}
