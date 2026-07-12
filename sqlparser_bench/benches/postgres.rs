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

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::ParsedSql;
use sqlparser_bench::postgres_corpus::{postgres_corpus, Family, QueryCase, Tier};

fn bytes(cases: &[&QueryCase]) -> u64 {
    cases.iter().map(|case| case.sql.len() as u64).sum()
}

fn end_to_end(c: &mut Criterion) {
    let corpus = postgres_corpus();
    let workload = corpus
        .iter()
        .filter(|case| case.family != Family::Scaling)
        .collect::<Vec<_>>();
    let dialect = PostgreSqlDialect {};

    let mut group = c.benchmark_group("postgres/e2e/macro");
    group.throughput(Throughput::Bytes(bytes(&workload)));
    group.bench_function("workload", |b| {
        b.iter(|| {
            for case in &workload {
                black_box(Parser::parse_sql(&dialect, black_box(&case.sql)).unwrap());
            }
        });
    });

    for family in Family::ALL {
        let subset = corpus
            .iter()
            .filter(|case| case.family == family)
            .collect::<Vec<_>>();
        group.throughput(Throughput::Bytes(bytes(&subset)));
        group.bench_with_input(
            BenchmarkId::new("family", family.as_str()),
            &subset,
            |b, subset| {
                b.iter(|| {
                    for case in subset {
                        black_box(Parser::parse_sql(&dialect, black_box(&case.sql)).unwrap());
                    }
                });
            },
        );
    }

    for tier in Tier::ALL {
        let subset = workload
            .iter()
            .copied()
            .filter(|case| case.tier == tier)
            .collect::<Vec<_>>();
        group.throughput(Throughput::Bytes(bytes(&subset)));
        group.bench_with_input(
            BenchmarkId::new("tier", tier.as_str()),
            &subset,
            |b, subset| {
                b.iter(|| {
                    for case in subset {
                        black_box(Parser::parse_sql(&dialect, black_box(&case.sql)).unwrap());
                    }
                });
            },
        );
    }
    group.finish();
}

fn parser_prepare(c: &mut Criterion) {
    let corpus = postgres_corpus();
    let workload = corpus
        .iter()
        .filter(|case| case.family != Family::Scaling)
        .collect::<Vec<_>>();
    let dialect = PostgreSqlDialect {};
    let mut group = c.benchmark_group("postgres/prepare");

    group.throughput(Throughput::Bytes(bytes(&workload)));
    group.bench_function("workload", |b| {
        b.iter(|| {
            for case in &workload {
                black_box(
                    Parser::new(&dialect)
                        .try_with_sql(black_box(&case.sql))
                        .unwrap(),
                );
            }
        });
    });

    for tier in Tier::ALL {
        let subset = workload
            .iter()
            .copied()
            .filter(|case| case.tier == tier)
            .collect::<Vec<_>>();
        group.throughput(Throughput::Bytes(bytes(&subset)));
        group.bench_with_input(
            BenchmarkId::new("tier", tier.as_str()),
            &subset,
            |b, subset| {
                b.iter(|| {
                    for case in subset {
                        black_box(
                            Parser::new(&dialect)
                                .try_with_sql(black_box(&case.sql))
                                .unwrap(),
                        );
                    }
                });
            },
        );
    }
    group.finish();
}

fn document(c: &mut Criterion) {
    let corpus = postgres_corpus();
    let workload = corpus
        .iter()
        .filter(|case| case.family != Family::Scaling)
        .collect::<Vec<_>>();
    let dialect = PostgreSqlDialect {};

    let mut group = c.benchmark_group("postgres/document/macro");
    group.throughput(Throughput::Bytes(bytes(&workload)));
    group.bench_function("workload", |b| {
        b.iter(|| {
            for case in &workload {
                black_box(ParsedSql::parse(&dialect, black_box(case.sql.as_str())).unwrap());
            }
        });
    });

    for family in Family::ALL {
        let subset = corpus
            .iter()
            .filter(|case| case.family == family)
            .collect::<Vec<_>>();
        group.throughput(Throughput::Bytes(bytes(&subset)));
        group.bench_with_input(
            BenchmarkId::new("family", family.as_str()),
            &subset,
            |b, subset| {
                b.iter(|| {
                    for case in subset {
                        black_box(
                            ParsedSql::parse(&dialect, black_box(case.sql.as_str())).unwrap(),
                        );
                    }
                });
            },
        );
    }
    group.finish();
}

fn public_tokenizer(c: &mut Criterion) {
    let corpus = postgres_corpus();
    let workload = corpus
        .iter()
        .filter(|case| case.family != Family::Scaling)
        .collect::<Vec<_>>();
    let dialect = PostgreSqlDialect {};
    let mut group = c.benchmark_group("postgres/tokenize_public");

    group.throughput(Throughput::Bytes(bytes(&workload)));
    group.bench_function("workload", |b| {
        b.iter(|| {
            for case in &workload {
                black_box(
                    Tokenizer::new(&dialect, black_box(&case.sql))
                        .tokenize_with_location()
                        .unwrap(),
                );
            }
        });
    });
    group.finish();
}

fn parser_core(c: &mut Criterion) {
    let corpus = postgres_corpus();
    let dialect = PostgreSqlDialect {};
    let workload = corpus
        .iter()
        .filter(|case| case.family != Family::Scaling)
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("postgres/parser_core");
    group.throughput(Throughput::Bytes(bytes(&workload)));
    group.bench_function("workload", |b| {
        b.iter_batched(
            || {
                workload
                    .iter()
                    .map(|case| Parser::new(&dialect).try_with_sql(&case.sql).unwrap())
                    .collect::<Vec<_>>()
            },
            |parsers| {
                for parser in parsers {
                    black_box(parser.parse_statements().unwrap());
                }
            },
            BatchSize::SmallInput,
        );
    });

    for family in Family::ALL {
        let subset = corpus
            .iter()
            .filter(|case| case.family == family)
            .collect::<Vec<_>>();
        group.throughput(Throughput::Bytes(bytes(&subset)));
        group.bench_with_input(
            BenchmarkId::new("family", family.as_str()),
            &subset,
            |b, subset| {
                b.iter_batched(
                    || {
                        subset
                            .iter()
                            .map(|case| Parser::new(&dialect).try_with_sql(&case.sql).unwrap())
                            .collect::<Vec<_>>()
                    },
                    |parsers| {
                        for parser in parsers {
                            black_box(parser.parse_statements().unwrap());
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn sentinels(c: &mut Criterion) {
    let corpus = postgres_corpus();
    let dialect = PostgreSqlDialect {};
    let mut group = c.benchmark_group("postgres/e2e/sentinel");

    for case in corpus.iter().filter(|case| case.sentinel) {
        group.throughput(Throughput::Bytes(case.sql.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(&case.id), case, |b, case| {
            b.iter(|| {
                black_box(Parser::parse_sql(&dialect, black_box(&case.sql)).unwrap());
            });
        });
    }
    group.finish();
}

fn scaling_curves(c: &mut Criterion) {
    let corpus = postgres_corpus();
    let dialect = PostgreSqlDialect {};
    let mut group = c.benchmark_group("postgres/e2e/scaling");

    for case in corpus.iter().filter(|case| case.family == Family::Scaling) {
        group.throughput(Throughput::Bytes(case.sql.len() as u64));
        let parameter = case.id.strip_prefix("scaling/").unwrap_or(&case.id);
        group.bench_with_input(BenchmarkId::from_parameter(parameter), case, |b, case| {
            b.iter(|| {
                black_box(Parser::parse_sql(&dialect, black_box(&case.sql)).unwrap());
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5))
        .sample_size(40)
        .noise_threshold(0.02)
        .significance_level(0.05);
    targets = end_to_end, document, parser_prepare, public_tokenizer, parser_core, sentinels, scaling_curves
}
criterion_main!(benches);
