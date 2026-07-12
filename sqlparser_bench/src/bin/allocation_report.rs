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

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::ParsedSql;
use sqlparser_bench::postgres_corpus::{postgres_corpus, Family, QueryCase};

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static BASELINE_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        record_live_allocation(layout.size() as u64);
        // SAFETY: The layout and contract are forwarded unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        record_live_allocation(layout.size() as u64);
        // SAFETY: The layout and contract are forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: The pointer and layout came from the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        if new_size >= layout.size() {
            record_live_allocation((new_size - layout.size()) as u64);
        } else {
            LIVE_BYTES.fetch_sub((layout.size() - new_size) as u64, Ordering::Relaxed);
        }
        // SAFETY: The pointer, layout, and requested size are forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn record_live_allocation(bytes: u64) {
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    let _ = PEAK_LIVE_BYTES.fetch_update(
        Ordering::Relaxed,
        Ordering::Relaxed,
        |peak| (live > peak).then_some(live),
    );
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    allocations: u64,
    reallocations: u64,
    allocated_bytes: u64,
    peak_live_bytes: u64,
    retained_bytes: u64,
}

#[derive(Default)]
struct ArenaSnapshot {
    source_bytes: u64,
    node_allocations: u64,
    requested_bytes: u64,
    committed_bytes: u64,
    slack_bytes: u64,
}

fn reset() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    REALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    let live = LIVE_BYTES.load(Ordering::Relaxed);
    BASELINE_LIVE_BYTES.store(live, Ordering::Relaxed);
    PEAK_LIVE_BYTES.store(live, Ordering::Relaxed);
}

fn snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        reallocations: REALLOCATIONS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        peak_live_bytes: PEAK_LIVE_BYTES
            .load(Ordering::Relaxed)
            .saturating_sub(BASELINE_LIVE_BYTES.load(Ordering::Relaxed)),
        retained_bytes: LIVE_BYTES
            .load(Ordering::Relaxed)
            .saturating_sub(BASELINE_LIVE_BYTES.load(Ordering::Relaxed)),
    }
}

fn measure(f: impl FnOnce()) -> AllocationSnapshot {
    reset();
    f();
    snapshot()
}

fn print_result(pipeline: &str, subset: &str, query_count: usize, result: AllocationSnapshot) {
    println!(
        "{pipeline:<16} {subset:<18} {query_count:>3}  {allocations:>8}  {reallocations:>8}  {bytes:>12}  {peak:>12}  {retained:>12}",
        allocations = result.allocations,
        reallocations = result.reallocations,
        bytes = result.allocated_bytes,
        peak = result.peak_live_bytes,
        retained = result.retained_bytes,
    );
}

fn print_arena_result(subset: &str, query_count: usize, result: &ArenaSnapshot) {
    println!(
        "arena_storage    {subset:<18} {query_count:>3}  {nodes:>8}  {source:>12}  {requested:>12}  {committed:>12}  {slack:>12}",
        nodes = result.node_allocations,
        source = result.source_bytes,
        requested = result.requested_bytes,
        committed = result.committed_bytes,
        slack = result.slack_bytes,
    );
}

fn main() {
    let corpus = postgres_corpus();
    let dialect = PostgreSqlDialect {};

    // Exclude lazy keyword-table initialization from steady-state allocation
    // counts, matching Criterion's warmed-up timing measurements.
    black_box(Parser::parse_sql(&dialect, "SELECT warmup").unwrap());

    println!("pipeline         subset             SQL   allocs  reallocs   bytes allocated     peak live      retained");
    println!("---------------- ------------------ --- -------- -------- --------------- ------------- -------------");

    let workload = corpus
        .iter()
        .filter(|case| case.family != Family::Scaling)
        .collect::<Vec<_>>();
    report_subset("workload", &workload, &dialect);

    for family in Family::ALL {
        let subset = corpus
            .iter()
            .filter(|case| case.family == family)
            .collect::<Vec<_>>();
        report_subset(family.as_str(), &subset, &dialect);
    }

    if std::env::args().any(|arg| arg == "--scaling") {
        println!();
        println!("Scaling cases:");
        for case in corpus.iter().filter(|case| case.family == Family::Scaling) {
            report_subset(&case.id, &[case], &dialect);
        }
    }

    if std::env::args().any(|arg| arg == "--arena-details") {
        println!("\ncase,source_bytes,tokens,nodes,requested,committed,slack");
        for case in &corpus {
            let tokens = Tokenizer::new(&dialect, &case.sql)
                .tokenize_with_location()
                .unwrap()
                .len();
            let document = ParsedSql::parse(&dialect, case.sql.as_str()).unwrap();
            let stats = document.arena_stats();
            println!(
                "{},{},{},{},{},{},{}",
                case.id,
                case.sql.len(),
                tokens,
                stats.node_allocations,
                stats.requested_bytes,
                stats.committed_bytes,
                stats.slack_bytes,
            );
        }
    }
}

fn report_subset(name: &str, cases: &[&QueryCase], dialect: &PostgreSqlDialect) {
    let end_to_end = measure(|| {
        for case in cases {
            black_box(Parser::parse_sql(dialect, &case.sql).unwrap());
        }
    });
    print_result("end_to_end", name, cases.len(), end_to_end);

    let mut arena = ArenaSnapshot::default();
    let document = measure(|| {
        for case in cases {
            let document = ParsedSql::parse(dialect, case.sql.as_str()).unwrap();
            let stats = document.arena_stats();
            arena.source_bytes += case.sql.len() as u64;
            arena.node_allocations += stats.node_allocations as u64;
            arena.requested_bytes += stats.requested_bytes as u64;
            arena.committed_bytes += stats.committed_bytes as u64;
            arena.slack_bytes += stats.slack_bytes as u64;
            black_box(document);
        }
    });
    print_result("document", name, cases.len(), document);
    print_arena_result(name, cases.len(), &arena);

    let prepare = measure(|| {
        for case in cases {
            black_box(Parser::new(dialect).try_with_sql(&case.sql).unwrap());
        }
    });
    print_result("prepare", name, cases.len(), prepare);

    let tokenize_public = measure(|| {
        for case in cases {
            black_box(
                Tokenizer::new(dialect, &case.sql)
                    .tokenize_with_location()
                    .unwrap(),
            );
        }
    });
    print_result("tokenize_public", name, cases.len(), tokenize_public);

    let parsers = cases
        .iter()
        .map(|case| Parser::new(dialect).try_with_sql(&case.sql).unwrap())
        .collect::<Vec<_>>();
    let parser_core = measure(|| {
        for parser in parsers {
            black_box(parser.parse_statements().unwrap());
        }
    });
    print_result("parser_core", name, cases.len(), parser_core);
}
