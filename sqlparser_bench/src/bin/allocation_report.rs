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
use sqlparser_bench::postgres_corpus::{postgres_corpus, Family, QueryCase};

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: The layout and contract are forwarded unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: The layout and contract are forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        // SAFETY: The pointer, layout, and requested size are forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    allocations: u64,
    reallocations: u64,
    allocated_bytes: u64,
}

fn reset() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    REALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
}

fn snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        reallocations: REALLOCATIONS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}

fn measure(f: impl FnOnce()) -> AllocationSnapshot {
    reset();
    f();
    snapshot()
}

fn print_result(pipeline: &str, subset: &str, query_count: usize, result: AllocationSnapshot) {
    println!(
        "{pipeline:<16} {subset:<18} {query_count:>3}  {allocations:>8}  {reallocations:>8}  {bytes:>12}",
        allocations = result.allocations,
        reallocations = result.reallocations,
        bytes = result.allocated_bytes,
    );
}

fn main() {
    let corpus = postgres_corpus();
    let dialect = PostgreSqlDialect {};

    // Exclude lazy keyword-table initialization from steady-state allocation
    // counts, matching Criterion's warmed-up timing measurements.
    black_box(Parser::parse_sql(&dialect, "SELECT warmup").unwrap());

    println!("pipeline         subset             SQL   allocs  reallocs   bytes allocated");
    println!("---------------- ------------------ --- -------- -------- ---------------");

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
        for case in corpus
            .iter()
            .filter(|case| case.family == Family::Scaling)
        {
            report_subset(&case.id, &[case], &dialect);
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
