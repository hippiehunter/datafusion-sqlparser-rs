# sqlparser_profile

A standalone profiling binary for analyzing sqlparser memory allocations and CPU usage.

## Purpose

This tool helps identify performance bottlenecks in the SQL parsing pipeline by running realistic complex queries through the parser. Use it with profiling tools like DHAT, flamegraph, or samply to get line-level insights into:

- Memory allocation patterns
- CPU hotspots
- Tokenization vs parsing overhead

## Query Sets

The profiler includes two sets of realistic SQL queries:

1. **Enterprise Dashboard** (~770 lines): A complex analytics query with:
   - 10 CTEs (Common Table Expressions)
   - Multiple JOINs (INNER, LEFT)
   - Window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM OVER, AVG OVER)
   - CASE expressions
   - Complex WHERE/GROUP BY/HAVING/ORDER BY clauses

2. **TPC-H Queries** (22 queries): Industry-standard benchmark queries covering:
   - Simple and complex aggregations
   - Nested subqueries
   - Derived tables
   - EXISTS/NOT EXISTS patterns
   - Various join patterns

## Usage

### Building

```bash
cd sqlparser_profile

# Standard release build (for CPU profiling)
cargo build --release

# With DHAT heap profiling support
cargo build --release --features dhat-heap
```

### CLI Options

```
Usage: sqlparser_profile [OPTIONS]

Options:
  -i, --iterations <N>   Number of iterations [default: 100]
  -m, --mode <MODE>      parse, tokenize, or both [default: parse]
  -q, --query <SET>      enterprise, tpch, or all [default: all]
  -d, --dialect <DIAL>   generic, postgres, or mysql [default: generic]
      --single           Single iteration (for heap profiling)
      --timing           Print timing information
  -h, --help             Print help
```

## Profiling Examples

### DHAT Heap Profiling

DHAT provides detailed heap allocation analysis.

```bash
# Build with DHAT support
cargo build --release --features dhat-heap

# Run single iteration (DHAT writes dhat-heap.json on exit)
./target/release/sqlparser_profile --single --query all

# View results with dhat-viewer or analyze the JSON
```

### Flamegraph (CPU profiling)

```bash
# Install cargo-flamegraph if needed
cargo install flamegraph

# Generate flamegraph
cargo flamegraph --release -- --iterations 1000 --mode parse --query all

# Output: flamegraph.svg
```

### Samply (macOS/Linux)

```bash
# Install samply
cargo install samply

# Profile with samply
cargo build --release
samply record ./target/release/sqlparser_profile --iterations 1000 --mode parse

# Opens interactive profiler UI
```

### perf (Linux)

```bash
cargo build --release

# Record with perf
perf record -g ./target/release/sqlparser_profile --iterations 1000 --mode parse

# View results
perf report
```

### Instruments (macOS)

```bash
cargo build --release

# Open Instruments.app, choose Time Profiler
# Target: ./target/release/sqlparser_profile --iterations 1000
```

## Focused Profiling

### Tokenization Only

```bash
./target/release/sqlparser_profile --mode tokenize --iterations 500 --query enterprise
```

### Specific Query Sets

```bash
# Just TPC-H queries
./target/release/sqlparser_profile --mode parse --query tpch --iterations 200

# Just the enterprise dashboard
./target/release/sqlparser_profile --mode parse --query enterprise --iterations 500
```

### Different Dialects

```bash
./target/release/sqlparser_profile --dialect postgres --query all
./target/release/sqlparser_profile --dialect mysql --query all
```

## Interpreting Results

### Flamegraph Hotspots

Look for wide bars in:
- `Parser::parse_*` functions - statement/expression parsing
- `Tokenizer::tokenize` - lexical analysis
- Memory allocation (`alloc`, `Vec::push`, `String::from`)

### DHAT Allocation Sites

The DHAT output shows:
- Total bytes allocated
- Allocation counts per call site
- Maximum live bytes

Common allocation hotspots:
- AST node construction
- Token vector building
- String interning for identifiers

## Design Notes

- Functions use `#[inline(never)]` to ensure they appear distinctly in profiles
- The `--single` flag is ideal for heap profiling (DHAT) since it captures one clean allocation trace
- The `--timing` flag helps validate profiling overhead
- Release builds include debug symbols (`debug = true` in profile) for accurate stack traces
