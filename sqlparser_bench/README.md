# PostgreSQL parsing benchmarks

This standalone crate measures PostgreSQL parsing without adding Criterion to
the main library's dependency graph. Its deterministic corpus contains OLTP
reads and writes, OLAP queries, PostgreSQL-native syntax, all 22 TPC-H queries
(with reference interval literals normalized to PostgreSQL), and explicit size
curves.

See [RESULTS.md](RESULTS.md) for the first optimization investigation and its
per-change timing and allocation results.

## Performance signal

The suite deliberately has several layers. No single number can distinguish a
tokenizer win from an AST-parser win or a broad improvement from a special-case
regression.

| Benchmark | Purpose |
| --- | --- |
| `postgres/e2e/macro/workload` | Stable north-star pass through real workload cases; synthetic curves are excluded |
| `postgres/e2e/macro/family/*` | OLTP, OLAP, native syntax, TPC-H, and scaling attribution |
| `postgres/e2e/macro/tier/*` | Detect size-sensitive regressions hidden by the workload mix |
| `postgres/prepare/*` | Parser construction plus its trivia-free tokenization path |
| `postgres/tokenize_public/*` | Public, trivia-preserving tokenizer API |
| `postgres/parser_core/*` | Parser and AST construction from pre-tokenized input |
| `postgres/e2e/sentinel/*` | Representative query shapes for actionable profiles |
| `postgres/e2e/scaling/*` | Width, boolean-term, join, nesting, and bulk-row curves |

Macro iterations always parse every query in their named subset exactly once.
Criterion reports byte throughput alongside wall time. Token cloning for the
`parser_core` input is performed by Criterion's untimed batch setup; AST
construction and destruction remain timed.

Do not compare macro baselines across corpus edits. Add or change corpus inputs
in a dedicated commit and establish a fresh baseline afterward.

## Run

Validate and summarize the corpus before benchmarking:

```shell
cargo test --manifest-path sqlparser_bench/Cargo.toml
cargo run --release --manifest-path sqlparser_bench/Cargo.toml --bin corpus_report
cargo run --release --manifest-path sqlparser_bench/Cargo.toml --bin allocation_report
cargo run --release --manifest-path sqlparser_bench/Cargo.toml --bin allocation_report -- --scaling
```

Run the complete suite, or use Criterion's name filter for a faster targeted
cycle:

```shell
cargo bench --manifest-path sqlparser_bench/Cargo.toml --bench postgres
cargo bench --manifest-path sqlparser_bench/Cargo.toml --bench postgres -- 'e2e/macro'
cargo bench --manifest-path sqlparser_bench/Cargo.toml --bench postgres -- 'parser_core/family/olap'
```

Capture a baseline from a clean build, make the parser change, and compare on
the same machine:

```shell
cargo bench --manifest-path sqlparser_bench/Cargo.toml --bench postgres -- --save-baseline before
cargo bench --manifest-path sqlparser_bench/Cargo.toml --bench postgres -- --baseline before
```

For low-noise results, use a fixed performance governor, minimize other system
load, keep the CPU affinity and compiler flags unchanged, and repeat surprising
results. Prefer the family/core/sentinel pattern over subtracting two noisy
timings.
