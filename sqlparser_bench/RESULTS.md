# PostgreSQL parser optimization results

Measurements were taken on the same checkout and machine with Criterion's
fixed 162-query representative workload. Synthetic scaling cases are excluded
from that north-star workload and measured separately. Times include AST
destruction, matching the benchmark's original behavior.

## Cumulative result

| Signal | Initial | Final | Change |
| --- | ---: | ---: | ---: |
| Representative end-to-end time | 1.896 ms | 1.511 ms | **-20.9%** |
| Representative throughput | 19.52 MiB/s | 24.49 MiB/s | **+26.3%** |
| End-to-end allocations | 27,859 | 15,459 | **-44.5%** |
| End-to-end reallocations | 5,955 | 4,037 | **-32.2%** |
| End-to-end requested bytes | 22.87 MB | 10.82 MB | **-52.7%** |
| Synthetic scaling-family time | 63.62 ms | 2.194 ms | **29.0x faster** |
| Scaling-family allocations | 2,663,173 | 28,866 | **-98.9%** |
| Scaling-family requested bytes | 1.375 GB | 11.26 MB | **-99.2%** |

The Criterion comparison against the named initial baseline reported a
20.85% end-to-end time reduction, with a 95% confidence interval of 18.68% to
22.44%.

## Experiments

### 1. Allocate compound-access storage lazily

`parse_subexpr` reserved capacity for four `AccessExpr` values for every
expression, even when no period or subscript followed. Starting with an empty
vector and allocating on the first actual access produced:

- parser-core: 5.9% faster;
- end-to-end: 2.7% faster;
- parser-core allocations: 26,924 to 24,393;
- parser-core requested bytes: 19.37 MB to 9.41 MB.

### 2. Canonicalize identifiers before ownership conversion

Borrowed words were converted to an owned identifier and then immediately
copied again by dialect canonicalization. Constructing the final identifier
directly removed 4,795 parser-core allocations. Parser-core measured 2.4%
faster, though an unrelated tokenizer control drifted during that sample, so
the allocation reduction is the more reliable attribution.

### 3. Borrow recursion state

The recursion guard now borrows its `Cell` instead of owning an `Rc<Cell<_>>`.
This removes one allocation per parser and refcount traffic at recursive entry,
but measured parser-core performance was neutral (confidence interval -1.9%
to +0.9%). It is retained as a simpler representation, not claimed as a CPU
win.

### 4. Use significant tokens for ordinary parser input

The parser-specific tokenizer omits whitespace/comment trivia, while the
public tokenizer remains unchanged. COPY statements conservatively fall back
to the trivia-preserving stream because inline COPY data consumes physical
tabs, spaces, and newlines. Placeholder adjacency is preserved through source
spans.

- parser preparation requested bytes: 3.50 MB to 1.86 MB (-47%);
- parser preparation reallocations: 783 to 648;
- end-to-end measured approximately 2% faster.

The benchmark now distinguishes `prepare`, `tokenize_public`, and
`parser_core`, so future parser-token changes remain attributable.

### 5. Defer rich diagnostics until failure

Successful statement parsing no longer builds context stacks, expected-token
sets, typo hints, or formatted errors for speculative branches. A failed fast
pass resets to its original parser position and repeats with the existing rich
diagnostics enabled.

- parser-core allocations: 19,436 to 15,545;
- parser-core reallocations: 5,172 to 3,747;
- parser-core measured 12.5% faster;
- end-to-end measured 11.9% faster.

Control benchmarks ran 3-6% faster during that sample, so a conservative
attribution is smaller than the raw result. Exact error-message tests and the
complete default test suite pass through the retry path.

### 6. Borrow tokens during precedence classification

PostgreSQL and generic precedence checks now inspect token references instead
of cloning `TokenWithSpan` values. This is semantically cleaner but measured
neutral (-0.36% parser-core, confidence interval crossing zero), because most
hot word tokens already contain borrowed text. A specialized PostgreSQL
precedence engine was not pursued without a stronger signal because it would
bypass dialect extension hooks.

### 7. Gate named-argument backtracking with structural lookahead

Arbitrary-expression named arguments previously parsed every ordinary
function argument once speculatively and again as an unnamed argument. Nested
`coalesce(nullif(...))` therefore grew exponentially. A delimiter-aware scan
now takes the speculative path only when a supported named-argument operator
exists at the argument's top level.

| Expression depth | Allocations before | Allocations after |
| ---: | ---: | ---: |
| 2 | 292 | 71 |
| 4 | 4,616 | 119 |
| 8 | 1,179,664 | 215 |

Depth-8 requested memory fell from 245.8 MB to 100 KB. The complete scaling
family fell from 63.62 ms to 2.194 ms. PostgreSQL and MSSQL tests covering
string, function, subquery, and compound-expression argument labels pass.

## Validation

- Complete default test suite: passed.
- Benchmark corpus: all 179 PostgreSQL cases passed.
- All benchmark IDs: smoke-tested successfully.
- Benchmark crate formatting and Clippy with warnings denied: passed.
- Main crate formatting: passed.
- Main crate Clippy: completed with existing warnings outside this change.

Two pre-existing feature-matrix failures remain outside this work:

- `--no-default-features`: missing `String` imports in existing dialect/AST
  modules;
- `--all-features`: the existing BigDecimal path calls a missing `.parse()`
  method in `parser/mod.rs`.
