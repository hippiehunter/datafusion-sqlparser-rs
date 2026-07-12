# Gantry SQL syntax ownership pipeline

Status: implemented and locally validated on 2026-07-11.

This document defines the ownership boundary shared by sqlparser, Gantry, and
DataFusion:

> One parsed document owns SQL syntax from parsing through preprocessing,
> worker transfer, prepared-statement storage, and logical planning. DataFusion
> borrows that syntax and produces a fully owned `LogicalPlan`.

The arena path is unconditional in standard-library builds. There is no arena
Cargo feature, runtime switch, session setting, or alternate public AST
selected by Gantry. Legacy sqlparser entry points remain source-compatible and
heap-backed; document parsing is the normal Gantry path. The existing `no_std`
compatibility surface remains heap-backed because it has no thread-local build
scope; Gantry and DataFusion are standard-library consumers.

The syntax owner deliberately stops at the logical-plan boundary. Cascades,
R-IR, physical/kernel plans, cached executable artifacts, and execution data do
not borrow parser memory. This prevents cached plans from pinning source text
and keeps arena lifetimes out of the execution engine.

## Implemented sqlparser representation

Recursive AST fields use `AstBox<T>`, a pointer-sized tagged owner:

- outside a document build, it behaves like `Box<T>` and uses the global heap;
- during `ParsedSql::parse_and_edit` or `ParsedSql::rewrite`, it allocates the
  pointee from the active document arena;
- cloning an arena-backed `AstBox` outside a build creates independent
  heap-backed syntax;
- `AstBox::into_owned` is the explicit detach operation.

The public document API is:

```rust,ignore
let document = ParsedSql::parse(&PostgreSqlDialect {}, sql)?;
let statement = document.statement(0)?; // StatementHandle

planner.sql_statement_to_plan_ref(statement.get())?;
```

`ParsedSql` owns:

- `Arc<str>` source text;
- statement roots;
- frozen arena chunks;
- arena allocation statistics.

`StatementHandle` owns `Arc<ParsedSql>` plus a root index. Cloning it increments
one document-level reference count and never walks the AST. A frozen document
and its handles are `Send + Sync`, which is required for Gantry's detached
worker handoff and prepared-statement storage.

The current arena consolidates recursive `AstBox` allocations. Strings, `Vec`
buffers, and other leaf-owned allocations still use their normal allocators,
and dropping a document still runs AST destructors before freeing its chunks.
It is therefore not yet a zero-destructor, every-byte-in-one-bump allocator.
The measured improvement comes from compact AST layout, fewer recursive global
allocations, and removing full-tree ownership copies across consumers.

Temporary work invoked inside a document edit must use:

```rust,ignore
sqlparser::arena::with_heap_ast_allocations(|| {
    // clone/mutate/render temporary syntax
})
```

This suspends the active arena with panic-safe restoration. It prevents a
short-lived normalization clone from permanently consuming space in the
frozen document.

## Primary simple-query pipeline

```text
PostgreSQL wire SQL
  |
  | hint extraction and optional text compatibility rewrite
  v
sql_preprocessing::preprocess_sql
  |
  +-- ParsedSql::parse_and_edit
  |     parser-created recursive nodes -> document arena
  |
  +-- in-place document passes
  |     byte strings and catalog comparisons
  |     Trifox compatibility
  |     dialect validation
  |     optional SQL-86 conversion
  |     qualified-alias normalization
  |     INSERT projection-alias normalization
  |     auto-parameterization
  |
  +-- derived metadata
  |     statement kind, lock clauses, referenced tables
  |     normalized cache key, parameter count, literal bindings
  |
  v
PreprocessedStatement
  |  metadata + StatementHandle (owner field is dropped last)
  |
  +-- direct AST statements ------------------------------+
  |     transaction/session/COPY/FDW/admin/DDL/routines   |
  |                                                       v
  +-- plannable statements -> detached worker closure owns handle
                                |
                                v
                 dispatch_logical_plan_through_envelope
                                |
                  cached LogicalPlan? -- yes --> lower/run
                                |
                                no
                                v
       create_planned_logical_plan_from_preprocessed_*
                                |
                  +-------------+----------------+
                  |                              |
          borrowed common path             scratch rewrite path
          Query/Insert/Update/Delete        catalog-dependent syntax
                  |                        rewrite or special DDL
                  |                              |
                  +-------------+----------------+
                                |
                                v
             SqlToRel::sql_statement_to_plan_with_context_ref
                                |
                 borrowed expression/query/relation planning
                                |
                                v
                       owned LogicalPlan
                                |
                views/RLS/Cascades/R-IR/kernel/execution
                                |
                       syntax owner can drop
```

The borrowed INSERT path is used when its target columns do not require
Gantry's catalog-dependent composite-row or timestamp-literal rewrite. Those
special schemas take one explicit scratch clone. SQL-86 context-dependent
qualification, CREATE TABLE inheritance/partition expansion, MERGE RETURNING,
and similar semantic rewrites also remain explicit clone boundaries.

The ordinary Query/INSERT/UPDATE/DELETE path has no whole-statement clone
between the frozen preprocessed document and DataFusion's logical planner.
DataFusion uses references and slices through query, CTE, set-expression,
SELECT, relation/join, value, expression, and DML planning. Narrow owned
scratch values remain where semantics genuinely require rewriting, including
recursive-CTE alias injection and named-window/SRF transformations.

## Prepared and extended-protocol pipeline

```text
Parse message
  |
  +-- preprocess once into ParsedSql
  +-- immutable placeholder arity/type/plan-shape visitors
  +-- best-effort borrowed eager logical planning
  v
PreparedStatement
  |  source String
  |  Arc<ParsedSql>
  |  optional Arc<LogicalPlan>
  |  parameter/cache metadata
  |
  +-- Describe borrows document or cached plan
  |
Bind message
  +-- values decode separately into ScalarValue storage
  v
Execute message
  |
  +-- preprocessing-config cache hit
  |       -> Arc<[PreprocessedStatement]>
  |
  +-- miss
  |       -> ParsedSql::rewrite into one new config-specific arena
  |          (source Arc is shared)
  |
  +-- schema-version plan hit -> reuse Arc<LogicalPlan>
  |   miss -> borrowed planning from config-specific StatementHandle
  |
  +-- detached worker owns the prepared/preprocessed Arc values
  v
ParamFrame + owned logical/kernel plan -> execute
```

Placeholders remain in immutable syntax. Bound values travel separately in
`ParamFrame`; Gantry does not clone and substitute the whole prepared AST for
normal execution. SQL `PREPARE`/`EXECUTE` uses the same document/preprocessing
machinery.

## Syntax retained after planning

Most query and mutation execution derives all required metadata before
lowering and does not retain syntax in its execution context. Three paths can
need syntax after initial planning:

- cursor open/materialization;
- EXPLAIN handling;
- CREATE TABLE AS SELECT decomposition.

They carry this explicit owner:

```rust,ignore
enum OwnedStatement {
    Document(StatementHandle),
    Detached(Arc<Statement>),
}
```

Wire/protocol syntax always uses `Document`. `Detached` is reserved for
programmatically constructed statements and small semantic rewrites such as a
`WHERE CURRENT OF`-sanitized DML statement. This replaces the previous
`Arc::new(statement.clone())` ownership escape.

## Other Gantry sqlparser activity

Not every parse in the workspace is a wire-query parse. The ownership rule is
selected by the lifetime and consumer:

| Activity | Representative implementation | Ownership rule |
| --- | --- | --- |
| Simple and multi-statement protocol SQL | `dbl-server/src/sql_preprocessing.rs` | One preprocessed `ParsedSql` per batch; handles cross worker boundaries |
| Extended Parse, Describe, Bind, Execute | `protocol/mod.rs`, `protocol/extended_query.rs` | Prepared statement stores `Arc<ParsedSql>`; config-specific rewrites produce cached documents |
| SQL `PREPARE` / `EXECUTE` | `protocol/statement_exec.rs` | Same prepared document and separate value bindings |
| View expansion and MV definition planning | `dbl-executor/src/context/view_expansion.rs`, `execution/mv_*` | One-shot catalog parse may remain heap-owned until migrated/cached; logical plans own the result |
| RLS, CHECK, defaults, partial-index and trigger predicates | `dbl-cascades/src/lower_rir/predicate_text.rs`, `dbl-operators/src/constraints/check.rs`, trigger modules | Short-lived expression parsing or a definition-owned syntax sidecar; never borrow into executable plans |
| PL/pgSQL bodies and embedded SQL | `dbl-plsql`, `pg_common/embedded_sql_binding.rs` | Routine compilation owns syntax until IR extraction; dynamic substitution is an explicit mutable scratch boundary |
| EPS/cache reconstruction SQL | `dbl-server/src/eps_replanner.rs`, `dbl-plan-store/src/expr_codec.rs` | Invocation-owned parse dropped after an owned plan/expression is reconstructed |
| Parser/normalizer/dialect unit tests | corresponding crate test modules | Legacy heap parser is intentional and exercises compatibility |

The secondary catalog/IR producers above are not allowed to place borrowed AST
references in their outputs. They can adopt `ParsedSql` independently when
profiling shows repeated parsing or retention is significant; their current
one-shot heap use does not break the protocol-to-logical-plan document
pipeline.

## Explicit clone and ownership boundaries

Whole-tree copying is permitted only where the operation is named and scoped:

- `ParsedSql::rewrite`: create a new immutable configuration-specific
  document;
- `AstBox::into_owned`: detach a subtree from a document;
- `with_heap_ast_allocations`: temporary clone/mutate/render work that is
  dropped immediately;
- catalog-dependent Gantry planner rewrites for affected statement shapes;
- DataFusion statement families whose logical nodes intentionally retain
  parser AST payloads;
- synthesized statements represented by `OwnedStatement::Detached`.

Immutable inspections must use `Visitor`, not clone the AST to satisfy
`VisitorMut`. Gantry's prepared-placeholder arity and parameter-sensitive plan
shape detectors follow this rule.

## Validation and measured effect

The implementation is validated at three layers:

1. sqlparser equivalence, dialect, visitor, all-feature, no-default-feature,
   thread-sharing, panic restoration, and owner-lifetime tests;
2. DataFusion SQL planner tests plus an allocation/timing benchmark that plans
   a mixed 11-statement OLTP/OLAP workload through both APIs;
3. Gantry `dbl-server` local-stack compilation and preprocessing ownership
   tests using local sqlparser and DataFusion patches.

Measured on the mixed DataFusion parse-to-plan workload after borrowed DML
planning:

| Pipeline | Allocations | Reallocations | Requested bytes |
| --- | ---: | ---: | ---: |
| owned parse + owned planner | 7,007 | 1,023 | 1,436,088 |
| document parse + cloned-statement adapter | 7,256 | 977 | 1,435,713 |
| document parse + borrowed planner | 6,811 | 977 | 1,341,679 |
| document improvement | 196 fewer | 46 fewer | 94,409 fewer (6.6%) |

The cloned-statement control uses the same document parser as the borrowed path
and changes only the planner handoff. Borrowing eliminates 445 allocations and
94,034 requested bytes (6.5%) relative to that control. Logical-plan renderings
are checked for equality for every workload statement before measurement.

An affinity-pinned, 40-sample Criterion confirmation measured the owned path at
376.29-376.81 microseconds and the document path at 376.13-376.83 microseconds:
statistically indistinguishable on this workload. The cloned-statement adapter
measured 406.16-407.53 microseconds, making the borrowed document path 7.4%
faster by median. An earlier unpinned run showed a 2.4% document-versus-owned
gain, but it did not survive the tighter control and is not treated as a
confirmed improvement. The confirmed timing win is removal of Gantry's former
full-tree clone before the borrowed planner.

The parser-only PostgreSQL corpus improved from the original 10,824,217
requested bytes to roughly 7.1-7.3 MB depending on owned/document mode, and
the owned parse benchmark was about 25% faster than the saved pre-optimization
baseline.

Benchmark results are workload and machine specific. Regressions should be
judged with allocation counts/bytes and stable timing together; arena committed
bytes and slack are reported separately so fewer allocator calls cannot hide
excessive retained memory.

## Remaining optimization opportunities

The ownership design is complete for the primary Gantry protocol-to-DataFusion
pipeline, but it intentionally leaves measurable follow-up work:

- replace `SqlNormalizer`'s temporary clone/mutate/render algorithm with an
  immutable canonical writer;
- migrate DataFusion's remaining retained-AST statement families to semantic
  owned logical nodes where appropriate;
- arena-pack selected vector/text leaf storage if profiles justify the API and
  destructor complexity;
- cache parsed catalog definitions that are repeatedly reparsed;
- extend the cross-layer workload with Gantry-specific Trifox, SQL-86,
  composite/timestamp INSERT, cursor, CTAS, and prepared replan cases.

Those changes can improve the signal without changing the ownership contract
defined here.
