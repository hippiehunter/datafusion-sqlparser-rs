# Gantry SQL pipeline and arena-shaped AST design

Status: proposed design, before the arena implementation.

This document records the SQL ownership contract between sqlparser, Gantry,
and DataFusion. It is based on these revisions:

- sqlparser: `28cba32c98d535df2ff59c9bc5319b352b566f3d`
- Gantry: `927d6f7d5655247c057d82b6cbe90440e73cfb82`
- DataFusion: `2d5a31d3ba08c9fceaa379584087a025453317c0`

The proposed boundary is deliberately narrow:

> A shareable parsed document owns all SQL syntax storage. Gantry keeps that
> document alive for as long as syntax is needed, DataFusion borrows syntax
> while producing a logical plan, and `LogicalPlan` remains fully owned.

The arena must not leak into DataFusion's logical plan, Cascades, R-IR, kernel
plans, or execution data. Doing so would pin source SQL in cached plans and
spread AST lifetimes through the entire engine. The arena is end-to-end for
syntax processing, through logical planning, but not beyond the syntax-to-plan
ownership boundary.

## What the consumer actually requires

Gantry does substantially more than parse a string and immediately discard the
tree:

1. A simple-query batch parses, validates, rewrites, classifies, normalizes,
   plans, and directly executes some statements from their AST.
2. An extended-protocol prepared statement retains its source AST across
   Parse, Describe, Bind, Execute, worker handoff, schema-version checks, and
   session preprocessing changes.
3. Gantry revisits syntax after planning for row locks, snapshot and MV hints,
   replica-identity checks, result names, user-defined type dependencies, and
   `MERGE RETURNING` handling.
4. Transaction, session, COPY, replication, FDW, administrative, and much of
   DDL dispatch never become a DataFusion logical plan. Their AST is the input
   to typed Gantry handlers.
5. Catalog text is parsed again for views, materialized views, policies, CHECK
   constraints, column defaults, partial-index predicates, trigger conditions,
   routine bodies, embedded PL/SQL, and cached-plan reconstruction.
6. Parsed values cross worker boundaries. A frozen syntax owner therefore must
   be `Send + Sync`; a request-local `Bump` plus naked borrowed roots is not a
   sufficient API.

These facts rule out an API shaped only like this:

```rust,ignore
let arena = Bump::new();
let statements = Parser::parse_sql_in(&arena, sql)?;
```

It works for a synchronous one-shot parse, but it cannot safely be stored in a
prepared statement without constructing a self-referential owner, and it does
not define how the tree moves to Gantry's detached worker.

## Current primary pipeline

The ordinary simple-query route is:

```text
PostgreSQL wire SQL
  |
  |  extract hint comments; optional text compatibility rewrite
  v
sql_preprocessing::preprocess_sql
  |
  +-- sqlparser::Parser::parse_sql -> Vec<Statement>
  |
  +-- mutable AST passes
  |     byte strings / catalog comparisons / Trifox compatibility
  |     dialect validation / optional SQL-86 joins / auto-parameters
  |
  +-- derived syntax metadata
  |     statement kind / locks / normalized SQL / referenced tables
  |
  v
PreprocessedStatement
  |
  +-- direct AST route ------------------------------------------+
  |     transaction/session/COPY/replication/FDW/admin/DDL       |
  |                                                              v
  +-- plannable route -> cached LogicalPlan? -> unified typed execution
                         |
                         | miss
                         v
                clone Statement into plan_stmt
                         |
                         +-- Gantry AST rewrites
                         |
                         +-- clone plan_stmt into DataFusion
                                      |
                                      v
                         SqlToRel::sql_statement_to_plan
                                      |
                                      v
                              owned LogicalPlan
                                      |
                    +-----------------+------------------+
                    |                                    |
                 expand views                       inject RLS
              (parse + plan SQL)                 (parse + plan Expr)
                    |                                    |
                    +-----------------+------------------+
                                      |
                                      v
                     Gantry post-plan AST-derived metadata
                                      |
                                      v
                       Cascades -> R-IR -> kernel DAG -> run
```

The relevant front door is
`dbl-server/src/postgres_handlers/protocol/mod.rs::DummyProcessor::on_query`.
It calls `dbl-server/src/sql_preprocessing.rs::preprocess_sql`, and plannable
statements eventually reach
`protocol/statement_exec.rs::execute_plannable_statement_ast` and
`execution/executor_dispatch_envelope.rs::dispatch_logical_plan_through_envelope_inner`.

On a logical-plan miss,
`execution/executor_plan_create.rs::create_planned_logical_plan_with_params_and_pg_types`
clones the statement, applies Gantry-specific mutations, and passes another
clone by value to DataFusion's
`SqlToRel::sql_statement_to_plan_with_context`. DataFusion's planner then
destructures and consumes the owned AST while constructing an owned
`LogicalPlan`.

### Prepared execution

The extended-protocol route has a different and longer lifetime:

```text
Parse message
  |
  +-- parse source -> Vec<Statement>
  +-- infer placeholder types from AST
  +-- normalize by cloning AST
  +-- best-effort eager logical plan
  v
PreparedStatement
  |  owns String source + Vec<Statement> + optional Arc<LogicalPlan>
  |
  +-- Describe reads AST or cached plan
  |
Bind message
  +-- decode values into ScalarValue; AST placeholders are not substituted
  v
Execute message
  |
  +-- config-signature hit? use Arc<[PreprocessedStatement]>
  |                         miss: clone raw AST and preprocess it
  |
  +-- schema-version hit? use cached Arc<LogicalPlan>
  |                      miss: plan from the preprocessed AST
  |
  +-- move Arc preprocessing result and values to detached worker
  v
ParamFrame + cached/new LogicalPlan -> lower -> execute
```

The extended parser is
`protocol/mod.rs::DBLQueryParser::parse_sql`. The retained owner today is
`protocol/extended_query.rs::PreparedStatement`, whose `ast` field is a
`Vec<Statement>`. Session-dependent preprocessing is cached by
`load_preprocessed_execution_context`; its miss path calls
`preprocess_statements(prepared_stmt.ast.clone(), ...)`.

PostgreSQL wire parameters already have the correct shape for an immutable
arena tree: placeholders stay in the planned template and bound values travel
separately in `ParamFrame`. Arena migration must preserve that separation.

SQL `PREPARE`/`EXECUTE` uses the same execution machinery, but its session
store currently clones the whole `PreparedStatement` on lookup. An arena-backed
AST makes that clone cheap only if the store also changes to
`Arc<PreparedStatement>`.

## Other sqlparser-driven Gantry pipelines

The main query path is not the whole migration surface.

| Producer | Syntax consumers | Present lifetime | Target owner |
| --- | --- | --- | --- |
| Simple or multi-statement wire query | preprocessing, direct dispatch, planner, post-plan metadata | request/worker | one preprocessed document for the batch |
| Extended Parse or SQL `PREPARE` | type inference, Describe, cache identity, replanning, direct dispatch | prepared statement/portal | raw document plus one config-keyed preprocessed document |
| View or materialized-view definition SQL | updatability analysis, `SqlToRel`, MV rewrite/refresh | repeatedly reparsed catalog text | catalog entry keyed by definition hash and schema version |
| RLS `USING` / `WITH CHECK` | DataFusion expression planning and R-IR lowering | repeatedly parsed during planning/lowering | parsed expression document cached with policy metadata |
| CHECK constraint | direct row evaluator and R-IR lowering | parser tree retained by a validator, sometimes rebuilt | parsed expression document owned by prepared constraint |
| Column default | `SqlToRel::sql_to_expr` | parsed while building context defaults | schema-versioned parsed expression or planned expression |
| Partial-index predicate | direct AST pattern extraction | parsed during scan rule application | index-definition sidecar |
| Trigger `WHEN` and trigger SQL | direct AST evaluator or typed DML execution | parsed while resolving/executing triggers | trigger-definition/compiled-function sidecar |
| PL/SQL routine body | `dbl-plsql` lowering and embedded-query extraction | compile time, with some execution-time reparsing | compiled routine owns parsed templates until lowering completes |
| PL/SQL dynamic SQL | placeholder/correlated-variable rewrite, render, then reparse | invocation | invocation document plus value bindings; no render/reparse cycle |
| EPS artifact SQL | DataFusion `DFParser`, then `SqlToRel` | cache reconstruction call | invocation document dropped after owned plan creation |
| DataFusion unparser output | syntax validation or artifact reconstruction | invocation/artifact | direct document parse when reconstruction is required |

Representative implementations are:

- `dbl-executor/src/context/view_expansion.rs::parse_view_sql`
- `dbl-executor/src/context/rls_injection.rs::parse_policy_expression`
- `dbl-executor/src/context/provider.rs::build_table_source_defaults`
- `dbl-cascades/src/lower_rir/predicate_text.rs::sql_text_to_datafusion_expr`
- `dbl-cascades/src/lower_rir/dml.rs::analyze_updatable_view`
- `dbl-cascades/src/rules/scan.rs::simple_sql_predicate_keys`
- `dbl-operators/src/constraints/check.rs::parse_check_expression`
- `dbl-operators/src/dml/trigger_executor.rs::parse_trigger_when_expr`
- `dbl-server/src/postgres_handlers/pg_common/embedded_sql_binding.rs`
- `dbl-server/src/eps_replanner.rs::SqlReplanner::replan_sql`

## Current avoidable ownership work

The current API makes a deep `Clone` the easiest way to retain an AST while a
consumer takes ownership or a visitor requires mutable access. Important sites
include:

| Site | Work done today |
| --- | --- |
| `SqlNormalizer::normalize_parsed_statement` | clones the complete statement, mutates the clone, then renders it |
| `collect_prepared_placeholder_arity` | clones the complete statement because its collector uses `VisitorMut` |
| `preprocess_statements` for a prepared config miss | clones the raw statement batch before rewriting |
| `create_planned_logical_plan_with_params_and_pg_types` | clones the statement for Gantry rewrites |
| call into `sql_statement_to_plan_with_context` | clones `plan_stmt` again because DataFusion consumes it |
| SQL-level prepared-statement lookup | derives/clones `PreparedStatement`, including source and AST |
| PL/SQL correlated binding | parses, clones the batch, rewrites, renders, and later reparses the rendered SQL |
| dynamic SQL parameter substitution | parses, clones each statement, rewrites, renders, and reparses in the normal execution entry point |
| CHECK row evaluation | recursively clones/substitutes the expression for each row |
| some Explain/CTAS/cursor hooks | clones a statement into an `Arc` after planning |

Some copying at the logical-plan boundary is intentional. A DataFusion
`LogicalPlan` must own resolved table references, field names, scalar values,
and expression data after the syntax document is gone. The target is to remove
syntax-tree ownership copies, not to make executable plans borrow source text.

## Proposed sqlparser ownership API

### Document owner

Parsing returns an owner, not a naked vector of roots:

```rust,ignore
pub struct ParsedSql {
    source: Arc<str>,
    arena: FrozenAstArena,
    statements: ArenaSlice<Statement<'static>>, // lifetime erased internally
}

impl ParsedSql {
    pub fn parse(
        dialect: &dyn Dialect,
        source: impl Into<Arc<str>>,
        options: ParseOptions,
    ) -> Result<Arc<Self>, ParserError>;

    pub fn statements<'doc>(&'doc self) -> &'doc [Statement<'doc>];
    pub fn source(&self) -> &str;
    pub fn statement_source(&self, index: usize) -> Option<&str>;
}
```

The `'static` in the private representation is an implementation detail. The
only public references are rebound to the borrow of `ParsedSql`. The owner
contains the source allocation and every arena chunk, so neither identifiers
nor child nodes can outlive it.

For storage across an API that cannot conveniently keep the batch owner and
index separate, sqlparser can provide an owned root handle:

```rust,ignore
#[derive(Clone)]
pub struct StatementHandle {
    document: Arc<ParsedSql>,
    index: u32,
}

impl StatementHandle {
    pub fn get<'a>(&'a self) -> &'a Statement<'a>;
}
```

Cloning a handle increments one document-level `Arc`; it never walks syntax.
There must not be an `Arc`, reference count, or allocator pointer on every AST
node.

### Arena AST representation

The arena representation is lifetime-parameterized and move-only:

```rust,ignore
pub struct Ident<'ast> {
    pub value: &'ast str,
    pub quote_style: Option<char>,
    pub span: SourceSpan,
}

pub enum Statement<'ast> {
    Query(ArenaBox<'ast, Query<'ast>>),
    Insert(Insert<'ast>),
    // ...
}

pub struct Query<'ast> {
    pub body: ArenaBox<'ast, SetExpr<'ast>>,
    pub locks: ArenaVec<'ast, LockClause<'ast>>,
    // ...
}
```

`ArenaBox`, `ArenaVec`, and arena text are non-owning views tied to the arena
lifetime. They do not implement a deep `Clone`. This is an intentional API
property:

- share a tree with `Arc<ParsedSql>::clone` or `StatementHandle::clone`;
- copy syntax only with an explicit `AstEdit::clone_subtree` operation;
- copy semantic leaf data into a logical plan where ownership really changes.

Removing `Clone` from `Statement` and `Expr` lets the compiler find accidental
full-tree copies across all three repositories.

The frozen tree exposes ordinary shared pattern matching. Mutable traversal is
available only inside a scoped editing/building API, never through
`Arc<ParsedSql>`.

The scope should be enforced with a higher-ranked callback so a caller cannot
retain a node or arena container after the builder is frozen:

```rust,ignore
pub fn parse_and_edit<F>(
    dialect: &dyn Dialect,
    source: Arc<str>,
    options: ParseOptions,
    edit: F,
) -> Result<Arc<ParsedSql>, ParserError>
where
    F: for<'ast> FnOnce(
        &mut AstEdit<'ast>,
        &mut ArenaSliceMut<'ast, Statement<'ast>>,
    ) -> Result<(), ParserError>;
```

`ParsedSql::rewrite` uses the same shape after cloning into a fresh mutable
arena. The frozen owner exposes no safe route back to `&mut Statement`.

### Source text and spans

`ParsedSql` owns an `Arc<str>`. Identifiers and literals borrow source slices
when their semantic value is already a slice of the input. Escaped or generated
text is copied into the arena. Root statements carry byte ranges, so Gantry can
obtain exact per-statement source without its current second statement splitter
or a lossy `Display` fallback.

Line/column diagnostics can remain, but byte offsets must be authoritative for
source slicing. A document must support inputs larger than a chosen compact
offset representation; it may use 32-bit offsets only with an explicit size
check and a wider fallback/error.

### Building, rewriting, and freezing

The parser builds in a mutable, thread-local arena and freezes once roots are
complete. Gantry's structural preprocessing runs in one edit session:

```rust,ignore
let cooked = raw.rewrite(|edit, statements| {
    for statement in statements {
        normalize_byte_string_literals(edit, statement);
        rewrite_catalog_subid_string_comparisons(edit, statement);
        trifox.rewrite_statement(edit, statement)?;
        sql86.convert_statement(edit, statement)?;
        auto_parameterize(edit, statement, policy)?;
    }
    Ok(())
})?;
```

For a simple query, Gantry can supply these passes before the parser's initial
builder is frozen and produce one document. A prepared statement retains the
raw frozen document and builds a config-specific preprocessed document when
needed.

The first implementation should deep-copy the raw tree into one destination
arena and run all mutable passes there. That gives simple ownership and one
bulk allocation domain. A persistent/path-copying arena can be investigated
later only if profiles show that the linear copy is material. It should not be
the initial correctness burden.

Normalization must become a read-only canonical writer/fingerprinter rather
than “clone, mutate, stringify.” It should traverse `Statement<'_>` and emit a
canonical key, parameter metadata, and hash directly. Read-only collectors must
use `Visitor`, not clone solely to satisfy `VisitorMut`.

### Arena implementation requirements

The allocator is more than a dependency choice. It needs these semantics:

- stable addresses for all nodes and text;
- parser checkpoints and rewind for speculative branches and the fast-error to
  rich-error retry path;
- one or a small number of chunks, with measured rather than fixed aggressive
  preallocation;
- bulk destruction without recursively dropping a deep AST;
- a frozen state that is safely `Send + Sync`;
- no mutation API after freeze;
- correct destruction for feature-gated payloads with non-trivial destructors,
  such as `BigDecimal`, either by changing their representation or registering
  drops;
- allocation statistics: requested bytes, committed chunk bytes, live bytes,
  and tail slack.

A plain `bumpalo::Bump` may be useful underneath, but its type must not define
the public ownership model, and a frozen wrapper needs an explicit safety
audit. In particular, collection types that retain `&Bump` can prevent the
frozen document from being `Send + Sync`; frozen AST containers should retain
only stable data pointers and lengths.

## Target Gantry types

The preprocessed document and its derived metadata should be separate:

```rust,ignore
pub struct PreprocessedBatch {
    pub syntax: Arc<ParsedSql>,
    pub statements: Arc<[PreprocessedStatementMeta]>,
}

pub struct PreprocessedStatementMeta {
    pub root_index: u32,
    pub kind: StatementKind,
    pub source_span: SourceSpan,
    pub normalized: NormalizedSqlKey,
    pub referenced_tables: Arc<[TableReference]>,
    pub row_lock_spec: Option<RowLockSpec>,
    pub operation: Option<StatementOperation>,
    pub normalized_param_count: u32,
    pub auto_param_values: Arc<[ScalarValue]>,
    pub disables_template_cache: bool,
    // Other post-parse facts proven useful on the hot path.
}
```

Metadata should contain semantic values, not cloned AST subtrees. For example,
`auto_param_literals: Vec<Expr>` should become bound scalar/literal data, and
lock clauses should become the execution `RowLockSpec` when possible.

The prepared owner becomes:

```rust,ignore
pub struct PreparedStatement {
    pub raw: Arc<ParsedSql>,
    pub template: ArcSwapOption<PreprocessedTemplateCacheEntry>,
    pub logical_plan: Option<Arc<LogicalPlan>>,
    // parameter types, cache identity, result metadata, and tier state
}

pub type SessionPreparedStatementStore =
    HashMap<String, Arc<PreparedStatement>>;
```

The original SQL comes from `raw.source()`; it does not need a second owned
`String`. A portal and detached worker clone the prepared/document `Arc`, not
the syntax tree.

`ExecutionContext::statement_for_plan` should become an optional owned
`StatementHandle` only for paths that genuinely retain syntax. Common query
and mutation execution should continue moving derived metadata across the
logical-plan boundary and leave the field empty.

### Binding without syntax substitution

Arena syntax is immutable after freeze. Runtime values therefore use sidecars:

- PostgreSQL `$N` values remain `ParamFrame` entries, as they do today.
- auto-parameterized literals become values in the same parameter mechanism;
- PL/SQL correlated variables use a binding environment accepted by lowering,
  rather than cloning, rendering, and reparsing SQL;
- CHECK and trigger evaluators resolve identifiers/placeholders from a row or
  trigger environment while walking the frozen expression, rather than
  constructing a substituted expression per row.

Structural compatibility transforms still use the scoped rewrite API. Value
binding does not.

## Target DataFusion planner API

DataFusion must stop taking syntax ownership:

```rust,ignore
impl<S: ContextProvider> SqlToRel<'_, S> {
    pub fn sql_statement_to_plan(
        &self,
        statement: &sqlparser::ast::Statement<'_>,
    ) -> Result<LogicalPlan>;

    pub fn sql_statement_to_plan_with_context(
        &self,
        statement: &sqlparser::ast::Statement<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan>;

    pub fn sql_to_expr(
        &self,
        expr: &sqlparser::ast::Expr<'_>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr>;
}
```

Internal planner methods follow the same rule:

- `query_to_plan(&Query<'_>, ...)`
- `select_to_plan(&Select<'_>, ...)`
- relation, CTE, values, DML, DDL, and expression helpers borrow their syntax
  inputs;
- only resolved DataFusion names, values, schemas, and expressions are copied
  into the returned plan.

DataFusion currently mutates or consumes AST in several places, including
projection-SRF rewrites, named-window matching, `mem::take` of SELECT fields,
and the expression conversion stack. Those operations need one of two shapes:

1. translate directly into DataFusion planner state without changing syntax;
2. allocate a temporary rewritten fragment in a planner scratch arena.

The scratch arena is invocation-local and never appears in `LogicalPlan`.

Expression conversion currently boxes an owned `SQLExpr` for each virtual
stack entry. A borrowed planner can use stack entries containing `&SQLExpr`
and avoid that second tree of boxes.

DataFusion's `DFParser` extensions need a matching document owner, for example
`DfParsedSql`, whose arena may contain both core sqlparser nodes and DataFusion
extension roots. EPS replanning then keeps that document alive only until
`statement_to_plan` returns its owned plan.

## Lifetime by pipeline

| Pipeline | Arena owner lifetime | Logical plan relationship |
| --- | --- | --- |
| Simple one-shot statement | protocol request through worker planning and AST hooks | plan owns all translated data; document drops after dispatch no longer needs syntax |
| Multi-statement simple batch | entire batch/implicit-transaction dispatch, or per statement if safely split | each plan is independent of the document |
| Extended prepared statement | Parse until Close/deallocate/session end | cached plan does not borrow raw or preprocessed documents |
| Config-specific prepared preprocessing | cached entry until config changes or prepared statement drops | schema-versioned cached plan is independently owned |
| Cursor/CTAS/Explain utility hook | explicit `StatementHandle` for the utility's required lifetime | only the hook retains syntax |
| View/policy/default/index definition | catalog cache entry keyed by definition/schema version | expanded/planned result is owned |
| CHECK/trigger direct evaluator | prepared validator/resolved trigger | no logical plan when evaluated directly |
| Static PL/SQL routine | compile/lowering phase; parsed embedded templates may be retained deliberately | compiled IR owns its semantic data |
| Dynamic SQL or EPS replan | invocation | document drops immediately after owned output is built |

## Migration and validation sequence

This is a cross-repository change and should land in measured, bisectable
stages. Each stage needs its own correctness and performance report before the
next one starts.

### A. Add the ownership signal before changing representation

Extend the benchmark signal with:

- parse plus Gantry-equivalent preprocessing and normalization;
- parse plus DataFusion `SqlToRel` cold planning;
- prepared Parse once, then repeated Describe/Bind/Execute metadata hits;
- a forced preprocessing-config miss and a forced schema-version plan miss;
- stored view, bare predicate, default expression, and PL/SQL binding cases;
- allocation calls, requested bytes, peak live bytes, retained prepared bytes,
  arena committed bytes, and arena slack.

Wall time alone cannot detect an arena that is fast but retains excessive
memory in long-lived prepared statements.

### B. Introduce `ParsedSql` and the arena parser

Add the owner, lifetime-shaped AST containers, source byte spans, checkpoints,
and freeze boundary. During development, parse the existing PostgreSQL corpus
through both representations and compare semantic AST, display output, spans,
errors, visitors, and serde where enabled.

Run the complete sqlparser test suite and the existing 179-case PostgreSQL
benchmark corpus. Investigate each family, not only the macro average.

### C. Make DataFusion planning borrowed

Change `SqlToRel` and expression planning to borrow arena syntax. Keep
`LogicalPlan` owned and add assertions/tests that no syntax/source owner is
retained. Compare logical-plan displays/fingerprints for DataFusion SQL tests,
TPC-H, Gantry OLTP, DML, DDL, and error cases.

Profile planner scratch allocations separately from parser allocations. An
arena parser win that merely moves the same boxes into `SqlToRel` is not a
successful end-to-end result.

### D. Migrate Gantry's protocol and prepared paths

Replace `Vec<Statement>` with `Arc<ParsedSql>`, introduce
`PreprocessedBatch`, make prepared stores hold `Arc<PreparedStatement>`, and
move derived hot-path metadata out of AST hooks. Validate:

- simple and extended protocol parity;
- Parse/Describe/Bind/Execute and portal worker handoff;
- session preprocessing changes;
- schema invalidation and replanning;
- transaction/session/COPY/admin/direct DDL routes;
- cached and uncached SELECT/DML/RETURNING/MERGE/CTAS/Explain/cursor paths;
- TPCC prepared hot execution and TPC-H cold planning.

### E. Migrate catalog fragments and direct evaluators

Cache parsed definitions and replace PL/SQL/check/trigger substitution with
binding environments. Validate catalog invalidation, RLS fail-closed behavior,
quoted identifiers, trigger conditions, routine compilation, view recursion,
and artifact replanning.

### F. Remove accidental deep-clone compatibility

Once all consumers use document ownership, remove `Clone` from arena
`Statement`/`Expr`, delete owned-AST adapters on the Gantry path, and use the
compiler plus allocation tests to prevent regressions.

## Performance acceptance criteria

Exact gains must come from measurements, not from the word “arena.” The first
implementation should be retained only if all of these hold:

1. No semantic or diagnostic regressions in sqlparser, DataFusion, or Gantry's
   SQL/protocol integration tests.
2. No statistically credible regression above 2% in any representative parser
   benchmark family without an explained tradeoff.
3. A material reduction in allocation calls and requested bytes for both
   parser-core and parse-plus-`SqlToRel`; the planning benchmark must show that
   allocations were removed rather than shifted downstream.
4. Prepared hot execution performs no syntax-tree deep clone and no reparse on
   a config/schema cache hit.
5. Long-lived prepared documents report bounded chunk slack and retained bytes
   competitive with or below the owned AST. Peak/live memory is a release gate,
   not an informational metric.
6. Document destruction is bulk and does not recursively walk or overflow on
   deeply nested valid syntax.

The parser's current representative workload is already at 15,459 allocation
calls and 10.82 MB of requested allocation after the lower-risk work. Arena
success should remove most AST node/list/string allocation calls, but targets
must be set from the new parse-plus-planning baselines because logical-plan
construction remains intentionally owned.

## Initial implementation decisions

These defaults keep the first arena version understandable and measurable:

- optimize the `std + visitor` feature surface used by both Gantry and this
  DataFusion fork first; keep serde/no-std behavior behind compatibility gates
  rather than letting unused deployment features dictate the initial layout;
- one document-level `Arc`, never per-node reference counting;
- immutable frozen AST and explicit scoped mutation before freeze;
- full copy into one destination arena for a prepared config rewrite, not
  persistent/path-copying trees initially;
- source-backed strings where no decoding/canonicalization is required;
- read-only normalization and metadata collectors;
- separate parameter/binding sidecars;
- owned DataFusion `LogicalPlan` as the hard lifetime boundary;
- feature-gated migration until all three repositories pass their parity and
  performance gates.

Open experiments, to be answered by profiles, are chunk growth policy,
source/text lane separation, compact offset width, and whether a later
persistent rewrite arena beats the simple full-arena copy for unusually large
prepared statements. None of those changes the document-owner API.
