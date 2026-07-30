# Oracle parser compatibility

## Contract

The compatibility target is Oracle AI Database 26ai.

Compatibility requires all of the following:

1. The tokenizer accepts Oracle lexical forms without rewriting the input.
2. The parser consumes the complete statement.
3. Oracle-specific semantics are represented by typed AST nodes.
4. `Display` output reparses to an equivalent AST.
5. Spans and attached tokens remain correct.

Opaque statement bodies and generic raw-SQL nodes do not satisfy the contract.

## Test structure

- `tests/sqlparser_oracle.rs` protects implemented behavior.
- `tests/sqlparser_oracle_compat.rs` is the compatibility suite.
- `tests/oracle_compat/relational.rs` covers lexical forms, expressions,
  queries, DML, transactions, relational DDL, types, conditions, and
  pseudocolumns.
- `tests/oracle_compat/plsql.rs` covers anonymous blocks, declarations,
  control flow, cursors, exceptions, dynamic SQL, bulk SQL, stored units,
  triggers, object types, call specifications, conditional compilation, JSON,
  and vectors.
- `tests/oracle_compat/statements.rs` covers every SQL statement family in the
  26ai SQL statement inventory.
- `tests/oracle_compat/inventory.rs` fails if an SQL statement family or PL/SQL
  language element has no fixture.

The current positive corpus contains 534 fixtures: 226 relational, 156 PL/SQL,
and 152 additional statement-family cases. It is paired with focused AST
assertions, formatter/reparse equality checks, dialect-isolation checks, and
invalid-grammar cases.

Each grammar obligation is explicitly classified as Oracle-specific or shared.
Every Oracle-specific obligation names a positive isolation fixture that must
parse with `OracleDialect` and be rejected by `PostgreSqlDialect`.

## Implementation order

### 1. Lexical layer

- Alternative quoting (`q'...'`) with paired and arbitrary delimiters
- Named and positional bind variables
- Oracle numeric suffixes
- National character literals
- Conditional-compilation tokens and inquiry directives
- Oracle identifier length and character rules

### 2. Expressions and queries

- Flashback and partition-extension clauses
- Complete hierarchical-query grammar
- Partitioned outer joins and `APPLY`
- `MODEL`, `PIVOT`, `UNPIVOT`, and `MATCH_RECOGNIZE`
- Row-limiting, `QUALIFY`, and `FOR UPDATE`
- Oracle aggregate, analytic, object, collection, XML, JSON, graph, domain,
  and vector syntax

### 3. DML and transaction control

- Multitable insert
- Oracle `RETURNING INTO`
- DML table and partition extensions
- Error logging
- Complete `MERGE`
- Oracle commit, rollback, savepoint, lock, role, and transaction clauses

### 4. Relational DDL

- Oracle column definitions and constraints
- Identity, virtual, invisible, period, external, temporary, immutable, and
  blockchain tables
- Oracle partitioning
- Index, view, materialized-view, sequence, synonym, domain, graph, and vector
  objects
- Privilege and metadata statements

### 5. PL/SQL AST

- A PL/SQL block node with declaration, executable, and exception sections
- PL/SQL declarations for scalars, constants, subtypes, records, collections,
  cursors, exceptions, and nested subprograms
- Native statement nodes for assignment, invocation, control flow, cursor
  operations, dynamic SQL, bulk SQL, pragmas, `GOTO`, `PIPE ROW`, and `NULL`
- PL/SQL expression additions including attributes, qualified expressions,
  collection methods, inquiry directives, and dangling predicates

### 6. Stored PL/SQL units

- Function and procedure specifications and bodies
- Package specifications, package bodies, initialization, and state clauses
- Simple, compound, schema, database, and instead-of triggers
- Object type specifications and bodies
- Java, C, and JavaScript call specifications
- Alter and drop forms for every stored unit

### 7. Administrative statements

Implement the remaining statement inventory in object-family groups. Each
family gets a typed AST, parser, formatter, span implementation, and focused
AST assertions.

## Completion gates

- Every inventory entry has at least one fixture.
- Every fixture parses locally and reparses after formatting.
- Every Oracle-specific construct has focused AST assertions.
- No PL/SQL body is stored as unparsed text.
- A final coverage pass compares the corpus against the SQL statement
  inventory, PL/SQL language-element inventory, SQL subclauses, data types,
  conditions, pseudocolumns, and 26ai release additions.
