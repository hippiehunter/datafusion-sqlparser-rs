# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build and Test
```bash
cargo build                    # Build the library
cargo test                     # Run all tests
cargo fmt                      # Format code
cargo clippy                   # Lint with clippy

# Run dialect-specific tests
cargo test postgres            # PostgreSQL dialect tests
cargo test mysql               # MySQL dialect tests
cargo test mssql               # MS SQL Server dialect tests

# Run specific test patterns
cargo test parse_select        # Tests matching pattern
cargo test -- --nocapture     # Show test output
```

### Features and Examples
```bash
cargo build --features serde           # Enable serde serialization
cargo build --features visitor         # Enable AST visitor pattern
cargo run --example cli FILENAME.sql   # Parse SQL file with CLI
```

### Benchmarking
```bash
cd sqlparser_bench
cargo bench -- --save-baseline main    # Save performance baseline
cargo bench -- --baseline main         # Compare against baseline
```

## Architecture

This is a hand-written recursive descent SQL parser with dialect support. Key components:

### Core Parsing Pipeline
1. **Tokenizer** (`src/tokenizer.rs`) - Lexical analysis with location tracking
2. **Parser** (`src/parser/mod.rs`) - Recursive descent with Pratt parser for expressions
3. **AST** (`src/ast/`) - Comprehensive syntax tree with span information
4. **Dialects** (`src/dialect/`) - SQL variant-specific parsing rules

### Main AST Modules
- `ast/query.rs` - SELECT statements and query expressions
- `ast/ddl.rs` - CREATE, ALTER, DROP statements
- `ast/dml.rs` - INSERT, UPDATE, DELETE statements  
- `ast/data_type.rs` - SQL type definitions
- `ast/value.rs` - Literals and constants
- `ast/operator.rs` - Binary/unary operators

### Dialect System
Each dialect (`GenericDialect`, `PostgreSqlDialect`, `MySqlDialect`, etc.) implements the `Dialect` trait to customize:
- Identifier rules and quoting
- Reserved keywords
- Operator precedence
- Syntax extensions

### Test Organization
- Integration tests by dialect: `tests/sqlparser_postgres.rs`, `tests/sqlparser_mysql.rs`, etc.
- Common functionality: `tests/sqlparser_common.rs`
- TPC-H benchmark queries: `tests/queries/tpch/`
- Test utilities: `tests/test_utils/mod.rs` and `src/test_utils.rs`

## Key Patterns

### Adding New SQL Syntax
1. Add AST nodes in appropriate `ast/` module
2. Implement parsing logic in `parser/mod.rs` or `parser/alter.rs`
3. Add Display implementation for SQL generation
4. Add comprehensive tests in relevant test file

### Supporting New Dialects
1. Create dialect module in `src/dialect/`
2. Implement `Dialect` trait with custom rules
3. Add dialect-specific tests in `tests/`
4. Export from `src/dialect/mod.rs`

### Parser Design
- Uses **recursive descent** for statements and clauses
- Uses **Pratt parser** for expressions with operator precedence
- Maintains **source spans** for error reporting and tooling
- Supports **round-trip** parsing (SQL → AST → SQL)

The codebase prioritizes correctness, performance, and SQL standard compliance across multiple database dialects.