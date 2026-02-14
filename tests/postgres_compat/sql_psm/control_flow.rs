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

//! Tests for PL/pgSQL control flow statements
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql-control-structures.html>

use crate::postgres_compat::common::*;

// =============================================================================
// IF Statements
// =============================================================================

#[test]
fn test_if_then() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS
    // Basic IF-THEN statement
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    IF TRUE THEN
        RAISE NOTICE 'condition is true';
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_if_then_else() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS
    // IF-THEN-ELSE statement
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    IF 1 > 2 THEN
        RAISE NOTICE 'impossible';
    ELSE
        RAISE NOTICE 'expected';
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_if_then_elsif() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS
    // IF-THEN-ELSIF statement
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS TEXT AS $$
BEGIN
    IF x > 0 THEN
        RETURN 'positive';
    ELSIF x < 0 THEN
        RETURN 'negative';
    ELSE
        RETURN 'zero';
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_if_with_multiple_elsif() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS
    // Multiple ELSIF branches
    pg_expect_parse_error!(
        r#"CREATE FUNCTION grade(score INTEGER) RETURNS CHAR AS $$
BEGIN
    IF score >= 90 THEN
        RETURN 'A';
    ELSIF score >= 80 THEN
        RETURN 'B';
    ELSIF score >= 70 THEN
        RETURN 'C';
    ELSIF score >= 60 THEN
        RETURN 'D';
    ELSE
        RETURN 'F';
    END IF;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// CASE Statements
// =============================================================================

#[test]
fn test_case_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS-CASE
    // Simple CASE statement
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS TEXT AS $$
BEGIN
    CASE x
        WHEN 1 THEN
            RETURN 'one';
        WHEN 2 THEN
            RETURN 'two';
        ELSE
            RETURN 'other';
    END CASE;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_case_searched() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS-CASE
    // Searched CASE statement (with boolean conditions)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS TEXT AS $$
BEGIN
    CASE
        WHEN x < 0 THEN
            RETURN 'negative';
        WHEN x = 0 THEN
            RETURN 'zero';
        WHEN x > 0 THEN
            RETURN 'positive';
    END CASE;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_case_without_else() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONDITIONALS-CASE
    // CASE without ELSE (raises error if no match)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(x INTEGER) RETURNS TEXT AS $$
BEGIN
    CASE x
        WHEN 1 THEN
            RETURN 'one';
        WHEN 2 THEN
            RETURN 'two';
    END CASE;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// LOOP Statements
// =============================================================================

#[test]
fn test_loop_basic() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONTROL-STRUCTURES-LOOPS
    // Basic infinite LOOP with EXIT
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER := 0;
BEGIN
    LOOP
        i := i + 1;
        EXIT WHEN i > 10;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_loop_with_label() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONTROL-STRUCTURES-LOOPS
    // LOOP with label for EXIT
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER := 0;
BEGIN
    <<my_loop>>
    LOOP
        i := i + 1;
        EXIT my_loop WHEN i > 10;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_loop_with_continue() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONTROL-STRUCTURES-LOOPS
    // LOOP with CONTINUE statement
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER := 0;
BEGIN
    LOOP
        i := i + 1;
        CONTINUE WHEN i % 2 = 0;
        RAISE NOTICE 'Odd number: %', i;
        EXIT WHEN i > 10;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// WHILE Loops
// =============================================================================

#[test]
fn test_while_basic() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONTROL-STRUCTURES-LOOPS-WHILE
    // Basic WHILE loop
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER := 0;
BEGIN
    WHILE i < 10 LOOP
        i := i + 1;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_while_with_exit() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONTROL-STRUCTURES-LOOPS-WHILE
    // WHILE loop with EXIT
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER := 0;
BEGIN
    WHILE i < 100 LOOP
        i := i + 1;
        EXIT WHEN i = 50;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// FOR Loops (Integer Range)
// =============================================================================

#[test]
fn test_for_integer_range() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-INTEGER-FOR-LOOPS
    // FOR loop over integer range
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..10 LOOP
        RAISE NOTICE 'i = %', i;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_for_integer_range_reverse() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-INTEGER-FOR-LOOPS
    // FOR loop in reverse order
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN REVERSE 10..1 LOOP
        RAISE NOTICE 'Countdown: %', i;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_for_integer_range_by_step() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-INTEGER-FOR-LOOPS
    // FOR loop with BY step
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 0..100 BY 10 LOOP
        RAISE NOTICE 'i = %', i;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_for_integer_dynamic_bounds() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-INTEGER-FOR-LOOPS
    // FOR loop with dynamic bounds from expressions
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(start_val INTEGER, end_val INTEGER) RETURNS void AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN start_val..end_val LOOP
        RAISE NOTICE 'i = %', i;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// FOR Loops (Query Results)
// =============================================================================

#[test]
fn test_for_query_loop() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
    // FOR loop over query results
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM users LOOP
        RAISE NOTICE 'User: %', r.name;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_for_query_with_parameters() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
    // FOR loop with parameterized query
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(min_age INTEGER) RETURNS void AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM users WHERE age >= min_age LOOP
        RAISE NOTICE 'User: % (age %)', r.name, r.age;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_for_dynamic_query() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
    // FOR loop with EXECUTE for dynamic query
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT) RETURNS void AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN EXECUTE 'SELECT * FROM ' || quote_ident(table_name) LOOP
        RAISE NOTICE 'Row: %', r;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// FOREACH Loops (Array Iteration)
// =============================================================================

#[test]
fn test_foreach_array() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-FOREACH-ARRAY
    // FOREACH loop over array elements
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    arr INTEGER[] := ARRAY[1, 2, 3, 4, 5];
    elem INTEGER;
BEGIN
    FOREACH elem IN ARRAY arr LOOP
        RAISE NOTICE 'Element: %', elem;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_foreach_slice() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-FOREACH-ARRAY
    // FOREACH with SLICE for multi-dimensional arrays
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    arr INTEGER[][] := ARRAY[[1,2],[3,4],[5,6]];
    slice INTEGER[];
BEGIN
    FOREACH slice SLICE 1 IN ARRAY arr LOOP
        RAISE NOTICE 'Slice: %', slice;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Nested Loops
// =============================================================================

#[test]
fn test_nested_loops() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONTROL-STRUCTURES-LOOPS
    // Nested FOR loops
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER;
    j INTEGER;
BEGIN
    FOR i IN 1..3 LOOP
        FOR j IN 1..3 LOOP
            RAISE NOTICE 'i=%, j=%', i, j;
        END LOOP;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_nested_loops_with_labels() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-CONTROL-STRUCTURES-LOOPS
    // Nested loops with labels for EXIT/CONTINUE
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER;
    j INTEGER;
BEGIN
    <<outer>>
    FOR i IN 1..5 LOOP
        <<inner>>
        FOR j IN 1..5 LOOP
            CONTINUE outer WHEN i = j;
            RAISE NOTICE 'i=%, j=%', i, j;
        END LOOP;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// EXIT and CONTINUE Statements
// =============================================================================

#[test]
fn test_exit_when() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-EXIT
    // EXIT WHEN condition
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER := 0;
BEGIN
    LOOP
        i := i + 1;
        EXIT WHEN i > 100;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_exit_with_label() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-EXIT
    // EXIT specific loop by label
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    <<outer>>
    LOOP
        <<inner>>
        LOOP
            EXIT outer;
        END LOOP;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_continue_when() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-CONTINUE
    // CONTINUE WHEN condition
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..10 LOOP
        CONTINUE WHEN i % 2 = 0;
        RAISE NOTICE 'Odd: %', i;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_continue_with_label() {
    // https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-STATEMENTS-CONTINUE
    // CONTINUE to specific loop by label
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    <<outer>>
    FOR i IN 1..5 LOOP
        FOR j IN 1..5 LOOP
            CONTINUE outer WHEN i = j;
        END LOOP;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}
