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

//! Tests for PL/pgSQL cursor operations
//!
//! Reference: <https://www.postgresql.org/docs/current/plpgsql-cursors.html>

use crate::postgres_compat::common::*;

// =============================================================================
// Cursor Declaration
// =============================================================================

#[test]
fn test_declare_cursor_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-DECLARATIONS
    // Simple cursor declaration in DECLARE block
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
BEGIN
    NULL;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_cursor_with_parameters() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-DECLARATIONS
    // Cursor with parameters
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR (min_age INTEGER) FOR SELECT * FROM users WHERE age >= min_age;
BEGIN
    NULL;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_cursor_no_scroll() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-DECLARATIONS
    // NO SCROLL cursor (forward-only)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs NO SCROLL CURSOR FOR SELECT * FROM users;
BEGIN
    NULL;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_declare_cursor_scroll() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-DECLARATIONS
    // SCROLL cursor (allows backward movement)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs SCROLL CURSOR FOR SELECT * FROM users;
BEGIN
    NULL;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// OPEN Cursor
// =============================================================================

#[test]
fn test_open_cursor_simple() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-OPENING
    // Opening a cursor
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
BEGIN
    OPEN curs;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_open_cursor_with_parameters() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-OPENING
    // Opening cursor with parameters
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR (min_age INTEGER) FOR SELECT * FROM users WHERE age >= min_age;
BEGIN
    OPEN curs(18);
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_open_cursor_for_query() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-OPENING
    // OPEN FOR query (unbound cursor variable)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR SELECT * FROM users;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_open_cursor_for_execute() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-OPENING
    // OPEN FOR EXECUTE (dynamic query)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test(table_name TEXT) RETURNS void AS $$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR EXECUTE 'SELECT * FROM ' || quote_ident(table_name);
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// FETCH from Cursor
// =============================================================================

#[test]
fn test_fetch_cursor() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH next row from cursor
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_next() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH NEXT from cursor (explicit)
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH NEXT FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_prior() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH PRIOR (previous row) - requires SCROLL cursor
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs SCROLL CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH NEXT FROM curs INTO rec;
    FETCH PRIOR FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_first() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH FIRST row
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs SCROLL CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH FIRST FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_last() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH LAST row
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs SCROLL CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH LAST FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_absolute() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH ABSOLUTE position
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs SCROLL CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH ABSOLUTE 5 FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_relative() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH RELATIVE offset
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs SCROLL CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH RELATIVE 3 FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_forward() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH FORWARD count
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH FORWARD 10 FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_fetch_cursor_backward() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FETCH BACKWARD count
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs SCROLL CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH LAST FROM curs INTO rec;
    FETCH BACKWARD 5 FROM curs INTO rec;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// MOVE Cursor
// =============================================================================

#[test]
fn test_move_cursor() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // MOVE cursor position without retrieving row
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
BEGIN
    OPEN curs;
    MOVE FORWARD 10 FROM curs;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// CLOSE Cursor
// =============================================================================

#[test]
fn test_close_cursor() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // Closing a cursor
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
BEGIN
    OPEN curs;
    -- Use cursor
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Cursor Loops
// =============================================================================

#[test]
fn test_cursor_loop() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-FOR-LOOP
    // FOR loop with cursor
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    FOR rec IN curs LOOP
        RAISE NOTICE 'User: %', rec.name;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_cursor_loop_with_exit() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-FOR-LOOP
    // Cursor loop with early exit
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
    rec RECORD;
    count INTEGER := 0;
BEGIN
    FOR rec IN curs LOOP
        count := count + 1;
        EXIT WHEN count > 10;
        RAISE NOTICE 'User: %', rec.name;
    END LOOP;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// Cursor Variables (refcursor)
// =============================================================================

#[test]
fn test_refcursor_variable() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-RETURNING
    // refcursor variable for returning cursor to caller
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS refcursor AS $$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR SELECT * FROM users;
    RETURN curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_refcursor_with_name() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-RETURNING
    // Named refcursor
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS refcursor AS $$
DECLARE
    curs refcursor := 'my_cursor';
BEGIN
    OPEN curs FOR SELECT * FROM users;
    RETURN curs;
END $$ LANGUAGE plpgsql"#
    );
}

// =============================================================================
// FOUND and cursor status
// =============================================================================

#[test]
fn test_cursor_found() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // FOUND variable after FETCH
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    FETCH curs INTO rec;
    IF FOUND THEN
        RAISE NOTICE 'Row found';
    ELSE
        RAISE NOTICE 'No more rows';
    END IF;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}

#[test]
fn test_cursor_loop_with_found() {
    // https://www.postgresql.org/docs/current/plpgsql-cursors.html#PLPGSQL-CURSOR-USING
    // Manual cursor loop using FOUND
    pg_expect_parse_error!(
        r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    curs CURSOR FOR SELECT * FROM users;
    rec RECORD;
BEGIN
    OPEN curs;
    LOOP
        FETCH curs INTO rec;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE 'User: %', rec.name;
    END LOOP;
    CLOSE curs;
END $$ LANGUAGE plpgsql"#
    );
}
