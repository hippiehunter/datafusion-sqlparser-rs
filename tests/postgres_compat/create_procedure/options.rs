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

//! Tests for CREATE PROCEDURE options and attributes
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createprocedure.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{ProcedureSecurity, Statement};

// ============================================================================
// LANGUAGE Options
// ============================================================================

#[test]
fn test_create_procedure_language_sql() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // LANGUAGE SQL for SQL-language procedures
    pg_test!(
        "CREATE PROCEDURE sql_proc() LANGUAGE SQL AS $$ INSERT INTO tbl VALUES (1) $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert!(proc.language.is_some());
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "SQL");
        }
    );
}

#[test]
fn test_create_procedure_language_plpgsql() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // LANGUAGE plpgsql is the most common procedural language
    pg_test!(
        "CREATE PROCEDURE plpgsql_proc() LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'test'; END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert!(proc.language.is_some());
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "plpgsql");
        }
    );
}

#[test]
fn test_create_procedure_language_c() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // External languages like C are supported
    pg_expect_parse_error!("CREATE PROCEDURE c_proc() LANGUAGE C AS 'my_library', 'my_function'");
}

#[test]
fn test_create_procedure_language_python() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // PL/Python and other procedural languages
    pg_expect_parse_error!(
        "CREATE PROCEDURE python_proc() LANGUAGE plpython3u AS $$ plpy.notice('test') $$"
    );
}

#[test]
fn test_create_procedure_no_language_clause() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // LANGUAGE clause is optional in some dialects (defaults to SQL)
    pg_test!(
        "CREATE PROCEDURE no_lang() AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let _ = extract_create_procedure(&stmt);
            // Language might be None or default to SQL
        }
    );
}

// ============================================================================
// SECURITY Options
// ============================================================================

#[test]
fn test_create_procedure_security_definer() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // SECURITY DEFINER runs with privileges of procedure owner
    pg_test!(
        "CREATE PROCEDURE sec_definer() SECURITY DEFINER AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.security, &Some(ProcedureSecurity::Definer));
        }
    );
}

#[test]
fn test_create_procedure_security_invoker() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // SECURITY INVOKER runs with privileges of caller (default)
    pg_test!(
        "CREATE PROCEDURE sec_invoker() SECURITY INVOKER AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.security, &Some(ProcedureSecurity::Invoker));
        }
    );
}

#[test]
fn test_create_procedure_external_security_definer() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // EXTERNAL SECURITY DEFINER is an alternative syntax
    pg_test!(
        "CREATE PROCEDURE ext_sec_definer() EXTERNAL SECURITY DEFINER AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.security, &Some(ProcedureSecurity::Definer));
        }
    );
}

#[test]
fn test_create_procedure_external_security_invoker() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // EXTERNAL SECURITY INVOKER is an alternative syntax
    pg_test!(
        "CREATE PROCEDURE ext_sec_invoker() EXTERNAL SECURITY INVOKER AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.security, &Some(ProcedureSecurity::Invoker));
        }
    );
}

// ============================================================================
// SET Configuration
// ============================================================================

#[test]
fn test_create_procedure_set_single_parameter() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // SET allows setting runtime configuration for the procedure
    pg_test!(
        "CREATE PROCEDURE set_param() SET search_path = public AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.set_options.len(), 1);
            assert_eq!(proc.set_options[0].to_string(), "SET search_path TO public");
        }
    );
}

#[test]
fn test_create_procedure_set_multiple_parameters() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Multiple SET clauses can be specified
    pg_test!(
        "CREATE PROCEDURE multi_set() SET search_path = public SET work_mem = '16MB' AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.set_options.len(), 2);
            assert_eq!(proc.set_options[0].to_string(), "SET search_path TO public");
            assert_eq!(proc.set_options[1].to_string(), "SET work_mem TO '16MB'");
        }
    );
}

#[test]
fn test_create_procedure_set_timezone() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Common use: setting timezone for the procedure
    pg_test!(
        "CREATE PROCEDURE set_tz() SET timezone = 'UTC' AS BEGIN SELECT NOW(); END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.set_options.len(), 1);
            assert_eq!(proc.set_options[0].to_string(), "SET timezone TO 'UTC'");
        }
    );
}

#[test]
fn test_create_procedure_set_from_current() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // SET can use FROM CURRENT to capture current session value
    pg_test!(
        "CREATE PROCEDURE set_current() SET search_path FROM CURRENT AS BEGIN SELECT 1; END",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.set_options.len(), 1);
            assert_eq!(
                proc.set_options[0].to_string(),
                "SET search_path FROM CURRENT"
            );
        }
    );
}

// ============================================================================
// Combined Options
// ============================================================================

#[test]
fn test_create_procedure_language_and_security() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Language and security options can be combined
    pg_test!(
        "CREATE PROCEDURE lang_sec() LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "plpgsql");
            assert_eq!(proc.security, &Some(ProcedureSecurity::Definer));
        }
    );
}

#[test]
fn test_create_procedure_all_options() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // All options combined
    pg_test!(
        "CREATE PROCEDURE all_opts() LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$ BEGIN END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "plpgsql");
            assert_eq!(proc.security, &Some(ProcedureSecurity::Definer));
            assert_eq!(proc.set_options.len(), 1);
            assert_eq!(proc.set_options[0].to_string(), "SET search_path TO public");
        }
    );
}

// ============================================================================
// Option Ordering
// ============================================================================

#[test]
fn test_create_procedure_language_before_as() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // LANGUAGE typically comes before AS
    pg_test!(
        "CREATE PROCEDURE lang_first() LANGUAGE plpgsql AS $$ BEGIN END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert_eq!(proc.language.as_ref().unwrap().to_string(), "plpgsql");
            assert!(proc.has_as);
        }
    );
}

#[test]
fn test_create_procedure_language_after_as() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Some dialects allow LANGUAGE after AS
    // This may or may not be supported
    pg_test!(
        "CREATE PROCEDURE lang_after() AS $$ BEGIN END $$ LANGUAGE plpgsql",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert!(proc.has_as);
            // Language position may vary
        }
    );
}

// ============================================================================
// TRANSFORM Options (PostgreSQL 9.5+, LIKELY FAIL)
// ============================================================================

#[test]
fn test_create_procedure_transform_not_supported() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // TRANSFORM controls type transformation for non-SQL languages
    // This is likely not yet implemented
    pg_expect_parse_error!(
        "CREATE PROCEDURE transform_test() TRANSFORM FOR TYPE int AS BEGIN SELECT 1; END"
    );
}

// ============================================================================
// WINDOW Option (functions only, should fail for procedures)
// ============================================================================

#[test]
fn test_create_procedure_window_not_valid() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // WINDOW is only valid for functions, not procedures
    pg_expect_parse_error!("CREATE PROCEDURE window_proc() WINDOW AS BEGIN SELECT 1; END");
}

// ============================================================================
// PostgreSQL-Specific Language Names
// ============================================================================

#[test]
fn test_create_procedure_language_internal() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // INTERNAL language for built-in procedures
    pg_expect_parse_error!("CREATE PROCEDURE internal_proc() LANGUAGE internal AS 'internal_func'");
}

#[test]
fn test_create_procedure_language_plperl() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // PL/Perl procedural language
    pg_expect_parse_error!(
        "CREATE PROCEDURE perl_proc() LANGUAGE plperl AS $$ elog(NOTICE, 'test'); $$"
    );
}

#[test]
fn test_create_procedure_language_pltcl() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // PL/Tcl procedural language
    pg_expect_parse_error!("CREATE PROCEDURE tcl_proc() LANGUAGE pltcl AS $$ elog NOTICE test $$");
}

// ============================================================================
// Quoted Language Names
// ============================================================================

#[test]
fn test_create_procedure_quoted_language_name() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Language names can be quoted (though unusual)
    pg_test!(
        r#"CREATE PROCEDURE quoted_lang() LANGUAGE "plpgsql" AS $$ BEGIN END $$"#,
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert!(proc.language.is_some());
        }
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_create_procedure_multiple_language_clauses_invalid() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Multiple LANGUAGE clauses should be an error
    pg_expect_parse_error!(
        "CREATE PROCEDURE multi_lang() LANGUAGE sql LANGUAGE plpgsql AS BEGIN END"
    );
}

#[test]
fn test_create_procedure_language_case_variations() {
    // https://www.postgresql.org/docs/current/sql-createprocedure.html
    // Language keyword is case-insensitive
    pg_test!(
        "CREATE PROCEDURE lang_case() language plpgsql AS $$ BEGIN END $$",
        |stmt: Statement| {
            let proc = extract_create_procedure(&stmt);
            assert!(proc.language.is_some());
        }
    );
}
