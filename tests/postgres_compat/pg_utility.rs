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

//! Tests for the PostgreSQL utility and transaction statements.
//!
//! References:
//! - <https://www.postgresql.org/docs/current/sql-vacuum.html>
//! - <https://www.postgresql.org/docs/current/sql-analyze.html>
//! - <https://www.postgresql.org/docs/current/sql-lock.html>
//! - <https://www.postgresql.org/docs/current/sql-abort.html>
//! - <https://www.postgresql.org/docs/current/sql-begin.html>
//! - <https://www.postgresql.org/docs/current/sql-set-transaction.html>
//! - <https://www.postgresql.org/docs/current/sql-prepare-transaction.html>
//! - <https://www.postgresql.org/docs/current/sql-fetch.html>
//! - <https://www.postgresql.org/docs/current/sql-move.html>
//! - <https://www.postgresql.org/docs/current/sql-select.html>
//! - <https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html>
//! - <https://www.postgresql.org/docs/current/sql-reset.html>

use sqlparser::ast::{
    Analyze, FetchDirection, Ident, PgLockTable, PgLockTableMode, PgRelationExpr,
    PreparedTransactionAction, Query, Reset, ResetStatement, Set, SetExpr, Statement,
    TransactionAccessMode, TransactionIsolationLevel, TransactionMode, VacuumOption,
    VacuumOptionName, VacuumOptionValue, VacuumStatement, Value,
};

use crate::postgres_compat::common::{try_parse_pg, verified_pg_stmt};

/// Parses `sql`, then asserts that the rendered statement re-parses to the very
/// same AST, and returns the statement.
fn verified(sql: &str) -> Statement {
    let statement = verified_pg_stmt(sql);
    let rendered = statement.to_string();
    let reparsed = try_parse_pg(&rendered).unwrap_or_else(|e| panic!("{rendered}: {e}"));
    assert_eq!(
        vec![statement.clone()],
        reparsed,
        "round-trip changed the AST: {sql} -> {rendered}"
    );
    statement
}

/// Like [`verified`], but also pins the rendered form.
fn verified_to(sql: &str, canonical: &str) -> Statement {
    let statement = verified(sql);
    assert_eq!(canonical, statement.to_string());
    statement
}

fn vacuum(sql: &str) -> VacuumStatement {
    match verified(sql) {
        Statement::Vacuum(statement) => statement,
        other => panic!("expected VACUUM, got {other:?}"),
    }
}

fn analyze(sql: &str) -> Analyze {
    match verified(sql) {
        Statement::Analyze(statement) => statement,
        other => panic!("expected ANALYZE, got {other:?}"),
    }
}

fn lock(sql: &str) -> PgLockTable {
    match verified(sql) {
        Statement::PgLockTable(statement) => statement,
        other => panic!("expected LOCK, got {other:?}"),
    }
}

fn query(sql: &str) -> Query {
    match verified(sql) {
        Statement::Query(query) => query.as_ref().clone(),
        other => panic!("expected a query, got {other:?}"),
    }
}

fn option(name: VacuumOptionName, value: Option<VacuumOptionValue>) -> VacuumOption {
    VacuumOption { name, value }
}

fn number(text: &str) -> Value {
    Value::Number(text.parse().unwrap_or_else(|_| panic!("{text}")), false)
}

// =============================================================================
// VACUUM
// =============================================================================

#[test]
fn vacuum_without_arguments_targets_the_whole_database() {
    let statement = vacuum("VACUUM");
    assert!(statement.relations.is_empty());
    assert!(statement.options.is_empty());
    assert_eq!(None, statement.table_name);
}

#[test]
fn vacuum_keyword_flags_parse_in_the_postgres_order() {
    let statement = vacuum("VACUUM FULL FREEZE VERBOSE ANALYZE se_vac_kw_a");
    assert!(statement.full);
    assert!(statement.freeze);
    assert!(statement.verbose);
    assert!(statement.analyze);
    assert!(statement.options.is_empty());
    assert_eq!(
        Some("se_vac_kw_a".to_owned()),
        statement.table_name.as_ref().map(ToString::to_string)
    );

    for sql in [
        "VACUUM FULL t",
        "VACUUM FREEZE t",
        "VACUUM VERBOSE t",
        "VACUUM ANALYZE t",
        "VACUUM FULL FREEZE t",
        "VACUUM FREEZE ANALYZE t",
    ] {
        verified_to(sql, sql);
    }
}

#[test]
fn vacuum_option_list_records_every_postgres_option() {
    let statement = vacuum(
        "VACUUM (FULL, FREEZE, VERBOSE, ANALYZE, DISABLE_PAGE_SKIPPING, SKIP_LOCKED, \
         INDEX_CLEANUP AUTO, PROCESS_MAIN FALSE, PROCESS_TOAST FALSE, TRUNCATE FALSE, \
         PARALLEL 2, BUFFER_USAGE_LIMIT '512 kB', SKIP_DATABASE_STATS, ONLY_DATABASE_STATS) t",
    );
    assert_eq!(
        vec![
            option(VacuumOptionName::Full, None),
            option(VacuumOptionName::Freeze, None),
            option(VacuumOptionName::Verbose, None),
            option(VacuumOptionName::Analyze, None),
            option(VacuumOptionName::DisablePageSkipping, None),
            option(VacuumOptionName::SkipLocked, None),
            option(
                VacuumOptionName::IndexCleanup,
                Some(VacuumOptionValue::Word(Ident::new("auto"))),
            ),
            option(
                VacuumOptionName::ProcessMain,
                Some(VacuumOptionValue::Boolean(false)),
            ),
            option(
                VacuumOptionName::ProcessToast,
                Some(VacuumOptionValue::Boolean(false)),
            ),
            option(
                VacuumOptionName::Truncate,
                Some(VacuumOptionValue::Boolean(false)),
            ),
            option(
                VacuumOptionName::Parallel,
                Some(VacuumOptionValue::Number(number("2"))),
            ),
            option(
                VacuumOptionName::BufferUsageLimit,
                Some(VacuumOptionValue::StringLiteral(Value::SingleQuotedString(
                    "512 kB".to_owned(),
                ))),
            ),
            option(VacuumOptionName::SkipDatabaseStats, None),
            option(VacuumOptionName::OnlyDatabaseStats, None),
        ],
        statement.options
    );
}

#[test]
fn vacuum_option_flags_mirror_the_keyword_flags() {
    let statement = vacuum("VACUUM (FULL, ANALYZE) t");
    assert!(statement.full);
    assert!(statement.analyze);

    let statement = vacuum("VACUUM (FULL TRUE, ANALYZE FALSE) t");
    assert!(statement.full);
    assert!(!statement.analyze);

    for (sql, enabled) in [
        ("VACUUM (FULL ON) t", true),
        ("VACUUM (FULL OFF) t", false),
        ("VACUUM (FULL 1) t", true),
        ("VACUUM (FULL 0) t", false),
    ] {
        assert_eq!(enabled, vacuum(sql).full, "{sql}");
    }
}

#[test]
fn vacuum_option_with_an_unrecognized_name_still_parses() {
    let statement = vacuum("VACUUM (bogus_option 3) t");
    assert_eq!(
        vec![option(
            VacuumOptionName::Other(Ident::new("bogus_option")),
            Some(VacuumOptionValue::Number(number("3"))),
        )],
        statement.options
    );
    assert!(try_parse_pg("VACUUM () t").is_err());
}

#[test]
fn vacuum_option_takes_a_negative_number() {
    let statement = vacuum("VACUUM (PARALLEL -1) t");
    assert_eq!(
        vec![option(
            VacuumOptionName::Parallel,
            Some(VacuumOptionValue::Number(number("-1"))),
        )],
        statement.options
    );
}

#[test]
fn vacuum_relation_carries_only_star_and_columns() {
    let statement = vacuum("VACUUM ONLY se_vac_only_part");
    assert!(statement.relations[0].relation.only);

    let statement = vacuum("VACUUM ONLY (t)");
    assert!(statement.relations[0].relation.only);
    assert!(statement.relations[0].relation.parenthesized);

    let statement = vacuum("VACUUM t *");
    assert!(statement.relations[0].relation.descendants);

    let statement = vacuum("VACUUM ANALYZE se_vac_cols_a (i, t)");
    assert_eq!(
        vec![Ident::new("i"), Ident::new("t")],
        statement.relations[0].columns.as_slice()
    );
}

#[test]
fn vacuum_takes_a_list_of_relations() {
    let statement = vacuum("VACUUM ANALYZE se_vac_multi_a, se_vac_multi_b (i)");
    assert_eq!(2, statement.relations.len());
    assert_eq!("se_vac_multi_a", statement.relations[0].to_string());
    assert_eq!("se_vac_multi_b (i)", statement.relations[1].to_string());
    assert_eq!(
        Some("se_vac_multi_a".to_owned()),
        statement.table_name.as_ref().map(ToString::to_string),
        "table_name keeps naming the first relation"
    );
}

#[test]
fn vacuum_keeps_the_redshift_threshold_and_boost_suffixes() {
    let statement = vacuum("VACUUM FULL t TO 75 PERCENT BOOST");
    assert!(statement.boost);
    assert_eq!(Some(number("75")), statement.threshold);
    assert_eq!("t", statement.relations[0].to_string());

    assert_eq!(
        "boost",
        vacuum("VACUUM boost").relations[0].to_string(),
        "a table may still be named boost"
    );
}

// =============================================================================
// ANALYZE
// =============================================================================

#[test]
fn analyze_without_arguments_targets_the_whole_database() {
    let statement = analyze("ANALYZE");
    assert!(statement.relations.is_empty());
    assert!(statement.options.is_empty());
    assert!(!statement.verbose);
}

#[test]
fn analyze_verbose_parses_in_both_spellings() {
    assert!(analyze("ANALYZE VERBOSE se_vac_anz_a").verbose);
    assert!(analyze("ANALYZE VERBOSE").verbose);

    let statement = analyze("ANALYZE (VERBOSE) se_vac_anz_a");
    assert!(statement.verbose);
    assert_eq!(
        vec![option(VacuumOptionName::Verbose, None)],
        statement.options
    );
}

#[test]
fn analyze_accepts_its_own_option_set() {
    let statement =
        analyze("ANALYZE (SKIP_LOCKED, VERBOSE, BUFFER_USAGE_LIMIT '512 kB') se_vac_anz_a");
    assert_eq!(
        vec![
            option(VacuumOptionName::SkipLocked, None),
            option(VacuumOptionName::Verbose, None),
            option(
                VacuumOptionName::BufferUsageLimit,
                Some(VacuumOptionValue::StringLiteral(Value::SingleQuotedString(
                    "512 kB".to_owned(),
                ))),
            ),
        ],
        statement.options
    );

    assert_eq!(
        vec![option(
            VacuumOptionName::BufferUsageLimit,
            Some(VacuumOptionValue::Number(number("0"))),
        )],
        analyze("ANALYZE (BUFFER_USAGE_LIMIT 0) se_vac_anz_a").options
    );
}

#[test]
fn analyze_relation_carries_only_star_and_columns() {
    assert!(
        analyze("ANALYZE ONLY se_vac_only_part").relations[0]
            .relation
            .only
    );
    assert!(analyze("ANALYZE t *").relations[0].relation.descendants);

    let statement = analyze("ANALYZE ONLY se_vac_only_part (a, b)");
    assert!(statement.relations[0].relation.only);
    assert_eq!(
        vec![Ident::new("a"), Ident::new("b")],
        statement.relations[0].columns.as_slice()
    );
}

#[test]
fn analyze_takes_a_list_of_relations() {
    let statement = analyze("ANALYZE se_vac_multi_a (i), ONLY se_vac_multi_b (t)");
    assert_eq!(2, statement.relations.len());
    assert_eq!("se_vac_multi_a (i)", statement.relations[0].to_string());
    assert_eq!(
        "ONLY se_vac_multi_b (t)",
        statement.relations[1].to_string()
    );
}

#[test]
fn analyze_table_keeps_the_hive_spelling() {
    let statement = analyze("ANALYZE TABLE test_table COMPUTE STATISTICS");
    assert!(statement.has_table_keyword);
    assert!(statement.compute_statistics);
    assert!(statement.relations.is_empty());
    assert_eq!("test_table", statement.table_name.to_string());
}

// =============================================================================
// Transactions
// =============================================================================

#[test]
fn abort_is_a_spelling_of_rollback() {
    for (sql, chain) in [
        ("ABORT", false),
        ("ABORT WORK", false),
        ("ABORT TRANSACTION", false),
        ("ABORT AND CHAIN", true),
        ("ABORT AND NO CHAIN", false),
        ("ABORT WORK AND CHAIN", true),
        ("ABORT TRANSACTION AND NO CHAIN", false),
    ] {
        match verified(sql) {
            Statement::Rollback {
                chain: parsed,
                savepoint,
                ..
            } => {
                assert_eq!(chain, parsed, "{sql}");
                assert_eq!(None, savepoint, "{sql}");
            }
            other => panic!("expected ROLLBACK for {sql}, got {other:?}"),
        }
    }
}

fn transaction_modes(sql: &str) -> Vec<TransactionMode> {
    match verified(sql) {
        Statement::StartTransaction { modes, .. } => modes,
        Statement::Set(set) => match &set.inner {
            Set::SetTransaction { modes, .. } => modes.clone(),
            other => panic!("expected SET TRANSACTION for {sql}, got {other:?}"),
        },
        other => panic!("expected a transaction statement for {sql}, got {other:?}"),
    }
}

#[test]
fn transaction_modes_include_deferrable() {
    assert_eq!(
        vec![
            TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
            TransactionMode::Deferrable(true),
        ],
        transaction_modes("START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE")
    );
    assert_eq!(
        vec![
            TransactionMode::IsolationLevel(TransactionIsolationLevel::ReadCommitted),
            TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
            TransactionMode::Deferrable(false),
        ],
        transaction_modes(
            "START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE, NOT DEFERRABLE"
        )
    );
    assert_eq!(
        vec![TransactionMode::Deferrable(true)],
        transaction_modes("BEGIN TRANSACTION DEFERRABLE")
    );
    assert_eq!(
        vec![TransactionMode::Deferrable(false)],
        transaction_modes("BEGIN NOT DEFERRABLE")
    );
    assert_eq!(
        vec![
            TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
            TransactionMode::Deferrable(true),
        ],
        transaction_modes("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE")
    );
    assert_eq!(
        vec![TransactionMode::Deferrable(false)],
        transaction_modes("SET SESSION CHARACTERISTICS AS TRANSACTION NOT DEFERRABLE")
    );
}

#[test]
fn transaction_modes_may_be_separated_by_spaces() {
    verified_to(
        "START TRANSACTION READ ONLY DEFERRABLE",
        "START TRANSACTION READ ONLY, DEFERRABLE",
    );
}

#[test]
fn two_phase_commit_commands_carry_the_gid() {
    for (sql, action) in [
        (
            "PREPARE TRANSACTION 'gid'",
            PreparedTransactionAction::Prepare,
        ),
        ("COMMIT PREPARED 'gid'", PreparedTransactionAction::Commit),
        (
            "ROLLBACK PREPARED 'gid'",
            PreparedTransactionAction::Rollback,
        ),
    ] {
        match verified_to(sql, sql) {
            Statement::PreparedTransaction(statement) => {
                assert_eq!(action, statement.action, "{sql}");
                assert_eq!(
                    Value::SingleQuotedString("gid".to_owned()),
                    statement.gid,
                    "{sql}"
                );
            }
            other => panic!("expected a two-phase command for {sql}, got {other:?}"),
        }
    }
}

#[test]
fn commit_and_rollback_take_work_transaction_and_chain() {
    for sql in [
        "COMMIT",
        "COMMIT WORK",
        "COMMIT TRANSACTION",
        "COMMIT AND CHAIN",
        "COMMIT AND NO CHAIN",
        "END",
        "END WORK AND CHAIN",
        "ROLLBACK",
        "ROLLBACK WORK AND CHAIN",
        "ROLLBACK TO SAVEPOINT sp",
    ] {
        verified(sql);
    }
}

// =============================================================================
// LOCK
// =============================================================================

#[test]
fn lock_table_takes_only_and_the_inheritance_star() {
    let statement = lock("LOCK TABLE se_lkt_targets * IN ACCESS EXCLUSIVE MODE");
    assert!(statement.relations[0].descendants);
    assert_eq!(Some(PgLockTableMode::AccessExclusive), statement.mode);
    assert_eq!(
        vec!["se_lkt_targets".to_owned()],
        statement
            .tables
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );

    let statement = lock("LOCK TABLE ONLY a, b * IN SHARE MODE");
    assert!(statement.only);
    assert!(statement.relations[0].only);
    assert!(!statement.relations[1].only);
    assert!(statement.relations[1].descendants);

    assert!(lock("LOCK TABLE ONLY (t)").relations[0].parenthesized);
}

#[test]
fn lock_table_covers_every_mode() {
    for (sql, mode) in [
        (
            "LOCK TABLE t IN ACCESS SHARE MODE",
            PgLockTableMode::AccessShare,
        ),
        ("LOCK TABLE t IN ROW SHARE MODE", PgLockTableMode::RowShare),
        (
            "LOCK TABLE t IN ROW EXCLUSIVE MODE",
            PgLockTableMode::RowExclusive,
        ),
        (
            "LOCK TABLE t IN SHARE UPDATE EXCLUSIVE MODE",
            PgLockTableMode::ShareUpdateExclusive,
        ),
        ("LOCK TABLE t IN SHARE MODE", PgLockTableMode::Share),
        (
            "LOCK TABLE t IN SHARE ROW EXCLUSIVE MODE",
            PgLockTableMode::ShareRowExclusive,
        ),
        ("LOCK TABLE t IN EXCLUSIVE MODE", PgLockTableMode::Exclusive),
        (
            "LOCK TABLE t IN ACCESS EXCLUSIVE MODE",
            PgLockTableMode::AccessExclusive,
        ),
    ] {
        assert_eq!(Some(mode), lock(sql).mode, "{sql}");
    }
    assert!(lock("LOCK TABLE t IN SHARE MODE NOWAIT").nowait);
    assert_eq!(None, lock("LOCK TABLE t").mode);
}

// =============================================================================
// RESET
// =============================================================================

#[test]
fn reset_names_the_two_word_parameters() {
    for (sql, parameter) in [
        ("RESET TIME ZONE", "timezone"),
        ("RESET SESSION AUTHORIZATION", "session_authorization"),
        ("RESET TRANSACTION ISOLATION LEVEL", "transaction_isolation"),
        ("RESET ROLE", "role"),
        (
            "RESET some_extension.some_parameter",
            "some_extension.some_parameter",
        ),
    ] {
        match verified(sql) {
            Statement::Reset(ResetStatement {
                reset: Reset::ConfigurationParameter(name),
                ..
            }) => assert_eq!(parameter, name.to_string(), "{sql}"),
            other => panic!("expected RESET for {sql}, got {other:?}"),
        }
    }
    match verified("RESET ALL") {
        Statement::Reset(ResetStatement { reset, .. }) => assert_eq!(Reset::ALL, reset),
        other => panic!("expected RESET ALL, got {other:?}"),
    }
}

// =============================================================================
// FETCH and MOVE
// =============================================================================

fn fetch_direction(sql: &str) -> FetchDirection {
    match verified(sql) {
        Statement::Fetch { direction, .. } | Statement::Move { direction, .. } => direction,
        other => panic!("expected FETCH or MOVE for {sql}, got {other:?}"),
    }
}

#[test]
fn fetch_takes_signed_counts() {
    assert_eq!(
        FetchDirection::Absolute {
            limit: number("-1")
        },
        fetch_direction("FETCH ABSOLUTE -1 FROM qy_cur_scroll")
    );
    assert_eq!(
        FetchDirection::Count {
            limit: number("-1")
        },
        fetch_direction("FETCH -1 FROM qy_cur_scroll")
    );
    assert_eq!(
        FetchDirection::Relative {
            limit: number("-2")
        },
        fetch_direction("FETCH RELATIVE -2 FROM c")
    );
    assert_eq!(
        FetchDirection::Count { limit: number("5") },
        fetch_direction("FETCH +5 IN c")
    );
    assert_eq!(
        FetchDirection::Forward {
            limit: Some(number("-3"))
        },
        fetch_direction("FETCH FORWARD -3 FROM c")
    );
    assert_eq!(
        FetchDirection::Backward {
            limit: Some(number("3"))
        },
        fetch_direction("FETCH BACKWARD 3 FROM c")
    );
    assert_eq!(
        FetchDirection::Count {
            limit: number("-1")
        },
        fetch_direction("MOVE -1 c")
    );
}

#[test]
fn fetch_and_move_cover_the_whole_synopsis() {
    for sql in [
        "FETCH c",
        "FETCH FROM c",
        "FETCH IN c",
        "FETCH NEXT FROM c",
        "FETCH PRIOR FROM c",
        "FETCH FIRST FROM c",
        "FETCH LAST FROM c",
        "FETCH ALL FROM c",
        "FETCH FORWARD c",
        "FETCH FORWARD ALL c",
        "FETCH BACKWARD c",
        "FETCH BACKWARD ALL FROM c",
        "MOVE c",
        "MOVE NEXT FROM c",
        "MOVE ALL c",
        "MOVE FORWARD ALL c",
        "MOVE BACKWARD 2 c",
    ] {
        verified(sql);
    }
    assert!(
        try_parse_pg("FETCH NEXT 1 FROM c").is_err(),
        "a direction keyword and a count are mutually exclusive"
    );
}

// =============================================================================
// TABLE as a query body
// =============================================================================

fn table_body(query: &Query) -> PgRelationExpr {
    match query.body.as_ref() {
        SetExpr::Table(table) => table
            .relation
            .clone()
            .unwrap_or_else(|| panic!("TABLE body without a relation")),
        other => panic!("expected a TABLE body, got {other:?}"),
    }
}

#[test]
fn table_is_a_query_body() {
    assert_eq!(
        "ts_ctf_bare",
        table_body(&query("TABLE ts_ctf_bare")).to_string()
    );
    assert_eq!(
        "public.vr_evt_drop_target",
        table_body(&query("TABLE public.vr_evt_drop_target")).to_string()
    );
    assert!(table_body(&query("TABLE ONLY t")).only);
    assert!(table_body(&query("TABLE ONLY (t)")).parenthesized);
    assert!(table_body(&query("TABLE t *")).descendants);
}

#[test]
fn table_accepts_the_query_suffixes() {
    assert!(query("TABLE qy_scf ORDER BY a DESC").order_by.is_some());
    assert!(query("TABLE qy_scf LIMIT 1").limit_clause.is_some());
    verified("TABLE t ORDER BY a DESC LIMIT 1 OFFSET 2");
}

#[test]
fn table_composes_with_set_operations_and_subqueries() {
    match query("TABLE a UNION TABLE b").body.as_ref() {
        SetExpr::SetOperation { left, right, .. } => {
            assert!(matches!(left.as_ref(), SetExpr::Table(_)));
            assert!(matches!(right.as_ref(), SetExpr::Table(_)));
        }
        other => panic!("expected a set operation, got {other:?}"),
    }
    verified("TABLE a EXCEPT ALL TABLE b");
    verified_to(
        "SELECT count(*) FROM (TABLE qy_scf) t",
        "SELECT count(*) FROM (TABLE qy_scf) AS t",
    );
    verified("WITH x AS (TABLE t) SELECT * FROM x");
    verified("CREATE TABLE new_table AS TABLE old_table");
    verified("INSERT INTO z TABLE t");
    verified("SELECT * FROM t WHERE a = ANY(TABLE u)");
}

// =============================================================================
// REFRESH MATERIALIZED VIEW
// =============================================================================

#[test]
fn refresh_materialized_view_takes_with_data() {
    for (sql, with_data) in [
        ("REFRESH MATERIALIZED VIEW vr_mv_nodata", None),
        (
            "REFRESH MATERIALIZED VIEW vr_mv_nodata WITH DATA",
            Some(true),
        ),
        (
            "REFRESH MATERIALIZED VIEW vr_mv_nodata WITH NO DATA",
            Some(false),
        ),
        (
            "REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv WITH DATA",
            Some(true),
        ),
    ] {
        match verified_to(sql, sql) {
            Statement::RefreshMaterializedView { with_data: got, .. } => {
                assert_eq!(with_data, got, "{sql}")
            }
            other => panic!("expected REFRESH for {sql}, got {other:?}"),
        }
    }
}
