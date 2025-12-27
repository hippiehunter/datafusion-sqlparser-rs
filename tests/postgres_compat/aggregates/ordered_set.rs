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

//! Tests for ordered-set and hypothetical-set aggregate syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createaggregate.html>

use crate::postgres_compat::common::*;

// ============================================================================
// Ordered-Set Aggregates
// ============================================================================

#[test]
fn test_create_aggregate_ordered_set_basic() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Ordered-set aggregate with ORDER BY clause
    pg_expect_parse_error!(
        "CREATE AGGREGATE percentile_disc (float8 ORDER BY anyelement) (
            SFUNC = ordered_set_transition,
            STYPE = internal,
            FINALFUNC = percentile_disc_final,
            FINALFUNC_EXTRA
        )"
    );
}

#[test]
fn test_create_aggregate_percentile_cont() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // percentile_cont ordered-set aggregate
    pg_expect_parse_error!(
        "CREATE AGGREGATE percentile_cont (float8 ORDER BY float8) (
            SFUNC = ordered_set_transition,
            STYPE = internal,
            FINALFUNC = percentile_cont_float8_final,
            FINALFUNC_EXTRA
        )"
    );
}

#[test]
fn test_create_aggregate_mode() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // mode() ordered-set aggregate (most frequent value)
    pg_expect_parse_error!(
        "CREATE AGGREGATE mode (ORDER BY anyelement) (
            SFUNC = ordered_set_transition_multi,
            STYPE = internal,
            FINALFUNC = mode_final,
            FINALFUNC_EXTRA
        )"
    );
}

#[test]
fn test_create_aggregate_ordered_set_multiple_args() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Ordered-set aggregate with multiple direct args
    pg_expect_parse_error!(
        "CREATE AGGREGATE percentile_disc (float8[] ORDER BY anyelement) (
            SFUNC = ordered_set_transition_multi,
            STYPE = internal,
            FINALFUNC = percentile_disc_multi_final,
            FINALFUNC_EXTRA
        )"
    );
}

// ============================================================================
// Hypothetical-Set Aggregates
// ============================================================================

#[test]
fn test_create_aggregate_hypothetical_rank() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // rank() hypothetical-set aggregate
    pg_expect_parse_error!(
        "CREATE AGGREGATE rank (VARIADIC \"any\" ORDER BY VARIADIC \"any\") (
            SFUNC = hypothetical_rank_final,
            STYPE = internal,
            FINALFUNC = hypothetical_rank_final,
            FINALFUNC_EXTRA,
            HYPOTHETICAL
        )"
    );
}

#[test]
fn test_create_aggregate_hypothetical_dense_rank() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // dense_rank() hypothetical-set aggregate
    pg_expect_parse_error!(
        "CREATE AGGREGATE dense_rank (VARIADIC \"any\" ORDER BY VARIADIC \"any\") (
            SFUNC = hypothetical_dense_rank_final,
            STYPE = internal,
            FINALFUNC = hypothetical_dense_rank_final,
            FINALFUNC_EXTRA,
            HYPOTHETICAL
        )"
    );
}

#[test]
fn test_create_aggregate_hypothetical_percent_rank() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // percent_rank() hypothetical-set aggregate
    pg_expect_parse_error!(
        "CREATE AGGREGATE percent_rank (VARIADIC \"any\" ORDER BY VARIADIC \"any\") (
            SFUNC = hypothetical_percent_rank_final,
            STYPE = internal,
            FINALFUNC = hypothetical_percent_rank_final,
            FINALFUNC_EXTRA,
            HYPOTHETICAL
        )"
    );
}

#[test]
fn test_create_aggregate_hypothetical_cume_dist() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // cume_dist() hypothetical-set aggregate
    pg_expect_parse_error!(
        "CREATE AGGREGATE cume_dist (VARIADIC \"any\" ORDER BY VARIADIC \"any\") (
            SFUNC = hypothetical_cume_dist_final,
            STYPE = internal,
            FINALFUNC = hypothetical_cume_dist_final,
            FINALFUNC_EXTRA,
            HYPOTHETICAL
        )"
    );
}

#[test]
fn test_create_aggregate_hypothetical_flag() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // HYPOTHETICAL flag marks aggregate as hypothetical-set
    pg_expect_parse_error!(
        "CREATE AGGREGATE my_rank (integer ORDER BY integer) (
            SFUNC = rank_sfunc,
            STYPE = internal,
            FINALFUNC = rank_final,
            HYPOTHETICAL
        )"
    );
}
