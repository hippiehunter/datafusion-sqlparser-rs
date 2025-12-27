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

//! Tests for CREATE AGGREGATE syntax
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-createaggregate.html>

use crate::postgres_compat::common::*;

// ============================================================================
// Basic CREATE AGGREGATE Syntax
// ============================================================================

#[test]
fn test_create_aggregate_basic() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Basic aggregate with SFUNC and STYPE
    pg_expect_parse_error!(
        "CREATE AGGREGATE myavg (numeric) (
            SFUNC = numeric_avg_accum,
            STYPE = internal,
            FINALFUNC = numeric_avg,
            INITCOND = '{0,0,0}'
        )"
    );
}

#[test]
fn test_create_aggregate_or_replace() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // CREATE OR REPLACE AGGREGATE
    pg_expect_parse_error!(
        "CREATE OR REPLACE AGGREGATE mysum (integer) (
            SFUNC = int4pl,
            STYPE = integer,
            INITCOND = '0'
        )"
    );
}

#[test]
fn test_create_aggregate_if_not_exists() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // CREATE AGGREGATE IF NOT EXISTS (PostgreSQL 15+)
    pg_expect_parse_error!(
        "CREATE AGGREGATE IF NOT EXISTS mycount (*) (
            SFUNC = int8inc,
            STYPE = bigint,
            INITCOND = '0'
        )"
    );
}

#[test]
fn test_create_aggregate_multiple_args() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Aggregate with multiple input arguments
    pg_expect_parse_error!(
        "CREATE AGGREGATE weighted_avg (value numeric, weight numeric) (
            SFUNC = weighted_avg_accum,
            STYPE = internal,
            FINALFUNC = weighted_avg_final
        )"
    );
}

// ============================================================================
// State Transition Functions
// ============================================================================

#[test]
fn test_create_aggregate_sfunc_stype() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // State function (SFUNC) and state type (STYPE) are required
    pg_expect_parse_error!(
        "CREATE AGGREGATE mysum (integer) (
            SFUNC = int4pl,
            STYPE = integer
        )"
    );
}

#[test]
fn test_create_aggregate_initcond() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Initial condition for state value
    pg_expect_parse_error!(
        "CREATE AGGREGATE myproduct (integer) (
            SFUNC = int4mul,
            STYPE = integer,
            INITCOND = '1'
        )"
    );
}

#[test]
fn test_create_aggregate_sspace() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Approximate state data size in bytes
    pg_expect_parse_error!(
        "CREATE AGGREGATE myagg (text) (
            SFUNC = text_concat,
            STYPE = text,
            SSPACE = 1024
        )"
    );
}

// ============================================================================
// Final Functions
// ============================================================================

#[test]
fn test_create_aggregate_finalfunc() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Final function to compute aggregate result
    pg_expect_parse_error!(
        "CREATE AGGREGATE myavg (numeric) (
            SFUNC = numeric_avg_accum,
            STYPE = internal,
            FINALFUNC = numeric_avg
        )"
    );
}

#[test]
fn test_create_aggregate_finalfunc_extra() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // FINALFUNC_EXTRA passes extra NULL arguments to final function
    pg_expect_parse_error!(
        "CREATE AGGREGATE mypercentile (value numeric, percentile numeric) (
            SFUNC = percentile_accum,
            STYPE = internal,
            FINALFUNC = percentile_final,
            FINALFUNC_EXTRA
        )"
    );
}

#[test]
fn test_create_aggregate_finalfunc_modify() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Control whether final function can modify state value
    pg_expect_parse_error!(
        "CREATE AGGREGATE myagg (numeric) (
            SFUNC = numeric_accum,
            STYPE = internal,
            FINALFUNC = numeric_final,
            FINALFUNC_MODIFY = READ_WRITE
        )"
    );
}

// ============================================================================
// Parallel Aggregation
// ============================================================================

#[test]
fn test_create_aggregate_combinefunc() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Combine function for parallel aggregation
    pg_expect_parse_error!(
        "CREATE AGGREGATE mysum (integer) (
            SFUNC = int4pl,
            STYPE = integer,
            COMBINEFUNC = int4pl,
            PARALLEL = SAFE
        )"
    );
}

#[test]
fn test_create_aggregate_serialfunc_deserialfunc() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Serialization functions for parallel aggregation
    pg_expect_parse_error!(
        "CREATE AGGREGATE myagg (numeric) (
            SFUNC = numeric_accum,
            STYPE = internal,
            SERIALFUNC = numeric_serialize,
            DESERIALFUNC = numeric_deserialize,
            COMBINEFUNC = numeric_combine,
            PARALLEL = SAFE
        )"
    );
}

#[test]
fn test_create_aggregate_parallel_restricted() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // PARALLEL RESTRICTED for aggregates that can't run in parallel workers
    pg_expect_parse_error!(
        "CREATE AGGREGATE myagg (text) (
            SFUNC = text_concat,
            STYPE = text,
            PARALLEL = RESTRICTED
        )"
    );
}

#[test]
fn test_create_aggregate_parallel_unsafe() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // PARALLEL UNSAFE for aggregates that can't be parallelized at all
    pg_expect_parse_error!(
        "CREATE AGGREGATE myagg (text) (
            SFUNC = text_concat,
            STYPE = text,
            PARALLEL = UNSAFE
        )"
    );
}

// ============================================================================
// Moving-Aggregate Mode
// ============================================================================

#[test]
fn test_create_aggregate_moving_state() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Moving-aggregate mode with MSFUNC, MINVFUNC, MSTYPE
    pg_expect_parse_error!(
        "CREATE AGGREGATE moving_avg (numeric) (
            SFUNC = numeric_avg_accum,
            STYPE = internal,
            FINALFUNC = numeric_avg,
            MSFUNC = numeric_avg_accum,
            MINVFUNC = numeric_avg_deaccum,
            MSTYPE = internal,
            MFINALFUNC = numeric_avg
        )"
    );
}

#[test]
fn test_create_aggregate_minitcond() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Initial condition for moving-aggregate state
    pg_expect_parse_error!(
        "CREATE AGGREGATE moving_sum (integer) (
            SFUNC = int4pl,
            STYPE = integer,
            MSFUNC = int4pl,
            MINVFUNC = int4mi,
            MSTYPE = integer,
            MINITCOND = '0'
        )"
    );
}

#[test]
fn test_create_aggregate_msspace() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Approximate moving-aggregate state data size
    pg_expect_parse_error!(
        "CREATE AGGREGATE moving_agg (text) (
            SFUNC = text_concat,
            STYPE = text,
            MSFUNC = text_concat,
            MINVFUNC = text_remove,
            MSTYPE = text,
            MSSPACE = 2048
        )"
    );
}

// ============================================================================
// Sort Operators
// ============================================================================

#[test]
fn test_create_aggregate_sortop() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Sort operator for MIN/MAX-like aggregates
    pg_expect_parse_error!(
        "CREATE AGGREGATE mymin (anyelement) (
            SFUNC = smaller,
            STYPE = anyelement,
            SORTOP = <
        )"
    );
}

#[test]
fn test_create_aggregate_sortop_qualified() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Sort operator with schema qualification
    pg_expect_parse_error!(
        "CREATE AGGREGATE mymax (numeric) (
            SFUNC = larger,
            STYPE = numeric,
            SORTOP = pg_catalog.>
        )"
    );
}

// ============================================================================
// Polymorphic Aggregates
// ============================================================================

#[test]
fn test_create_aggregate_anyelement() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Polymorphic aggregate with anyelement
    pg_expect_parse_error!(
        "CREATE AGGREGATE array_agg (anyelement) (
            SFUNC = array_append,
            STYPE = anyarray,
            INITCOND = '{}'
        )"
    );
}

#[test]
fn test_create_aggregate_anycompatible() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Polymorphic aggregate with anycompatible (PostgreSQL 13+)
    pg_expect_parse_error!(
        "CREATE AGGREGATE my_collect (anycompatible) (
            SFUNC = anycompatible_array_append,
            STYPE = anycompatiblearray,
            INITCOND = '{}'
        )"
    );
}

// ============================================================================
// Star Aggregate
// ============================================================================

#[test]
fn test_create_aggregate_star() {
    // https://www.postgresql.org/docs/current/sql-createaggregate.html
    // Aggregate with * argument (like COUNT(*))
    pg_expect_parse_error!(
        "CREATE AGGREGATE mycount (*) (
            SFUNC = int8inc,
            STYPE = bigint,
            INITCOND = '0',
            PARALLEL = SAFE
        )"
    );
}
