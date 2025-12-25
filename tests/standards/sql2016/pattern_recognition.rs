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

//! SQL:2016 Row Pattern Recognition Tests (ISO/IEC 9075-2, Features R010-R030)
//!
//! MATCH_RECOGNIZE is SQL's pattern matching facility for finding patterns
//! in sequences of rows, commonly used for financial analysis, IoT, and
//! complex event processing.

use crate::standards::common::verified_standard_stmt;

// ==================== R010: Row Pattern Recognition Basic ====================

#[test]
fn r010_01_match_recognize_basic() {
    // SQL:2016 R010: Basic MATCH_RECOGNIZE
    verified_standard_stmt("SELECT * FROM stock_data MATCH_RECOGNIZE(ORDER BY trade_date PATTERN (A B+ C) DEFINE B AS B.price > PREV(B.price))",
    );
}

#[test]
fn r010_02_match_recognize_partition_by() {
    verified_standard_stmt("SELECT * FROM stock_data MATCH_RECOGNIZE(PARTITION BY symbol ORDER BY trade_date PATTERN (A B+ C) DEFINE B AS B.price > PREV(B.price))",
    );
}

#[test]
fn r010_03_match_recognize_measures() {
    verified_standard_stmt("SELECT * FROM stock_data MATCH_RECOGNIZE(PARTITION BY symbol ORDER BY trade_date MEASURES A.trade_date AS start_date, LAST(C.trade_date) AS end_date, A.price AS start_price PATTERN (A B+ C) DEFINE B AS B.price > PREV(B.price))",
    );
}

// ==================== R011: Row Pattern Navigation ====================

#[test]
fn r011_01_prev_function() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A B) DEFINE B AS B.value > PREV(B.value))",
    );
}

#[test]
fn r011_02_prev_with_offset() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A B C) DEFINE C AS C.value > PREV(C.value, 2))",
    );
}

#[test]
fn r011_03_next_function() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A B) DEFINE A AS A.value < NEXT(A.value))",
    );
}

#[test]
fn r011_04_first_last() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts MEASURES FIRST(A.value) AS first_val, LAST(B.value) AS last_val PATTERN (A+ B+) DEFINE B AS B.value > A.value)",
    );
}

// ==================== R012: Row Pattern Quantifiers ====================

#[test]
fn r012_01_one_or_more() {
    verified_standard_stmt(
        "SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A+) DEFINE A AS A.flag = true)",
    );
}

#[test]
fn r012_02_zero_or_more() {
    verified_standard_stmt(
        "SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A*) DEFINE A AS A.flag = true)",
    );
}

#[test]
fn r012_03_optional() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A B? C) DEFINE A AS A.type = 'start', C AS C.type = 'end')",
    );
}

#[test]
fn r012_04_exact_count() {
    verified_standard_stmt(
        "SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A{3}) DEFINE A AS A.flag = true)",
    );
}

#[test]
fn r012_05_range_quantifier() {
    verified_standard_stmt(
        "SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A{2,5}) DEFINE A AS A.flag = true)",
    );
}

#[test]
fn r012_06_minimum_quantifier() {
    verified_standard_stmt(
        "SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A{3,}) DEFINE A AS A.flag = true)",
    );
}

#[test]
fn r012_07_reluctant_quantifier() {
    verified_standard_stmt(
        "SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A+?) DEFINE A AS A.value > 0)",
    );
}

// ==================== R013: Row Pattern Alternation ====================

#[test]
fn r013_01_alternation() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN ((A B) | (C D)) DEFINE A AS A.type = 'x', B AS B.type = 'y', C AS C.type = 'p', D AS D.type = 'q')",
    );
}

#[test]
fn r013_02_complex_pattern() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts PATTERN (A (B | C)+ D) DEFINE B AS B.value > 0, C AS C.value < 0)",
    );
}

// ==================== R020: One Row Per Match ====================

#[test]
fn r020_01_one_row_per_match() {
    verified_standard_stmt("SELECT * FROM stock_data MATCH_RECOGNIZE(PARTITION BY symbol ORDER BY trade_date MEASURES FIRST(A.trade_date) AS start_date, LAST(C.trade_date) AS end_date ONE ROW PER MATCH PATTERN (A B+ C) DEFINE B AS B.price > PREV(B.price))",
    );
}

#[test]
fn r020_02_all_rows_per_match() {
    verified_standard_stmt("SELECT * FROM stock_data MATCH_RECOGNIZE(PARTITION BY symbol ORDER BY trade_date MEASURES CLASSIFIER() AS pattern_var, MATCH_NUMBER() AS match_num ALL ROWS PER MATCH PATTERN (A B+ C) DEFINE B AS B.price > PREV(B.price))",
    );
}

#[test]
fn r020_03_all_rows_with_unmatched() {
    verified_standard_stmt("SELECT * FROM stock_data MATCH_RECOGNIZE(ORDER BY trade_date ALL ROWS PER MATCH WITH UNMATCHED ROWS PATTERN (A B+ C) DEFINE B AS B.price > PREV(B.price))",
    );
}

// ==================== R021: Pattern Match Functions ====================

#[test]
fn r021_01_classifier() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts MEASURES CLASSIFIER() AS matched_var ALL ROWS PER MATCH PATTERN (A | B | C) DEFINE A AS A.type = 'a', B AS B.type = 'b')",
    );
}

#[test]
fn r021_02_match_number() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts MEASURES MATCH_NUMBER() AS match_num ALL ROWS PER MATCH PATTERN (A+) DEFINE A AS A.value > 0)",
    );
}

// ==================== R022: After Match Skip ====================

#[test]
fn r022_01_after_match_skip_past_last_row() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts AFTER MATCH SKIP PAST LAST ROW PATTERN (A B+ C) DEFINE B AS B.value > PREV(B.value))",
    );
}

#[test]
fn r022_02_after_match_skip_to_next_row() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts AFTER MATCH SKIP TO NEXT ROW PATTERN (A B+ C) DEFINE B AS B.value > PREV(B.value))",
    );
}

#[test]
fn r022_03_after_match_skip_to_first() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts AFTER MATCH SKIP TO FIRST B PATTERN (A B+ C) DEFINE B AS B.value > PREV(B.value))",
    );
}

#[test]
fn r022_04_after_match_skip_to_last() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts AFTER MATCH SKIP TO LAST B PATTERN (A B+ C) DEFINE B AS B.value > PREV(B.value))",
    );
}

// ==================== R030: Subset Clause ====================

#[test]
fn r030_01_subset() {
    verified_standard_stmt("SELECT * FROM t MATCH_RECOGNIZE(ORDER BY ts MEASURES SUM(UP.price) AS total_up PATTERN (STRT DOWN+ UP+) SUBSET UP = (UP1, UP2) DEFINE DOWN AS DOWN.price < PREV(DOWN.price))",
    );
}

// ==================== Complex Examples ====================

#[test]
fn complex_v_shape_pattern() {
    // Classic "V-shape" pattern: price drops then rises
    verified_standard_stmt("SELECT * FROM stock_data MATCH_RECOGNIZE(PARTITION BY symbol ORDER BY trade_date MEASURES STRT.trade_date AS start_date, LAST(DOWN.trade_date) AS bottom_date, LAST(UP.trade_date) AS end_date, STRT.price AS start_price, LAST(DOWN.price) AS bottom_price, LAST(UP.price) AS end_price ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (STRT DOWN+ UP+) DEFINE DOWN AS DOWN.price < PREV(DOWN.price), UP AS UP.price > PREV(UP.price)) AS mr",
    );
}

#[test]
fn complex_session_detection() {
    // Detect user sessions with gaps > 30 minutes
    verified_standard_stmt("SELECT * FROM events MATCH_RECOGNIZE(PARTITION BY user_id ORDER BY event_time MEASURES FIRST(A.event_time) AS session_start, LAST(A.event_time) AS session_end, COUNT(*) AS event_count ONE ROW PER MATCH PATTERN (A+) DEFINE A AS A.event_time - PREV(A.event_time) < INTERVAL '30' MINUTE OR PREV(A.event_time) IS NULL)"
    );
}
