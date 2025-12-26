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

//! SQL:2016 Persistent Stored Modules (PSM) Tests
//!
//! Procedural SQL features defined in ISO/IEC 9075-4:2016.
//!
//! ## Feature Coverage
//!
//! - LOOP ... END LOOP statements
//! - REPEAT ... UNTIL ... END REPEAT statements
//! - WHILE ... DO ... END WHILE statements
//! - FOR ... AS ... DO ... END FOR statements (cursor iteration)
//! - IF ... THEN ... ELSEIF ... ELSE ... END IF statements
//! - CASE statement (procedural)
//! - LEAVE statement (exit loop/block)
//! - ITERATE statement (continue loop)
//! - BEGIN ... END blocks
//! - DECLARE variables
//! - Exception handling (DECLARE ... HANDLER, SIGNAL, RESIGNAL)
//! - Cursors (DECLARE CURSOR, OPEN, FETCH, CLOSE)
//! - RETURN statement
//! - CALL statement

use crate::standards::common::verified_standard_stmt;
use sqlparser::ast::{DeclareType, FetchDirection, FetchPosition, Statement};

// =============================================================================
// LOOP Statements
// =============================================================================

mod loop_statements {
    use super::*;

    #[test]
    fn loop_basic() {
        // SQL:2016 PSM: Basic LOOP statement
        verified_standard_stmt("LOOP SELECT 1; END LOOP");
    }

    #[test]
    fn loop_with_label() {
        // SQL:2016 PSM: LOOP with label
        verified_standard_stmt("my_loop: LOOP SELECT 1; END LOOP");
        verified_standard_stmt("outer_loop: LOOP SELECT x FROM t; END LOOP");
    }

    #[test]
    fn loop_with_multiple_statements() {
        // SQL:2016 PSM: LOOP with multiple statements
        verified_standard_stmt("LOOP SELECT 1; SELECT 2; SELECT 3; END LOOP");
        verified_standard_stmt("LOOP INSERT INTO t VALUES (1); UPDATE t SET x = 2; END LOOP");
    }

    #[test]
    fn loop_nested() {
        // SQL:2016 PSM: Nested LOOP statements
        // Nested labeled loops have parsing issues - labels at nested level not supported yet
        verified_standard_stmt("outer: LOOP inner: LOOP SELECT 1; END LOOP; END LOOP");
        // Unlabeled nested loops work
        verified_standard_stmt("LOOP LOOP LOOP SELECT x; END LOOP; END LOOP; END LOOP");
    }

    #[test]
    fn loop_with_leave() {
        // SQL:2016 PSM: LOOP with LEAVE statement
        verified_standard_stmt("my_loop: LOOP LEAVE my_loop; END LOOP");
        // LEAVE without label not yet implemented
        verified_standard_stmt("LOOP IF x > 10 THEN LEAVE; END IF; END LOOP");
    }

    #[test]
    fn loop_with_iterate() {
        // SQL:2016 PSM: LOOP with ITERATE statement
        verified_standard_stmt("my_loop: LOOP ITERATE my_loop; END LOOP");
        // ITERATE without label not yet implemented
        verified_standard_stmt("LOOP IF x < 10 THEN ITERATE; END IF; END LOOP");
    }
}

// =============================================================================
// REPEAT Statements
// =============================================================================

mod repeat_statements {
    use super::*;

    #[test]
    fn repeat_basic() {
        // SQL:2016 PSM: Basic REPEAT statement
        verified_standard_stmt("REPEAT SELECT 1; UNTIL x > 10 END REPEAT");
        verified_standard_stmt("REPEAT INSERT INTO t VALUES (i); UNTIL i >= 100 END REPEAT");
    }

    #[test]
    fn repeat_with_label() {
        // SQL:2016 PSM: REPEAT with label
        verified_standard_stmt("my_repeat: REPEAT SELECT 1; UNTIL done END REPEAT");
        verified_standard_stmt(
            "retry_loop: REPEAT UPDATE t SET x = x + 1; UNTIL success END REPEAT",
        );
    }

    #[test]
    fn repeat_multiple_statements() {
        // SQL:2016 PSM: REPEAT with multiple statements
        verified_standard_stmt("REPEAT SELECT 1; SELECT 2; UNTIL x > 5 END REPEAT");
        verified_standard_stmt(
            "REPEAT INSERT INTO log VALUES (i); SELECT i + 1; UNTIL finished END REPEAT",
        );
    }

    #[test]
    fn repeat_nested() {
        // SQL:2016 PSM: Nested REPEAT statements - NOT YET IMPLEMENTED
        // Nested labeled statements not fully supported
        verified_standard_stmt(
            "outer: REPEAT inner: REPEAT SELECT 1; UNTIL x > 5 END REPEAT; UNTIL y > 10 END REPEAT",
        );
    }

    #[test]
    fn repeat_complex_condition() {
        // SQL:2016 PSM: REPEAT with complex condition
        verified_standard_stmt(
            "REPEAT UPDATE t SET x = x + 1; UNTIL x > 100 OR done = true END REPEAT",
        );
        verified_standard_stmt(
            "REPEAT SELECT COUNT(*) FROM t; UNTIL COUNT(*) >= 1000 AND status = 'ready' END REPEAT",
        );
    }

    #[test]
    fn repeat_with_leave() {
        // SQL:2016 PSM: REPEAT with LEAVE
        verified_standard_stmt("my_repeat: REPEAT LEAVE my_repeat; UNTIL false END REPEAT");
    }

    #[test]
    fn repeat_with_iterate() {
        // SQL:2016 PSM: REPEAT with ITERATE
        verified_standard_stmt("my_repeat: REPEAT ITERATE my_repeat; UNTIL x > 10 END REPEAT");
    }
}

// =============================================================================
// WHILE Statements
// =============================================================================

mod while_statements {
    use super::*;

    #[test]
    fn while_basic() {
        // SQL:2016 PSM: Basic WHILE statement - NOT YET IMPLEMENTED
        // WHILE with DO keyword is not yet fully supported
        verified_standard_stmt("WHILE x < 10 DO SELECT 1; END WHILE");
        verified_standard_stmt("WHILE i <= 100 DO INSERT INTO t VALUES (i); END WHILE");
    }

    #[test]
    fn while_with_label() {
        // SQL:2016 PSM: WHILE with label - NOT YET IMPLEMENTED
        verified_standard_stmt("my_while: WHILE x > 0 DO SELECT x; END WHILE");
        verified_standard_stmt(
            "process_loop: WHILE NOT finished DO UPDATE t SET status = 'processing'; END WHILE",
        );
    }

    #[test]
    fn while_multiple_statements() {
        // SQL:2016 PSM: WHILE with multiple statements - NOT YET IMPLEMENTED
        verified_standard_stmt("WHILE x < 100 DO SELECT x; SELECT x + 1; END WHILE");
        verified_standard_stmt("WHILE count < limit DO INSERT INTO log VALUES (count); UPDATE state SET count = count + 1; END WHILE");
    }

    #[test]
    fn while_nested() {
        // SQL:2016 PSM: Nested WHILE statements - NOT YET IMPLEMENTED
        verified_standard_stmt(
            "outer: WHILE x < 10 DO inner: WHILE y < 5 DO SELECT x, y; END WHILE; END WHILE",
        );
        verified_standard_stmt("WHILE a > 0 DO WHILE b > 0 DO SELECT a * b; END WHILE; END WHILE");
    }

    #[test]
    fn while_complex_condition() {
        // SQL:2016 PSM: WHILE with complex condition
        verified_standard_stmt("WHILE x > 0 AND y < 100 DO SELECT x; END WHILE");
        verified_standard_stmt(
            "WHILE (count < max_count OR retries > 0) AND status <> 'error' DO SELECT 1; END WHILE",
        );
    }

    #[test]
    fn while_with_leave() {
        // SQL:2016 PSM: WHILE with LEAVE - NOT YET IMPLEMENTED
        verified_standard_stmt("my_while: WHILE true DO LEAVE my_while; END WHILE");
    }

    #[test]
    fn while_with_iterate() {
        // SQL:2016 PSM: WHILE with ITERATE - NOT YET IMPLEMENTED
        verified_standard_stmt("my_while: WHILE x < 100 DO ITERATE my_while; END WHILE");
    }
}

// =============================================================================
// LEAVE Statements
// =============================================================================

mod leave_statements {
    use super::*;

    #[test]
    fn leave_basic() {
        // SQL:2016 PSM: Basic LEAVE statement
        verified_standard_stmt("my_loop: LOOP LEAVE my_loop; END LOOP");
        // BEGIN blocks with labels not yet supported
        verified_standard_stmt("my_block: BEGIN LEAVE my_block; END");
    }

    #[test]
    fn leave_in_loop() {
        // SQL:2016 PSM: LEAVE in LOOP
        verified_standard_stmt(
            "my_loop: LOOP IF x > 10 THEN LEAVE my_loop; END IF; SELECT x; END LOOP",
        );
    }

    #[test]
    fn leave_in_repeat() {
        // SQL:2016 PSM: LEAVE in REPEAT
        verified_standard_stmt(
            "my_repeat: REPEAT IF error THEN LEAVE my_repeat; END IF; UNTIL done END REPEAT",
        );
    }

    #[test]
    fn leave_in_while() {
        // SQL:2016 PSM: LEAVE in WHILE - NOT YET IMPLEMENTED
        // WHILE with DO is not yet supported
        verified_standard_stmt(
            "my_while: WHILE true DO IF x = 0 THEN LEAVE my_while; END IF; END WHILE",
        );
    }

    #[test]
    fn leave_nested_outer() {
        // SQL:2016 PSM: LEAVE outer loop from nested loop - NOT YET IMPLEMENTED
        // Nested labeled loops parsing needs improvement
        verified_standard_stmt("outer: LOOP inner: LOOP LEAVE outer; END LOOP; END LOOP");
    }

    #[test]
    fn leave_in_begin_end() {
        // SQL:2016 PSM: LEAVE in BEGIN...END block - NOT YET IMPLEMENTED
        verified_standard_stmt(
            "my_block: BEGIN IF condition THEN LEAVE my_block; END IF; SELECT 1; END",
        );
    }
}

// =============================================================================
// ITERATE Statements
// =============================================================================

mod iterate_statements {
    use super::*;

    #[test]
    fn iterate_basic() {
        // SQL:2016 PSM: Basic ITERATE statement
        verified_standard_stmt("my_loop: LOOP ITERATE my_loop; END LOOP");
    }

    #[test]
    fn iterate_in_loop() {
        // SQL:2016 PSM: ITERATE in LOOP
        verified_standard_stmt(
            "my_loop: LOOP IF x < 10 THEN ITERATE my_loop; END IF; SELECT x; END LOOP",
        );
    }

    #[test]
    fn iterate_in_repeat() {
        // SQL:2016 PSM: ITERATE in REPEAT
        verified_standard_stmt("my_repeat: REPEAT IF skip THEN ITERATE my_repeat; END IF; SELECT 1; UNTIL done END REPEAT");
    }

    #[test]
    fn iterate_in_while() {
        // SQL:2016 PSM: ITERATE in WHILE - NOT YET IMPLEMENTED
        // WHILE with DO is not yet supported
        verified_standard_stmt("my_while: WHILE x < 100 DO IF x % 2 = 0 THEN ITERATE my_while; END IF; SELECT x; END WHILE");
    }

    #[test]
    fn iterate_nested_outer() {
        // SQL:2016 PSM: ITERATE outer loop from nested loop - NOT YET IMPLEMENTED
        // Nested labeled loops need better support
        verified_standard_stmt("outer: LOOP inner: LOOP ITERATE outer; END LOOP; END LOOP");
    }
}

// =============================================================================
// IF Statements
// =============================================================================

mod if_statements {
    use super::*;

    #[test]
    fn if_then_basic() {
        // SQL:2016 PSM: Basic IF...THEN statement
        verified_standard_stmt("IF x > 0 THEN SELECT 1; END IF");
        verified_standard_stmt("IF condition THEN UPDATE t SET status = 'active'; END IF");
    }

    #[test]
    fn if_then_else() {
        // SQL:2016 PSM: IF...THEN...ELSE statement
        verified_standard_stmt("IF x > 0 THEN SELECT 1; ELSE SELECT 0; END IF");
        verified_standard_stmt(
            "IF found THEN UPDATE t SET x = 1; ELSE INSERT INTO t VALUES (1); END IF",
        );
    }

    #[test]
    fn if_then_elseif() {
        // SQL:2016 PSM: IF...THEN...ELSEIF statement
        verified_standard_stmt(
            "IF x > 10 THEN SELECT 'high'; ELSEIF x > 5 THEN SELECT 'medium'; END IF",
        );
        verified_standard_stmt("IF score >= 90 THEN SELECT 'A'; ELSEIF score >= 80 THEN SELECT 'B'; ELSEIF score >= 70 THEN SELECT 'C'; END IF");
    }

    #[test]
    fn if_then_elseif_else() {
        // SQL:2016 PSM: IF...THEN...ELSEIF...ELSE statement
        verified_standard_stmt("IF x > 10 THEN SELECT 'high'; ELSEIF x > 5 THEN SELECT 'medium'; ELSE SELECT 'low'; END IF");
        verified_standard_stmt("IF grade = 'A' THEN SELECT 4.0; ELSEIF grade = 'B' THEN SELECT 3.0; ELSEIF grade = 'C' THEN SELECT 2.0; ELSE SELECT 0.0; END IF");
    }

    #[test]
    fn if_multiple_statements() {
        // SQL:2016 PSM: IF with multiple statements
        verified_standard_stmt("IF x > 0 THEN SELECT 1; SELECT 2; SELECT 3; END IF");
        verified_standard_stmt("IF valid THEN INSERT INTO log VALUES ('start'); UPDATE t SET status = 'running'; INSERT INTO log VALUES ('end'); END IF");
    }

    #[test]
    fn if_nested() {
        // SQL:2016 PSM: Nested IF statements
        verified_standard_stmt(
            "IF x > 0 THEN IF y > 0 THEN SELECT 'both positive'; END IF; END IF",
        );
        verified_standard_stmt(
            "IF a THEN SELECT 1; ELSE IF b THEN SELECT 2; ELSE SELECT 3; END IF; END IF",
        );
    }

    #[test]
    fn if_complex_condition() {
        // SQL:2016 PSM: IF with complex conditions
        verified_standard_stmt("IF x > 0 AND y < 100 THEN SELECT 'valid'; END IF");
        verified_standard_stmt("IF (status = 'active' OR status = 'pending') AND count > 0 THEN UPDATE t SET processed = true; END IF");
    }
}

// =============================================================================
// CASE Statements (Procedural)
// =============================================================================

mod case_statements {
    use super::*;

    #[test]
    fn case_simple() {
        // SQL:2016 PSM: Simple CASE statement
        verified_standard_stmt(
            "CASE x WHEN 1 THEN SELECT 'one'; WHEN 2 THEN SELECT 'two'; END CASE",
        );
        verified_standard_stmt("CASE status WHEN 'active' THEN UPDATE t SET x = 1; WHEN 'inactive' THEN UPDATE t SET x = 0; END CASE");
    }

    #[test]
    fn case_simple_with_else() {
        // SQL:2016 PSM: Simple CASE with ELSE
        verified_standard_stmt("CASE x WHEN 1 THEN SELECT 'one'; WHEN 2 THEN SELECT 'two'; ELSE SELECT 'other'; END CASE");
        verified_standard_stmt("CASE color WHEN 'red' THEN SELECT 1; WHEN 'green' THEN SELECT 2; WHEN 'blue' THEN SELECT 3; ELSE SELECT 0; END CASE");
    }

    #[test]
    fn case_searched() {
        // SQL:2016 PSM: Searched CASE statement
        verified_standard_stmt("CASE WHEN x > 10 THEN SELECT 'high'; WHEN x > 5 THEN SELECT 'medium'; ELSE SELECT 'low'; END CASE");
        verified_standard_stmt("CASE WHEN score >= 90 THEN SELECT 'A'; WHEN score >= 80 THEN SELECT 'B'; WHEN score >= 70 THEN SELECT 'C'; ELSE SELECT 'F'; END CASE");
    }

    #[test]
    fn case_multiple_statements() {
        // SQL:2016 PSM: CASE with multiple statements per branch
        verified_standard_stmt(
            "CASE x WHEN 1 THEN SELECT 1; SELECT 2; WHEN 2 THEN SELECT 3; SELECT 4; END CASE",
        );
        verified_standard_stmt("CASE status WHEN 'start' THEN INSERT INTO log VALUES ('begin'); UPDATE state SET running = true; END CASE");
    }

    #[test]
    fn case_nested() {
        // SQL:2016 PSM: Nested CASE statements
        verified_standard_stmt(
            "CASE x WHEN 1 THEN CASE y WHEN 1 THEN SELECT 'both one'; END CASE; END CASE",
        );
    }

    #[test]
    fn case_complex_when() {
        // SQL:2016 PSM: CASE with complex WHEN conditions
        verified_standard_stmt("CASE WHEN x > 0 AND y > 0 THEN SELECT 'positive'; WHEN x < 0 AND y < 0 THEN SELECT 'negative'; ELSE SELECT 'mixed'; END CASE");
    }
}

// =============================================================================
// BEGIN...END Blocks
// =============================================================================

mod begin_end_blocks {
    use super::*;

    #[test]
    fn begin_end_basic() {
        // SQL:2016 PSM: Basic BEGIN...END block - NOT YET IMPLEMENTED
        // BEGIN...END as standalone statement blocks are not yet supported
        verified_standard_stmt("BEGIN SELECT 1; END");
        verified_standard_stmt("BEGIN INSERT INTO t VALUES (1); UPDATE t SET x = 2; END");
    }

    #[test]
    fn begin_end_with_label() {
        // SQL:2016 PSM: BEGIN...END with label - NOT YET IMPLEMENTED
        verified_standard_stmt("my_block: BEGIN SELECT 1; END");
        verified_standard_stmt("transaction_block: BEGIN INSERT INTO accounts VALUES (1); UPDATE balances SET total = total + 100; END");
    }

    #[test]
    fn begin_end_nested() {
        // SQL:2016 PSM: Nested BEGIN...END blocks - NOT YET IMPLEMENTED
        verified_standard_stmt("BEGIN BEGIN SELECT 1; END; BEGIN SELECT 2; END; END");
        verified_standard_stmt("outer: BEGIN inner: BEGIN SELECT x; END; SELECT y; END");
    }

    #[test]
    fn begin_end_with_declare() {
        // SQL:2016 PSM: BEGIN...END with DECLARE - NOT YET IMPLEMENTED
        verified_standard_stmt("BEGIN DECLARE x INT; SELECT x; END");
    }
}

// =============================================================================
// DECLARE Statements
// =============================================================================

mod declare_statements {
    use super::*;

    #[test]
    fn declare_variable() {
        // SQL:2016 PSM: DECLARE variable - NOT YET IMPLEMENTED
        verified_standard_stmt("DECLARE x INT");
    }

    #[test]
    fn declare_variable_with_default() {
        // SQL:2016 PSM: DECLARE variable with DEFAULT - NOT YET IMPLEMENTED
        verified_standard_stmt("DECLARE x INT DEFAULT 0");
    }

    #[test]
    fn declare_multiple_variables() {
        // SQL:2016 PSM: DECLARE multiple variables - NOT YET IMPLEMENTED
        verified_standard_stmt("DECLARE x INT, y VARCHAR(100), z DECIMAL(10,2)");
    }

    #[test]
    fn declare_cursor() {
        // SQL:2016 PSM: DECLARE CURSOR
        let stmt = verified_standard_stmt("DECLARE cur CURSOR FOR SELECT * FROM t");
        match stmt {
            Statement::Declare { stmts, .. } => {
                assert_eq!(stmts.len(), 1);
                let decl = &stmts[0];
                assert_eq!(decl.names.len(), 1);
                assert_eq!(decl.names[0].value, "cur");
                assert_eq!(decl.declare_type, Some(DeclareType::Cursor));
                assert!(decl.for_query.is_some());
            }
            _ => panic!("Expected Declare statement, got {:?}", stmt),
        }
    }

    #[test]
    fn declare_handler() {
        // SQL:2016 PSM: DECLARE HANDLER - NOT YET IMPLEMENTED
        verified_standard_stmt(
            "DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN SELECT 'error'; END",
        );
    }

    #[test]
    fn declare_condition() {
        // SQL:2016 PSM: DECLARE CONDITION - NOT YET IMPLEMENTED
        verified_standard_stmt("DECLARE duplicate_key CONDITION FOR SQLSTATE '23000'");
    }
}

// =============================================================================
// Exception Handling
// =============================================================================

mod exception_handling {
    use super::*;

    #[test]
    fn signal_basic() {
        // SQL:2016 PSM: SIGNAL statement - NOT YET IMPLEMENTED
        verified_standard_stmt("SIGNAL SQLSTATE '45000'");
    }

    #[test]
    fn signal_with_message() {
        // SQL:2016 PSM: SIGNAL with message - NOT YET IMPLEMENTED
        verified_standard_stmt("SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Custom error'");
    }

    #[test]
    fn resignal_basic() {
        // SQL:2016 PSM: RESIGNAL statement - NOT YET IMPLEMENTED
        verified_standard_stmt("RESIGNAL");
    }

    #[test]
    fn resignal_with_state() {
        // SQL:2016 PSM: RESIGNAL with SQLSTATE - NOT YET IMPLEMENTED
        verified_standard_stmt("RESIGNAL SQLSTATE '45000'");
    }

    #[test]
    fn begin_exception_end() {
        // SQL:2016 PSM: BEGIN...EXCEPTION...END - NOT YET IMPLEMENTED
        verified_standard_stmt(
            "BEGIN SELECT 1; EXCEPTION WHEN SQLEXCEPTION THEN SELECT 'error'; END",
        );
    }
}

// =============================================================================
// Cursor Operations
// =============================================================================

mod cursor_operations {
    use super::*;

    #[test]
    fn open_cursor() {
        // SQL:2016 PSM: OPEN cursor
        let stmt = verified_standard_stmt("OPEN cur");
        match stmt {
            Statement::Open(open_stmt) => {
                assert_eq!(open_stmt.cursor_name.value, "cur");
            }
            _ => panic!("Expected Open statement, got {:?}", stmt),
        }
    }

    #[test]
    fn fetch_cursor() {
        // SQL:2016 PSM: FETCH cursor
        // Note: FETCH requires a direction keyword (NEXT, PRIOR, etc.)
        // This test covers the basic FETCH with implicit NEXT direction
        let stmt = verified_standard_stmt("FETCH NEXT FROM cur INTO x");
        match stmt {
            Statement::Fetch {
                name,
                into,
                direction,
                position,
                ..
            } => {
                assert_eq!(name.value, "cur");
                assert_eq!(direction, FetchDirection::Next);
                assert_eq!(position, FetchPosition::From);
                assert!(into.is_some());
                if let Some(obj_name) = into {
                    assert_eq!(obj_name.to_string(), "x");
                }
            }
            _ => panic!("Expected Fetch statement, got {:?}", stmt),
        }
    }

    #[test]
    fn close_cursor() {
        // SQL:2016 PSM: CLOSE cursor
        verified_standard_stmt("CLOSE cur");
        verified_standard_stmt("CLOSE my_cursor");
    }

    #[test]
    fn fetch_next() {
        // SQL:2016 PSM: FETCH NEXT
        let stmt = verified_standard_stmt("FETCH NEXT FROM cur INTO x");
        match stmt {
            Statement::Fetch {
                name,
                direction,
                position,
                into,
                ..
            } => {
                assert_eq!(name.value, "cur");
                assert_eq!(direction, FetchDirection::Next);
                assert_eq!(position, FetchPosition::From);
                assert!(into.is_some());
            }
            _ => panic!("Expected Fetch statement, got {:?}", stmt),
        }
    }

    #[test]
    fn fetch_prior() {
        // SQL:2016 PSM: FETCH PRIOR
        let stmt = verified_standard_stmt("FETCH PRIOR FROM cur INTO x");
        match stmt {
            Statement::Fetch {
                name,
                direction,
                position,
                into,
                ..
            } => {
                assert_eq!(name.value, "cur");
                assert_eq!(direction, FetchDirection::Prior);
                assert_eq!(position, FetchPosition::From);
                assert!(into.is_some());
            }
            _ => panic!("Expected Fetch statement, got {:?}", stmt),
        }
    }

    #[test]
    fn fetch_first_last() {
        // SQL:2016 PSM: FETCH FIRST/LAST
        let stmt = verified_standard_stmt("FETCH FIRST FROM cur INTO x");
        match stmt {
            Statement::Fetch {
                name,
                direction,
                position,
                into,
                ..
            } => {
                assert_eq!(name.value, "cur");
                assert_eq!(direction, FetchDirection::First);
                assert_eq!(position, FetchPosition::From);
                assert!(into.is_some());
            }
            _ => panic!("Expected Fetch statement, got {:?}", stmt),
        }

        // Test FETCH LAST as well
        let stmt = verified_standard_stmt("FETCH LAST FROM cur INTO x");
        match stmt {
            Statement::Fetch {
                name,
                direction,
                position,
                into,
                ..
            } => {
                assert_eq!(name.value, "cur");
                assert_eq!(direction, FetchDirection::Last);
                assert_eq!(position, FetchPosition::From);
                assert!(into.is_some());
            }
            _ => panic!("Expected Fetch statement, got {:?}", stmt),
        }
    }
}

// =============================================================================
// FOR Statements (Cursor Iteration)
// =============================================================================

mod for_statements {
    use super::*;
    use sqlparser::ast::ForStatement;

    #[test]
    fn for_basic() {
        // SQL:2016 PSM: Basic FOR statement without explicit cursor
        let stmt = verified_standard_stmt(
            "FOR r AS SELECT id, name FROM employees DO INSERT INTO log VALUES (r.id); END FOR",
        );
        match stmt {
            Statement::For(for_stmt) => {
                assert_eq!(for_stmt.loop_name.value, "r");
                assert!(for_stmt.cursor_name.is_none());
                assert!(for_stmt.label.is_none());
                assert!(for_stmt.end_label.is_none());
            }
            _ => panic!("Expected For statement, got {:?}", stmt),
        }
    }

    #[test]
    fn for_with_cursor() {
        // SQL:2016 PSM: FOR with explicit cursor name
        let stmt = verified_standard_stmt(
            "FOR row AS cur CURSOR FOR SELECT * FROM t DO SELECT row.a; END FOR",
        );
        match stmt {
            Statement::For(for_stmt) => {
                assert_eq!(for_stmt.loop_name.value, "row");
                assert_eq!(for_stmt.cursor_name.as_ref().unwrap().value, "cur");
                assert!(for_stmt.label.is_none());
                assert!(for_stmt.end_label.is_none());
            }
            _ => panic!("Expected For statement, got {:?}", stmt),
        }
    }

    #[test]
    fn for_with_label() {
        // SQL:2016 PSM: Labeled FOR statement
        let stmt =
            verified_standard_stmt("my_for: FOR r AS SELECT id FROM t DO SELECT r.id; END FOR");
        match stmt {
            Statement::For(for_stmt) => {
                assert_eq!(for_stmt.label.as_ref().unwrap().value, "my_for");
                assert_eq!(for_stmt.loop_name.value, "r");
                assert!(for_stmt.end_label.is_none());
            }
            _ => panic!("Expected For statement, got {:?}", stmt),
        }

        // With end label
        let stmt = verified_standard_stmt(
            "my_for: FOR r AS SELECT id FROM t DO SELECT r.id; END FOR my_for",
        );
        match stmt {
            Statement::For(for_stmt) => {
                assert_eq!(for_stmt.label.as_ref().unwrap().value, "my_for");
                assert_eq!(for_stmt.end_label.as_ref().unwrap().value, "my_for");
            }
            _ => panic!("Expected For statement, got {:?}", stmt),
        }
    }

    #[test]
    fn for_with_label_and_cursor() {
        // SQL:2016 PSM: Labeled FOR with explicit cursor
        let stmt = verified_standard_stmt(
            "my_for: FOR row AS cur CURSOR FOR SELECT * FROM t DO SELECT row.x; END FOR my_for",
        );
        match stmt {
            Statement::For(for_stmt) => {
                assert_eq!(for_stmt.label.as_ref().unwrap().value, "my_for");
                assert_eq!(for_stmt.loop_name.value, "row");
                assert_eq!(for_stmt.cursor_name.as_ref().unwrap().value, "cur");
                assert_eq!(for_stmt.end_label.as_ref().unwrap().value, "my_for");
            }
            _ => panic!("Expected For statement, got {:?}", stmt),
        }
    }

    #[test]
    fn for_multiple_statements() {
        // SQL:2016 PSM: FOR with multiple statements in body
        verified_standard_stmt(
            "FOR r AS SELECT id, name FROM t DO INSERT INTO log VALUES (r.id); UPDATE stats SET count = count + 1; END FOR",
        );
    }

    #[test]
    fn for_nested() {
        // SQL:2016 PSM: Nested FOR statements
        verified_standard_stmt(
            "outer: FOR a AS SELECT x FROM t1 DO inner: FOR b AS SELECT y FROM t2 DO SELECT a.x, b.y; END FOR; END FOR",
        );
    }

    #[test]
    fn for_with_leave() {
        // SQL:2016 PSM: FOR with LEAVE
        verified_standard_stmt(
            "my_for: FOR r AS SELECT id FROM t DO IF r.id > 100 THEN LEAVE my_for; END IF; END FOR",
        );
    }

    #[test]
    fn for_with_iterate() {
        // SQL:2016 PSM: FOR with ITERATE
        verified_standard_stmt(
            "my_for: FOR r AS SELECT id FROM t DO IF r.id % 2 = 0 THEN ITERATE my_for; END IF; SELECT r.id; END FOR",
        );
    }

    #[test]
    fn for_complex_query() {
        // SQL:2016 PSM: FOR with complex query
        verified_standard_stmt(
            "FOR r AS SELECT e.id, e.name, d.dept_name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id WHERE e.active = true DO INSERT INTO report VALUES (r.id, r.name, r.dept_name); END FOR",
        );
    }
}

// =============================================================================
// RETURN Statements
// =============================================================================

mod return_statements {
    use super::*;

    #[test]
    fn return_basic() {
        // SQL:2016 PSM: RETURN statement
        verified_standard_stmt("CREATE FUNCTION f() RETURNS INT RETURN 42");
        verified_standard_stmt("CREATE FUNCTION get_value() RETURNS VARCHAR RETURN 'hello'");
    }

    #[test]
    fn return_expression() {
        // SQL:2016 PSM: RETURN with expression
        verified_standard_stmt("CREATE FUNCTION add(a INT, b INT) RETURNS INT RETURN a + b");
        verified_standard_stmt("CREATE FUNCTION square(x INT) RETURNS INT RETURN x * x");
    }

    #[test]
    fn return_in_procedure() {
        // SQL:2016 PSM: RETURN in procedure (exit without value) - NOT YET IMPLEMENTED
        verified_standard_stmt(
            "CREATE PROCEDURE proc() BEGIN IF error THEN RETURN; END IF; SELECT 1; END",
        );
    }
}

// =============================================================================
// CALL Statements
// =============================================================================

mod call_statements {
    use super::*;

    #[test]
    fn call_basic() {
        // SQL:2016 PSM: Basic CALL statement
        verified_standard_stmt("CALL proc()");
        verified_standard_stmt("CALL my_procedure()");
    }

    #[test]
    fn call_with_arguments() {
        // SQL:2016 PSM: CALL with arguments
        verified_standard_stmt("CALL proc(1, 2, 3)");
        verified_standard_stmt("CALL update_user('john', 'doe', 30)");
    }

    #[test]
    fn call_with_expressions() {
        // SQL:2016 PSM: CALL with expression arguments
        verified_standard_stmt("CALL calculate(x + y, z * 2)");
        verified_standard_stmt("CALL process(COUNT(*), MAX(value))");
    }
}

// =============================================================================
// Complex Integration Tests
// =============================================================================

mod integration_tests {
    use super::*;

    #[test]
    fn loop_with_if_and_leave() {
        // Complex LOOP with IF and LEAVE
        verified_standard_stmt(
            "my_loop: LOOP IF x > 100 THEN LEAVE my_loop; END IF; SELECT x; END LOOP",
        );
    }

    #[test]
    fn while_with_case() {
        // WHILE with CASE statement - NOT YET IMPLEMENTED
        // WHILE with DO not yet supported
        verified_standard_stmt("WHILE counter < max DO CASE action WHEN 'insert' THEN INSERT INTO t VALUES (counter); WHEN 'update' THEN UPDATE t SET x = counter; END CASE; END WHILE");
    }

    #[test]
    fn nested_loops_with_labels() {
        // Nested loops with labels and control flow - NOT YET IMPLEMENTED
        // Nested labels not fully supported
        verified_standard_stmt("outer: LOOP inner: LOOP IF x > 10 THEN LEAVE outer; ELSEIF x > 5 THEN LEAVE inner; ELSE ITERATE inner; END IF; END LOOP; END LOOP");
    }

    #[test]
    fn repeat_with_begin_end() {
        // REPEAT with BEGIN...END blocks
        // Semicolon after END is optional and normalized away
        use crate::standards::common::one_statement_parses_to_std;
        one_statement_parses_to_std(
            "my_repeat: REPEAT BEGIN SELECT x; SELECT y; END; UNTIL x > 100 END REPEAT",
            "my_repeat: REPEAT BEGIN SELECT x; SELECT y; END UNTIL x > 100 END REPEAT",
        );
    }

    #[test]
    fn if_with_nested_loops() {
        // IF statement with nested loops - NOT YET IMPLEMENTED
        // Labels within IF branches and WHILE DO not supported
        verified_standard_stmt("IF condition THEN outer: LOOP inner: WHILE y < 10 DO SELECT x, y; END WHILE; END LOOP; ELSE SELECT 'skipped'; END IF");
    }

    #[test]
    fn case_with_multiple_control_structures() {
        // CASE with multiple control structures - NOT YET IMPLEMENTED
        // LEAVE without label and WHILE DO not supported
        verified_standard_stmt("CASE mode WHEN 'loop' THEN LOOP SELECT 1; LEAVE; END LOOP; WHEN 'repeat' THEN REPEAT SELECT 2; UNTIL true END REPEAT; WHEN 'while' THEN WHILE false DO SELECT 3; END WHILE; END CASE");
    }

    #[test]
    fn complex_nested_structure() {
        // Complex nested control structures
        // Semicolon after END in ELSE branch is optional and normalized away
        use crate::standards::common::one_statement_parses_to_std;
        one_statement_parses_to_std(
            "outer: BEGIN inner_loop: LOOP IF x > 100 THEN LEAVE outer; END IF; CASE x % 3 WHEN 0 THEN SELECT 'divisible by 3'; WHEN 1 THEN ITERATE inner_loop; ELSE BEGIN SELECT x; SELECT x * 2; END; END CASE; END LOOP; END",
            "outer: BEGIN inner_loop: LOOP IF x > 100 THEN LEAVE outer; END IF; CASE x % 3 WHEN 0 THEN SELECT 'divisible by 3'; WHEN 1 THEN ITERATE inner_loop; ELSE BEGIN SELECT x; SELECT x * 2; END END CASE; END LOOP; END",
        );
    }

    #[test]
    fn while_repeat_loop_combination() {
        // Combination of WHILE, REPEAT, and LOOP - NOT YET IMPLEMENTED
        // WHILE DO and nested labels not supported
        verified_standard_stmt("outer: WHILE x < 100 DO middle: REPEAT inner: LOOP IF x % 5 = 0 THEN LEAVE inner; END IF; ITERATE inner; END LOOP; UNTIL y > 50 END REPEAT; END WHILE");
    }

    #[test]
    fn multiple_leave_iterate() {
        // Multiple LEAVE and ITERATE in nested loops
        verified_standard_stmt(
            "l1: LOOP l2: LOOP l3: LOOP IF a THEN LEAVE l1; ELSEIF b THEN LEAVE l2; ELSEIF c THEN ITERATE l3; ELSEIF d THEN ITERATE l2; ELSE ITERATE l1; END IF; END LOOP; END LOOP; END LOOP"
        );
    }

    #[test]
    fn procedure_with_control_flow() {
        // CREATE PROCEDURE with control flow
        // Empty parens () round-trip to (), no parens round-trips to no parens
        let stmt = verified_standard_stmt(
            "CREATE PROCEDURE process_data() AS BEGIN my_loop: LOOP IF finished THEN LEAVE my_loop; END IF; CASE status WHEN 'pending' THEN SELECT 'processing'; WHEN 'error' THEN LEAVE my_loop; END CASE; END LOOP; END"
        );
        match stmt {
            Statement::CreateProcedure {
                name, params, body, ..
            } => {
                assert_eq!(name.to_string(), "process_data");
                assert_eq!(params, Some(vec![]));
                // Verify the body contains BEGIN...END with a labeled LOOP
                match body {
                    _ => {
                        // Body is successfully parsed - detailed AST validation could be added here
                        // For now, we verify it parses and round-trips correctly
                    }
                }
            }
            _ => panic!("Expected CreateProcedure statement, got {:?}", stmt),
        }
    }

    #[test]
    fn function_with_loop() {
        // CREATE FUNCTION with LOOP - NOT YET IMPLEMENTED
        // Function body with labeled loops needs better support
        verified_standard_stmt("CREATE FUNCTION factorial(n INT) RETURNS INT AS BEGIN result: LOOP IF n <= 1 THEN LEAVE result; END IF; END LOOP; RETURN n; END");
    }

    #[test]
    fn trigger_with_control_flow() {
        // CREATE TRIGGER with control flow
        verified_standard_stmt(
            "CREATE TRIGGER validate_insert BEFORE INSERT ON t FOR EACH ROW BEGIN IF NEW.value < 0 THEN CASE NEW.type WHEN 'a' THEN SELECT 'error type a'; WHEN 'b' THEN SELECT 'error type b'; END CASE; END IF; END"
        );
    }
}
