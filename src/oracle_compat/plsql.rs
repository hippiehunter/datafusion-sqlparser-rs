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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::OracleCase;

pub const PLSQL_CASES: &[OracleCase] = &[
    OracleCase {
        id: "plsql.block.empty",
        feature: "Block",
        sql: "BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.block.declare",
        feature: "Block",
        sql: "DECLARE message VARCHAR2(100) := 'hello'; BEGIN DBMS_OUTPUT.PUT_LINE(message); END;",
    },
    OracleCase {
        id: "plsql.block.nested",
        feature: "Block",
        sql: "DECLARE outer_value NUMBER := 1; BEGIN DECLARE inner_value NUMBER := 2; BEGIN outer_value := outer_value + inner_value; END; END;",
    },
    OracleCase {
        id: "plsql.block.label",
        feature: "Block",
        sql: "<<main>> DECLARE n NUMBER := 0; BEGIN n := main.n + 1; END main;",
    },
    OracleCase {
        id: "plsql.declaration.scalar",
        feature: "Scalar Variable Declaration",
        sql: "DECLARE amount NUMBER(12,2) NOT NULL := 0; enabled BOOLEAN DEFAULT TRUE; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.constant",
        feature: "Constant Declaration",
        sql: "DECLARE tax_rate CONSTANT PLS_INTEGER := 7; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.subtype",
        feature: "Subtype Declaration",
        sql: "DECLARE SUBTYPE short_text IS VARCHAR2(30) NOT NULL; name short_text := 'Ada'; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.percent_type",
        feature: "%TYPE Attribute",
        sql: "DECLARE employee_name employees.last_name%TYPE; BEGIN SELECT last_name INTO employee_name FROM employees WHERE employee_id = 100; END;",
    },
    OracleCase {
        id: "plsql.declaration.percent_rowtype",
        feature: "%ROWTYPE Attribute",
        sql: "DECLARE employee_row employees%ROWTYPE; BEGIN SELECT * INTO employee_row FROM employees WHERE employee_id = 100; END;",
    },
    OracleCase {
        id: "plsql.declaration.record",
        feature: "Record Variable Declaration",
        sql: "DECLARE TYPE employee_rec_t IS RECORD (employee_id NUMBER, name VARCHAR2(100), hired_on DATE); employee_rec employee_rec_t; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.associative_array",
        feature: "Collection Variable Declaration",
        sql: "DECLARE TYPE salary_map_t IS TABLE OF NUMBER INDEX BY VARCHAR2(100); salaries salary_map_t; BEGIN salaries('Ada') := 1000; END;",
    },
    OracleCase {
        id: "plsql.declaration.nested_table",
        feature: "Collection Variable Declaration",
        sql: "DECLARE TYPE number_list_t IS TABLE OF NUMBER; numbers number_list_t := number_list_t(1, 2, 3); BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.varray",
        feature: "Collection Variable Declaration",
        sql: "DECLARE TYPE colors_t IS VARRAY(3) OF VARCHAR2(20); colors colors_t := colors_t('red', 'green'); BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.exception",
        feature: "Exception Declaration",
        sql: "DECLARE invalid_state EXCEPTION; BEGIN RAISE invalid_state; END;",
    },
    OracleCase {
        id: "plsql.declaration.cursor",
        feature: "Explicit Cursor Declaration and Definition",
        sql: "DECLARE CURSOR employee_cursor(p_department_id departments.department_id%TYPE) RETURN employees%ROWTYPE IS SELECT * FROM employees WHERE department_id = p_department_id; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.ref_cursor",
        feature: "Cursor Variable Declaration",
        sql: "DECLARE TYPE employee_cursor_t IS REF CURSOR RETURN employees%ROWTYPE; employee_cursor employee_cursor_t; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.qualified_expression",
        feature: "Qualified Expression",
        sql: "DECLARE TYPE pair_t IS RECORD (x NUMBER, y NUMBER); pair pair_t := pair_t(x => 1, y => 2); BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.assignment",
        feature: "Assignment Statement",
        sql: "DECLARE n NUMBER; BEGIN n := 42; END;",
    },
    OracleCase {
        id: "plsql.assignment.composite",
        feature: "Assignment Statement",
        sql: "DECLARE employee employees%ROWTYPE; BEGIN employee.employee_id := 100; END;",
    },
    OracleCase {
        id: "plsql.if",
        feature: "IF Statement",
        sql: "BEGIN IF salary > 20000 THEN bonus := 1000; ELSIF salary > 10000 THEN bonus := 500; ELSE bonus := 0; END IF; END;",
    },
    OracleCase {
        id: "plsql.case.simple",
        feature: "CASE Statement",
        sql: "BEGIN CASE grade WHEN 'A' THEN merit := 3; WHEN 'B' THEN merit := 2; ELSE merit := 0; END CASE; END;",
    },
    OracleCase {
        id: "plsql.case.searched",
        feature: "CASE Statement",
        sql: "BEGIN CASE WHEN score >= 90 THEN grade := 'A'; WHEN score >= 80 THEN grade := 'B'; ELSE grade := 'C'; END CASE; END;",
    },
    OracleCase {
        id: "plsql.case.extended_controls",
        feature: "CASE Statement",
        sql: "BEGIN CASE selector WHEN 1, 2 THEN result := 'small'; WHEN > 2, <= 5 THEN result := 'medium'; ELSE result := 'large'; END CASE; END;",
    },
    OracleCase {
        id: "plsql.loop.basic",
        feature: "Basic LOOP Statement",
        sql: "BEGIN LOOP counter := counter + 1; EXIT WHEN counter >= 10; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.loop.while",
        feature: "WHILE LOOP Statement",
        sql: "BEGIN WHILE counter < 10 LOOP counter := counter + 1; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.loop.numeric_for",
        feature: "FOR LOOP Statement",
        sql: "BEGIN FOR i IN REVERSE 1..10 LOOP total := total + i; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.loop.cursor_for",
        feature: "Cursor FOR LOOP Statement",
        sql: "BEGIN FOR employee IN (SELECT employee_id, last_name FROM employees) LOOP DBMS_OUTPUT.PUT_LINE(employee.last_name); END LOOP; END;",
    },
    OracleCase {
        id: "plsql.loop.iterator",
        feature: "Iterator",
        sql: "BEGIN FOR item MUTABLE ITERATOR IN numbers LOOP item := item * 2; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.loop.exit",
        feature: "EXIT Statement",
        sql: "BEGIN <<scan>> LOOP EXIT scan WHEN done; END LOOP scan; END;",
    },
    OracleCase {
        id: "plsql.loop.continue",
        feature: "CONTINUE Statement",
        sql: "BEGIN FOR i IN 1..10 LOOP CONTINUE WHEN MOD(i, 2) = 0; total := total + i; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.goto",
        feature: "GOTO Statement",
        sql: "BEGIN GOTO finished; work := 1; <<finished>> NULL; END;",
    },
    OracleCase {
        id: "plsql.null",
        feature: "NULL Statement",
        sql: "BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.exception.handlers",
        feature: "Exception Handler",
        sql: "BEGIN risky_operation; EXCEPTION WHEN NO_DATA_FOUND THEN NULL; WHEN ZERO_DIVIDE OR VALUE_ERROR THEN RAISE; WHEN OTHERS THEN log_error(SQLCODE, SQLERRM); END;",
    },
    OracleCase {
        id: "plsql.exception.raise",
        feature: "RAISE Statement",
        sql: "DECLARE invalid_state EXCEPTION; BEGIN RAISE invalid_state; EXCEPTION WHEN invalid_state THEN RAISE; END;",
    },
    OracleCase {
        id: "plsql.pragma.exception_init",
        feature: "EXCEPTION_INIT Pragma",
        sql: "DECLARE deadlock_detected EXCEPTION; PRAGMA EXCEPTION_INIT(deadlock_detected, -60); BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.pragma.autonomous_transaction",
        feature: "AUTONOMOUS_TRANSACTION Pragma",
        sql: "DECLARE PRAGMA AUTONOMOUS_TRANSACTION; BEGIN INSERT INTO audit_log(message) VALUES ('event'); COMMIT; END;",
    },
    OracleCase {
        id: "plsql.pragma.inline",
        feature: "INLINE Pragma",
        sql: "BEGIN PRAGMA INLINE(calculate_total, 'YES'); total := calculate_total; END;",
    },
    OracleCase {
        id: "plsql.pragma.deprecate",
        feature: "DEPRECATE Pragma",
        sql: "CREATE OR REPLACE PROCEDURE old_api IS PRAGMA DEPRECATE(old_api, 'Use new_api'); BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.pragma.coverage",
        feature: "COVERAGE Pragma",
        sql: "BEGIN PRAGMA COVERAGE('dead-code'); NULL; END;",
    },
    OracleCase {
        id: "plsql.pragma.udf",
        feature: "UDF Pragma",
        sql: "CREATE OR REPLACE FUNCTION fast_double(n NUMBER) RETURN NUMBER IS PRAGMA UDF; BEGIN RETURN n * 2; END;",
    },
    OracleCase {
        id: "plsql.pragma.serially_reusable",
        feature: "SERIALLY_REUSABLE Pragma",
        sql: "CREATE OR REPLACE PACKAGE session_state IS PRAGMA SERIALLY_REUSABLE; counter NUMBER := 0; END session_state;",
    },
    OracleCase {
        id: "plsql.pragma.restrict_references",
        feature: "RESTRICT_REFERENCES Pragma",
        sql: "CREATE OR REPLACE PACKAGE math_api IS FUNCTION twice(n NUMBER) RETURN NUMBER; PRAGMA RESTRICT_REFERENCES(twice, WNDS, RNDS, WNPS, RNPS); END math_api;",
    },
    OracleCase {
        id: "plsql.pragma.suppresses_warning",
        feature: "SUPPRESSES_WARNING_6009 Pragma",
        sql: "CREATE OR REPLACE PROCEDURE intentional_swallow IS PRAGMA SUPPRESSES_WARNING_6009(intentional_swallow); BEGIN NULL; EXCEPTION WHEN OTHERS THEN NULL; END;",
    },
    OracleCase {
        id: "plsql.cursor.open_fetch_close",
        feature: "OPEN Statement",
        sql: "DECLARE CURSOR c IS SELECT employee_id FROM employees; id NUMBER; BEGIN OPEN c; FETCH c INTO id; CLOSE c; END;",
    },
    OracleCase {
        id: "plsql.cursor.fetch_bulk",
        feature: "FETCH Statement",
        sql: "DECLARE CURSOR c IS SELECT employee_id FROM employees; TYPE ids_t IS TABLE OF NUMBER; ids ids_t; BEGIN OPEN c; FETCH c BULK COLLECT INTO ids LIMIT 100; CLOSE c; END;",
    },
    OracleCase {
        id: "plsql.cursor.open_for",
        feature: "OPEN FOR Statement",
        sql: "DECLARE c SYS_REFCURSOR; BEGIN OPEN c FOR SELECT * FROM employees WHERE department_id = :department_id; END;",
    },
    OracleCase {
        id: "plsql.cursor.attributes",
        feature: "Named Cursor Attribute",
        sql: "DECLARE CURSOR c IS SELECT employee_id FROM employees; id NUMBER; BEGIN OPEN c; LOOP FETCH c INTO id; EXIT WHEN c%NOTFOUND; END LOOP; IF c%ISOPEN THEN CLOSE c; END IF; END;",
    },
    OracleCase {
        id: "plsql.cursor.sql_attributes",
        feature: "Implicit Cursor Attribute",
        sql: "BEGIN UPDATE employees SET salary = salary + 1; IF SQL%FOUND THEN rows_changed := SQL%ROWCOUNT; END IF; END;",
    },
    OracleCase {
        id: "plsql.sql.select_into",
        feature: "SELECT INTO Statement",
        sql: "DECLARE employee employees%ROWTYPE; BEGIN SELECT * INTO employee FROM employees WHERE employee_id = 100; END;",
    },
    OracleCase {
        id: "plsql.sql.returning",
        feature: "RETURNING INTO Clause",
        sql: "DECLARE new_salary NUMBER; BEGIN UPDATE employees SET salary = salary + 100 WHERE employee_id = 100 RETURNING salary INTO new_salary; END;",
    },
    OracleCase {
        id: "plsql.sql.bulk_collect",
        feature: "SELECT INTO Statement",
        sql: "DECLARE TYPE names_t IS TABLE OF employees.last_name%TYPE; names names_t; BEGIN SELECT last_name BULK COLLECT INTO names FROM employees ORDER BY last_name; END;",
    },
    OracleCase {
        id: "plsql.forall.indices",
        feature: "FORALL Statement",
        sql: "DECLARE TYPE ids_t IS TABLE OF NUMBER INDEX BY PLS_INTEGER; ids ids_t; BEGIN FORALL i IN INDICES OF ids SAVE EXCEPTIONS DELETE FROM employees WHERE employee_id = ids(i); END;",
    },
    OracleCase {
        id: "plsql.forall.values",
        feature: "FORALL Statement",
        sql: "DECLARE TYPE ids_t IS TABLE OF NUMBER; ids ids_t; TYPE indexes_t IS TABLE OF PLS_INTEGER; chosen_indexes indexes_t; BEGIN FORALL i IN VALUES OF chosen_indexes UPDATE employees SET active = TRUE WHERE employee_id = ids(i); END;",
    },
    OracleCase {
        id: "plsql.dynamic.execute_immediate",
        feature: "EXECUTE IMMEDIATE Statement",
        sql: "BEGIN EXECUTE IMMEDIATE 'UPDATE employees SET salary = salary * :1 WHERE department_id = :2' USING 1.05, 10; END;",
    },
    OracleCase {
        id: "plsql.dynamic.into",
        feature: "EXECUTE IMMEDIATE Statement",
        sql: "DECLARE count_value NUMBER; BEGIN EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM employees WHERE department_id = :1' INTO count_value USING 10; END;",
    },
    OracleCase {
        id: "plsql.dynamic.bulk",
        feature: "EXECUTE IMMEDIATE Statement",
        sql: "DECLARE TYPE names_t IS TABLE OF VARCHAR2(100); names names_t; BEGIN EXECUTE IMMEDIATE 'SELECT last_name FROM employees' BULK COLLECT INTO names; END;",
    },
    OracleCase {
        id: "plsql.dynamic.returning",
        feature: "EXECUTE IMMEDIATE Statement",
        sql: "DECLARE new_salary NUMBER; BEGIN EXECUTE IMMEDIATE 'UPDATE employees SET salary = salary + :1 WHERE employee_id = :2 RETURNING salary INTO :3' USING 100, 10 RETURNING INTO new_salary; END;",
    },
    OracleCase {
        id: "plsql.collection.methods",
        feature: "Collection Method Invocation",
        sql: "DECLARE TYPE numbers_t IS TABLE OF NUMBER; numbers numbers_t := numbers_t(1, 2); BEGIN numbers.EXTEND; numbers(3) := 3; numbers.DELETE(1); count_value := numbers.COUNT; index_value := numbers.FIRST; END;",
    },
    OracleCase {
        id: "plsql.pipe_row",
        feature: "PIPE ROW Statement",
        sql: "BEGIN FOR i IN 1..10 LOOP PIPE ROW(number_t(i)); END LOOP; RETURN; END;",
    },
    OracleCase {
        id: "plsql.return",
        feature: "RETURN Statement",
        sql: "BEGIN IF value < 0 THEN RETURN; END IF; value := value + 1; END;",
    },
    OracleCase {
        id: "create.procedure.basic",
        feature: "CREATE PROCEDURE",
        sql: "CREATE OR REPLACE PROCEDURE raise_salary(p_employee_id IN employees.employee_id%TYPE, p_percent IN NUMBER DEFAULT 0.05) IS BEGIN UPDATE employees SET salary = salary * (1 + p_percent) WHERE employee_id = p_employee_id; END raise_salary;",
    },
    OracleCase {
        id: "create.procedure.authid",
        feature: "CREATE PROCEDURE",
        sql: "CREATE OR REPLACE EDITIONABLE PROCEDURE run_report(p_result OUT NOCOPY SYS_REFCURSOR) AUTHID CURRENT_USER ACCESSIBLE BY (PACKAGE reporting_api) IS BEGIN OPEN p_result FOR SELECT * FROM employees; END;",
    },
    OracleCase {
        id: "create.procedure.if_not_exists",
        feature: "CREATE PROCEDURE",
        sql: "CREATE PROCEDURE IF NOT EXISTS initialize_app AUTHID DEFINER IS BEGIN NULL; END;",
    },
    OracleCase {
        id: "alter.procedure",
        feature: "ALTER PROCEDURE",
        sql: "ALTER PROCEDURE raise_salary COMPILE REUSE SETTINGS",
    },
    OracleCase {
        id: "drop.procedure",
        feature: "DROP PROCEDURE",
        sql: "DROP PROCEDURE raise_salary",
    },
    OracleCase {
        id: "create.function.basic",
        feature: "CREATE FUNCTION",
        sql: "CREATE OR REPLACE FUNCTION annual_salary(p_monthly NUMBER) RETURN NUMBER DETERMINISTIC IS BEGIN RETURN p_monthly * 12; END annual_salary;",
    },
    OracleCase {
        id: "create.function.result_cache",
        feature: "RESULT_CACHE Clause",
        sql: "CREATE OR REPLACE FUNCTION department_name(p_id NUMBER) RETURN VARCHAR2 RESULT_CACHE RELIES_ON (departments) AUTHID DEFINER IS result VARCHAR2(100); BEGIN SELECT department_name INTO result FROM departments WHERE department_id = p_id; RETURN result; END;",
    },
    OracleCase {
        id: "create.function.pipelined",
        feature: "PIPELINED Clause",
        sql: "CREATE OR REPLACE FUNCTION generate_numbers(p_count PLS_INTEGER) RETURN number_table_t PIPELINED PARALLEL_ENABLE(PARTITION p_count BY ANY) IS BEGIN FOR i IN 1..p_count LOOP PIPE ROW(i); END LOOP; RETURN; END;",
    },
    OracleCase {
        id: "create.function.sql_macro",
        feature: "SQL_MACRO Clause",
        sql: "CREATE OR REPLACE FUNCTION active_employees RETURN VARCHAR2 SQL_MACRO(TABLE) IS BEGIN RETURN q'[SELECT * FROM employees WHERE active IS TRUE]'; END;",
    },
    OracleCase {
        id: "create.function.aggregate",
        feature: "AGGREGATE Clause",
        sql: "CREATE FUNCTION second_max(input NUMBER) RETURN NUMBER PARALLEL_ENABLE AGGREGATE USING second_max_impl_t",
    },
    OracleCase {
        id: "create.function.call_spec.java",
        feature: "Call Specification",
        sql: "CREATE OR REPLACE FUNCTION java_hash(value VARCHAR2) RETURN NUMBER AS LANGUAGE JAVA NAME 'Hashing.hash(java.lang.String) return int'",
    },
    OracleCase {
        id: "create.function.call_spec.c",
        feature: "Call Specification",
        sql: "CREATE OR REPLACE FUNCTION c_hash(value VARCHAR2) RETURN BINARY_INTEGER AS LANGUAGE C NAME \"hash_value\" LIBRARY hash_lib PARAMETERS (value STRING, RETURN INT)",
    },
    OracleCase {
        id: "alter.function",
        feature: "ALTER FUNCTION",
        sql: "ALTER FUNCTION annual_salary COMPILE DEBUG REUSE SETTINGS",
    },
    OracleCase {
        id: "drop.function",
        feature: "DROP FUNCTION",
        sql: "DROP FUNCTION annual_salary",
    },
    OracleCase {
        id: "create.package.spec",
        feature: "CREATE PACKAGE",
        sql: "CREATE OR REPLACE EDITIONABLE PACKAGE employee_api AUTHID DEFINER ACCESSIBLE BY (PROCEDURE app_entry) IS TYPE employee_ids_t IS TABLE OF NUMBER INDEX BY PLS_INTEGER; PROCEDURE raise_salary(p_id NUMBER, p_percent NUMBER); FUNCTION employee_name(p_id NUMBER) RETURN VARCHAR2; END employee_api;",
    },
    OracleCase {
        id: "create.package.body",
        feature: "CREATE PACKAGE BODY",
        sql: "CREATE OR REPLACE PACKAGE BODY employee_api IS PROCEDURE raise_salary(p_id NUMBER, p_percent NUMBER) IS BEGIN UPDATE employees SET salary = salary * (1 + p_percent) WHERE employee_id = p_id; END; FUNCTION employee_name(p_id NUMBER) RETURN VARCHAR2 IS result VARCHAR2(100); BEGIN SELECT last_name INTO result FROM employees WHERE employee_id = p_id; RETURN result; END; BEGIN NULL; END employee_api;",
    },
    OracleCase {
        id: "create.package.if_not_exists",
        feature: "CREATE PACKAGE",
        sql: "CREATE PACKAGE IF NOT EXISTS app_constants IS version CONSTANT VARCHAR2(20) := '1.0'; END app_constants;",
    },
    OracleCase {
        id: "alter.package.spec",
        feature: "ALTER PACKAGE",
        sql: "ALTER PACKAGE employee_api COMPILE SPECIFICATION REUSE SETTINGS",
    },
    OracleCase {
        id: "alter.package.body",
        feature: "ALTER PACKAGE",
        sql: "ALTER PACKAGE employee_api COMPILE BODY DEBUG",
    },
    OracleCase {
        id: "drop.package",
        feature: "DROP PACKAGE",
        sql: "DROP PACKAGE employee_api",
    },
    OracleCase {
        id: "drop.package.body",
        feature: "DROP PACKAGE BODY",
        sql: "DROP PACKAGE BODY employee_api",
    },
    OracleCase {
        id: "create.trigger.row",
        feature: "CREATE TRIGGER",
        sql: "CREATE OR REPLACE TRIGGER employees_biu BEFORE INSERT OR UPDATE OF salary ON employees FOR EACH ROW WHEN (NEW.salary < 0) BEGIN :NEW.salary := 0; END;",
    },
    OracleCase {
        id: "create.trigger.statement",
        feature: "CREATE TRIGGER",
        sql: "CREATE OR REPLACE TRIGGER employees_audit AFTER INSERT OR UPDATE OR DELETE ON employees BEGIN INSERT INTO audit_log(message) VALUES (ORA_SYSEVENT); END;",
    },
    OracleCase {
        id: "create.trigger.instead_of",
        feature: "CREATE TRIGGER",
        sql: "CREATE OR REPLACE TRIGGER active_employees_ioi INSTEAD OF INSERT ON active_employees FOR EACH ROW BEGIN INSERT INTO employees(employee_id, last_name) VALUES (:NEW.employee_id, :NEW.last_name); END;",
    },
    OracleCase {
        id: "create.trigger.compound",
        feature: "CREATE TRIGGER",
        sql: "CREATE OR REPLACE TRIGGER employees_compound FOR INSERT OR UPDATE ON employees COMPOUND TRIGGER count_rows PLS_INTEGER := 0; BEFORE STATEMENT IS BEGIN count_rows := 0; END BEFORE STATEMENT; AFTER EACH ROW IS BEGIN count_rows := count_rows + 1; END AFTER EACH ROW; AFTER STATEMENT IS BEGIN log_count(count_rows); END AFTER STATEMENT; END;",
    },
    OracleCase {
        id: "create.trigger.ddl",
        feature: "CREATE TRIGGER",
        sql: "CREATE OR REPLACE TRIGGER schema_ddl_audit AFTER CREATE OR ALTER OR DROP ON SCHEMA BEGIN log_ddl(ORA_SYSEVENT, ORA_DICT_OBJ_NAME); END;",
    },
    OracleCase {
        id: "create.trigger.database",
        feature: "CREATE TRIGGER",
        sql: "CREATE OR REPLACE TRIGGER logon_audit AFTER LOGON ON DATABASE BEGIN log_session(SYS_CONTEXT('USERENV', 'SESSION_USER')); END;",
    },
    OracleCase {
        id: "create.trigger.if_not_exists",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER IF NOT EXISTS employees_insert_audit AFTER INSERT ON employees BEGIN log_change('INSERT'); END;",
    },
    OracleCase {
        id: "create.trigger.disabled_ordered_crossedition",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER employees_forward BEFORE UPDATE ON employees FOR EACH ROW FORWARD CROSSEDITION FOLLOWS employees_legacy DISABLE BEGIN :NEW.updated_at := SYSTIMESTAMP; END;",
    },
    OracleCase {
        id: "create.trigger.reverse_precedes",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER employees_reverse BEFORE UPDATE ON employees FOR EACH ROW REVERSE CROSSEDITION PRECEDES employees_forward ENABLE BEGIN :NEW.legacy_value := :NEW.current_value; END;",
    },
    OracleCase {
        id: "create.trigger.call",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER employees_call AFTER INSERT ON employees FOR EACH ROW CALL audit_employee_change(NEW.employee_id)",
    },
    OracleCase {
        id: "create.trigger.ddl_events",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER schema_ddl_events AFTER ANALYZE OR ASSOCIATE STATISTICS OR AUDIT OR COMMENT OR CREATE OR DISASSOCIATE STATISTICS OR DROP OR GRANT OR NOAUDIT OR RENAME OR REVOKE OR TRUNCATE OR DDL ON SCHEMA BEGIN log_ddl_event(ORA_SYSEVENT); END;",
    },
    OracleCase {
        id: "create.trigger.database_after_events",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER pdb_after_events AFTER STARTUP OR DB_ROLE_CHANGE OR SERVERERROR OR LOGON OR SUSPEND OR CLONE ON PLUGGABLE DATABASE BEGIN log_database_event(ORA_SYSEVENT); END;",
    },
    OracleCase {
        id: "create.trigger.database_before_events",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER database_before_events BEFORE SHUTDOWN OR LOGOFF OR UNPLUG ON DATABASE BEGIN log_database_event(ORA_SYSEVENT); END;",
    },
    OracleCase {
        id: "create.trigger.named_schema_set_container",
        feature: "CREATE TRIGGER",
        sql: "CREATE TRIGGER schema_container_change AFTER SET CONTAINER ON hr.SCHEMA BEGIN log_container_change; END;",
    },
    OracleCase {
        id: "alter.trigger",
        feature: "ALTER TRIGGER",
        sql: "ALTER TRIGGER employees_biu ENABLE",
    },
    OracleCase {
        id: "drop.trigger",
        feature: "DROP TRIGGER",
        sql: "DROP TRIGGER employees_biu",
    },
    OracleCase {
        id: "create.type.object",
        feature: "CREATE TYPE",
        sql: "CREATE OR REPLACE TYPE employee_t AS OBJECT (employee_id NUMBER, last_name VARCHAR2(100), MEMBER FUNCTION display_name RETURN VARCHAR2, STATIC FUNCTION create_employee(id NUMBER, name VARCHAR2) RETURN employee_t) NOT FINAL",
    },
    OracleCase {
        id: "create.type.collection",
        feature: "CREATE TYPE",
        sql: "CREATE TYPE number_table_t AS TABLE OF NUMBER",
    },
    OracleCase {
        id: "create.type.varray",
        feature: "CREATE TYPE",
        sql: "CREATE TYPE color_list_t AS VARRAY(10) OF VARCHAR2(30) NOT NULL",
    },
    OracleCase {
        id: "create.type.body",
        feature: "CREATE TYPE BODY",
        sql: "CREATE OR REPLACE TYPE BODY employee_t IS MEMBER FUNCTION display_name RETURN VARCHAR2 IS BEGIN RETURN SELF.last_name; END; STATIC FUNCTION create_employee(id NUMBER, name VARCHAR2) RETURN employee_t IS BEGIN RETURN employee_t(id, name); END; END;",
    },
    OracleCase {
        id: "alter.type",
        feature: "ALTER TYPE",
        sql: "ALTER TYPE employee_t ADD ATTRIBUTE (email VARCHAR2(320)) CASCADE INCLUDING TABLE DATA",
    },
    OracleCase {
        id: "drop.type",
        feature: "DROP TYPE",
        sql: "DROP TYPE employee_t FORCE",
    },
    OracleCase {
        id: "drop.type.body",
        feature: "DROP TYPE BODY",
        sql: "DROP TYPE BODY employee_t",
    },
    OracleCase {
        id: "create.library",
        feature: "CREATE LIBRARY",
        sql: "CREATE OR REPLACE LIBRARY hash_lib AS '/opt/oracle/lib/libhash.so' AGENT 'extproc' CREDENTIAL app_credential",
    },
    OracleCase {
        id: "alter.library",
        feature: "ALTER LIBRARY",
        sql: "ALTER LIBRARY hash_lib COMPILE",
    },
    OracleCase {
        id: "drop.library",
        feature: "DROP LIBRARY",
        sql: "DROP LIBRARY hash_lib",
    },
    OracleCase {
        id: "plsql.clause.accessible_by",
        feature: "ACCESSIBLE BY Clause",
        sql: "CREATE OR REPLACE PROCEDURE internal_api ACCESSIBLE BY (PACKAGE app_api, PROCEDURE app_entry) IS BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.cursor.close",
        feature: "CLOSE Statement",
        sql: "DECLARE c SYS_REFCURSOR; BEGIN OPEN c FOR SELECT 1 FROM dual; CLOSE c; END;",
    },
    OracleCase {
        id: "plsql.clause.compile",
        feature: "COMPILE Clause",
        sql: "ALTER FUNCTION annual_salary COMPILE PLSQL_OPTIMIZE_LEVEL = 3 REUSE SETTINGS",
    },
    OracleCase {
        id: "plsql.datatype.attribute",
        feature: "Datatype Attribute",
        sql: "DECLARE employee employees%ROWTYPE; employee_name employees.last_name%TYPE; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.clause.default_collation",
        feature: "DEFAULT COLLATION Clause",
        sql: "CREATE OR REPLACE PROCEDURE compare_names DEFAULT COLLATION USING_NLS_COMP IS BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.delete.extension",
        feature: "DELETE Statement Extension",
        sql: "DECLARE deleted_name employees.last_name%TYPE; BEGIN DELETE FROM employees WHERE employee_id = 100 RETURNING last_name INTO deleted_name; END;",
    },
    OracleCase {
        id: "plsql.clause.deterministic",
        feature: "DETERMINISTIC Clause",
        sql: "CREATE OR REPLACE FUNCTION double_value(n NUMBER) RETURN NUMBER DETERMINISTIC IS BEGIN RETURN n * 2; END;",
    },
    OracleCase {
        id: "plsql.package.element_specification",
        feature: "Element Specification",
        sql: "CREATE OR REPLACE PACKAGE app_api IS value NUMBER; PROCEDURE initialize; FUNCTION version RETURN VARCHAR2; END app_api;",
    },
    OracleCase {
        id: "plsql.expression",
        feature: "Expression",
        sql: "DECLARE result NUMBER; BEGIN result := (2 + 3) * POWER(4, 2) - CASE WHEN TRUE THEN 1 ELSE 0 END; END;",
    },
    OracleCase {
        id: "plsql.formal_parameters",
        feature: "Formal Parameter Declaration",
        sql: "CREATE OR REPLACE PROCEDURE parameter_modes(p_in IN NUMBER, p_out OUT VARCHAR2, p_value IN OUT NOCOPY CLOB, p_default DATE DEFAULT SYSDATE) IS BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.local_function",
        feature: "Function Declaration and Definition",
        sql: "DECLARE result NUMBER; FUNCTION twice(n NUMBER) RETURN NUMBER IS BEGIN RETURN n * 2; END; BEGIN result := twice(21); END;",
    },
    OracleCase {
        id: "plsql.clause.authid",
        feature: "Invoker’s Rights and Definer’s Rights Clause",
        sql: "CREATE OR REPLACE PROCEDURE current_user_api AUTHID CURRENT_USER IS BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.insert.extension",
        feature: "INSERT Statement Extension",
        sql: "DECLARE new_id NUMBER; BEGIN INSERT INTO employees(employee_id, last_name) VALUES (employee_seq.NEXTVAL, 'Ada') RETURNING employee_id INTO new_id; END;",
    },
    OracleCase {
        id: "plsql.clause.parallel_enable",
        feature: "PARALLEL_ENABLE Clause",
        sql: "CREATE OR REPLACE FUNCTION parallel_transform(p SYS_REFCURSOR) RETURN number_table_t PIPELINED PARALLEL_ENABLE(PARTITION p BY ANY) IS BEGIN RETURN; END;",
    },
    OracleCase {
        id: "plsql.local_procedure",
        feature: "Procedure Declaration and Definition",
        sql: "DECLARE PROCEDURE write_message(message VARCHAR2) IS BEGIN DBMS_OUTPUT.PUT_LINE(message); END; BEGIN write_message('hello'); END;",
    },
    OracleCase {
        id: "plsql.clause.resettable",
        feature: "RESETTABLE Clause",
        sql: "CREATE OR REPLACE PACKAGE resettable_state RESETTABLE AS FUNCTION current_user_name RETURN VARCHAR2; END;",
    },
    OracleCase {
        id: "plsql.clause.sharing",
        feature: "SHARING Clause",
        sql: "CREATE OR REPLACE PACKAGE app_common_api SHARING = METADATA IS PROCEDURE initialize; END app_common_api;",
    },
    OracleCase {
        id: "plsql.update.extension",
        feature: "UPDATE Statement Extensions",
        sql: "DECLARE changed_ids SYS.ODCINUMBERLIST; BEGIN UPDATE employees SET active = TRUE WHERE department_id = 10 RETURNING employee_id BULK COLLECT INTO changed_ids; END;",
    },
    OracleCase {
        id: "plsql.conditional_compilation",
        feature: "Conditional compilation",
        sql: "BEGIN $IF $$PLSQL_DEBUG $THEN DBMS_OUTPUT.PUT_LINE('debug'); $ELSE NULL; $END END;",
    },
    OracleCase {
        id: "plsql.inquiry_directives",
        feature: "Inquiry directives",
        sql: "DECLARE unit_name VARCHAR2(128) := $$PLSQL_UNIT; unit_kind VARCHAR2(128) := $$PLSQL_UNIT_TYPE; version NUMBER := $$PLSQL_VERSION; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.sqlcode_sqlerrm",
        feature: "SQLCODE and SQLERRM",
        sql: "BEGIN RAISE NO_DATA_FOUND; EXCEPTION WHEN OTHERS THEN log_error(SQLCODE, SQLERRM); END;",
    },
    OracleCase {
        id: "plsql.boolean_sql_type",
        feature: "SQL BOOLEAN in PL/SQL",
        sql: "DECLARE enabled BOOLEAN := TRUE; BEGIN INSERT INTO flags(id, enabled) VALUES (1, enabled); SELECT enabled INTO enabled FROM flags WHERE id = 1; END;",
    },
    OracleCase {
        id: "plsql.vector.dense",
        feature: "VECTOR in PL/SQL",
        sql: "DECLARE embedding VECTOR(3, FLOAT32) := TO_VECTOR('[1, 2, 3]'); distance BINARY_DOUBLE; BEGIN distance := VECTOR_DISTANCE(embedding, TO_VECTOR('[3, 2, 1]'), COSINE); END;",
    },
    OracleCase {
        id: "plsql.vector.binary",
        feature: "BINARY VECTOR in PL/SQL",
        sql: "DECLARE bits VECTOR(16, BINARY) := TO_VECTOR('[255, 0]', 16, BINARY); BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.vector.sparse",
        feature: "SPARSE VECTOR in PL/SQL",
        sql: "DECLARE embedding VECTOR(1000, FLOAT32, SPARSE) := TO_VECTOR('{\"1\":1.0,\"999\":0.5}'); BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.vector.arithmetic",
        feature: "VECTOR arithmetic in PL/SQL",
        sql: "DECLARE a VECTOR(3, FLOAT32) := TO_VECTOR('[1,2,3]'); b VECTOR(3, FLOAT32) := TO_VECTOR('[4,5,6]'); result VECTOR(3, FLOAT32); BEGIN result := a + b; END;",
    },
    OracleCase {
        id: "plsql.json.record_constructor",
        feature: "JSON constructor for PL/SQL records",
        sql: "DECLARE TYPE person_t IS RECORD (name VARCHAR2(100), age NUMBER); person person_t := person_t('Ada', 36); document JSON; BEGIN document := JSON(person); END;",
    },
    OracleCase {
        id: "plsql.json.collection_constructor",
        feature: "JSON constructor for PL/SQL collections",
        sql: "DECLARE TYPE names_t IS TABLE OF VARCHAR2(100); names names_t := names_t('Ada', 'Grace'); document JSON; BEGIN document := JSON(names); END;",
    },
    OracleCase {
        id: "plsql.bulk_exceptions",
        feature: "Bulk SQL exception attributes",
        sql: "BEGIN FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP log_error(SQL%BULK_EXCEPTIONS(i).ERROR_INDEX, SQL%BULK_EXCEPTIONS(i).ERROR_CODE); END LOOP; END;",
    },
    OracleCase {
        id: "plsql.current_of",
        feature: "WHERE CURRENT OF",
        sql: "DECLARE CURSOR c IS SELECT employee_id FROM employees FOR UPDATE; BEGIN FOR employee IN c LOOP UPDATE employees SET active = TRUE WHERE CURRENT OF c; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.continue_handler_scope",
        feature: "Exception continuation",
        sql: "BEGIN FOR i IN 1..10 LOOP BEGIN process_item(i); EXCEPTION WHEN OTHERS THEN log_error(SQLCODE, SQLERRM); END; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.declaration.record.defaults",
        feature: "Record Variable Declaration",
        sql: "DECLARE TYPE settings_t IS RECORD (enabled BOOLEAN DEFAULT TRUE, retries PLS_INTEGER := 3, label VARCHAR2(30)); settings settings_t; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.declaration.associative_array.integer",
        feature: "Collection Variable Declaration",
        sql: "DECLARE TYPE values_t IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER; values_by_id values_t; BEGIN values_by_id(10) := 'ten'; END;",
    },
    OracleCase {
        id: "plsql.declaration.cursor.default_parameter",
        feature: "Explicit Cursor Declaration and Definition",
        sql: "DECLARE CURSOR employees_in_department(p_department_id NUMBER DEFAULT 10) IS SELECT employee_id FROM employees WHERE department_id = p_department_id; BEGIN NULL; END;",
    },
    OracleCase {
        id: "plsql.case.without_else",
        feature: "CASE Statement",
        sql: "BEGIN CASE status WHEN 'READY' THEN start_work; WHEN 'WAITING' THEN queue_work; END CASE; END;",
    },
    OracleCase {
        id: "plsql.loop.numeric_forward",
        feature: "FOR LOOP Statement",
        sql: "BEGIN FOR i IN lower_bound..upper_bound LOOP total := total + i; END LOOP; END;",
    },
    OracleCase {
        id: "plsql.loop.continue_labeled",
        feature: "CONTINUE Statement",
        sql: "BEGIN <<outer_loop>> FOR i IN 1..10 LOOP CONTINUE outer_loop WHEN MOD(i, 2) = 0; total := total + i; END LOOP outer_loop; END;",
    },
    OracleCase {
        id: "plsql.exception.multiple_named",
        feature: "Exception Handler",
        sql: "BEGIN process_value; EXCEPTION WHEN NO_DATA_FOUND OR TOO_MANY_ROWS THEN use_default; WHEN OTHERS THEN RAISE; END;",
    },
    OracleCase {
        id: "plsql.cursor.open_with_arguments",
        feature: "OPEN Statement",
        sql: "DECLARE CURSOR c(p_department_id NUMBER) IS SELECT employee_id FROM employees WHERE department_id = p_department_id; id NUMBER; BEGIN OPEN c(20); FETCH c INTO id; CLOSE c; END;",
    },
    OracleCase {
        id: "plsql.cursor.open_dynamic_using",
        feature: "OPEN FOR Statement",
        sql: "DECLARE c SYS_REFCURSOR; statement_text VARCHAR2(4000) := 'SELECT * FROM employees WHERE department_id = :1'; BEGIN OPEN c FOR statement_text USING 20; END;",
    },
    OracleCase {
        id: "plsql.forall.range_save_exceptions",
        feature: "FORALL Statement",
        sql: "DECLARE TYPE ids_t IS TABLE OF NUMBER; ids ids_t := ids_t(10, 20, 30); BEGIN FORALL i IN 1..ids.COUNT SAVE EXCEPTIONS DELETE FROM employees WHERE employee_id = ids(i); END;",
    },
    OracleCase {
        id: "plsql.dynamic.using_modes",
        feature: "EXECUTE IMMEDIATE Statement",
        sql: "DECLARE input_value NUMBER := 10; output_value NUMBER; BEGIN EXECUTE IMMEDIATE 'BEGIN calculate_value(:input, :output); END;' USING IN input_value, OUT output_value; END;",
    },
    OracleCase {
        id: "plsql.collection.navigation",
        feature: "Collection Method Invocation",
        sql: "DECLARE TYPE values_t IS TABLE OF NUMBER; values_list values_t := values_t(10, 20, 30); idx PLS_INTEGER; BEGIN idx := values_list.FIRST; idx := values_list.NEXT(idx); values_list.TRIM; END;",
    },
    OracleCase {
        id: "create.procedure.parameter_assignment_default",
        feature: "Formal Parameter Declaration",
        sql: "CREATE OR REPLACE PROCEDURE configure(p_enabled BOOLEAN := TRUE, p_limit PLS_INTEGER := 100) IS BEGIN NULL; END;",
    },
    OracleCase {
        id: "create.function.authid_result_cache",
        feature: "CREATE FUNCTION",
        sql: "CREATE OR REPLACE FUNCTION lookup_name(p_id NUMBER) RETURN VARCHAR2 AUTHID CURRENT_USER RESULT_CACHE IS result VARCHAR2(100); BEGIN SELECT name INTO result FROM lookup_values WHERE id = p_id; RETURN result; END;",
    },
    OracleCase {
        id: "create.package.body.initialization_exception",
        feature: "CREATE PACKAGE BODY",
        sql: "CREATE OR REPLACE PACKAGE BODY session_state IS PROCEDURE initialize IS BEGIN counter := 0; END; BEGIN initialize; EXCEPTION WHEN OTHERS THEN counter := -1; END session_state;",
    },
    OracleCase {
        id: "create.trigger.referencing_aliases",
        feature: "CREATE TRIGGER",
        sql: "CREATE OR REPLACE TRIGGER employees_history AFTER UPDATE ON employees REFERENCING OLD AS old_row NEW AS new_row FOR EACH ROW BEGIN INSERT INTO employee_history(employee_id, old_salary, new_salary) VALUES (:old_row.employee_id, :old_row.salary, :new_row.salary); END;",
    },
    OracleCase {
        id: "plsql.cursor.open_static_using",
        feature: "OPEN FOR Statement",
        sql: "DECLARE c SYS_REFCURSOR; BEGIN OPEN c FOR SELECT * FROM employees WHERE department_id = :1 USING IN 20; END;",
    },
    OracleCase {
        id: "plsql.cursor.open_named_arguments",
        feature: "OPEN Statement",
        sql: "DECLARE CURSOR c(p_department_id NUMBER, p_active BOOLEAN DEFAULT TRUE) IS SELECT employee_id FROM employees WHERE department_id = p_department_id AND active = p_active; BEGIN OPEN c(p_active => FALSE, p_department_id => 20); CLOSE c; END;",
    },
];
