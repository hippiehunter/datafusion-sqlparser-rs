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

use super::common::{assert_all_parse, assert_unique_case_ids, OracleCase};

pub const RELATIONAL_CASES: &[OracleCase] = &[
    // Lexical structure, names, literals, binds, and expressions.
    OracleCase {
        id: "lex.identifier.characters",
        feature: "Lexical: identifiers",
        sql: "SELECT employee_id, dept_$# FROM hr.employees",
    },
    OracleCase {
        id: "lex.identifier.quoted",
        feature: "Lexical: quoted identifiers",
        sql: "SELECT \"Case Sensitive\" FROM \"MixedCaseTable\"",
    },
    OracleCase {
        id: "lex.string.national",
        feature: "Lexical: national character literal",
        sql: "SELECT N'Grüße' FROM dual",
    },
    OracleCase {
        id: "lex.string.alternative_quote",
        feature: "Lexical: alternative quoting mechanism",
        sql: "SELECT q'[Sam's string]' FROM dual",
    },
    OracleCase {
        id: "lex.string.alternative_quote_paired",
        feature: "Lexical: alternative quoting mechanism",
        sql: "SELECT q'{SELECT * FROM employees WHERE name = 'Ada'}' FROM dual",
    },
    OracleCase {
        id: "lex.number.binary_float",
        feature: "Lexical: floating-point literals",
        sql: "SELECT 1.25f, 6.022d, -0.0f FROM dual",
    },
    OracleCase {
        id: "lex.datetime.ansi",
        feature: "Lexical: datetime literals",
        sql: "SELECT DATE '2026-07-29', TIMESTAMP '2026-07-29 12:34:56.123456' FROM dual",
    },
    OracleCase {
        id: "lex.datetime.time_zone",
        feature: "Lexical: timestamp with time zone literal",
        sql: "SELECT TIMESTAMP '2026-07-29 12:34:56 America/Los_Angeles' FROM dual",
    },
    OracleCase {
        id: "lex.interval.year_month",
        feature: "Lexical: year-to-month interval",
        sql: "SELECT INTERVAL '2-6' YEAR TO MONTH FROM dual",
    },
    OracleCase {
        id: "lex.interval.day_second",
        feature: "Lexical: day-to-second interval",
        sql: "SELECT INTERVAL '4 05:12:10.222' DAY TO SECOND(3) FROM dual",
    },
    OracleCase {
        id: "lex.bind.named",
        feature: "Lexical: bind variables",
        sql: "SELECT * FROM employees WHERE department_id = :department_id",
    },
    OracleCase {
        id: "lex.bind.numeric",
        feature: "Lexical: positional bind variables",
        sql: "SELECT * FROM employees WHERE employee_id = :1",
    },
    OracleCase {
        id: "expr.concatenation",
        feature: "Expressions: concatenation",
        sql: "SELECT first_name || ' ' || last_name FROM employees",
    },
    OracleCase {
        id: "expr.case",
        feature: "Expressions: CASE",
        sql: "SELECT CASE status WHEN 'A' THEN 'active' ELSE 'inactive' END FROM accounts",
    },
    OracleCase {
        id: "expr.decode",
        feature: "Expressions: DECODE",
        sql: "SELECT DECODE(status, 'A', 'active', 'inactive') FROM accounts",
    },
    OracleCase {
        id: "expr.cast.multiset",
        feature: "Expressions: CAST MULTISET",
        sql: "SELECT CAST(MULTISET(SELECT department_name FROM departments) AS name_list_t) FROM dual",
    },
    OracleCase {
        id: "expr.cursor",
        feature: "Expressions: CURSOR",
        sql: "SELECT department_name, CURSOR(SELECT employee_id FROM employees e WHERE e.department_id = d.department_id) FROM departments d",
    },
    OracleCase {
        id: "expr.object_access",
        feature: "Expressions: object access",
        sql: "SELECT VALUE(p).address.city FROM persons p",
    },
    OracleCase {
        id: "expr.collection",
        feature: "Expressions: collection access",
        sql: "SELECT phone_list(1) FROM contacts",
    },
    OracleCase {
        id: "expr.collate",
        feature: "Expressions: COLLATE",
        sql: "SELECT name COLLATE BINARY_CI FROM customers",
    },
    OracleCase {
        id: "expr.at_local",
        feature: "Expressions: AT LOCAL",
        sql: "SELECT order_ts AT LOCAL FROM orders",
    },
    OracleCase {
        id: "expr.at_time_zone",
        feature: "Expressions: AT TIME ZONE",
        sql: "SELECT order_ts AT TIME ZONE 'UTC' FROM orders",
    },
    OracleCase {
        id: "expr.boolean",
        feature: "Expressions: SQL BOOLEAN",
        sql: "SELECT TRUE, FALSE, NOT is_active FROM accounts WHERE is_active IS TRUE",
    },
    // SELECT and query subclauses.
    OracleCase {
        id: "select.dual",
        feature: "SELECT",
        sql: "SELECT 1 FROM dual",
    },
    OracleCase {
        id: "select.no_from",
        feature: "SELECT",
        sql: "SELECT 1",
    },
    OracleCase {
        id: "select.with",
        feature: "SELECT: subquery factoring",
        sql: "WITH department_costs AS (SELECT department_id, SUM(salary) cost FROM employees GROUP BY department_id) SELECT * FROM department_costs",
    },
    OracleCase {
        id: "select.with.function",
        feature: "SELECT: PL/SQL declarations in WITH",
        sql: "WITH FUNCTION twice(n NUMBER) RETURN NUMBER IS BEGIN RETURN n * 2; END; SELECT twice(21) FROM dual",
    },
    OracleCase {
        id: "select.with.search_cycle",
        feature: "SELECT: recursive SEARCH and CYCLE",
        sql: "WITH org(emp_id, manager_id) AS (SELECT employee_id, manager_id FROM employees UNION ALL SELECT e.employee_id, e.manager_id FROM employees e JOIN org o ON e.manager_id = o.emp_id) SEARCH DEPTH FIRST BY emp_id SET order_col CYCLE emp_id SET is_cycle TO 1 DEFAULT 0 SELECT * FROM org",
    },
    OracleCase {
        id: "select.minus",
        feature: "SELECT: MINUS",
        sql: "SELECT department_id FROM departments MINUS SELECT department_id FROM employees",
    },
    OracleCase {
        id: "select.intersect_all",
        feature: "SELECT: INTERSECT ALL",
        sql: "SELECT department_id FROM departments INTERSECT ALL SELECT department_id FROM employees",
    },
    OracleCase {
        id: "select.order_nulls",
        feature: "SELECT: ORDER BY",
        sql: "SELECT employee_id FROM employees ORDER BY commission_pct DESC NULLS LAST",
    },
    OracleCase {
        id: "select.row_limiting",
        feature: "SELECT: row_limiting_clause",
        sql: "SELECT * FROM employees ORDER BY employee_id OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
    },
    OracleCase {
        id: "select.row_limiting.percent_ties",
        feature: "SELECT: row_limiting_clause",
        sql: "SELECT * FROM employees ORDER BY salary DESC FETCH FIRST 10 PERCENT ROWS WITH TIES",
    },
    OracleCase {
        id: "select.for_update",
        feature: "SELECT: for_update_clause",
        sql: "SELECT * FROM employees WHERE department_id = 10 FOR UPDATE OF salary NOWAIT",
    },
    OracleCase {
        id: "select.for_update.skip_locked",
        feature: "SELECT: for_update_clause",
        sql: "SELECT * FROM jobs_queue WHERE state = 'READY' FOR UPDATE SKIP LOCKED",
    },
    OracleCase {
        id: "select.grouping_sets",
        feature: "SELECT: GROUPING SETS",
        sql: "SELECT department_id, job_id, SUM(salary) FROM employees GROUP BY GROUPING SETS ((department_id, job_id), (department_id), ())",
    },
    OracleCase {
        id: "select.rollup",
        feature: "SELECT: ROLLUP",
        sql: "SELECT department_id, job_id, SUM(salary) FROM employees GROUP BY ROLLUP(department_id, job_id)",
    },
    OracleCase {
        id: "select.cube",
        feature: "SELECT: CUBE",
        sql: "SELECT department_id, job_id, SUM(salary) FROM employees GROUP BY CUBE(department_id, job_id)",
    },
    OracleCase {
        id: "select.hierarchical.start_first",
        feature: "SELECT: hierarchical_query_clause",
        sql: "SELECT employee_id, LEVEL FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id",
    },
    OracleCase {
        id: "select.hierarchical.connect_first",
        feature: "SELECT: hierarchical_query_clause",
        sql: "SELECT employee_id FROM employees CONNECT BY PRIOR employee_id = manager_id START WITH manager_id IS NULL",
    },
    OracleCase {
        id: "select.hierarchical.nocycle",
        feature: "SELECT: hierarchical_query_clause",
        sql: "SELECT employee_id, CONNECT_BY_ISCYCLE FROM employees START WITH manager_id IS NULL CONNECT BY NOCYCLE PRIOR employee_id = manager_id",
    },
    OracleCase {
        id: "select.hierarchical.order_siblings",
        feature: "SELECT: hierarchical_query_clause",
        sql: "SELECT employee_id FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id ORDER SIBLINGS BY last_name",
    },
    OracleCase {
        id: "select.hierarchical.root_path",
        feature: "SELECT: hierarchical query operators",
        sql: "SELECT CONNECT_BY_ROOT last_name, SYS_CONNECT_BY_PATH(last_name, '/') FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id",
    },
    OracleCase {
        id: "select.legacy_outer_join",
        feature: "SELECT: outer join operator",
        sql: "SELECT a.id, b.value FROM a, b WHERE a.id = b.id(+)",
    },
    OracleCase {
        id: "select.partitioned_outer_join",
        feature: "SELECT: partitioned outer join",
        sql: "SELECT d.day, s.amount FROM calendar d LEFT OUTER JOIN sales s PARTITION BY (s.product_id) ON s.day = d.day",
    },
    OracleCase {
        id: "select.partitioned_outer_join.right",
        feature: "SELECT: partitioned outer join",
        sql: "SELECT d.day, s.amount FROM calendar d RIGHT OUTER JOIN sales s PARTITION BY (s.product_id, s.region_id) ON s.day = d.day",
    },
    OracleCase {
        id: "select.cross_apply",
        feature: "SELECT: CROSS APPLY",
        sql: "SELECT d.department_name, e.employee_name FROM departments d CROSS APPLY (SELECT first_name || last_name employee_name FROM employees e WHERE e.department_id = d.department_id) e",
    },
    OracleCase {
        id: "select.outer_apply",
        feature: "SELECT: OUTER APPLY",
        sql: "SELECT d.department_name, e.employee_name FROM departments d OUTER APPLY (SELECT first_name employee_name FROM employees e WHERE e.department_id = d.department_id) e",
    },
    OracleCase {
        id: "select.lateral",
        feature: "SELECT: lateral inline view",
        sql: "SELECT d.department_name, e.first_name FROM departments d, LATERAL (SELECT first_name FROM employees e WHERE e.department_id = d.department_id) e",
    },
    OracleCase {
        id: "select.flashback.scn",
        feature: "SELECT: flashback_query_clause",
        sql: "SELECT * FROM employees AS OF SCN 123456",
    },
    OracleCase {
        id: "select.flashback.timestamp",
        feature: "SELECT: flashback_query_clause",
        sql: "SELECT * FROM employees AS OF TIMESTAMP SYSTIMESTAMP - INTERVAL '1' HOUR",
    },
    OracleCase {
        id: "select.flashback.versions",
        feature: "SELECT: flashback_query_clause",
        sql: "SELECT versions_startscn, versions_endscn, employee_id FROM employees VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE",
    },
    OracleCase {
        id: "select.flashback.versions_timestamp",
        feature: "SELECT: flashback_query_clause",
        sql: "SELECT * FROM employees VERSIONS BETWEEN TIMESTAMP :start_time AND :end_time",
    },
    OracleCase {
        id: "select.flashback.alias",
        feature: "SELECT: flashback_query_clause",
        sql: "SELECT e.employee_id FROM employees AS OF SCN :snapshot_scn e",
    },
    OracleCase {
        id: "select.sample",
        feature: "SELECT: sample_clause",
        sql: "SELECT * FROM employees SAMPLE BLOCK (10) SEED (42)",
    },
    OracleCase {
        id: "select.partition_extension",
        feature: "SELECT: partition_extension_clause",
        sql: "SELECT * FROM sales PARTITION (sales_q1_2026)",
    },
    OracleCase {
        id: "select.pivot",
        feature: "SELECT: pivot_clause",
        sql: "SELECT * FROM (SELECT product, quarter, amount FROM sales) PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2))",
    },
    OracleCase {
        id: "select.unpivot",
        feature: "SELECT: unpivot_clause",
        sql: "SELECT * FROM quarterly_sales UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2'))",
    },
    OracleCase {
        id: "select.match_recognize",
        feature: "SELECT: row_pattern_clause",
        sql: "SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY tstamp MEASURES FIRST(x.tstamp) AS start_tstamp, LAST(y.tstamp) AS bottom_tstamp ONE ROW PER MATCH AFTER MATCH SKIP TO LAST y PATTERN (x+ y+ z+) DEFINE x AS x.price > PREV(x.price), y AS y.price < PREV(y.price), z AS z.price > PREV(z.price))",
    },
    OracleCase {
        id: "select.model",
        feature: "SELECT: model_clause",
        sql: "SELECT country, year, sales FROM sales_view MODEL PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) RULES (sales[2027] = sales[2026] * 1.1)",
    },
    OracleCase {
        id: "select.container",
        feature: "SELECT: containers_clause",
        sql: "SELECT con_id, table_name FROM CONTAINERS(user_tables)",
    },
    OracleCase {
        id: "select.analytic",
        feature: "SELECT: analytic functions",
        sql: "SELECT employee_id, SUM(salary) OVER (PARTITION BY department_id ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM employees",
    },
    OracleCase {
        id: "select.analytic.range_interval",
        feature: "SELECT: analytic windowing clause",
        sql: "SELECT SUM(amount) OVER (ORDER BY order_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) FROM orders",
    },
    OracleCase {
        id: "select.listagg",
        feature: "SELECT: LISTAGG",
        sql: "SELECT department_id, LISTAGG(last_name, ', ' ON OVERFLOW TRUNCATE '...' WITH COUNT) WITHIN GROUP (ORDER BY last_name) FROM employees GROUP BY department_id",
    },
    OracleCase {
        id: "select.keep_dense_rank",
        feature: "SELECT: FIRST and LAST aggregate",
        sql: "SELECT department_id, MAX(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct) FROM employees GROUP BY department_id",
    },
    OracleCase {
        id: "select.json_object",
        feature: "SELECT: SQL/JSON generation",
        sql: "SELECT JSON_OBJECT('id' VALUE employee_id, 'name' VALUE first_name ABSENT ON NULL RETURNING JSON) FROM employees",
    },
    OracleCase {
        id: "select.json_table",
        feature: "SELECT: JSON_TABLE",
        sql: "SELECT jt.* FROM orders o, JSON_TABLE(o.payload, '$.items[*]' COLUMNS (item_id NUMBER PATH '$.id', qty NUMBER PATH '$.qty' DEFAULT 0 ON ERROR)) jt",
    },
    OracleCase {
        id: "select.json_exists",
        feature: "SELECT: JSON_EXISTS",
        sql: "SELECT * FROM orders WHERE JSON_EXISTS(payload, '$?(@.total > $min)' PASSING 100 AS \"min\" ERROR ON ERROR)",
    },
    OracleCase {
        id: "select.xmltable",
        feature: "SELECT: XMLTABLE",
        sql: "SELECT x.* FROM purchase_orders p, XMLTABLE('/PurchaseOrder/LineItems/LineItem' PASSING p.document COLUMNS item_no NUMBER PATH '@ItemNumber', description VARCHAR2(100) PATH 'Description') x",
    },
    OracleCase {
        id: "select.vector_distance",
        feature: "SELECT: vector expressions",
        sql: "SELECT item_id FROM items ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE) FETCH APPROXIMATE FIRST 10 ROWS ONLY",
    },
    OracleCase {
        id: "select.graph_table",
        feature: "SELECT: GRAPH_TABLE",
        sql: "SELECT * FROM GRAPH_TABLE (social_graph MATCH (a IS person) -[e IS follows]-> (b IS person) COLUMNS (a.name AS person_a, b.name AS person_b))",
    },
    // DML and data manipulation subclauses.
    OracleCase {
        id: "insert.values",
        feature: "INSERT",
        sql: "INSERT INTO employees (employee_id, last_name) VALUES (1001, 'Lovelace')",
    },
    OracleCase {
        id: "insert.returning",
        feature: "INSERT",
        sql: "INSERT INTO employees (employee_id, last_name) VALUES (1001, 'Lovelace') RETURNING employee_id INTO :new_id",
    },
    OracleCase {
        id: "insert.multitable_all",
        feature: "INSERT",
        sql: "INSERT ALL INTO employees_history VALUES (employee_id, hire_date) INTO audit_log VALUES (employee_id, 'COPIED') SELECT employee_id, hire_date FROM employees",
    },
    OracleCase {
        id: "insert.multitable_conditional",
        feature: "INSERT",
        sql: "INSERT FIRST WHEN salary > 20000 THEN INTO high_earners VALUES (employee_id, salary) WHEN salary > 10000 THEN INTO mid_earners VALUES (employee_id, salary) ELSE INTO other_earners VALUES (employee_id, salary) SELECT employee_id, salary FROM employees",
    },
    OracleCase {
        id: "insert.partition",
        feature: "INSERT",
        sql: "INSERT INTO sales PARTITION (sales_q3_2026) (sale_id, amount) VALUES (1, 100)",
    },
    OracleCase {
        id: "insert.error_logging",
        feature: "INSERT",
        sql: "INSERT INTO target_table SELECT * FROM source_table LOG ERRORS INTO err$_target_table ('load-1') REJECT LIMIT UNLIMITED",
    },
    OracleCase {
        id: "update.basic",
        feature: "UPDATE",
        sql: "UPDATE employees SET salary = salary * 1.05 WHERE department_id = 10",
    },
    OracleCase {
        id: "update.correlated",
        feature: "UPDATE",
        sql: "UPDATE employees e SET (salary, commission_pct) = (SELECT salary, commission_pct FROM employee_updates u WHERE u.employee_id = e.employee_id) WHERE EXISTS (SELECT 1 FROM employee_updates u WHERE u.employee_id = e.employee_id)",
    },
    OracleCase {
        id: "update.returning",
        feature: "UPDATE",
        sql: "UPDATE employees SET salary = salary + 100 WHERE employee_id = 100 RETURNING salary INTO :new_salary",
    },
    OracleCase {
        id: "update.error_logging",
        feature: "UPDATE",
        sql: "UPDATE employees SET salary = salary * 2 LOG ERRORS INTO err$_employees ('raise') REJECT LIMIT 10",
    },
    OracleCase {
        id: "delete.basic",
        feature: "DELETE",
        sql: "DELETE FROM employees WHERE employee_id = 100",
    },
    OracleCase {
        id: "delete.returning",
        feature: "DELETE",
        sql: "DELETE FROM employees WHERE employee_id = 100 RETURNING last_name INTO :old_name",
    },
    OracleCase {
        id: "delete.partition",
        feature: "DELETE",
        sql: "DELETE FROM sales PARTITION (sales_q1_2026) WHERE amount = 0",
    },
    OracleCase {
        id: "merge.full",
        feature: "MERGE",
        sql: "MERGE INTO bonuses d USING (SELECT employee_id, salary FROM employees) s ON (d.employee_id = s.employee_id) WHEN MATCHED THEN UPDATE SET d.bonus = s.salary * 0.1 DELETE WHERE s.salary = 0 WHEN NOT MATCHED THEN INSERT (employee_id, bonus) VALUES (s.employee_id, s.salary * 0.05) WHERE s.salary > 0",
    },
    OracleCase {
        id: "merge.error_logging",
        feature: "MERGE",
        sql: "MERGE INTO target d USING source s ON (d.id = s.id) WHEN MATCHED THEN UPDATE SET d.value = s.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value) LOG ERRORS INTO err$_target REJECT LIMIT UNLIMITED",
    },
    OracleCase {
        id: "call.routine",
        feature: "CALL",
        sql: "CALL raise_salary(100, 0.05)",
    },
    // Transactions, locking, privileges, and metadata.
    OracleCase {
        id: "transaction.commit.write",
        feature: "COMMIT",
        sql: "COMMIT WRITE IMMEDIATE NOWAIT",
    },
    OracleCase {
        id: "transaction.commit.comment",
        feature: "COMMIT",
        sql: "COMMIT COMMENT 'batch-42'",
    },
    OracleCase {
        id: "transaction.rollback.savepoint",
        feature: "ROLLBACK",
        sql: "ROLLBACK TO SAVEPOINT before_update",
    },
    OracleCase {
        id: "transaction.savepoint",
        feature: "SAVEPOINT",
        sql: "SAVEPOINT before_update",
    },
    OracleCase {
        id: "transaction.set.read_only",
        feature: "SET TRANSACTION",
        sql: "SET TRANSACTION READ ONLY NAME 'reporting'",
    },
    OracleCase {
        id: "transaction.set.isolation",
        feature: "SET TRANSACTION",
        sql: "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    },
    OracleCase {
        id: "transaction.set.constraints",
        feature: "SET CONSTRAINT[S]",
        sql: "SET CONSTRAINTS ALL DEFERRED",
    },
    OracleCase {
        id: "lock.table",
        feature: "LOCK TABLE",
        sql: "LOCK TABLE employees IN SHARE ROW EXCLUSIVE MODE NOWAIT",
    },
    OracleCase {
        id: "grant.object",
        feature: "GRANT",
        sql: "GRANT SELECT, UPDATE (salary) ON employees TO reporting_role WITH GRANT OPTION",
    },
    OracleCase {
        id: "grant.system",
        feature: "GRANT",
        sql: "GRANT CREATE SESSION, CREATE TABLE TO app_user WITH ADMIN OPTION",
    },
    OracleCase {
        id: "revoke.object",
        feature: "REVOKE",
        sql: "REVOKE UPDATE ON employees FROM reporting_role",
    },
    OracleCase {
        id: "set.role",
        feature: "SET ROLE",
        sql: "SET ROLE reporting_role IDENTIFIED BY secret",
    },
    OracleCase {
        id: "comment.table",
        feature: "COMMENT",
        sql: "COMMENT ON TABLE employees IS 'Application employees'",
    },
    OracleCase {
        id: "comment.column",
        feature: "COMMENT",
        sql: "COMMENT ON COLUMN employees.employee_id IS 'Stable identifier'",
    },
    OracleCase {
        id: "rename.table",
        feature: "RENAME",
        sql: "RENAME employees_stage TO employees_ready",
    },
    OracleCase {
        id: "explain.plan",
        feature: "EXPLAIN PLAN",
        sql: "EXPLAIN PLAN SET STATEMENT_ID = 'q1' INTO plan_table FOR SELECT * FROM employees",
    },
    // Core relational DDL. Object families with procedural bodies live in
    // plsql.rs.
    OracleCase {
        id: "create.table.columns",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE employees (employee_id NUMBER(10) CONSTRAINT employees_pk PRIMARY KEY, last_name VARCHAR2(100 CHAR) NOT NULL, hired_on DATE DEFAULT SYSDATE)",
    },
    OracleCase {
        id: "create.table.identity",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE messages (message_id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH 100 CACHE 20), body CLOB)",
    },
    OracleCase {
        id: "create.table.virtual_invisible",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE order_lines (qty NUMBER, unit_price NUMBER, total NUMBER GENERATED ALWAYS AS (qty * unit_price) VIRTUAL, internal_note VARCHAR2(100) INVISIBLE)",
    },
    OracleCase {
        id: "create.table.period",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE projects (project_id NUMBER, start_time TIMESTAMP, end_time TIMESTAMP, PERIOD FOR project_time (start_time, end_time))",
    },
    OracleCase {
        id: "create.table.interval_partition",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE sales (sale_id NUMBER, sold_on DATE, amount NUMBER) PARTITION BY RANGE (sold_on) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH')) (PARTITION p0 VALUES LESS THAN (DATE '2026-01-01'))",
    },
    OracleCase {
        id: "create.table.reference_partition",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE order_items (order_id NUMBER, item_id NUMBER, CONSTRAINT oi_order_fk FOREIGN KEY (order_id) REFERENCES orders(order_id)) PARTITION BY REFERENCE (oi_order_fk)",
    },
    OracleCase {
        id: "create.table.external",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE ext_employees (employee_id NUMBER, last_name VARCHAR2(100)) ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER DEFAULT DIRECTORY data_dir ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE) LOCATION ('employees.csv')) REJECT LIMIT UNLIMITED",
    },
    OracleCase {
        id: "create.table.global_temporary",
        feature: "CREATE TABLE",
        sql: "CREATE GLOBAL TEMPORARY TABLE session_items (item_id NUMBER) ON COMMIT DELETE ROWS",
    },
    OracleCase {
        id: "create.table.private_temporary",
        feature: "CREATE TABLE",
        sql: "CREATE PRIVATE TEMPORARY TABLE ora$ptt_work (item_id NUMBER) ON COMMIT DROP DEFINITION",
    },
    OracleCase {
        id: "create.table.blockchain",
        feature: "CREATE TABLE",
        sql: "CREATE BLOCKCHAIN TABLE ledger (id NUMBER, payload JSON) NO DROP UNTIL 31 DAYS IDLE NO DELETE UNTIL 31 DAYS AFTER INSERT HASHING USING SHA2_512 VERSION v1",
    },
    OracleCase {
        id: "create.table.immutable",
        feature: "CREATE TABLE",
        sql: "CREATE IMMUTABLE TABLE audit_events (event_id NUMBER, payload JSON) NO DROP UNTIL 30 DAYS IDLE NO DELETE",
    },
    OracleCase {
        id: "alter.table.add",
        feature: "ALTER TABLE",
        sql: "ALTER TABLE employees ADD (preferred_name VARCHAR2(100), active BOOLEAN DEFAULT TRUE NOT NULL)",
    },
    OracleCase {
        id: "alter.table.modify",
        feature: "ALTER TABLE",
        sql: "ALTER TABLE employees MODIFY (last_name VARCHAR2(200 CHAR) COLLATE BINARY_CI)",
    },
    OracleCase {
        id: "alter.table.constraint",
        feature: "ALTER TABLE",
        sql: "ALTER TABLE employees ADD CONSTRAINT employees_dept_fk FOREIGN KEY (department_id) REFERENCES departments(department_id) DEFERRABLE INITIALLY DEFERRED",
    },
    OracleCase {
        id: "alter.table.partition",
        feature: "ALTER TABLE",
        sql: "ALTER TABLE sales SPLIT PARTITION future AT (DATE '2027-01-01') INTO (PARTITION sales_2026, PARTITION future) UPDATE GLOBAL INDEXES",
    },
    OracleCase {
        id: "drop.table",
        feature: "DROP TABLE",
        sql: "DROP TABLE employees_stage CASCADE CONSTRAINTS PURGE",
    },
    OracleCase {
        id: "drop.table.recyclebin",
        feature: "DROP TABLE",
        sql: "DROP TABLE employees_stage",
    },
    OracleCase {
        id: "truncate.table",
        feature: "TRUNCATE TABLE",
        sql: "TRUNCATE TABLE employees_stage DROP ALL STORAGE CASCADE",
    },
    OracleCase {
        id: "truncate.table.reuse_storage",
        feature: "TRUNCATE TABLE",
        sql: "TRUNCATE TABLE employees_stage REUSE STORAGE",
    },
    OracleCase {
        id: "truncate.table.drop_storage",
        feature: "TRUNCATE TABLE",
        sql: "TRUNCATE TABLE employees_stage DROP STORAGE",
    },
    OracleCase {
        id: "create.index",
        feature: "CREATE INDEX",
        sql: "CREATE INDEX employees_name_ix ON employees (UPPER(last_name), first_name DESC) ONLINE",
    },
    OracleCase {
        id: "create.index.unique",
        feature: "CREATE INDEX",
        sql: "CREATE UNIQUE INDEX employees_email_uix ON employees (LOWER(email))",
    },
    OracleCase {
        id: "create.index.bitmap",
        feature: "CREATE INDEX",
        sql: "CREATE BITMAP INDEX employees_dept_bix ON employees (department_id) LOCAL",
    },
    OracleCase {
        id: "create.index.domain",
        feature: "CREATE INDEX",
        sql: "CREATE INDEX documents_text_ix ON documents (body) INDEXTYPE IS ctxsys.context PARAMETERS ('SYNC (ON COMMIT)')",
    },
    OracleCase {
        id: "create.vector_index",
        feature: "CREATE VECTOR INDEX",
        sql: "CREATE VECTOR INDEX items_embedding_hnsw ON items (embedding) ORGANIZATION INMEMORY NEIGHBOR GRAPH DISTANCE COSINE WITH TARGET ACCURACY 95 PARAMETERS (TYPE HNSW, NEIGHBORS 32)",
    },
    OracleCase {
        id: "alter.index",
        feature: "ALTER INDEX",
        sql: "ALTER INDEX employees_name_ix REBUILD ONLINE",
    },
    OracleCase {
        id: "alter.index.offline",
        feature: "ALTER INDEX",
        sql: "ALTER INDEX employees_name_ix REBUILD",
    },
    OracleCase {
        id: "drop.index",
        feature: "DROP INDEX",
        sql: "DROP INDEX employees_name_ix ONLINE",
    },
    OracleCase {
        id: "drop.index.offline",
        feature: "DROP INDEX",
        sql: "DROP INDEX employees_name_ix",
    },
    OracleCase {
        id: "create.view",
        feature: "CREATE VIEW",
        sql: "CREATE OR REPLACE FORCE EDITIONING VIEW active_employees (employee_id, last_name) AS SELECT employee_id, last_name FROM employees WHERE active IS TRUE WITH READ ONLY",
    },
    OracleCase {
        id: "create.view.object",
        feature: "CREATE VIEW",
        sql: "CREATE VIEW employee_objects OF employee_t WITH OBJECT IDENTIFIER (employee_id) AS SELECT employee_id, last_name FROM employees",
    },
    OracleCase {
        id: "alter.view",
        feature: "ALTER VIEW",
        sql: "ALTER VIEW active_employees COMPILE",
    },
    OracleCase {
        id: "drop.view",
        feature: "DROP VIEW",
        sql: "DROP VIEW active_employees CASCADE CONSTRAINTS",
    },
    OracleCase {
        id: "create.materialized_view",
        feature: "CREATE MATERIALIZED VIEW",
        sql: "CREATE MATERIALIZED VIEW department_totals BUILD IMMEDIATE REFRESH FAST ON COMMIT ENABLE QUERY REWRITE AS SELECT department_id, COUNT(*) employee_count, SUM(salary) total_salary FROM employees GROUP BY department_id",
    },
    OracleCase {
        id: "create.materialized_view_log",
        feature: "CREATE MATERIALIZED VIEW LOG",
        sql: "CREATE MATERIALIZED VIEW LOG ON employees WITH PRIMARY KEY, ROWID, SEQUENCE (department_id, salary) INCLUDING NEW VALUES",
    },
    OracleCase {
        id: "alter.materialized_view",
        feature: "ALTER MATERIALIZED VIEW",
        sql: "ALTER MATERIALIZED VIEW department_totals REFRESH FAST ON DEMAND",
    },
    OracleCase {
        id: "drop.materialized_view",
        feature: "DROP MATERIALIZED VIEW",
        sql: "DROP MATERIALIZED VIEW department_totals PRESERVE TABLE",
    },
    OracleCase {
        id: "create.sequence",
        feature: "CREATE SEQUENCE",
        sql: "CREATE SEQUENCE employee_seq START WITH 1000 INCREMENT BY 1 MINVALUE 1 NOMAXVALUE CACHE 50 NOORDER NOCYCLE",
    },
    OracleCase {
        id: "alter.sequence",
        feature: "ALTER SEQUENCE",
        sql: "ALTER SEQUENCE employee_seq RESTART START WITH 2000 CACHE 100",
    },
    OracleCase {
        id: "drop.sequence",
        feature: "DROP SEQUENCE",
        sql: "DROP SEQUENCE employee_seq",
    },
    OracleCase {
        id: "create.synonym",
        feature: "CREATE SYNONYM",
        sql: "CREATE OR REPLACE PUBLIC SYNONYM emp FOR hr.employees",
    },
    OracleCase {
        id: "alter.synonym",
        feature: "ALTER SYNONYM",
        sql: "ALTER PUBLIC SYNONYM emp COMPILE",
    },
    OracleCase {
        id: "drop.synonym",
        feature: "DROP SYNONYM",
        sql: "DROP PUBLIC SYNONYM emp FORCE",
    },
    OracleCase {
        id: "create.directory",
        feature: "CREATE DIRECTORY",
        sql: "CREATE OR REPLACE DIRECTORY data_dir AS '/srv/oracle/data' SHARING = NONE",
    },
    OracleCase {
        id: "drop.directory",
        feature: "DROP DIRECTORY",
        sql: "DROP DIRECTORY data_dir",
    },
    OracleCase {
        id: "create.database_link",
        feature: "CREATE DATABASE LINK",
        sql: "CREATE DATABASE LINK reporting CONNECT TO report_user IDENTIFIED BY password USING 'reporting_service'",
    },
    OracleCase {
        id: "alter.database_link",
        feature: "ALTER DATABASE LINK",
        sql: "ALTER DATABASE LINK reporting CONNECT TO report_user IDENTIFIED BY new_password",
    },
    OracleCase {
        id: "drop.database_link",
        feature: "DROP DATABASE LINK",
        sql: "DROP DATABASE LINK reporting",
    },
    OracleCase {
        id: "create.synonym.private",
        feature: "CREATE SYNONYM",
        sql: "CREATE SYNONYM local_emp FOR hr.employees",
    },
    OracleCase {
        id: "create.directory.unshared",
        feature: "CREATE DIRECTORY",
        sql: "CREATE DIRECTORY export_dir AS '/srv/oracle/export'",
    },
    OracleCase {
        id: "create.database_link.public",
        feature: "CREATE DATABASE LINK",
        sql: "CREATE PUBLIC DATABASE LINK shared_reporting CONNECT TO report_user IDENTIFIED BY password USING 'reporting_service'",
    },
    OracleCase {
        id: "alter.database_link.public",
        feature: "ALTER DATABASE LINK",
        sql: "ALTER PUBLIC DATABASE LINK shared_reporting CONNECT TO report_user IDENTIFIED BY new_password",
    },
    OracleCase {
        id: "drop.database_link.public",
        feature: "DROP DATABASE LINK",
        sql: "DROP PUBLIC DATABASE LINK shared_reporting",
    },
    OracleCase {
        id: "create.sequence.no_cache",
        feature: "CREATE SEQUENCE",
        sql: "CREATE SEQUENCE audit_seq NOMINVALUE MAXVALUE 999999 NOCACHE ORDER CYCLE",
    },
    OracleCase {
        id: "types.numeric",
        feature: "Data types: numeric",
        sql: "CREATE TABLE numeric_types (a NUMBER(38,10), b FLOAT(126), c BINARY_FLOAT, d BINARY_DOUBLE, e INTEGER)",
    },
    OracleCase {
        id: "types.character",
        feature: "Data types: character",
        sql: "CREATE TABLE character_types (a CHAR(10 BYTE), b VARCHAR2(100 CHAR), c NCHAR(10), d NVARCHAR2(100), e CLOB, f NCLOB)",
    },
    OracleCase {
        id: "types.binary_lob",
        feature: "Data types: binary and LOB",
        sql: "CREATE TABLE binary_types (a RAW(2000), b LONG RAW, c BLOB, d BFILE)",
    },
    OracleCase {
        id: "types.datetime",
        feature: "Data types: datetime and interval",
        sql: "CREATE TABLE temporal_types (a DATE, b TIMESTAMP(9), c TIMESTAMP WITH TIME ZONE, d TIMESTAMP WITH LOCAL TIME ZONE, e INTERVAL YEAR(4) TO MONTH, f INTERVAL DAY(3) TO SECOND(6))",
    },
    OracleCase {
        id: "types.rowid",
        feature: "Data types: row identifiers",
        sql: "CREATE TABLE rowid_types (a ROWID, b UROWID(4000))",
    },
    OracleCase {
        id: "types.json_boolean_vector",
        feature: "Data types: JSON, BOOLEAN, and VECTOR",
        sql: "CREATE TABLE modern_types (document JSON, enabled BOOLEAN, dense_embedding VECTOR(768, FLOAT32), bits VECTOR(1024, BINARY), sparse_embedding VECTOR(10000, FLOAT32, SPARSE))",
    },
    OracleCase {
        id: "conditions.comparison",
        feature: "Conditions: comparison",
        sql: "SELECT * FROM employees WHERE salary >= ALL (SELECT salary FROM employees WHERE department_id = 10) AND employee_id != :employee_id",
    },
    OracleCase {
        id: "conditions.membership",
        feature: "Conditions: membership and existence",
        sql: "SELECT * FROM employees e WHERE e.department_id IN (10, 20) AND EXISTS (SELECT 1 FROM departments d WHERE d.department_id = e.department_id)",
    },
    OracleCase {
        id: "conditions.pattern",
        feature: "Conditions: pattern matching",
        sql: "SELECT * FROM employees WHERE last_name LIKEC 'A_%' ESCAPE '\\' OR REGEXP_LIKE(last_name, '^[[:alpha:]]+$', 'i')",
    },
    OracleCase {
        id: "conditions.pattern.ucs2",
        feature: "Conditions: pattern matching",
        sql: "SELECT * FROM names WHERE value LIKE2 N'A_%'",
    },
    OracleCase {
        id: "conditions.pattern.ucs4.negated",
        feature: "Conditions: pattern matching",
        sql: "SELECT * FROM names WHERE value NOT LIKE4 N'Z%'",
    },
    OracleCase {
        id: "conditions.floating",
        feature: "Conditions: floating-point",
        sql: "SELECT * FROM measurements WHERE reading IS NAN OR reading IS INFINITE",
    },
    OracleCase {
        id: "conditions.object",
        feature: "Conditions: object and collection",
        sql: "SELECT * FROM object_store o WHERE VALUE(o) IS OF (ONLY employee_t) AND tag_t('urgent') MEMBER OF o.tags",
    },
    OracleCase {
        id: "conditions.object.multiple_types",
        feature: "Conditions: object type",
        sql: "SELECT * FROM object_store o WHERE VALUE(o) IS OF (employee_t, contractor_t)",
    },
    OracleCase {
        id: "conditions.object.not_member",
        feature: "Conditions: collection membership",
        sql: "SELECT * FROM object_store o WHERE tag_t('archived') NOT MEMBER OF o.tags",
    },
    OracleCase {
        id: "conditions.floating.negated",
        feature: "Conditions: floating-point",
        sql: "SELECT * FROM measurements WHERE reading IS NOT NAN AND reading IS NOT INFINITE",
    },
    OracleCase {
        id: "conditions.json",
        feature: "Conditions: SQL/JSON",
        sql: "SELECT * FROM documents WHERE payload IS JSON OBJECT WITH UNIQUE KEYS AND JSON_EQUAL(payload, :expected)",
    },
    OracleCase {
        id: "conditions.vector",
        feature: "Conditions: vector",
        sql: "SELECT * FROM items WHERE VECTOR_DISTANCE(embedding, :query_vector, EUCLIDEAN) < 0.25",
    },
    OracleCase {
        id: "pseudocolumns.row",
        feature: "Pseudocolumns: row",
        sql: "SELECT ROWID, ROWNUM, ORA_ROWSCN FROM employees WHERE ROWNUM <= 10",
    },
    OracleCase {
        id: "pseudocolumns.sequence",
        feature: "Pseudocolumns: sequence",
        sql: "SELECT employee_seq.NEXTVAL, employee_seq.CURRVAL FROM dual",
    },
    OracleCase {
        id: "pseudocolumns.hierarchical",
        feature: "Pseudocolumns: hierarchical query",
        sql: "SELECT LEVEL, CONNECT_BY_ISLEAF, CONNECT_BY_ISCYCLE FROM employees START WITH manager_id IS NULL CONNECT BY NOCYCLE PRIOR employee_id = manager_id",
    },
    OracleCase {
        id: "expr.extract",
        feature: "Expressions: datetime",
        sql: "SELECT EXTRACT(TIMEZONE_REGION FROM order_ts), order_ts AT TIME ZONE SESSIONTIMEZONE FROM orders",
    },
    OracleCase {
        id: "expr.trim",
        feature: "Expressions: character",
        sql: "SELECT TRIM(LEADING '0' FROM account_code), TRANSLATE(name USING NCHAR_CS) FROM accounts",
    },
    OracleCase {
        id: "expr.object_treat",
        feature: "Expressions: object",
        sql: "SELECT TREAT(VALUE(p) AS employee_t).employee_id, SYS_TYPEID(VALUE(p)) FROM persons p",
    },
    OracleCase {
        id: "expr.translate.database_charset",
        feature: "Expressions: character",
        sql: "SELECT TRANSLATE(name USING CHAR_CS) FROM accounts",
    },
    OracleCase {
        id: "select.qualify",
        feature: "SELECT: QUALIFY",
        sql: "SELECT employee_id, department_id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS position FROM employees QUALIFY position <= 3",
    },
    OracleCase {
        id: "select.qualify.with_where",
        feature: "SELECT: QUALIFY",
        sql: "SELECT employee_id, ROW_NUMBER() OVER (ORDER BY salary DESC) AS position FROM employees WHERE active IS TRUE QUALIFY position = 1",
    },
    OracleCase {
        id: "select.table_collection",
        feature: "SELECT: table collection expression",
        sql: "SELECT column_value FROM TABLE(number_table_t(1, 2, 3))",
    },
    OracleCase {
        id: "select.inline_external",
        feature: "SELECT: inline external table",
        sql: "SELECT * FROM EXTERNAL ((employee_id NUMBER, last_name VARCHAR2(100)) TYPE ORACLE_LOADER DEFAULT DIRECTORY data_dir ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE) LOCATION ('employees.csv'))",
    },
    OracleCase {
        id: "select.match_recognize.subset",
        feature: "SELECT: row pattern subsets",
        sql: "SELECT * FROM ticker MATCH_RECOGNIZE (ORDER BY tstamp ALL ROWS PER MATCH PATTERN ((up down)+) SUBSET movement = (up, down) DEFINE up AS up.price > PREV(up.price), down AS down.price < PREV(down.price))",
    },
    OracleCase {
        id: "select.match_recognize.navigation",
        feature: "SELECT: row pattern navigation",
        sql: "SELECT * FROM ticker MATCH_RECOGNIZE (ORDER BY tstamp MEASURES MATCH_NUMBER() AS match_no, CLASSIFIER() AS kind, FIRST(up.price) AS first_price ONE ROW PER MATCH PATTERN (up+) DEFINE up AS up.price > PREV(up.price))",
    },
    OracleCase {
        id: "select.pivot.xml",
        feature: "SELECT: PIVOT XML",
        sql: "SELECT * FROM (SELECT product, quarter, amount FROM sales) PIVOT XML (SUM(amount) FOR quarter IN (ANY))",
    },
    OracleCase {
        id: "select.unpivot.multicolumn",
        feature: "SELECT: multi-column UNPIVOT",
        sql: "SELECT * FROM quarterly_sales UNPIVOT ((amount, quantity) FOR quarter IN ((q1_amount, q1_qty) AS 'Q1', (q2_amount, q2_qty) AS 'Q2'))",
    },
    OracleCase {
        id: "select.group_by.vector",
        feature: "SELECT: GROUP BY VECTOR",
        sql: "SELECT department_id, job_id, SUM(salary) FROM employees GROUP BY VECTOR ((department_id), (job_id), ())",
    },
    OracleCase {
        id: "select.annotations",
        feature: "SELECT: annotations",
        sql: "SELECT ANNOTATIONS(employees), employee_id FROM employees",
    },
    OracleCase {
        id: "select.domain_functions",
        feature: "SELECT: domain functions",
        sql: "SELECT DOMAIN_DISPLAY(value), DOMAIN_ORDER(value), DOMAIN_NAME(value) FROM domain_values",
    },
    OracleCase {
        id: "select.with.search_breadth",
        feature: "SELECT: recursive subquery factoring",
        sql: "WITH org(emp_id, manager_id) AS (SELECT employee_id, manager_id FROM employees UNION ALL SELECT e.employee_id, e.manager_id FROM employees e JOIN org o ON e.manager_id = o.emp_id) SEARCH BREADTH FIRST BY emp_id, manager_id SET order_col CYCLE emp_id SET is_cycle TO 'Y' DEFAULT 'N' SELECT * FROM org",
    },
    OracleCase {
        id: "select.with.procedure",
        feature: "SELECT: PL/SQL declarations in WITH",
        sql: "WITH PROCEDURE emit_value(n NUMBER) IS BEGIN DBMS_OUTPUT.PUT_LINE(n); END; SELECT 42 FROM dual",
    },
    OracleCase {
        id: "select.keep_dense_rank.first",
        feature: "SELECT: KEEP DENSE_RANK",
        sql: "SELECT MIN(salary) KEEP (DENSE_RANK FIRST ORDER BY commission_pct NULLS LAST) FROM employees",
    },
    OracleCase {
        id: "select.pivot.xml.subquery",
        feature: "SELECT: PIVOT XML",
        sql: "SELECT * FROM sales PIVOT XML (SUM(amount) FOR quarter IN (SELECT quarter FROM reporting_quarters))",
    },
    OracleCase {
        id: "select.fetch_approximate.next",
        feature: "SELECT: approximate row limiting",
        sql: "SELECT item_id FROM items ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE) FETCH APPROXIMATE NEXT 25 ROWS ONLY",
    },
    OracleCase {
        id: "select.group_by.vector.multicolumn",
        feature: "SELECT: GROUP BY VECTOR",
        sql: "SELECT department_id, job_id, manager_id, SUM(salary) FROM employees GROUP BY VECTOR ((department_id, job_id), (manager_id), ())",
    },
    OracleCase {
        id: "select.json_exists.passing_unknown",
        feature: "SELECT: JSON_EXISTS",
        sql: "SELECT JSON_EXISTS(payload, '$?(@.total > $minimum)' PASSING 100 AS \"minimum\", threshold AS \"threshold\" UNKNOWN ON ERROR) FROM orders",
    },
    OracleCase {
        id: "select.model.multirule",
        feature: "SELECT: model_clause",
        sql: "SELECT country, year, sales, margin FROM sales_view MODEL PARTITION BY (country, region) DIMENSION BY (year) MEASURES (sales, margin) RULES (sales[2027] = sales[2026] * 1.1, margin[2027] = margin[2026] + 10)",
    },
    OracleCase {
        id: "select.graph_table.is_labels",
        feature: "SELECT: SQL property graph",
        sql: "SELECT * FROM GRAPH_TABLE (social_graph MATCH (a IS person) -[e IS follows]-> (b IS person) COLUMNS (a.name AS source_name, b.name AS target_name))",
    },
    OracleCase {
        id: "insert.multitable_all.conditional",
        feature: "INSERT",
        sql: "INSERT ALL WHEN salary >= 10000 THEN INTO salary_audit VALUES (employee_id, salary) INTO bonus_queue VALUES (employee_id) ELSE INTO salary_review VALUES (employee_id, salary) SELECT employee_id, salary FROM employees",
    },
    OracleCase {
        id: "insert.error_logging.minimal",
        feature: "INSERT",
        sql: "INSERT INTO target_table SELECT * FROM source_table LOG ERRORS REJECT LIMIT 5",
    },
    OracleCase {
        id: "update.error_logging.tag_only",
        feature: "UPDATE",
        sql: "UPDATE employees SET salary = salary * 1.05 LOG ERRORS ('annual-raise') REJECT LIMIT UNLIMITED",
    },
    OracleCase {
        id: "merge.action_conditions",
        feature: "MERGE",
        sql: "MERGE INTO bonuses d USING employees s ON (d.employee_id = s.employee_id) WHEN MATCHED THEN UPDATE SET d.bonus = s.salary * 0.1 WHERE s.active = TRUE DELETE WHERE s.salary = 0 WHEN NOT MATCHED THEN INSERT (employee_id, bonus) VALUES (s.employee_id, s.salary * 0.05) WHERE s.salary > 0",
    },
    OracleCase {
        id: "alter.table.split_partition.no_index_update",
        feature: "ALTER TABLE",
        sql: "ALTER TABLE sales SPLIT PARTITION future AT (DATE '2027-01-01') INTO (PARTITION sales_2026, PARTITION future)",
    },
    OracleCase {
        id: "create.view.noforce_noneditioning",
        feature: "CREATE VIEW",
        sql: "CREATE OR REPLACE NOFORCE NONEDITIONING VIEW active_employees AS SELECT employee_id FROM employees WHERE active IS TRUE WITH CHECK OPTION CONSTRAINT active_employees_ck",
    },
    OracleCase {
        id: "create.materialized_view.deferred",
        feature: "CREATE MATERIALIZED VIEW",
        sql: "CREATE MATERIALIZED VIEW department_totals_deferred BUILD DEFERRED REFRESH COMPLETE ON DEMAND DISABLE QUERY REWRITE AS SELECT department_id, COUNT(*) employee_count FROM employees GROUP BY department_id",
    },
    OracleCase {
        id: "create.materialized_view_log.rowid",
        feature: "CREATE MATERIALIZED VIEW LOG",
        sql: "CREATE MATERIALIZED VIEW LOG ON departments WITH ROWID INCLUDING NEW VALUES",
    },
    OracleCase {
        id: "create.table.interval_partition.multibound",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE sales_archive (sale_id NUMBER, sold_on DATE) PARTITION BY RANGE (sold_on) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH')) (PARTITION p0 VALUES LESS THAN (DATE '2025-01-01'), PARTITION p1 VALUES LESS THAN (DATE '2026-01-01'))",
    },
    OracleCase {
        id: "create.table.private_temporary.preserve",
        feature: "CREATE TABLE",
        sql: "CREATE PRIVATE TEMPORARY TABLE ora$ptt_session (item_id NUMBER, payload JSON) ON COMMIT PRESERVE DEFINITION",
    },
    OracleCase {
        id: "create.table.external.multilocation",
        feature: "CREATE TABLE",
        sql: "CREATE TABLE ext_events (event_id NUMBER, payload VARCHAR2(4000)) ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER DEFAULT DIRECTORY data_dir ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE) LOCATION ('events-1.csv', 'events-2.csv')) REJECT LIMIT 25",
    },
    OracleCase {
        id: "select.model.options_aliases",
        feature: "SELECT: model_clause",
        sql: "SELECT year, sales FROM sales_view MODEL RETURN UPDATED ROWS DIMENSION BY (year AS yr) MEASURES (sales AS s) IGNORE NAV UNIQUE DIMENSION RULES UPSERT SEQUENTIAL ORDER (s[2027] = s[2026] * 1.1)",
    },
    OracleCase {
        id: "select.model.iterate",
        feature: "SELECT: model_clause",
        sql: "SELECT dim_col, cur_val FROM model_input MODEL DIMENSION BY (dim_col) MEASURES (cur_val) RULES UPDATE SEQUENTIAL ORDER ITERATE (1000) UNTIL (PREVIOUS(cur_val[1]) - cur_val[1] < 1) (cur_val[1] = cur_val[1] / 2)",
    },
    OracleCase {
        id: "select.model.iterate_until_unparenthesized",
        feature: "SELECT: model_clause",
        sql: "SELECT dim_col, cur_val FROM model_input MODEL DIMENSION BY (dim_col) MEASURES (cur_val) RULES ITERATE (100) UNTIL cur_val[1] < 1 (cur_val[1] = cur_val[1] / 2)",
    },
    OracleCase {
        id: "select.model.reference",
        feature: "SELECT: model_clause",
        sql: "SELECT country, year, sales FROM sales_view MODEL KEEP NAV RETURN ALL ROWS REFERENCE historical ON (SELECT country, year, sales FROM sales_history) PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) UNIQUE SINGLE REFERENCE MAIN forecast PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) UNIQUE DIMENSION RULES UPDATE (sales[2027] = historical.sales[2026])",
    },
    OracleCase {
        id: "select.model.symbolic_multicell",
        feature: "SELECT: model_clause",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES UPSERT ALL (UPDATE sales[product = 'Mouse', year = 2027] = sales['Mouse', 2026], UPSERT sales[product = 'Keyboard', year = 2027] = sales['Keyboard', 2026])",
    },
    OracleCase {
        id: "select.model.ordered_rule",
        feature: "SELECT: model_clause",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES SEQUENTIAL ORDER (UPDATE sales['Mouse', 2027] ORDER BY product, year DESC = sales['Mouse', 2026])",
    },
    OracleCase {
        id: "select.model.for_list",
        feature: "SELECT: model_clause",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES UPSERT (sales[FOR product IN ('Mouse', 'Keyboard'), 2027] = sales[CV(product), 2026])",
    },
    OracleCase {
        id: "select.model.for_multiple",
        feature: "SELECT: model_clause",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES UPSERT (sales[FOR product IN ('Mouse', 'Keyboard'), FOR year IN (2027, 2028)] = sales[CV(product), CV(year) - 1])",
    },
    OracleCase {
        id: "select.model.for_range",
        feature: "SELECT: model_clause",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES (sales[FOR year FROM 2027 TO 2030 INCREMENT 1] = sales[CV(year) - 1] * 1.1)",
    },
    OracleCase {
        id: "select.model.for_range_like",
        feature: "SELECT: model_clause",
        sql: "SELECT period_key, sales FROM sales_view MODEL DIMENSION BY (period_key) MEASURES (sales) RULES (sales[FOR period_key LIKE 'FY-%' FROM 2030 TO 2027 DECREMENT 1] = 0)",
    },
    OracleCase {
        id: "select.model.for_subquery",
        feature: "SELECT: model_clause",
        sql: "SELECT product, sales FROM sales_view MODEL DIMENSION BY (product) MEASURES (sales) RULES (sales[FOR product IN (SELECT product_name FROM interesting_products)] = 0)",
    },
    OracleCase {
        id: "select.model.for_multicolumn_list",
        feature: "SELECT: model_clause",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES (sales[FOR (product, year) IN (('Mouse', 2027), ('Keyboard', 2028))] = sales[CV(product), CV(year) - 1])",
    },
    OracleCase {
        id: "select.model.for_multicolumn_subquery",
        feature: "SELECT: model_clause",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES (sales[FOR (product, year) IN (SELECT product_name, forecast_year FROM forecast_targets)] = 0)",
    },
];

#[test]
fn relational_case_ids_are_unique() {
    assert_unique_case_ids(RELATIONAL_CASES);
}

#[test]
fn oracle_relational_sql_frontier() {
    assert_all_parse(RELATIONAL_CASES);
}
