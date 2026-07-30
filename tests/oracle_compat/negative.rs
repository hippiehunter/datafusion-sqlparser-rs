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

use std::collections::BTreeSet;

use sqlparser::dialect::OracleDialect;
use sqlparser::parser::Parser;

pub(super) struct NegativeCase {
    pub id: &'static str,
    sql: &'static str,
}

pub(super) const NEGATIVE_CASES: &[NegativeCase] = &[
    NegativeCase {
        id: "lex.alternative_quote.unclosed",
        sql: "SELECT q'[Oracle text' FROM dual",
    },
    NegativeCase {
        id: "query.flashback.missing_expression",
        sql: "SELECT * FROM employees AS OF SCN",
    },
    NegativeCase {
        id: "query.flashback.missing_kind",
        sql: "SELECT * FROM employees AS OF 123456",
    },
    NegativeCase {
        id: "query.flashback.versions.missing_end",
        sql: "SELECT * FROM employees VERSIONS BETWEEN SCN MINVALUE AND",
    },
    NegativeCase {
        id: "query.partitioned_inner_join",
        sql: "SELECT * FROM calendar d INNER JOIN sales s PARTITION BY (s.product_id) ON s.day = d.day",
    },
    NegativeCase {
        id: "query.qualify.missing_condition",
        sql: "SELECT employee_id FROM employees QUALIFY",
    },
    NegativeCase {
        id: "query.cycle.missing_cycle_value",
        sql: "WITH org(emp_id) AS (SELECT employee_id FROM employees) CYCLE emp_id SET is_cycle DEFAULT 0 SELECT * FROM org",
    },
    NegativeCase {
        id: "query.cycle.missing_default_value",
        sql: "WITH org(emp_id) AS (SELECT employee_id FROM employees) CYCLE emp_id SET is_cycle TO 1 SELECT * FROM org",
    },
    NegativeCase {
        id: "query.cycle.standard_using_form",
        sql: "WITH org(emp_id) AS (SELECT employee_id FROM employees) CYCLE emp_id SET is_cycle TO 1 DEFAULT 0 USING path_col SELECT * FROM org",
    },
    NegativeCase {
        id: "query.keep.missing_rank_direction",
        sql: "SELECT MAX(salary) KEEP (DENSE_RANK ORDER BY commission_pct) FROM employees",
    },
    NegativeCase {
        id: "query.keep.missing_order_by",
        sql: "SELECT MAX(salary) KEEP (DENSE_RANK LAST commission_pct) FROM employees",
    },
    NegativeCase {
        id: "query.pivot_xml.static_values",
        sql: "SELECT * FROM sales PIVOT XML (SUM(amount) FOR quarter IN ('Q1', 'Q2'))",
    },
    NegativeCase {
        id: "query.pivot_non_xml.any",
        sql: "SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN (ANY))",
    },
    NegativeCase {
        id: "query.fetch_approximate.missing_direction",
        sql: "SELECT * FROM items FETCH APPROXIMATE 10 ROWS ONLY",
    },
    NegativeCase {
        id: "query.fetch_approximate.percent",
        sql: "SELECT * FROM items FETCH APPROXIMATE FIRST 10 PERCENT ROWS ONLY",
    },
    NegativeCase {
        id: "query.fetch_approximate.with_ties",
        sql: "SELECT * FROM items FETCH APPROXIMATE FIRST 10 ROWS WITH TIES",
    },
    NegativeCase {
        id: "query.group_by_vector.empty",
        sql: "SELECT COUNT(*) FROM employees GROUP BY VECTOR ()",
    },
    NegativeCase {
        id: "query.group_by_vector.unwrapped_member",
        sql: "SELECT COUNT(*) FROM employees GROUP BY VECTOR (department_id)",
    },
    NegativeCase {
        id: "query.model.missing_dimension",
        sql: "SELECT country, year, sales FROM sales_view MODEL PARTITION BY (country) MEASURES (sales) RULES (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.empty_rules",
        sql: "SELECT country, year, sales FROM sales_view MODEL PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) RULES ()",
    },
    NegativeCase {
        id: "query.model.rule_missing_assignment",
        sql: "SELECT country, year, sales FROM sales_view MODEL PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) RULES (sales[2027])",
    },
    NegativeCase {
        id: "query.model.rule_missing_value",
        sql: "SELECT country, year, sales FROM sales_view MODEL PARTITION BY (country) DIMENSION BY (year) MEASURES (sales) RULES (sales[2027] =)",
    },
    NegativeCase {
        id: "query.model.return_missing_rows",
        sql: "SELECT year, sales FROM sales_view MODEL RETURN UPDATED DIMENSION BY (year) MEASURES (sales) (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.keep_missing_nav",
        sql: "SELECT year, sales FROM sales_view MODEL KEEP DIMENSION BY (year) MEASURES (sales) (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.unique_invalid_kind",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) UNIQUE REFERENCE (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.automatic_missing_order",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES AUTOMATIC (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.automatic_with_iterate",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES AUTOMATIC ORDER ITERATE (10) (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.iterate_without_rules",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) ITERATE (10) (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.reference_missing_query",
        sql: "SELECT year, sales FROM sales_view MODEL REFERENCE history ON DIMENSION BY (year) MEASURES (sales) DIMENSION BY (year) MEASURES (sales) (sales[2027] = sales[2026])",
    },
    NegativeCase {
        id: "query.model.for_missing_dimension",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES (sales[FOR IN (2027)] = 0)",
    },
    NegativeCase {
        id: "query.model.for_empty_list",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES (sales[FOR year IN ()] = 0)",
    },
    NegativeCase {
        id: "query.model.for_range_missing_from",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES (sales[FOR year TO 2030 INCREMENT 1] = 0)",
    },
    NegativeCase {
        id: "query.model.for_range_missing_direction",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES (sales[FOR year FROM 2027 TO 2030 1] = 0)",
    },
    NegativeCase {
        id: "query.model.for_range_missing_step",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES (sales[FOR year FROM 2027 TO 2030 INCREMENT] = 0)",
    },
    NegativeCase {
        id: "query.model.for_multicolumn_empty_dimensions",
        sql: "SELECT year, sales FROM sales_view MODEL DIMENSION BY (year) MEASURES (sales) RULES (sales[FOR () IN ((2027))] = 0)",
    },
    NegativeCase {
        id: "query.model.for_multicolumn_empty_rows",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES (sales[FOR (product, year) IN ()] = 0)",
    },
    NegativeCase {
        id: "query.model.for_multicolumn_mixed_selector",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES (sales[2027, FOR (product, year) IN (('Mouse', 2027))] = 0)",
    },
    NegativeCase {
        id: "query.model.for_with_order_by",
        sql: "SELECT product, year, sales FROM sales_view MODEL DIMENSION BY (product, year) MEASURES (sales) RULES (sales[FOR product IN ('Mouse'), 2027] ORDER BY product = 0)",
    },
    NegativeCase {
        id: "query.json_exists.passing_missing_alias",
        sql: "SELECT JSON_EXISTS(payload, '$.id' PASSING 100) FROM orders",
    },
    NegativeCase {
        id: "query.json_exists.passing_missing_expression",
        sql: "SELECT JSON_EXISTS(payload, '$.id' PASSING AS \"id\") FROM orders",
    },
    NegativeCase {
        id: "plsql.open_dynamic.missing_using_argument",
        sql: "DECLARE c SYS_REFCURSOR; statement_text VARCHAR2(100); BEGIN OPEN c FOR statement_text USING; END;",
    },
    NegativeCase {
        id: "plsql.trigger.referencing_empty",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees REFERENCING FOR EACH ROW BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.referencing_missing_alias",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees REFERENCING OLD AS FOR EACH ROW BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.referencing_duplicate_old",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees REFERENCING OLD AS before_row OLD AS previous_row FOR EACH ROW BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.when_without_row",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees WHEN (NEW.salary > OLD.salary) BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.or_replace_if_not_exists",
        sql: "CREATE OR REPLACE TRIGGER IF NOT EXISTS employees_history AFTER UPDATE ON employees BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.crossedition_missing_kind",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees FORWARD BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.ordering_missing_trigger",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees FOLLOWS, employees_legacy BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.referencing_with_call",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees REFERENCING NEW AS new_row FOR EACH ROW CALL audit_change(new_row.employee_id)",
    },
    NegativeCase {
        id: "plsql.trigger.call_missing_routine",
        sql: "CREATE TRIGGER employees_history AFTER UPDATE ON employees CALL",
    },
    NegativeCase {
        id: "plsql.trigger.associate_missing_statistics",
        sql: "CREATE TRIGGER schema_audit AFTER ASSOCIATE ON SCHEMA BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.set_missing_container",
        sql: "CREATE TRIGGER container_audit AFTER SET ON DATABASE BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.pluggable_missing_database",
        sql: "CREATE TRIGGER pdb_audit AFTER LOGON ON PLUGGABLE BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.trigger.unknown_event",
        sql: "CREATE TRIGGER schema_audit AFTER SELECT ON SCHEMA BEGIN NULL; END;",
    },
    NegativeCase {
        id: "plsql.open_static.missing_using_argument",
        sql: "DECLARE c SYS_REFCURSOR; BEGIN OPEN c FOR SELECT * FROM employees WHERE department_id = :1 USING; END;",
    },
    NegativeCase {
        id: "alter.table.split_partition.missing_at",
        sql: "ALTER TABLE sales SPLIT PARTITION future INTO (PARTITION sales_2026, PARTITION future)",
    },
    NegativeCase {
        id: "alter.table.split_partition.one_target",
        sql: "ALTER TABLE sales SPLIT PARTITION future AT (DATE '2027-01-01') INTO (PARTITION sales_2026)",
    },
    NegativeCase {
        id: "alter.table.split_partition.invalid_index_clause",
        sql: "ALTER TABLE sales SPLIT PARTITION future AT (DATE '2027-01-01') UPDATE GLOBAL INDEXES INTO (PARTITION sales_2026, PARTITION future)",
    },
    NegativeCase {
        id: "alter.view.missing_compile",
        sql: "ALTER VIEW active_employees",
    },
    NegativeCase {
        id: "alter.view.compile_trailing_clause",
        sql: "ALTER VIEW active_employees COMPILE ONLINE",
    },
    NegativeCase {
        id: "alter.materialized_view.refresh_missing_method",
        sql: "ALTER MATERIALIZED VIEW department_totals REFRESH ON DEMAND",
    },
    NegativeCase {
        id: "alter.materialized_view.refresh_missing_mode",
        sql: "ALTER MATERIALIZED VIEW department_totals REFRESH FAST ON",
    },
    NegativeCase {
        id: "alter.materialized_view.refresh_invalid_order",
        sql: "ALTER MATERIALIZED VIEW department_totals ON DEMAND REFRESH FAST",
    },
    NegativeCase {
        id: "create.view.object.missing_identifier",
        sql: "CREATE VIEW employee_objects OF employee_t AS SELECT employee_id FROM employees",
    },
    NegativeCase {
        id: "create.view.object.empty_identifier",
        sql: "CREATE VIEW employee_objects OF employee_t WITH OBJECT IDENTIFIER () AS SELECT employee_id FROM employees",
    },
    NegativeCase {
        id: "create.view.read_only.invalid_order",
        sql: "CREATE VIEW active_employees WITH READ ONLY AS SELECT * FROM employees",
    },
    NegativeCase {
        id: "create.materialized_view.build_missing_mode",
        sql: "CREATE MATERIALIZED VIEW mv BUILD REFRESH FAST ON COMMIT AS SELECT 1 FROM dual",
    },
    NegativeCase {
        id: "create.materialized_view.refresh_missing_mode",
        sql: "CREATE MATERIALIZED VIEW mv REFRESH FAST ON AS SELECT 1 FROM dual",
    },
    NegativeCase {
        id: "create.materialized_view.query_rewrite_incomplete",
        sql: "CREATE MATERIALIZED VIEW mv ENABLE QUERY AS SELECT 1 FROM dual",
    },
    NegativeCase {
        id: "create.materialized_view_log.duplicate_option",
        sql: "CREATE MATERIALIZED VIEW LOG ON employees WITH ROWID, ROWID",
    },
    NegativeCase {
        id: "create.materialized_view_log.dangling_comma",
        sql: "CREATE MATERIALIZED VIEW LOG ON employees WITH PRIMARY KEY,",
    },
    NegativeCase {
        id: "create.materialized_view_log.incomplete_new_values",
        sql: "CREATE MATERIALIZED VIEW LOG ON employees WITH ROWID INCLUDING NEW",
    },
    NegativeCase {
        id: "drop.user.invalid_cascade_constraints",
        sql: "DROP USER app_user CASCADE CONSTRAINTS",
    },
    NegativeCase {
        id: "drop.user.duplicate_cascade",
        sql: "DROP USER app_user CASCADE CASCADE",
    },
    NegativeCase {
        id: "drop.user.if_not_exists",
        sql: "DROP USER IF NOT EXISTS app_user",
    },
    NegativeCase {
        id: "drop.database.if_exists",
        sql: "DROP DATABASE IF EXISTS",
    },
    NegativeCase {
        id: "dml.error_logging.missing_errors",
        sql: "UPDATE employees SET salary = 1 LOG INTO err$_employees",
    },
    NegativeCase {
        id: "dml.error_logging.missing_reject_limit",
        sql: "UPDATE employees SET salary = 1 LOG ERRORS REJECT",
    },
    NegativeCase {
        id: "dml.error_logging.invalid_clause_order",
        sql: "UPDATE employees SET salary = 1 LOG ERRORS ('tag') INTO err$_employees",
    },
    NegativeCase {
        id: "merge.update.delete_missing_where",
        sql: "MERGE INTO target d USING source s ON (d.id = s.id) WHEN MATCHED THEN UPDATE SET d.value = s.value DELETE",
    },
    NegativeCase {
        id: "merge.insert.where_missing_condition",
        sql: "MERGE INTO target d USING source s ON (d.id = s.id) WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id) WHERE",
    },
    NegativeCase {
        id: "insert.multitable.first_without_when",
        sql: "INSERT FIRST INTO target VALUES (id) SELECT id FROM source",
    },
    NegativeCase {
        id: "insert.multitable.when_without_target",
        sql: "INSERT ALL WHEN id > 0 THEN SELECT id FROM source",
    },
    NegativeCase {
        id: "insert.multitable.else_without_target",
        sql: "INSERT ALL WHEN id > 0 THEN INTO target VALUES (id) ELSE SELECT id FROM source",
    },
    NegativeCase {
        id: "insert.multitable.target_without_values",
        sql: "INSERT ALL INTO target SELECT id FROM source",
    },
    NegativeCase {
        id: "query.graph_table.node_missing_label",
        sql: "SELECT * FROM GRAPH_TABLE (social_graph MATCH (a IS) COLUMNS (a.name AS name))",
    },
    NegativeCase {
        id: "query.graph_table.edge_missing_label",
        sql: "SELECT * FROM GRAPH_TABLE (social_graph MATCH (a)-[e IS]->(b) COLUMNS (a.name AS name))",
    },
    NegativeCase {
        id: "query.with_function.missing_terminator",
        sql: "WITH FUNCTION twice(n NUMBER) RETURN NUMBER IS BEGIN RETURN n * 2; END SELECT twice(21) FROM dual",
    },
    NegativeCase {
        id: "query.with_function.missing_return_type",
        sql: "WITH FUNCTION twice(n NUMBER) IS BEGIN RETURN n * 2; END; SELECT twice(21) FROM dual",
    },
    NegativeCase {
        id: "create.table.interval.missing_partitions",
        sql: "CREATE TABLE sales (sold_on DATE) PARTITION BY RANGE (sold_on) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))",
    },
    NegativeCase {
        id: "create.table.interval.missing_boundary",
        sql: "CREATE TABLE sales (sold_on DATE) PARTITION BY RANGE (sold_on) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH')) (PARTITION p0 VALUES LESS THAN ())",
    },
    NegativeCase {
        id: "create.table.reference.empty_constraint",
        sql: "CREATE TABLE order_items (order_id NUMBER) PARTITION BY REFERENCE ()",
    },
    NegativeCase {
        id: "create.table.private.missing_definition",
        sql: "CREATE PRIVATE TEMPORARY TABLE ora$ptt_work (item_id NUMBER) ON COMMIT DROP",
    },
    NegativeCase {
        id: "create.table.blockchain.missing_hashing",
        sql: "CREATE BLOCKCHAIN TABLE ledger (id NUMBER) NO DROP UNTIL 31 DAYS IDLE NO DELETE",
    },
    NegativeCase {
        id: "create.table.immutable.invalid_hashing",
        sql: "CREATE IMMUTABLE TABLE ledger (id NUMBER) NO DROP UNTIL 31 DAYS IDLE NO DELETE HASHING USING SHA2_512 VERSION v1",
    },
    NegativeCase {
        id: "create.table.external.missing_directory",
        sql: "CREATE TABLE ext_t (id NUMBER) ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE) LOCATION ('x.csv'))",
    },
    NegativeCase {
        id: "create.table.external.missing_location",
        sql: "CREATE TABLE ext_t (id NUMBER) ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER DEFAULT DIRECTORY data_dir ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE))",
    },
    NegativeCase {
        id: "select.inline_external.missing_columns",
        sql: "SELECT * FROM EXTERNAL (() TYPE ORACLE_LOADER DEFAULT DIRECTORY data_dir ACCESS PARAMETERS (RECORDS DELIMITED BY NEWLINE) LOCATION ('x.csv'))",
    },
    NegativeCase {
        id: "drop.table.invalid_option_order",
        sql: "DROP TABLE employees_stage PURGE CASCADE CONSTRAINTS",
    },
    NegativeCase {
        id: "drop.view.invalid_purge",
        sql: "DROP VIEW active_employees PURGE",
    },
    NegativeCase {
        id: "drop.index.invalid_preserve",
        sql: "DROP INDEX employees_name_ix PRESERVE TABLE",
    },
    NegativeCase {
        id: "truncate.conflicting_storage",
        sql: "TRUNCATE TABLE employees_stage DROP STORAGE REUSE STORAGE",
    },
    NegativeCase {
        id: "truncate.invalid_restrict",
        sql: "TRUNCATE TABLE employees_stage RESTRICT",
    },
    NegativeCase {
        id: "create.index.missing_table",
        sql: "CREATE INDEX employees_name_ix ON (last_name)",
    },
    NegativeCase {
        id: "create.index.domain.missing_indextype",
        sql: "CREATE INDEX documents_text_ix ON documents (body) INDEXTYPE IS",
    },
    NegativeCase {
        id: "create.index.vector.missing_accuracy",
        sql: "CREATE VECTOR INDEX items_embedding_hnsw ON items (embedding) ORGANIZATION INMEMORY NEIGHBOR GRAPH DISTANCE COSINE",
    },
    NegativeCase {
        id: "alter.index.invalid_order",
        sql: "ALTER INDEX employees_name_ix ONLINE REBUILD",
    },
    NegativeCase {
        id: "create.table.identity.missing_identity",
        sql: "CREATE TABLE messages (message_id NUMBER GENERATED BY DEFAULT ON NULL AS)",
    },
    NegativeCase {
        id: "alter.table.add.empty_columns",
        sql: "ALTER TABLE employees ADD ()",
    },
    NegativeCase {
        id: "alter.table.modify.missing_definition",
        sql: "ALTER TABLE employees MODIFY (last_name)",
    },
    NegativeCase {
        id: "query.hierarchy.missing_condition",
        sql: "SELECT * FROM employees CONNECT BY",
    },
    NegativeCase {
        id: "dml.insert.multitable.missing_branch",
        sql: "INSERT ALL SELECT * FROM employees",
    },
    NegativeCase {
        id: "dml.returning.missing_target",
        sql: "UPDATE employees SET salary = 1 RETURNING salary INTO",
    },
    NegativeCase {
        id: "plsql.if.mismatched_end",
        sql: "BEGIN IF TRUE THEN NULL; END; END;",
    },
    NegativeCase {
        id: "plsql.loop.mismatched_end",
        sql: "BEGIN LOOP NULL; END; END;",
    },
    NegativeCase {
        id: "plsql.declaration.missing_terminator",
        sql: "DECLARE amount NUMBER BEGIN NULL; END;",
    },
    NegativeCase {
        id: "alter.duality.clause_order",
        sql: "ALTER JSON RELATIONAL DUALITY VIEW orders_dv ENABLE REPLICATION LOGICAL",
    },
    NegativeCase {
        id: "drop.domain.option_order",
        sql: "DROP DOMAIN email_domain PRESERVE FORCE",
    },
    NegativeCase {
        id: "drop.tablespace.option_order",
        sql: "DROP TABLESPACE app_data CASCADE CONSTRAINTS INCLUDING CONTENTS AND DATAFILES",
    },
    NegativeCase {
        id: "lock.invalid_mode",
        sql: "LOCK TABLE employees IN SHARE BANANA MODE",
    },
    NegativeCase {
        id: "audit.invalid_outcome",
        sql: "AUDIT POLICY app_audit BY app_user WHENEVER MAYBE",
    },
    NegativeCase {
        id: "truncate.invalid_storage",
        sql: "TRUNCATE CLUSTER employee_cluster KEEP STORAGE",
    },
    NegativeCase {
        id: "create.missing_definition",
        sql: "CREATE ANALYTIC VIEW sales_av",
    },
    NegativeCase {
        id: "create.unknown_clause",
        sql: "CREATE ANALYTIC VIEW sales_av RUBBISH",
    },
    NegativeCase {
        id: "create.attribute_dimension.missing_level",
        sql: "CREATE ATTRIBUTE DIMENSION time_attr_dim USING calendar ATTRIBUTES (year, month)",
    },
    NegativeCase {
        id: "create.audit_policy.missing_action",
        sql: "CREATE AUDIT POLICY app_audit ACTIONS ON hr.employees",
    },
    NegativeCase {
        id: "create.cluster.missing_size",
        sql: "CREATE CLUSTER employee_cluster (department_id NUMBER)",
    },
    NegativeCase {
        id: "create.database.missing_system_user",
        sql: "CREATE DATABASE appdb USER SYS IDENTIFIED BY password CHARACTER SET AL32UTF8",
    },
    NegativeCase {
        id: "create.flexible_domain.missing_selector",
        sql: "CREATE FLEXIBLE DOMAIN order_value (amount AS NUMBER) CHOOSE DOMAIN USING ()",
    },
    NegativeCase {
        id: "create.java.unbalanced_body",
        sql: "CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED \"Example\" AS public class Example {",
    },
    NegativeCase {
        id: "create.duality_view.missing_mapping_colon",
        sql: "CREATE JSON RELATIONAL DUALITY VIEW orders_dv AS SELECT JSON {'_id' o.order_id} FROM orders o",
    },
    NegativeCase {
        id: "create.property_graph.missing_reference",
        sql: "CREATE PROPERTY GRAPH social_graph VERTEX TABLES (persons KEY (person_id)) EDGE TABLES (follows KEY (follow_id) SOURCE KEY (from_id) DESTINATION KEY (to_id) REFERENCES persons(person_id))",
    },
    NegativeCase {
        id: "create.schema.empty",
        sql: "CREATE SCHEMA AUTHORIZATION app_user",
    },
    NegativeCase {
        id: "create.tablespace.next_without_autoextend",
        sql: "CREATE TABLESPACE app_data DATAFILE '/tmp/app_data.dbf' SIZE 100M NEXT 10M",
    },
    NegativeCase {
        id: "set.transaction.missing_mode",
        sql: "SET TRANSACTION NAME 'reporting'",
    },
    NegativeCase {
        id: "set.transaction.trailing_clause",
        sql: "SET TRANSACTION READ ONLY NAME 'reporting' EXTRA",
    },
    NegativeCase {
        id: "set.role.missing_password",
        sql: "SET ROLE reporting_role IDENTIFIED BY",
    },
    NegativeCase {
        id: "grant.system.missing_privilege",
        sql: "GRANT CREATE TO app_user",
    },
    NegativeCase {
        id: "grant.system.incomplete_admin_option",
        sql: "GRANT CREATE SESSION TO app_user WITH ADMIN",
    },
    NegativeCase {
        id: "explain.plan.missing_statement",
        sql: "EXPLAIN PLAN INTO plan_table FOR",
    },
    NegativeCase {
        id: "create.synonym.missing_target",
        sql: "CREATE SYNONYM emp FOR",
    },
    NegativeCase {
        id: "create.directory.missing_path",
        sql: "CREATE DIRECTORY data_dir AS",
    },
    NegativeCase {
        id: "create.database_link.missing_service",
        sql: "CREATE DATABASE LINK reporting CONNECT TO report_user IDENTIFIED BY password USING",
    },
    NegativeCase {
        id: "alter.database_link.missing_password",
        sql: "ALTER DATABASE LINK reporting CONNECT TO report_user IDENTIFIED BY",
    },
    NegativeCase {
        id: "drop.directory.invalid_force",
        sql: "DROP DIRECTORY data_dir FORCE",
    },
    NegativeCase {
        id: "create.sequence.conflicting_cache",
        sql: "CREATE SEQUENCE employee_seq CACHE 20 NOCACHE",
    },
    NegativeCase {
        id: "create.sequence.duplicate_cycle",
        sql: "CREATE SEQUENCE employee_seq CYCLE NOCYCLE",
    },
    NegativeCase {
        id: "condition.likec.missing_pattern",
        sql: "SELECT * FROM employees WHERE last_name LIKEC",
    },
    NegativeCase {
        id: "condition.is_of.empty_types",
        sql: "SELECT * FROM object_store o WHERE VALUE(o) IS OF ()",
    },
    NegativeCase {
        id: "condition.member.missing_collection",
        sql: "SELECT * FROM object_store o WHERE tag_t('urgent') MEMBER OF",
    },
    NegativeCase {
        id: "expression.translate.missing_charset",
        sql: "SELECT TRANSLATE(name USING) FROM accounts",
    },
    NegativeCase {
        id: "expression.treat.missing_type",
        sql: "SELECT TREAT(VALUE(p) AS) FROM persons p",
    },
];

#[test]
fn oracle_rejects_invalid_grammar_cases() {
    let failures = NEGATIVE_CASES
        .iter()
        .filter(|case| Parser::parse_sql(&OracleDialect {}, case.sql).is_ok())
        .map(|case| format!("{}: {}", case.id, case.sql))
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "Oracle parser accepted {} invalid cases:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn negative_case_ids_are_unique() {
    let ids = NEGATIVE_CASES
        .iter()
        .map(|case| case.id)
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), NEGATIVE_CASES.len());
}
