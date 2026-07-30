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

use super::common::covered_features;
use super::plsql::PLSQL_CASES;
use super::relational::RELATIONAL_CASES;
use super::statements::STATEMENT_CASES;

const SQL_STATEMENTS: &[&str] = &[
    "ADMINISTER KEY MANAGEMENT",
    "ALTER ANALYTIC VIEW",
    "ALTER ASSERTION",
    "ALTER ATTRIBUTE DIMENSION",
    "ALTER AUDIT POLICY",
    "ALTER CLUSTER",
    "ALTER DATABASE",
    "ALTER DATABASE DICTIONARY",
    "ALTER DATABASE LINK",
    "ALTER DIMENSION",
    "ALTER DIRECTIVE (VALIDATE)",
    "ALTER DISKGROUP",
    "ALTER DOMAIN",
    "ALTER END USER",
    "ALTER FLASHBACK ARCHIVE",
    "ALTER FUNCTION",
    "ALTER HIERARCHY",
    "ALTER INDEX",
    "ALTER INDEXTYPE",
    "ALTER INMEMORY JOIN GROUP",
    "ALTER JAVA",
    "ALTER JSON RELATIONAL DUALITY VIEW",
    "ALTER LIBRARY",
    "ALTER LOCKDOWN PROFILE",
    "ALTER MATERIALIZED VIEW",
    "ALTER MATERIALIZED VIEW LOG",
    "ALTER MATERIALIZED ZONEMAP",
    "ALTER MLE ENV",
    "ALTER MLE MODULE",
    "ALTER OPERATOR",
    "ALTER OUTLINE",
    "ALTER PACKAGE",
    "ALTER PLUGGABLE DATABASE",
    "ALTER PMEM FILESTORE",
    "ALTER PROCEDURE",
    "ALTER PROFILE",
    "ALTER PROPERTY GRAPH",
    "ALTER RESOURCE COST",
    "ALTER ROLE",
    "ALTER ROLLBACK SEGMENT",
    "ALTER SEQUENCE",
    "ALTER SESSION",
    "ALTER SYNONYM",
    "ALTER SYSTEM",
    "ALTER TABLE",
    "ALTER TABLESPACE",
    "ALTER TABLESPACE SET",
    "ALTER TRIGGER",
    "ALTER TYPE",
    "ALTER USER",
    "ALTER VIEW",
    "ANALYZE",
    "ASSOCIATE STATISTICS",
    "AUDIT (Unified Auditing)",
    "CALL",
    "COMMENT",
    "COMMIT",
    "CREATE ANALYTIC VIEW",
    "CREATE APPLICATION IDENTITY",
    "CREATE ASSERTION",
    "CREATE ATTRIBUTE DIMENSION",
    "CREATE AUDIT POLICY",
    "CREATE CLUSTER",
    "CREATE CONTEXT",
    "CREATE CONTROLFILE",
    "CREATE DATA GRANT",
    "CREATE DATA ROLE",
    "CREATE DATABASE",
    "CREATE DATABASE LINK",
    "CREATE DIMENSION",
    "CREATE DIRECTIVE (VALIDATE)",
    "CREATE DIRECTORY",
    "CREATE DISKGROUP",
    "CREATE DOMAIN",
    "CREATE EDITION",
    "CREATE END USER",
    "CREATE END USER CONTEXT",
    "CREATE FLASHBACK ARCHIVE",
    "CREATE FLEXIBLE DOMAIN",
    "CREATE FUNCTION",
    "CREATE HIERARCHY",
    "CREATE HYBRID VECTOR INDEX",
    "CREATE ICEBERG TABLE",
    "CREATE INDEX",
    "CREATE INDEXTYPE",
    "CREATE INMEMORY JOIN GROUP",
    "CREATE JAVA",
    "CREATE JSON RELATIONAL DUALITY VIEW",
    "CREATE LIBRARY",
    "CREATE LOCKDOWN PROFILE",
    "CREATE LOGICAL PARTITION TRACKING",
    "CREATE MATERIALIZED VIEW",
    "CREATE MATERIALIZED VIEW LOG",
    "CREATE MATERIALIZED ZONEMAP",
    "CREATE MLE ENV",
    "CREATE MLE MODULE",
    "CREATE MULTI COLUMN DOMAIN",
    "CREATE OPERATOR",
    "CREATE OUTLINE",
    "CREATE PACKAGE",
    "CREATE PACKAGE BODY",
    "CREATE PFILE",
    "CREATE PLUGGABLE DATABASE",
    "CREATE PMEM FILESTORE",
    "CREATE PROCEDURE",
    "CREATE PROFILE",
    "CREATE PROPERTY GRAPH",
    "CREATE RESTORE POINT",
    "CREATE ROLE",
    "CREATE ROLLBACK SEGMENT",
    "CREATE SCHEMA",
    "CREATE SEQUENCE",
    "CREATE SINGLE COLUMN DOMAIN",
    "CREATE SPFILE",
    "CREATE SYNONYM",
    "CREATE TABLE",
    "CREATE TABLESPACE",
    "CREATE TABLESPACE SET",
    "CREATE TRIGGER",
    "CREATE TYPE",
    "CREATE TYPE BODY",
    "CREATE USER",
    "CREATE VECTOR INDEX",
    "CREATE VIEW",
    "DELETE",
    "DISASSOCIATE STATISTICS",
    "DROP ANALYTIC VIEW",
    "DROP APPLICATION IDENTITY",
    "DROP ASSERTION",
    "DROP ATTRIBUTE DIMENSION",
    "DROP AUDIT POLICY",
    "DROP CLUSTER",
    "DROP CONTEXT",
    "DROP DATA GRANT",
    "DROP DATA ROLE",
    "DROP DATABASE",
    "DROP DATABASE LINK",
    "DROP DIMENSION",
    "DROP DIRECTIVE (VALIDATE)",
    "DROP DIRECTORY",
    "DROP DISKGROUP",
    "DROP DOMAIN",
    "DROP EDITION",
    "DROP END USER",
    "DROP FLASHBACK ARCHIVE",
    "DROP FUNCTION",
    "DROP HIERARCHY",
    "DROP ICEBERG TABLE",
    "DROP INDEX",
    "DROP INDEXTYPE",
    "DROP INMEMORY JOIN GROUP",
    "DROP JAVA",
    "DROP LIBRARY",
    "DROP LOCKDOWN PROFILE",
    "DROP MATERIALIZED VIEW",
    "DROP MATERIALIZED VIEW LOG",
    "DROP MATERIALIZED ZONEMAP",
    "DROP MLE ENV",
    "DROP MLE MODULE",
    "DROP OPERATOR",
    "DROP OUTLINE",
    "DROP PACKAGE",
    "DROP PLUGGABLE DATABASE",
    "DROP PMEM FILESTORE",
    "DROP PROCEDURE",
    "DROP PROFILE",
    "DROP PROPERTY GRAPH",
    "DROP RESTORE POINT",
    "DROP ROLE",
    "DROP ROLLBACK SEGMENT",
    "DROP SEQUENCE",
    "DROP SYNONYM",
    "DROP TABLE",
    "DROP TABLESPACE",
    "DROP TABLESPACE SET",
    "DROP TRIGGER",
    "DROP TYPE",
    "DROP TYPE BODY",
    "DROP USER",
    "DROP VIEW",
    "EXPLAIN PLAN",
    "FLASHBACK DATABASE",
    "FLASHBACK TABLE",
    "GRANT",
    "GRANT DATA ROLE",
    "INSERT",
    "LOCK TABLE",
    "MERGE",
    "NOAUDIT (Traditional Auditing)",
    "NOAUDIT (Unified Auditing)",
    "PURGE",
    "RENAME",
    "REVOKE",
    "REVOKE DATA ROLE",
    "ROLLBACK",
    "SAVEPOINT",
    "SELECT",
    "SET CONSTRAINT[S]",
    "SET ROLE",
    "SET TRANSACTION",
    "SET USE DATA GRANTS ONLY",
    "TRUNCATE CLUSTER",
    "TRUNCATE TABLE",
    "UPDATE",
];

const PLSQL_LANGUAGE_ELEMENTS: &[&str] = &[
    "ACCESSIBLE BY Clause",
    "AGGREGATE Clause",
    "Assignment Statement",
    "AUTONOMOUS_TRANSACTION Pragma",
    "Basic LOOP Statement",
    "Block",
    "Call Specification",
    "CASE Statement",
    "CLOSE Statement",
    "Collection Method Invocation",
    "Collection Variable Declaration",
    "COMPILE Clause",
    "Constant Declaration",
    "CONTINUE Statement",
    "COVERAGE Pragma",
    "Cursor FOR LOOP Statement",
    "Cursor Variable Declaration",
    "Datatype Attribute",
    "DEFAULT COLLATION Clause",
    "DELETE Statement Extension",
    "DEPRECATE Pragma",
    "DETERMINISTIC Clause",
    "Element Specification",
    "EXCEPTION_INIT Pragma",
    "Exception Declaration",
    "Exception Handler",
    "EXECUTE IMMEDIATE Statement",
    "EXIT Statement",
    "Explicit Cursor Declaration and Definition",
    "Expression",
    "FETCH Statement",
    "FOR LOOP Statement",
    "FORALL Statement",
    "Formal Parameter Declaration",
    "Function Declaration and Definition",
    "GOTO Statement",
    "IF Statement",
    "Implicit Cursor Attribute",
    "INLINE Pragma",
    "Invoker’s Rights and Definer’s Rights Clause",
    "INSERT Statement Extension",
    "Iterator",
    "Named Cursor Attribute",
    "NULL Statement",
    "OPEN Statement",
    "OPEN FOR Statement",
    "PARALLEL_ENABLE Clause",
    "PIPE ROW Statement",
    "PIPELINED Clause",
    "Procedure Declaration and Definition",
    "Qualified Expression",
    "RAISE Statement",
    "Record Variable Declaration",
    "RESETTABLE Clause",
    "RESTRICT_REFERENCES Pragma",
    "RETURN Statement",
    "RETURNING INTO Clause",
    "RESULT_CACHE Clause",
    "Scalar Variable Declaration",
    "SELECT INTO Statement",
    "SERIALLY_REUSABLE Pragma",
    "SHARING Clause",
    "SQL_MACRO Clause",
    "SUPPRESSES_WARNING_6009 Pragma",
    "UDF Pragma",
    "UPDATE Statement Extensions",
    "WHILE LOOP Statement",
    "%ROWTYPE Attribute",
    "%TYPE Attribute",
];

fn missing<'a>(
    required: &'a [&'static str],
    covered: &BTreeSet<&'static str>,
) -> Vec<&'a &'static str> {
    required
        .iter()
        .filter(|feature| !covered.contains(**feature))
        .collect()
}

#[test]
fn every_sql_statement_has_a_fixture() {
    let mut covered = covered_features(RELATIONAL_CASES);
    covered.extend(covered_features(PLSQL_CASES));
    covered.extend(covered_features(STATEMENT_CASES));
    let missing = missing(SQL_STATEMENTS, &covered);

    assert!(
        missing.is_empty(),
        "{} SQL statement families have no fixture:\n{}",
        missing.len(),
        missing
            .iter()
            .map(|feature| format!("- {feature}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn every_plsql_language_element_has_a_fixture() {
    let covered = covered_features(PLSQL_CASES);
    let missing = missing(PLSQL_LANGUAGE_ELEMENTS, &covered);

    assert!(
        missing.is_empty(),
        "{} PL/SQL language elements have no fixture:\n{}",
        missing.len(),
        missing
            .iter()
            .map(|feature| format!("- {feature}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn inventory_entries_are_unique() {
    let sql = SQL_STATEMENTS.iter().collect::<BTreeSet<_>>();
    let plsql = PLSQL_LANGUAGE_ELEMENTS.iter().collect::<BTreeSet<_>>();
    assert_eq!(sql.len(), SQL_STATEMENTS.len());
    assert_eq!(plsql.len(), PLSQL_LANGUAGE_ELEMENTS.len());
}
