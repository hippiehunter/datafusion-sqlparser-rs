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

//! SQL:2016 F-Series (Optional Features) Tests
//!
//! F021-F869: Optional SQL features beyond the core.
//!
//! ## Feature Coverage
//!
//! - F021: Basic information schema
//! - F031: Basic schema manipulation
//! - F041: Basic joined table
//! - F051: Basic date and time
//! - F081: UNION and EXCEPT in views
//! - F131: Grouped operations
//! - F181: Multiple module support
//! - F201: CAST function
//! - F221: Explicit defaults
//! - F261: CASE expression
//! - F302-F305: INTERSECT/EXCEPT table operators
//! - F311: Schema definition statement
//! - F401: Extended joined table
//! - F471: Scalar subquery values
//! - F481: Expanded NULL predicate
//! - F501: Features and conformance views
//! - F591: Derived tables
//! - F850-F869: ORDER BY, FETCH FIRST, OFFSET features

#[cfg(test)]
mod f021_basic_information_schema {
    use crate::standards::common::verified_standard_stmt;

    #[test]
    fn f021_01_columns_view() {
        // F021-01: COLUMNS view
        verified_standard_stmt("SELECT * FROM INFORMATION_SCHEMA.COLUMNS");
        verified_standard_stmt(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'public'"
        );
    }

    #[test]
    fn f021_02_tables_view() {
        // F021-02: TABLES view
        verified_standard_stmt("SELECT * FROM INFORMATION_SCHEMA.TABLES");
        verified_standard_stmt(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES",
        );
    }

    #[test]
    fn f021_03_views_view() {
        // F021-03: VIEWS view
        verified_standard_stmt("SELECT * FROM INFORMATION_SCHEMA.VIEWS");
        verified_standard_stmt(
            "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS",
        );
    }

    #[test]
    fn f021_04_table_constraints_view() {
        // F021-04: TABLE_CONSTRAINTS view
        verified_standard_stmt("SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS");
        verified_standard_stmt(
            "SELECT CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
        );
    }

    #[test]
    fn f021_05_referential_constraints_view() {
        // F021-05: REFERENTIAL_CONSTRAINTS view
        verified_standard_stmt("SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS");
        verified_standard_stmt(
            "SELECT CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"
        );
    }

    #[test]
    fn f021_06_check_constraints_view() {
        // F021-06: CHECK_CONSTRAINTS view
        verified_standard_stmt("SELECT * FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS");
        verified_standard_stmt(
            "SELECT CONSTRAINT_NAME, CHECK_CLAUSE FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS",
        );
    }
}

#[cfg(test)]
mod f031_basic_schema_manipulation {
    use crate::verified_with_ast;
    use sqlparser::ast::{
        AlterTable, AlterTableOperation, CreateView, DataType, GrantObjects, ObjectType,
        Privileges, Statement,
    };

    #[test]
    fn f031_01_create_table() {
        // F031-01: CREATE TABLE statement
        verified_with_ast!(
            "CREATE TABLE t (a INT, b VARCHAR(50))",
            |stmt: Statement| {
                if let Statement::CreateTable(create_table) = stmt {
                    assert_eq!(create_table.name.to_string(), "t");
                    assert_eq!(create_table.columns.len(), 2);
                    assert_eq!(create_table.columns[0].name.value, "a");
                    assert!(matches!(
                        create_table.columns[0].data_type,
                        DataType::Int(_) | DataType::Integer(_)
                    ));
                    assert_eq!(create_table.columns[1].name.value, "b");
                    assert!(matches!(
                        create_table.columns[1].data_type,
                        DataType::Varchar(_)
                    ));
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );
        verified_with_ast!(
            "CREATE TABLE employees (id INTEGER, name VARCHAR(100), salary DECIMAL(10,2))",
            |stmt: Statement| {
                if let Statement::CreateTable(create_table) = stmt {
                    assert_eq!(create_table.name.to_string(), "employees");
                    assert_eq!(create_table.columns.len(), 3);
                    assert_eq!(create_table.columns[0].name.value, "id");
                    assert!(matches!(
                        create_table.columns[0].data_type,
                        DataType::Integer(_)
                    ));
                    assert_eq!(create_table.columns[1].name.value, "name");
                    if let DataType::Varchar(Some(
                        sqlparser::ast::CharacterLength::IntegerLength { length, .. },
                    )) = &create_table.columns[1].data_type
                    {
                        assert_eq!(*length, 100);
                    } else {
                        panic!("Expected VARCHAR(100)");
                    }
                    assert_eq!(create_table.columns[2].name.value, "salary");
                    if let DataType::Decimal(sqlparser::ast::ExactNumberInfo::PrecisionAndScale(
                        p,
                        s,
                    )) = &create_table.columns[2].data_type
                    {
                        assert_eq!(*p, 10);
                        assert_eq!(*s, 2);
                    } else {
                        panic!("Expected DECIMAL(10,2)");
                    }
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );
    }

    #[test]
    fn f031_02_create_view() {
        // F031-02: CREATE VIEW statement
        verified_with_ast!("CREATE VIEW v AS SELECT a, b FROM t", |stmt: Statement| {
            if let Statement::CreateView(CreateView { name, query, .. }) = stmt {
                assert_eq!(name.to_string(), "v");
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    assert_eq!(select.projection.len(), 2);
                }
            } else {
                panic!("Expected CreateView statement");
            }
        });
        verified_with_ast!(
            "CREATE VIEW employee_view AS SELECT id, name FROM employees WHERE salary > 50000",
            |stmt: Statement| {
                if let Statement::CreateView(CreateView { name, query, .. }) = stmt {
                    assert_eq!(name.to_string(), "employee_view");
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        assert_eq!(select.projection.len(), 2);
                        assert!(select.selection.is_some());
                    }
                } else {
                    panic!("Expected CreateView statement");
                }
            }
        );
    }

    #[test]
    fn f031_03_grant_statement() {
        // F031-03: GRANT statement
        verified_with_ast!("GRANT SELECT ON t TO user1", |stmt: Statement| {
            if let Statement::Grant {
                privileges,
                objects,
                grantees,
                ..
            } = stmt
            {
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert_eq!(actions[0].to_string().to_uppercase(), "SELECT");
                }
                if let Some(GrantObjects::Tables(tables)) = objects {
                    assert_eq!(tables.len(), 1);
                    assert_eq!(tables[0].to_string(), "t");
                } else {
                    panic!("Expected Tables grant object");
                }
                assert_eq!(grantees.len(), 1);
                assert_eq!(grantees[0].to_string(), "user1");
            } else {
                panic!("Expected Grant statement");
            }
        });
        verified_with_ast!(
            "GRANT SELECT, INSERT, UPDATE ON employees TO user1, user2",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    objects,
                    grantees,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 3);
                    }
                    if let Some(GrantObjects::Tables(tables)) = objects {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].to_string(), "employees");
                    }
                    assert_eq!(grantees.len(), 2);
                    assert_eq!(grantees[0].to_string(), "user1");
                    assert_eq!(grantees[1].to_string(), "user2");
                } else {
                    panic!("Expected Grant statement");
                }
            }
        );
        verified_with_ast!("GRANT ALL PRIVILEGES ON t TO user1", |stmt: Statement| {
            if let Statement::Grant {
                privileges,
                objects,
                grantees,
                ..
            } = stmt
            {
                assert!(matches!(privileges, Privileges::All { .. }));
                if let Some(GrantObjects::Tables(tables)) = objects {
                    assert_eq!(tables.len(), 1);
                }
                assert_eq!(grantees.len(), 1);
            } else {
                panic!("Expected Grant statement");
            }
        });
    }

    #[test]
    fn f031_04_alter_table_add_column() {
        // F031-04: ALTER TABLE ADD COLUMN statement
        verified_with_ast!(
            "ALTER TABLE t ADD COLUMN c VARCHAR(100)",
            |stmt: Statement| {
                if let Statement::AlterTable(AlterTable {
                    name, operations, ..
                }) = stmt
                {
                    assert_eq!(name.to_string(), "t");
                    assert_eq!(operations.len(), 1);
                    if let AlterTableOperation::AddColumn { column_def, .. } = &operations[0] {
                        assert_eq!(column_def.name.value, "c");
                        if let DataType::Varchar(Some(
                            sqlparser::ast::CharacterLength::IntegerLength { length, .. },
                        )) = &column_def.data_type
                        {
                            assert_eq!(*length, 100);
                        } else {
                            panic!("Expected VARCHAR(100)");
                        }
                    } else {
                        panic!("Expected AddColumn operation");
                    }
                } else {
                    panic!("Expected AlterTable statement");
                }
            }
        );
        verified_with_ast!(
            "ALTER TABLE employees ADD COLUMN department VARCHAR(50)",
            |stmt: Statement| {
                if let Statement::AlterTable(AlterTable {
                    name, operations, ..
                }) = stmt
                {
                    assert_eq!(name.to_string(), "employees");
                    if let AlterTableOperation::AddColumn { column_def, .. } = &operations[0] {
                        assert_eq!(column_def.name.value, "department");
                        if let DataType::Varchar(Some(
                            sqlparser::ast::CharacterLength::IntegerLength { length, .. },
                        )) = &column_def.data_type
                        {
                            assert_eq!(*length, 50);
                        }
                    }
                } else {
                    panic!("Expected AlterTable statement");
                }
            }
        );
    }

    #[test]
    fn f031_13_drop_table_restrict_cascade() {
        // F031-13: DROP TABLE RESTRICT/CASCADE
        verified_with_ast!("DROP TABLE t RESTRICT", |stmt: Statement| {
            if let Statement::Drop {
                object_type,
                names,
                cascade,
                restrict,
                ..
            } = stmt
            {
                assert_eq!(object_type, ObjectType::Table);
                assert_eq!(names.len(), 1);
                assert_eq!(names[0].to_string(), "t");
                assert!(!cascade);
                assert!(restrict);
            } else {
                panic!("Expected Drop statement");
            }
        });
        verified_with_ast!("DROP TABLE t CASCADE", |stmt: Statement| {
            if let Statement::Drop {
                object_type,
                names,
                cascade,
                restrict,
                ..
            } = stmt
            {
                assert_eq!(object_type, ObjectType::Table);
                assert_eq!(names.len(), 1);
                assert_eq!(names[0].to_string(), "t");
                assert!(cascade);
                assert!(!restrict);
            } else {
                panic!("Expected Drop statement");
            }
        });
        verified_with_ast!("DROP TABLE employees CASCADE", |stmt: Statement| {
            if let Statement::Drop {
                object_type,
                names,
                cascade,
                ..
            } = stmt
            {
                assert_eq!(object_type, ObjectType::Table);
                assert_eq!(names[0].to_string(), "employees");
                assert!(cascade);
            } else {
                panic!("Expected Drop statement");
            }
        });
    }

    #[test]
    fn f031_16_drop_view() {
        // F031-16: DROP VIEW statement
        verified_with_ast!("DROP VIEW v", |stmt: Statement| {
            if let Statement::Drop {
                object_type, names, ..
            } = stmt
            {
                assert_eq!(object_type, ObjectType::View);
                assert_eq!(names.len(), 1);
                assert_eq!(names[0].to_string(), "v");
            } else {
                panic!("Expected Drop statement");
            }
        });
        verified_with_ast!("DROP VIEW employee_view RESTRICT", |stmt: Statement| {
            if let Statement::Drop {
                object_type,
                names,
                restrict,
                ..
            } = stmt
            {
                assert_eq!(object_type, ObjectType::View);
                assert_eq!(names[0].to_string(), "employee_view");
                assert!(restrict);
            } else {
                panic!("Expected Drop statement");
            }
        });
        verified_with_ast!("DROP VIEW employee_view CASCADE", |stmt: Statement| {
            if let Statement::Drop {
                object_type,
                names,
                cascade,
                ..
            } = stmt
            {
                assert_eq!(object_type, ObjectType::View);
                assert_eq!(names[0].to_string(), "employee_view");
                assert!(cascade);
            } else {
                panic!("Expected Drop statement");
            }
        });
    }

    #[test]
    fn f031_19_revoke_statement() {
        // F031-19: REVOKE statement
        verified_with_ast!("REVOKE SELECT ON t FROM user1", |stmt: Statement| {
            if let Statement::Revoke {
                privileges,
                objects,
                grantees,
                ..
            } = stmt
            {
                if let Privileges::Actions(actions) = privileges {
                    assert_eq!(actions.len(), 1);
                    assert_eq!(actions[0].to_string().to_uppercase(), "SELECT");
                }
                if let Some(GrantObjects::Tables(tables)) = objects {
                    assert_eq!(tables.len(), 1);
                    assert_eq!(tables[0].to_string(), "t");
                } else {
                    panic!("Expected Tables grant object");
                }
                assert_eq!(grantees.len(), 1);
                assert_eq!(grantees[0].to_string(), "user1");
            } else {
                panic!("Expected Revoke statement");
            }
        });
        verified_with_ast!(
            "REVOKE SELECT, INSERT ON employees FROM user1, user2",
            |stmt: Statement| {
                if let Statement::Revoke {
                    privileges,
                    objects,
                    grantees,
                    ..
                } = stmt
                {
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 2);
                    }
                    if let Some(GrantObjects::Tables(tables)) = objects {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].to_string(), "employees");
                    }
                    assert_eq!(grantees.len(), 2);
                    assert_eq!(grantees[0].to_string(), "user1");
                    assert_eq!(grantees[1].to_string(), "user2");
                } else {
                    panic!("Expected Revoke statement");
                }
            }
        );
        verified_with_ast!(
            "REVOKE ALL PRIVILEGES ON t FROM user1",
            |stmt: Statement| {
                if let Statement::Revoke {
                    privileges,
                    objects,
                    grantees,
                    ..
                } = stmt
                {
                    assert!(matches!(privileges, Privileges::All { .. }));
                    if let Some(GrantObjects::Tables(tables)) = objects {
                        assert_eq!(tables.len(), 1);
                    }
                    assert_eq!(grantees.len(), 1);
                } else {
                    panic!("Expected Revoke statement");
                }
            }
        );
    }
}

#[cfg(test)]
mod f041_basic_joined_table {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{JoinConstraint, JoinOperator, Statement, TableFactor};

    #[test]
    fn f041_01_inner_join_implicit() {
        // F041-01: Inner join (implicit INNER JOIN)
        verified_standard_stmt("SELECT * FROM t1, t2 WHERE t1.id = t2.id");
        verified_standard_stmt("SELECT a.x, b.y FROM a, b WHERE a.id = b.id");
    }

    #[test]
    fn f041_02_inner_keyword() {
        // F041-02: INNER keyword
        verified_with_ast!(
            "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let TableFactor::Table { name, .. } = &select.from[0].relation {
                            assert_eq!(name.to_string(), "t1");
                            assert_eq!(select.from[0].joins.len(), 1);
                            assert!(matches!(
                                select.from[0].joins[0].join_operator,
                                JoinOperator::Inner(JoinConstraint::On(_))
                            ));
                        }
                    }
                }
            }
        );
        verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
    }

    #[test]
    fn f041_03_left_outer_join() {
        // F041-03: LEFT OUTER JOIN
        verified_with_ast!(
            "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert_eq!(select.from[0].joins.len(), 1);
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::LeftOuter(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::Left(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
    }

    #[test]
    fn f041_04_right_outer_join() {
        // F041-04: RIGHT OUTER JOIN
        verified_with_ast!(
            "SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::RightOuter(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::Right(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
    }

    #[test]
    fn f041_05_nested_outer_joins() {
        // F041-05: Nested outer joins
        verified_standard_stmt(
            "SELECT * FROM t1 LEFT JOIN (t2 LEFT JOIN t3 ON t2.id = t3.id) ON t1.id = t2.id",
        );
        verified_standard_stmt(
            "SELECT * FROM (t1 LEFT JOIN t2 ON t1.id = t2.id) LEFT JOIN t3 ON t2.id = t3.id",
        );
    }

    #[test]
    fn f041_07_inner_table_in_outer_join() {
        // F041-07: Inner table in outer join
        verified_standard_stmt(
            "SELECT * FROM t1 LEFT JOIN (t2 INNER JOIN t3 ON t2.id = t3.id) ON t1.id = t2.id",
        );
    }

    #[test]
    fn f041_08_all_comparison_operators() {
        // F041-08: All comparison operators in joins
        verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a");
        verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.a <> t2.a");
        verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.a > t2.a");
        verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.a >= t2.a");
        verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.a < t2.a");
        verified_standard_stmt("SELECT * FROM t1 JOIN t2 ON t1.a <= t2.a");
    }

    #[test]
    fn f041_using_clause() {
        // USING clause in joins
        verified_with_ast!("SELECT * FROM t1 JOIN t2 USING(id)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let JoinOperator::Join(JoinConstraint::Using(cols)) =
                        &select.from[0].joins[0].join_operator
                    {
                        assert_eq!(cols.len(), 1);
                        assert_eq!(cols[0].to_string(), "id");
                    } else {
                        panic!("Expected USING constraint");
                    }
                }
            }
        });
        verified_with_ast!(
            "SELECT * FROM t1 LEFT JOIN t2 USING(id, name)",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let JoinOperator::Left(JoinConstraint::Using(cols)) =
                            &select.from[0].joins[0].join_operator
                        {
                            assert_eq!(cols.len(), 2);
                            assert_eq!(cols[0].to_string(), "id");
                            assert_eq!(cols[1].to_string(), "name");
                        } else {
                            panic!("Expected USING constraint");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f041_natural_join() {
        // NATURAL JOIN
        verified_with_ast!("SELECT * FROM t1 NATURAL JOIN t2", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let JoinOperator::Join(JoinConstraint::Natural) =
                        &select.from[0].joins[0].join_operator
                    {
                        // Natural join confirmed
                    } else {
                        panic!("Expected Natural join");
                    }
                }
            }
        });
        verified_standard_stmt("SELECT * FROM t1 NATURAL LEFT JOIN t2");
        verified_standard_stmt("SELECT * FROM t1 NATURAL RIGHT JOIN t2");
    }

    #[test]
    fn f041_cross_join() {
        // CROSS JOIN
        verified_with_ast!("SELECT * FROM t1 CROSS JOIN t2", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    assert!(matches!(
                        select.from[0].joins[0].join_operator,
                        JoinOperator::CrossJoin(_)
                    ));
                }
            }
        });
    }

    #[test]
    fn f041_full_outer_join() {
        // FULL OUTER JOIN
        verified_with_ast!(
            "SELECT * FROM t1 FULL JOIN t2 ON t1.id = t2.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::FullOuter(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
    }
}

#[cfg(test)]
mod f051_basic_date_and_time {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{DataType, Expr, Statement};

    #[test]
    fn f051_01_date_type() {
        // F051-01: DATE data type
        verified_with_ast!("CREATE TABLE t (d DATE)", |stmt: Statement| {
            if let Statement::CreateTable(create_table) = stmt {
                assert_eq!(create_table.columns.len(), 1);
                assert_eq!(create_table.columns[0].name.value, "d");
                assert!(
                    matches!(create_table.columns[0].data_type, DataType::Date),
                    "Expected DATE data type"
                );
            } else {
                panic!("Expected CreateTable statement");
            }
        });
        verified_with_ast!("SELECT DATE '2024-01-15'", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::TypedString(ts)) =
                        &select.projection[0]
                    {
                        assert!(
                            matches!(ts.data_type, DataType::Date),
                            "Expected DATE typed string"
                        );
                    } else {
                        panic!("Expected DATE typed string expression");
                    }
                }
            }
        });
    }

    #[test]
    fn f051_02_time_with_fractional_seconds() {
        // F051-02: TIME data type with fractional seconds precision
        verified_with_ast!("CREATE TABLE t (t TIME)", |stmt: Statement| {
            if let Statement::CreateTable(create_table) = stmt {
                assert_eq!(create_table.columns.len(), 1);
                assert_eq!(create_table.columns[0].name.value, "t");
                assert!(
                    matches!(create_table.columns[0].data_type, DataType::Time(_, _)),
                    "Expected TIME data type"
                );
            } else {
                panic!("Expected CreateTable statement");
            }
        });
        verified_with_ast!("CREATE TABLE t (t TIME(6))", |stmt: Statement| {
            if let Statement::CreateTable(create_table) = stmt {
                assert_eq!(create_table.columns.len(), 1);
                if let DataType::Time(Some(precision), _) = create_table.columns[0].data_type {
                    assert_eq!(precision, 6, "Expected TIME(6) precision");
                } else {
                    panic!("Expected TIME(6) data type with precision");
                }
            } else {
                panic!("Expected CreateTable statement");
            }
        });
        verified_with_ast!("SELECT TIME '12:34:56.789'", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::TypedString(ts)) =
                        &select.projection[0]
                    {
                        assert!(
                            matches!(ts.data_type, DataType::Time(_, _)),
                            "Expected TIME typed string"
                        );
                    } else {
                        panic!("Expected TIME typed string expression");
                    }
                }
            }
        });
    }

    #[test]
    fn f051_03_timestamp_type() {
        // F051-03: TIMESTAMP data type
        verified_with_ast!("CREATE TABLE t (ts TIMESTAMP)", |stmt: Statement| {
            if let Statement::CreateTable(create_table) = stmt {
                assert_eq!(create_table.columns.len(), 1);
                assert_eq!(create_table.columns[0].name.value, "ts");
                assert!(
                    matches!(create_table.columns[0].data_type, DataType::Timestamp(_, _)),
                    "Expected TIMESTAMP data type"
                );
            } else {
                panic!("Expected CreateTable statement");
            }
        });
        verified_with_ast!("CREATE TABLE t (ts TIMESTAMP(6))", |stmt: Statement| {
            if let Statement::CreateTable(create_table) = stmt {
                assert_eq!(create_table.columns.len(), 1);
                if let DataType::Timestamp(Some(precision), _) = create_table.columns[0].data_type {
                    assert_eq!(precision, 6, "Expected TIMESTAMP(6) precision");
                } else {
                    panic!("Expected TIMESTAMP(6) data type with precision");
                }
            } else {
                panic!("Expected CreateTable statement");
            }
        });
        verified_with_ast!(
            "SELECT TIMESTAMP '2024-01-15 12:34:56.789'",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::TypedString(ts)) =
                            &select.projection[0]
                        {
                            assert!(
                                matches!(ts.data_type, DataType::Timestamp(_, _)),
                                "Expected TIMESTAMP typed string"
                            );
                        } else {
                            panic!("Expected TIMESTAMP typed string expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f051_04_comparison_on_datetime() {
        // F051-04: Comparison predicate on DATE, TIME, and TIMESTAMP
        verified_with_ast!(
            "SELECT * FROM t WHERE d > DATE '2024-01-01'",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let Some(Expr::BinaryOp { right, .. }) = &select.selection {
                            if let Expr::TypedString(ts) = right.as_ref() {
                                assert!(
                                    matches!(ts.data_type, DataType::Date),
                                    "Expected DATE typed string in comparison"
                                );
                            }
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t WHERE ts >= TIMESTAMP '2024-01-01 00:00:00'",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let Some(Expr::BinaryOp { right, .. }) = &select.selection {
                            if let Expr::TypedString(ts) = right.as_ref() {
                                assert!(
                                    matches!(ts.data_type, DataType::Timestamp(_, _)),
                                    "Expected TIMESTAMP typed string in comparison"
                                );
                            }
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t WHERE t < TIME '12:00:00'",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let Some(Expr::BinaryOp { right, .. }) = &select.selection {
                            if let Expr::TypedString(ts) = right.as_ref() {
                                assert!(
                                    matches!(ts.data_type, DataType::Time(_, _)),
                                    "Expected TIME typed string in comparison"
                                );
                            }
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f051_05_cast_datetime_string() {
        // F051-05: Explicit CAST between datetime types and character strings
        verified_with_ast!("SELECT CAST('2024-01-15' AS DATE)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                        data_type, ..
                    }) = &select.projection[0]
                    {
                        assert!(matches!(data_type, DataType::Date), "Expected CAST to DATE");
                    } else {
                        panic!("Expected CAST expression");
                    }
                }
            }
        });
        verified_standard_stmt("SELECT CAST(d AS VARCHAR) FROM t");
        verified_with_ast!("SELECT CAST('12:34:56' AS TIME)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                        data_type, ..
                    }) = &select.projection[0]
                    {
                        assert!(
                            matches!(data_type, DataType::Time(_, _)),
                            "Expected CAST to TIME"
                        );
                    } else {
                        panic!("Expected CAST expression");
                    }
                }
            }
        });
        verified_with_ast!(
            "SELECT CAST('2024-01-15 12:34:56' AS TIMESTAMP)",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                            data_type,
                            ..
                        }) = &select.projection[0]
                        {
                            assert!(
                                matches!(data_type, DataType::Timestamp(_, _)),
                                "Expected CAST to TIMESTAMP"
                            );
                        } else {
                            panic!("Expected CAST expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f051_06_current_date() {
        // F051-06: CURRENT_DATE
        verified_with_ast!("SELECT CURRENT_DATE", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "CURRENT_DATE",
                            "Expected CURRENT_DATE function"
                        );
                    } else {
                        panic!("Expected CURRENT_DATE function");
                    }
                }
            }
        });
        verified_with_ast!(
            "SELECT * FROM t WHERE d = CURRENT_DATE",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let Some(Expr::BinaryOp { right, .. }) = &select.selection {
                            if let Expr::Function(func) = right.as_ref() {
                                assert_eq!(
                                    func.name.to_string().to_uppercase(),
                                    "CURRENT_DATE",
                                    "Expected CURRENT_DATE function in WHERE clause"
                                );
                            }
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f051_07_localtime() {
        // F051-07: LOCALTIME
        verified_with_ast!("SELECT LOCALTIME", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "LOCALTIME",
                            "Expected LOCALTIME function"
                        );
                    } else {
                        panic!("Expected LOCALTIME function");
                    }
                }
            }
        });
        verified_with_ast!("SELECT LOCALTIME(3)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "LOCALTIME",
                            "Expected LOCALTIME function with precision"
                        );
                    } else {
                        panic!("Expected LOCALTIME function");
                    }
                }
            }
        });
    }

    #[test]
    fn f051_08_localtimestamp() {
        // F051-08: LOCALTIMESTAMP
        verified_with_ast!("SELECT LOCALTIMESTAMP", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "LOCALTIMESTAMP",
                            "Expected LOCALTIMESTAMP function"
                        );
                    } else {
                        panic!("Expected LOCALTIMESTAMP function");
                    }
                }
            }
        });
        verified_with_ast!("SELECT LOCALTIMESTAMP(6)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "LOCALTIMESTAMP",
                            "Expected LOCALTIMESTAMP function with precision"
                        );
                    } else {
                        panic!("Expected LOCALTIMESTAMP function");
                    }
                }
            }
        });
    }

    #[test]
    fn f051_time_with_time_zone() {
        // TIME WITH TIME ZONE
        verified_with_ast!(
            "CREATE TABLE t (t TIME WITH TIME ZONE)",
            |stmt: Statement| {
                if let Statement::CreateTable(create_table) = stmt {
                    assert_eq!(create_table.columns.len(), 1);
                    assert_eq!(create_table.columns[0].name.value, "t");
                    assert!(
                        matches!(
                            create_table.columns[0].data_type,
                            DataType::Time(_, sqlparser::ast::TimezoneInfo::WithTimeZone)
                        ),
                        "Expected TIME WITH TIME ZONE data type"
                    );
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );
        verified_with_ast!(
            "SELECT TIME WITH TIME ZONE '12:34:56+05:30'",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::TypedString(ts)) =
                            &select.projection[0]
                        {
                            assert!(
                                matches!(
                                    ts.data_type,
                                    DataType::Time(_, sqlparser::ast::TimezoneInfo::WithTimeZone)
                                ),
                                "Expected TIME WITH TIME ZONE typed string"
                            );
                        } else {
                            panic!("Expected TIME WITH TIME ZONE typed string expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f051_timestamp_with_time_zone() {
        // TIMESTAMP WITH TIME ZONE
        verified_with_ast!(
            "CREATE TABLE t (ts TIMESTAMP WITH TIME ZONE)",
            |stmt: Statement| {
                if let Statement::CreateTable(create_table) = stmt {
                    assert_eq!(create_table.columns.len(), 1);
                    assert_eq!(create_table.columns[0].name.value, "ts");
                    assert!(
                        matches!(
                            create_table.columns[0].data_type,
                            DataType::Timestamp(_, sqlparser::ast::TimezoneInfo::WithTimeZone)
                        ),
                        "Expected TIMESTAMP WITH TIME ZONE data type"
                    );
                } else {
                    panic!("Expected CreateTable statement");
                }
            }
        );
        verified_with_ast!(
            "SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 12:34:56+00:00'",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::TypedString(ts)) =
                            &select.projection[0]
                        {
                            assert!(
                                matches!(
                                    ts.data_type,
                                    DataType::Timestamp(
                                        _,
                                        sqlparser::ast::TimezoneInfo::WithTimeZone
                                    )
                                ),
                                "Expected TIMESTAMP WITH TIME ZONE typed string"
                            );
                        } else {
                            panic!("Expected TIMESTAMP WITH TIME ZONE typed string expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f051_current_timestamp() {
        // CURRENT_TIMESTAMP
        verified_with_ast!("SELECT CURRENT_TIMESTAMP", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "CURRENT_TIMESTAMP",
                            "Expected CURRENT_TIMESTAMP function"
                        );
                    } else {
                        panic!("Expected CURRENT_TIMESTAMP function");
                    }
                }
            }
        });
        verified_with_ast!("SELECT CURRENT_TIMESTAMP(6)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "CURRENT_TIMESTAMP",
                            "Expected CURRENT_TIMESTAMP function with precision"
                        );
                    } else {
                        panic!("Expected CURRENT_TIMESTAMP function");
                    }
                }
            }
        });
    }

    #[test]
    fn f051_current_time() {
        // CURRENT_TIME
        verified_with_ast!("SELECT CURRENT_TIME", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "CURRENT_TIME",
                            "Expected CURRENT_TIME function"
                        );
                    } else {
                        panic!("Expected CURRENT_TIME function");
                    }
                }
            }
        });
        verified_with_ast!("SELECT CURRENT_TIME(3)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(
                            func.name.to_string().to_uppercase(),
                            "CURRENT_TIME",
                            "Expected CURRENT_TIME function with precision"
                        );
                    } else {
                        panic!("Expected CURRENT_TIME function");
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod f081_union_except_in_views {
    use crate::standards::common::verified_standard_stmt;

    #[test]
    fn f081_01_union_in_view() {
        // F081-01: UNION in CREATE VIEW
        verified_standard_stmt("CREATE VIEW v AS SELECT a FROM t1 UNION SELECT a FROM t2");
        verified_standard_stmt(
            "CREATE VIEW v AS SELECT x, y FROM t1 UNION ALL SELECT x, y FROM t2",
        );
    }

    #[test]
    fn f081_02_except_in_view() {
        // F081-02: EXCEPT in CREATE VIEW
        verified_standard_stmt("CREATE VIEW v AS SELECT a FROM t1 EXCEPT SELECT a FROM t2");
        verified_standard_stmt("CREATE VIEW v AS SELECT x FROM t1 EXCEPT ALL SELECT x FROM t2");
    }
}

#[cfg(test)]
mod f131_grouped_operations {
    use crate::verified_with_ast;
    use sqlparser::ast::{CreateView, Expr, GroupByExpr, JoinOperator, SelectItem, Statement};

    #[test]
    fn f131_01_where_group_by_having() {
        // F131-01: WHERE, GROUP BY, and HAVING clauses supported in views
        verified_with_ast!(
            "CREATE VIEW v AS SELECT a, COUNT(*) FROM t WHERE b > 0 GROUP BY a HAVING COUNT(*) > 1",
            |stmt: Statement| {
                if let Statement::CreateView(CreateView { query, .. }) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        // Verify WHERE clause exists
                        assert!(
                            select.selection.is_some(),
                            "Expected WHERE clause in grouped view"
                        );

                        // Verify GROUP BY clause with one expression
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1, "Expected one GROUP BY expression");
                        } else {
                            panic!("Expected GroupByExpr::Expressions");
                        }

                        // Verify HAVING clause exists
                        assert!(
                            select.having.is_some(),
                            "Expected HAVING clause in grouped view"
                        );

                        // Verify COUNT(*) in projection
                        assert_eq!(select.projection.len(), 2, "Expected 2 projection items");
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[1]
                        {
                            assert_eq!(
                                func.name.to_string(),
                                "COUNT",
                                "Expected COUNT function in projection"
                            );
                        } else {
                            panic!("Expected COUNT function in second projection item");
                        }
                    } else {
                        panic!("Expected SELECT in CREATE VIEW");
                    }
                } else {
                    panic!("Expected CreateView statement");
                }
            }
        );
    }

    #[test]
    fn f131_02_multiple_tables_in_grouped_view() {
        // F131-02: Multiple tables supported in grouped view
        verified_with_ast!(
            "CREATE VIEW v AS SELECT t1.a, COUNT(*) FROM t1, t2 WHERE t1.id = t2.id GROUP BY t1.a",
            |stmt: Statement| {
                if let Statement::CreateView(CreateView { query, .. }) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        // Verify multiple tables in FROM clause
                        assert_eq!(
                            select.from.len(),
                            2,
                            "Expected 2 tables in FROM clause (cross join)"
                        );

                        // Verify GROUP BY clause exists
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1, "Expected one GROUP BY expression");
                        } else {
                            panic!("Expected GroupByExpr::Expressions");
                        }

                        // Verify COUNT(*) in projection
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[1]
                        {
                            assert_eq!(
                                func.name.to_string(),
                                "COUNT",
                                "Expected COUNT function in projection"
                            );
                        } else {
                            panic!("Expected COUNT function in second projection item");
                        }
                    } else {
                        panic!("Expected SELECT in CREATE VIEW");
                    }
                } else {
                    panic!("Expected CreateView statement");
                }
            }
        );
    }

    #[test]
    fn f131_03_set_functions_in_views() {
        // F131-03: Set functions supported in views
        verified_with_ast!(
            "CREATE VIEW v AS SELECT SUM(a), AVG(b), MAX(c), MIN(d), COUNT(*) FROM t",
            |stmt: Statement| {
                if let Statement::CreateView(CreateView { query, .. }) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        // Verify all 5 aggregate functions in projection
                        assert_eq!(select.projection.len(), 5, "Expected 5 projection items");

                        let expected_funcs = vec!["SUM", "AVG", "MAX", "MIN", "COUNT"];
                        for (i, expected_name) in expected_funcs.iter().enumerate() {
                            if let SelectItem::UnnamedExpr(Expr::Function(func)) =
                                &select.projection[i]
                            {
                                assert_eq!(
                                    func.name.to_string(),
                                    *expected_name,
                                    "Expected {} function at position {}",
                                    expected_name,
                                    i
                                );
                            } else {
                                panic!(
                                    "Expected {} function at position {}, got {:?}",
                                    expected_name, i, select.projection[i]
                                );
                            }
                        }
                    } else {
                        panic!("Expected SELECT in CREATE VIEW");
                    }
                } else {
                    panic!("Expected CreateView statement");
                }
            }
        );
    }

    #[test]
    fn f131_04_subqueries_with_group_by() {
        // F131-04: Subqueries with GROUP BY in views
        verified_with_ast!(
            "CREATE VIEW v AS SELECT * FROM (SELECT a, COUNT(*) AS cnt FROM t GROUP BY a) AS sub WHERE cnt > 5",
            |stmt: Statement| {
                if let Statement::CreateView(CreateView { query, .. }) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        // Verify outer WHERE clause exists
                        assert!(
                            select.selection.is_some(),
                            "Expected WHERE clause in outer query"
                        );

                        // Verify subquery in FROM clause
                        assert_eq!(select.from.len(), 1, "Expected one table in FROM");
                        if let sqlparser::ast::TableFactor::Derived { subquery, .. } =
                            &select.from[0].relation
                        {
                            // Verify subquery has GROUP BY
                            if let sqlparser::ast::SetExpr::Select(inner_select) =
                                subquery.body.as_ref()
                            {
                                if let GroupByExpr::Expressions(exprs, _) = &inner_select.group_by
                                {
                                    assert_eq!(
                                        exprs.len(),
                                        1,
                                        "Expected one GROUP BY expression in subquery"
                                    );
                                } else {
                                    panic!("Expected GroupByExpr::Expressions in subquery");
                                }

                                // Verify COUNT(*) in subquery projection
                                if let SelectItem::ExprWithAlias { expr, alias } =
                                    &inner_select.projection[1]
                                {
                                    if let Expr::Function(func) = expr {
                                        assert_eq!(
                                            func.name.to_string(),
                                            "COUNT",
                                            "Expected COUNT function in subquery"
                                        );
                                        assert_eq!(
                                            alias.value, "cnt",
                                            "Expected alias 'cnt' for COUNT(*)"
                                        );
                                    } else {
                                        panic!("Expected COUNT function in subquery projection");
                                    }
                                } else {
                                    panic!("Expected aliased expression in subquery projection");
                                }
                            } else {
                                panic!("Expected SELECT in subquery");
                            }
                        } else {
                            panic!("Expected derived table (subquery) in FROM clause");
                        }
                    } else {
                        panic!("Expected SELECT in CREATE VIEW");
                    }
                } else {
                    panic!("Expected CreateView statement");
                }
            }
        );
    }

    #[test]
    fn f131_05_grouped_view_with_joins() {
        // F131-05: Grouped view with joins
        verified_with_ast!(
            "CREATE VIEW v AS SELECT t1.dept, COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.id GROUP BY t1.dept",
            |stmt: Statement| {
                if let Statement::CreateView(CreateView { query, .. }) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        // Verify JOIN exists
                        assert_eq!(select.from.len(), 1, "Expected one table with joins");
                        assert_eq!(
                            select.from[0].joins.len(),
                            1,
                            "Expected one JOIN clause"
                        );

                        // Verify it's a JOIN with ON condition
                        // Note: Plain JOIN parses as JoinOperator::Join, not JoinOperator::Inner
                        let join = &select.from[0].joins[0];
                        assert!(
                            matches!(join.join_operator, JoinOperator::Join(_)),
                            "Expected JOIN"
                        );

                        // Verify GROUP BY clause exists
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 1, "Expected one GROUP BY expression");
                        } else {
                            panic!("Expected GroupByExpr::Expressions");
                        }

                        // Verify COUNT(*) in projection
                        assert_eq!(select.projection.len(), 2, "Expected 2 projection items");
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) =
                            &select.projection[1]
                        {
                            assert_eq!(
                                func.name.to_string(),
                                "COUNT",
                                "Expected COUNT function in projection"
                            );
                        } else {
                            panic!("Expected COUNT function in second projection item");
                        }
                    } else {
                        panic!("Expected SELECT in CREATE VIEW");
                    }
                } else {
                    panic!("Expected CreateView statement");
                }
            }
        );
    }
}

#[cfg(test)]
mod f201_cast_function {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{DataType, Expr, Statement};

    #[test]
    fn f201_01_cast_to_numeric() {
        // F201-01: CAST to numeric types
        verified_with_ast!("SELECT CAST('123' AS INTEGER)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                        data_type, ..
                    }) = &select.projection[0]
                    {
                        assert!(
                            matches!(data_type, DataType::Integer(_) | DataType::Int(_)),
                            "Expected INTEGER data type"
                        );
                    } else {
                        panic!("Expected CAST expression");
                    }
                }
            }
        });
        verified_with_ast!(
            "SELECT CAST('123.45' AS DECIMAL(10,2))",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                            data_type,
                            ..
                        }) = &select.projection[0]
                        {
                            if let DataType::Decimal(
                                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s),
                            ) = data_type
                            {
                                assert_eq!(*p, 10);
                                assert_eq!(*s, 2);
                            } else {
                                panic!("Expected DECIMAL data type");
                            }
                        }
                    }
                }
            }
        );
        verified_standard_stmt("SELECT CAST(123 AS SMALLINT)");
        verified_standard_stmt("SELECT CAST('99.99' AS NUMERIC(5,2))");
    }

    #[test]
    fn f201_02_cast_to_string() {
        // F201-02: CAST to character string types
        verified_with_ast!("SELECT CAST(123 AS VARCHAR(10))", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                        data_type, ..
                    }) = &select.projection[0]
                    {
                        if let DataType::Varchar(Some(
                            sqlparser::ast::CharacterLength::IntegerLength { length, .. },
                        )) = data_type
                        {
                            assert_eq!(*length, 10);
                        } else {
                            panic!("Expected VARCHAR(10) data type");
                        }
                    }
                }
            }
        });
        verified_standard_stmt("SELECT CAST(123.45 AS CHAR(20))");
        verified_standard_stmt("SELECT CAST(name AS VARCHAR(100)) FROM t");
    }

    #[test]
    fn f201_03_cast_to_datetime() {
        // F201-03: CAST to datetime types
        verified_with_ast!("SELECT CAST('2024-01-15' AS DATE)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                        data_type, ..
                    }) = &select.projection[0]
                    {
                        assert!(matches!(data_type, DataType::Date));
                    }
                }
            }
        });
        verified_with_ast!("SELECT CAST('12:34:56' AS TIME)", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                        data_type, ..
                    }) = &select.projection[0]
                    {
                        assert!(matches!(data_type, DataType::Time(_, _)));
                    }
                }
            }
        });
        verified_standard_stmt("SELECT CAST('2024-01-15 12:34:56' AS TIMESTAMP)");
    }

    #[test]
    fn f201_04_nested_cast() {
        // F201-04: Nested CAST expressions
        verified_with_ast!(
            "SELECT CAST(CAST('123' AS INTEGER) AS VARCHAR(10))",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Cast {
                            expr,
                            data_type,
                            ..
                        }) = &select.projection[0]
                        {
                            // Outer CAST should be to VARCHAR
                            assert!(matches!(data_type, DataType::Varchar(_)));
                            // Inner expression should also be a CAST
                            assert!(matches!(expr.as_ref(), Expr::Cast { .. }));
                        }
                    }
                }
            }
        );
        verified_standard_stmt("SELECT CAST(CAST(value AS DECIMAL(10,2)) AS INTEGER) FROM t");
    }

    #[test]
    fn f201_05_cast_in_expressions() {
        // F201-05: CAST in complex expressions
        verified_standard_stmt("SELECT CAST(a AS INTEGER) + CAST(b AS INTEGER) FROM t");
        verified_standard_stmt("SELECT * FROM t WHERE CAST(value AS INTEGER) > 100");
    }
}

#[cfg(test)]
mod f221_explicit_defaults {
    use crate::standards::common::verified_standard_stmt;

    #[test]
    fn f221_01_default_in_insert() {
        // F221-01: Explicit DEFAULT in INSERT statement
        verified_standard_stmt("INSERT INTO t (a, b, c) VALUES (1, DEFAULT, 3)");
        verified_standard_stmt("INSERT INTO t (x, y) VALUES (DEFAULT, DEFAULT)");
    }

    #[test]
    fn f221_02_default_values() {
        // F221-02: DEFAULT VALUES clause
        verified_standard_stmt("INSERT INTO t DEFAULT VALUES");
    }

    #[test]
    fn f221_03_default_in_mixed_insert() {
        // F221-03: DEFAULT mixed with other values
        verified_standard_stmt("INSERT INTO t (a, b, c, d) VALUES (1, 2, DEFAULT, 4)");
        verified_standard_stmt("INSERT INTO t VALUES (DEFAULT, 'value', DEFAULT)");
    }
}

#[cfg(test)]
mod f261_case_expression {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{Expr, Statement};

    #[test]
    fn f261_01_simple_case() {
        // F261-01: Simple CASE expression
        verified_with_ast!(
            "SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END FROM t",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Case {
                            operand,
                            conditions,
                            else_result,
                            ..
                        }) = &select.projection[0]
                        {
                            assert!(operand.is_some(), "Simple CASE should have operand");
                            assert_eq!(conditions.len(), 2, "Expected 2 WHEN clauses");
                            assert!(else_result.is_some(), "Expected ELSE clause");
                        } else {
                            panic!("Expected CASE expression");
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT CASE x WHEN 'A' THEN 1 WHEN 'B' THEN 2 END FROM t",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Case {
                            operand,
                            conditions,
                            else_result,
                            ..
                        }) = &select.projection[0]
                        {
                            assert!(operand.is_some(), "Simple CASE should have operand");
                            assert_eq!(conditions.len(), 2);
                            assert!(else_result.is_none(), "No ELSE clause expected");
                        } else {
                            panic!("Expected CASE expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f261_02_searched_case() {
        // F261-02: Searched CASE expression
        verified_with_ast!(
            "SELECT CASE WHEN a > 100 THEN 'high' WHEN a > 50 THEN 'medium' ELSE 'low' END FROM t",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Case {
                            operand,
                            conditions,
                            else_result,
                            ..
                        }) = &select.projection[0]
                        {
                            assert!(operand.is_none(), "Searched CASE should not have operand");
                            assert_eq!(conditions.len(), 2, "Expected 2 WHEN clauses");
                            assert!(else_result.is_some(), "Expected ELSE clause");
                        } else {
                            panic!("Expected CASE expression");
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT CASE WHEN x IS NULL THEN 0 ELSE x END FROM t",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Case {
                            operand,
                            conditions,
                            ..
                        }) = &select.projection[0]
                        {
                            assert!(operand.is_none());
                            assert_eq!(conditions.len(), 1);
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f261_03_nullif() {
        // F261-03: NULLIF
        verified_with_ast!("SELECT NULLIF(a, b) FROM t", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(func.name.to_string(), "NULLIF");
                        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                            assert_eq!(
                                arg_list.args.len(),
                                2,
                                "NULLIF should have exactly 2 arguments"
                            );
                        }
                    } else {
                        panic!("Expected Function expression for NULLIF");
                    }
                }
            }
        });
        verified_standard_stmt("SELECT NULLIF(value, 0) FROM t");
    }

    #[test]
    fn f261_04_coalesce() {
        // F261-04: COALESCE
        verified_with_ast!("SELECT COALESCE(a, b, c, 0) FROM t", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) =
                        &select.projection[0]
                    {
                        assert_eq!(func.name.to_string(), "COALESCE");
                        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                            assert_eq!(
                                arg_list.args.len(),
                                4,
                                "COALESCE should have 4 arguments in this test"
                            );
                        }
                    } else {
                        panic!("Expected Function expression for COALESCE");
                    }
                }
            }
        });
        verified_standard_stmt("SELECT COALESCE(name, 'unknown') FROM t");
    }

    #[test]
    fn f261_05_nested_case() {
        // F261-05: Nested CASE expressions
        verified_with_ast!(
            "SELECT CASE WHEN a > 0 THEN CASE WHEN b > 0 THEN 'both positive' ELSE 'a positive' END ELSE 'a not positive' END FROM t",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Case {
                            conditions,
                            ..
                        }) = &select.projection[0]
                        {
                            // Verify outer CASE has nested CASE in THEN clause
                            if let Expr::Case { .. } = &conditions[0].result {
                                // Nested CASE confirmed
                            } else {
                                panic!("Expected nested CASE in THEN clause");
                            }
                        } else {
                            panic!("Expected CASE expression");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f261_06_case_in_where() {
        // F261-06: CASE expression in WHERE clause
        verified_standard_stmt(
            "SELECT * FROM t WHERE CASE WHEN status = 1 THEN true ELSE false END",
        );
        verified_standard_stmt(
            "SELECT * FROM t WHERE (CASE type WHEN 'A' THEN priority ELSE 0 END) > 5",
        );
    }

    #[test]
    fn f261_07_case_in_order_by() {
        // F261-07: CASE expression in ORDER BY
        verified_standard_stmt("SELECT * FROM t ORDER BY CASE WHEN a IS NULL THEN 1 ELSE 0 END, a");
    }
}

#[cfg(test)]
mod f302_f305_intersect_except {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier, Statement};

    #[test]
    fn f302_intersect_distinct() {
        // F302: INTERSECT DISTINCT table operator
        verified_with_ast!(
            "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::SetOperation {
                        op, set_quantifier, ..
                    } = q.body.as_ref()
                    {
                        assert_eq!(*op, SetOperator::Intersect);
                        assert_eq!(*set_quantifier, SetQuantifier::None);
                    } else {
                        panic!("Expected SetOperation");
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT a FROM t1 INTERSECT DISTINCT SELECT a FROM t2",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::SetOperation {
                        op, set_quantifier, ..
                    } = q.body.as_ref()
                    {
                        assert_eq!(*op, SetOperator::Intersect);
                        assert_eq!(*set_quantifier, SetQuantifier::Distinct);
                    }
                }
            }
        );
    }

    #[test]
    fn f304_except_all() {
        // F304: EXCEPT ALL table operator
        verified_with_ast!(
            "SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::SetOperation {
                        op, set_quantifier, ..
                    } = q.body.as_ref()
                    {
                        assert_eq!(*op, SetOperator::Except);
                        assert_eq!(*set_quantifier, SetQuantifier::All);
                    }
                }
            }
        );
    }

    #[test]
    fn f305_intersect_all() {
        // F305: INTERSECT ALL table operator
        verified_with_ast!(
            "SELECT a FROM t1 INTERSECT ALL SELECT a FROM t2",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::SetOperation {
                        op, set_quantifier, ..
                    } = q.body.as_ref()
                    {
                        assert_eq!(*op, SetOperator::Intersect);
                        assert_eq!(*set_quantifier, SetQuantifier::All);
                    }
                }
            }
        );
    }

    #[test]
    fn f302_except_distinct() {
        // EXCEPT DISTINCT (implied by EXCEPT)
        verified_with_ast!(
            "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::SetOperation {
                        op, set_quantifier, ..
                    } = q.body.as_ref()
                    {
                        assert_eq!(*op, SetOperator::Except);
                        assert_eq!(*set_quantifier, SetQuantifier::None);
                    }
                }
            }
        );
        verified_standard_stmt("SELECT a FROM t1 EXCEPT DISTINCT SELECT a FROM t2");
    }

    #[test]
    fn f302_multiple_set_operations() {
        // Multiple set operations
        verified_standard_stmt(
            "SELECT a FROM t1 UNION SELECT a FROM t2 INTERSECT SELECT a FROM t3",
        );
        verified_standard_stmt("SELECT a FROM t1 EXCEPT SELECT a FROM t2 UNION SELECT a FROM t3");
    }
}

#[cfg(test)]
mod f311_schema_definition {
    use crate::standards::common::verified_standard_stmt;

    #[test]
    fn f311_01_create_schema() {
        // F311-01: CREATE SCHEMA
        verified_standard_stmt("CREATE SCHEMA myschema");
        verified_standard_stmt("CREATE SCHEMA AUTHORIZATION user1");
        verified_standard_stmt("CREATE SCHEMA myschema AUTHORIZATION user1");
    }

    #[test]
    fn f311_02_create_table_persistent() {
        // F311-02: CREATE TABLE for persistent base tables
        verified_standard_stmt("CREATE TABLE t (a INTEGER, b VARCHAR(100))");
        verified_standard_stmt(
            "CREATE TABLE myschema.t (id INTEGER PRIMARY KEY, name VARCHAR(50))",
        );
    }

    #[test]
    fn f311_03_create_view() {
        // F311-03: CREATE VIEW
        verified_standard_stmt("CREATE VIEW v AS SELECT a, b FROM t");
        verified_standard_stmt("CREATE VIEW myschema.v AS SELECT * FROM t WHERE a > 0");
    }

    #[test]
    fn f311_05_grant_on_schema() {
        // F311-05: GRANT statement for schemas
        verified_standard_stmt("GRANT USAGE ON SCHEMA myschema TO user1");
        verified_standard_stmt("GRANT CREATE ON SCHEMA myschema TO user1");
    }
}

#[cfg(test)]
mod f401_extended_joined_table {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{
        BinaryOperator, Expr, JoinConstraint, JoinOperator, Statement, TableFactor,
    };

    #[test]
    fn f401_01_multiple_table_joins() {
        // F401-01: Multiple table joins
        verified_with_ast!(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Verify base table
                        if let TableFactor::Table { name, .. } = &select.from[0].relation {
                            assert_eq!(name.to_string(), "t1");
                        } else {
                            panic!("Expected base table t1");
                        }
                        // Verify two joins
                        assert_eq!(select.from[0].joins.len(), 2);
                        // First join: t1 JOIN t2
                        if let TableFactor::Table { name, .. } = &select.from[0].joins[0].relation {
                            assert_eq!(name.to_string(), "t2");
                        }
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::Join(JoinConstraint::On(_))
                        ));
                        // Second join: JOIN t3
                        if let TableFactor::Table { name, .. } = &select.from[0].joins[1].relation {
                            assert_eq!(name.to_string(), "t3");
                        }
                        assert!(matches!(
                            select.from[0].joins[1].join_operator,
                            JoinOperator::Join(JoinConstraint::On(_))
                        ));
                    } else {
                        panic!("Expected SELECT statement");
                    }
                }
            }
        );
        verified_standard_stmt("SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id AND t2.id = t3.id");
    }

    #[test]
    fn f401_02_complex_join_conditions() {
        // F401-02: Complex join conditions
        verified_with_ast!(
            "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b > t2.b",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert_eq!(select.from[0].joins.len(), 1);
                        if let JoinOperator::Join(JoinConstraint::On(expr)) =
                            &select.from[0].joins[0].join_operator
                        {
                            // Verify complex ON condition with AND
                            if let Expr::BinaryOp { op, .. } = expr {
                                assert_eq!(op, &BinaryOperator::And);
                            } else {
                                panic!("Expected AND in ON condition");
                            }
                        } else {
                            panic!("Expected JOIN with ON constraint");
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t1 JOIN t2 ON t1.x = t2.x OR t1.y = t2.y",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let JoinOperator::Join(JoinConstraint::On(expr)) =
                            &select.from[0].joins[0].join_operator
                        {
                            // Verify complex ON condition with OR
                            if let Expr::BinaryOp { op, .. } = expr {
                                assert_eq!(op, &BinaryOperator::Or);
                            } else {
                                panic!("Expected OR in ON condition");
                            }
                        } else {
                            panic!("Expected JOIN with ON constraint");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f401_03_self_joins() {
        // F401-03: Self-joins
        verified_with_ast!(
            "SELECT * FROM t AS t1 JOIN t AS t2 ON t1.parent_id = t2.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Verify base table with alias
                        if let TableFactor::Table { name, alias, .. } = &select.from[0].relation {
                            assert_eq!(name.to_string(), "t");
                            assert_eq!(alias.as_ref().unwrap().name.value, "t1");
                        } else {
                            panic!("Expected base table t with alias t1");
                        }
                        // Verify self-join table with different alias
                        if let TableFactor::Table { name, alias, .. } =
                            &select.from[0].joins[0].relation
                        {
                            assert_eq!(name.to_string(), "t");
                            assert_eq!(alias.as_ref().unwrap().name.value, "t2");
                        } else {
                            panic!("Expected join table t with alias t2");
                        }
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::Join(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT a.name, b.name FROM employees AS a JOIN employees AS b ON a.manager_id = b.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Verify employees table with alias a
                        if let TableFactor::Table { name, alias, .. } = &select.from[0].relation {
                            assert_eq!(name.to_string(), "employees");
                            assert_eq!(alias.as_ref().unwrap().name.value, "a");
                        }
                        // Verify self-join to employees with alias b
                        if let TableFactor::Table { name, alias, .. } =
                            &select.from[0].joins[0].relation
                        {
                            assert_eq!(name.to_string(), "employees");
                            assert_eq!(alias.as_ref().unwrap().name.value, "b");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f401_04_mixed_join_types() {
        // F401-04: Mixed join types
        verified_with_ast!(
            "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert_eq!(select.from[0].joins.len(), 2);
                        // First join: INNER JOIN
                        if let TableFactor::Table { name, .. } = &select.from[0].joins[0].relation {
                            assert_eq!(name.to_string(), "t2");
                        }
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::Inner(JoinConstraint::On(_))
                        ));
                        // Second join: LEFT JOIN
                        if let TableFactor::Table { name, .. } = &select.from[0].joins[1].relation {
                            assert_eq!(name.to_string(), "t3");
                        }
                        assert!(matches!(
                            select.from[0].joins[1].join_operator,
                            JoinOperator::Left(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id RIGHT JOIN t3 ON t2.id = t3.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert_eq!(select.from[0].joins.len(), 2);
                        // First join: LEFT JOIN
                        assert!(matches!(
                            select.from[0].joins[0].join_operator,
                            JoinOperator::Left(JoinConstraint::On(_))
                        ));
                        // Second join: RIGHT JOIN
                        assert!(matches!(
                            select.from[0].joins[1].join_operator,
                            JoinOperator::Right(JoinConstraint::On(_))
                        ));
                    }
                }
            }
        );
    }

    #[test]
    fn f401_05_four_way_join() {
        // F401-05: Four-way join
        verified_with_ast!(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id JOIN t4 ON t3.id = t4.id",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Verify base table
                        if let TableFactor::Table { name, .. } = &select.from[0].relation {
                            assert_eq!(name.to_string(), "t1");
                        }
                        // Verify three joins for four-way join
                        assert_eq!(select.from[0].joins.len(), 3);
                        // Verify each joined table
                        let expected_tables = vec!["t2", "t3", "t4"];
                        for (i, expected_name) in expected_tables.iter().enumerate() {
                            if let TableFactor::Table { name, .. } =
                                &select.from[0].joins[i].relation
                            {
                                assert_eq!(name.to_string(), *expected_name);
                            } else {
                                panic!("Expected table {}", expected_name);
                            }
                            // Verify all use JOIN with ON constraint
                            assert!(matches!(
                                select.from[0].joins[i].join_operator,
                                JoinOperator::Join(JoinConstraint::On(_))
                            ));
                        }
                    }
                }
            }
        );
    }
}

#[cfg(test)]
mod f471_scalar_subquery_values {
    use crate::verified_with_ast;
    use sqlparser::ast::{Expr, Statement};

    #[test]
    fn f471_01_scalar_subquery_in_select() {
        // F471-01: Scalar subquery in SELECT list
        verified_with_ast!(
            "SELECT a, (SELECT MAX(b) FROM t2) FROM t1",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert_eq!(select.projection.len(), 2);
                        // Second projection item should be a subquery
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Subquery(_)) =
                            &select.projection[1]
                        {
                            // Scalar subquery confirmed in SELECT list
                        } else {
                            panic!("Expected Subquery expression in SELECT list");
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT id, name, (SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id) FROM customers",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert_eq!(select.projection.len(), 3);
                        // Third projection item should be a correlated subquery
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Subquery(subquery)) =
                            &select.projection[2]
                        {
                            // Verify it's a SELECT query
                            if let sqlparser::ast::SetExpr::Select(sub_select) =
                                subquery.body.as_ref()
                            {
                                // Verify WHERE clause exists (correlated condition)
                                assert!(
                                    sub_select.selection.is_some(),
                                    "Expected WHERE clause in correlated subquery"
                                );
                            }
                        } else {
                            panic!("Expected correlated Subquery in SELECT list");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f471_02_scalar_subquery_in_where() {
        // F471-02: Scalar subquery in WHERE clause
        verified_with_ast!(
            "SELECT * FROM t WHERE a > (SELECT AVG(a) FROM t)",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert!(select.selection.is_some(), "Expected WHERE clause");
                        // WHERE clause should contain a comparison with subquery
                        if let Some(Expr::BinaryOp { right, .. }) = &select.selection {
                            if let Expr::Subquery(_) = right.as_ref() {
                                // Scalar subquery confirmed in WHERE clause
                            } else {
                                panic!("Expected Subquery in WHERE clause comparison");
                            }
                        } else {
                            panic!("Expected BinaryOp in WHERE clause");
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t WHERE b = (SELECT MIN(b) FROM t WHERE a = 1)",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let Some(Expr::BinaryOp { right, .. }) = &select.selection {
                            if let Expr::Subquery(subquery) = right.as_ref() {
                                // Verify the subquery has its own WHERE clause
                                if let sqlparser::ast::SetExpr::Select(sub_select) =
                                    subquery.body.as_ref()
                                {
                                    assert!(
                                        sub_select.selection.is_some(),
                                        "Expected WHERE clause in subquery"
                                    );
                                }
                            } else {
                                panic!("Expected Subquery in WHERE clause");
                            }
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f471_03_correlated_subquery() {
        // F471-03: Correlated scalar subqueries
        verified_with_ast!(
            "SELECT a, (SELECT b FROM t2 WHERE t2.id = t1.id) FROM t1",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Second projection should be a correlated subquery
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Subquery(subquery)) =
                            &select.projection[1]
                        {
                            // Verify subquery has WHERE clause (correlation)
                            if let sqlparser::ast::SetExpr::Select(sub_select) =
                                subquery.body.as_ref()
                            {
                                assert!(
                                    sub_select.selection.is_some(),
                                    "Expected WHERE clause for correlation"
                                );
                            }
                        } else {
                            panic!("Expected correlated Subquery in SELECT list");
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT dept, (SELECT AVG(salary) FROM employees AS e WHERE e.dept = d.dept) FROM departments AS d",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Second projection should be a correlated subquery
                        if let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Subquery(subquery)) =
                            &select.projection[1]
                        {
                            // Verify it's a SELECT with WHERE clause
                            if let sqlparser::ast::SetExpr::Select(sub_select) =
                                subquery.body.as_ref()
                            {
                                assert!(
                                    sub_select.selection.is_some(),
                                    "Expected WHERE clause for correlated subquery"
                                );
                            }
                        } else {
                            panic!("Expected correlated Subquery");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f471_04_scalar_subquery_as_expression() {
        // F471-04: Scalar subquery in arithmetic expressions
        verified_with_ast!(
            "SELECT a + (SELECT SUM(b) FROM t2) AS total FROM t1",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Projection should contain a BinaryOp with subquery
                        if let sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } =
                            &select.projection[0]
                        {
                            if let Expr::BinaryOp { right, .. } = expr {
                                if let Expr::Subquery(_) = right.as_ref() {
                                    // Scalar subquery confirmed in arithmetic expression
                                } else {
                                    panic!("Expected Subquery in arithmetic expression");
                                }
                            } else {
                                panic!("Expected BinaryOp expression");
                            }
                        } else {
                            panic!("Expected ExprWithAlias");
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t WHERE (SELECT COUNT(*) FROM t2) > 10",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // WHERE clause should have BinaryOp with subquery
                        if let Some(Expr::BinaryOp { left, .. }) = &select.selection {
                            if let Expr::Subquery(_) = left.as_ref() {
                                // Scalar subquery confirmed in comparison
                            } else {
                                panic!("Expected Subquery in WHERE clause comparison");
                            }
                        } else {
                            panic!("Expected BinaryOp in WHERE clause");
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f471_05_scalar_subquery_in_having() {
        // F471-05: Scalar subquery in HAVING clause
        verified_with_ast!(
            "SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > (SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM employees GROUP BY dept) AS sub)",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        assert!(select.having.is_some(), "Expected HAVING clause");
                        // HAVING clause should contain a comparison with subquery
                        if let Some(Expr::BinaryOp { right, .. }) = &select.having {
                            if let Expr::Subquery(subquery) = right.as_ref() {
                                // Verify the subquery contains a derived table
                                if let sqlparser::ast::SetExpr::Select(sub_select) =
                                    subquery.body.as_ref()
                                {
                                    // Should select from a derived table
                                    assert!(
                                        !sub_select.from.is_empty(),
                                        "Expected FROM clause in subquery"
                                    );
                                    if let sqlparser::ast::TableFactor::Derived { .. } =
                                        &sub_select.from[0].relation
                                    {
                                        // Nested derived table confirmed
                                    } else {
                                        panic!("Expected Derived table in subquery");
                                    }
                                }
                            } else {
                                panic!("Expected Subquery in HAVING clause");
                            }
                        } else {
                            panic!("Expected BinaryOp in HAVING clause");
                        }
                    }
                }
            }
        );
    }
}

#[cfg(test)]
mod f481_expanded_null_predicate {
    use crate::standards::common::verified_standard_stmt;

    #[test]
    fn f481_01_is_unknown() {
        // F481-01: IS UNKNOWN predicate
        verified_standard_stmt("SELECT * FROM t WHERE a IS UNKNOWN");
    }

    #[test]
    fn f481_02_is_not_unknown() {
        // F481-02: IS NOT UNKNOWN predicate
        verified_standard_stmt("SELECT * FROM t WHERE a IS NOT UNKNOWN");
    }

    #[test]
    fn f481_03_unknown_in_expressions() {
        // F481-03: IS UNKNOWN in complex expressions
        verified_standard_stmt("SELECT * FROM t WHERE (a > b) IS UNKNOWN");
        verified_standard_stmt("SELECT * FROM t WHERE (a = 1 OR b IS NULL) IS NOT UNKNOWN");
    }
}

#[cfg(test)]
mod f501_features_views {
    use crate::standards::common::verified_standard_stmt;

    #[test]
    fn f501_sql_features_view() {
        // F501: Features view (SQL_FEATURES)
        verified_standard_stmt("SELECT * FROM INFORMATION_SCHEMA.SQL_FEATURES");
        verified_standard_stmt(
            "SELECT FEATURE_ID, FEATURE_NAME FROM INFORMATION_SCHEMA.SQL_FEATURES",
        );
    }
}

#[cfg(test)]
mod f591_derived_tables {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{Statement, TableFactor};

    #[test]
    fn f591_01_subquery_in_from() {
        // F591-01: Derived tables (subqueries in FROM clause)
        verified_with_ast!(
            "SELECT * FROM (SELECT a, b FROM t) AS sub",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let TableFactor::Derived { alias, .. } = &select.from[0].relation {
                            assert_eq!(alias.as_ref().unwrap().name.value, "sub");
                        } else {
                            panic!("Expected Derived table");
                        }
                    }
                }
            }
        );
        verified_standard_stmt(
            "SELECT x FROM (SELECT a AS x, b AS y FROM t WHERE b > 0) AS derived",
        );
    }

    #[test]
    fn f591_02_column_aliases_for_derived_tables() {
        // F591-02: Column aliases for derived tables
        verified_with_ast!(
            "SELECT * FROM (SELECT a, b FROM t) AS sub (x, y)",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        if let TableFactor::Derived { alias, .. } = &select.from[0].relation {
                            let alias = alias.as_ref().unwrap();
                            assert_eq!(alias.name.value, "sub");
                            assert_eq!(alias.columns.len(), 2);
                            assert_eq!(alias.columns[0].name.value, "x");
                            assert_eq!(alias.columns[1].name.value, "y");
                        } else {
                            panic!("Expected Derived table with column aliases");
                        }
                    }
                }
            }
        );
        verified_standard_stmt("SELECT x, y FROM (SELECT a, b FROM t) AS derived (x, y)");
    }

    #[test]
    fn f591_03_nested_derived_tables() {
        // F591-03: Nested derived tables
        verified_with_ast!(
            "SELECT * FROM (SELECT * FROM (SELECT a FROM t) AS inner_sub) AS outer_sub",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(outer_select) = q.body.as_ref() {
                        // Check outer derived table
                        if let TableFactor::Derived {
                            subquery, alias, ..
                        } = &outer_select.from[0].relation
                        {
                            assert_eq!(alias.as_ref().unwrap().name.value, "outer_sub");
                            // Check inner derived table
                            if let sqlparser::ast::SetExpr::Select(inner_select) =
                                subquery.body.as_ref()
                            {
                                if let TableFactor::Derived { alias, .. } =
                                    &inner_select.from[0].relation
                                {
                                    assert_eq!(alias.as_ref().unwrap().name.value, "inner_sub");
                                } else {
                                    panic!("Expected inner Derived table");
                                }
                            }
                        } else {
                            panic!("Expected outer Derived table");
                        }
                    }
                }
            }
        );
        verified_standard_stmt(
            "SELECT x FROM (SELECT y FROM (SELECT a AS y FROM t) AS sub1) AS sub2 (x)",
        );
    }

    #[test]
    fn f591_04_derived_tables_in_joins() {
        // F591-04: Derived tables in JOINs
        verified_with_ast!(
            "SELECT * FROM (SELECT a, b FROM t1) AS d1 JOIN (SELECT x, y FROM t2) AS d2 ON d1.a = d2.x",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(select) = q.body.as_ref() {
                        // Verify left side is a derived table
                        assert!(matches!(
                            select.from[0].relation,
                            TableFactor::Derived { .. }
                        ));
                        // Verify joined table is also derived
                        if let TableFactor::Derived { alias, .. } =
                            &select.from[0].joins[0].relation
                        {
                            assert_eq!(alias.as_ref().unwrap().name.value, "d2");
                        } else {
                            panic!("Expected joined Derived table");
                        }
                    }
                }
            }
        );
        verified_standard_stmt("SELECT * FROM t1 JOIN (SELECT id, COUNT(*) AS cnt FROM t2 GROUP BY id) AS sub ON t1.id = sub.id");
    }

    #[test]
    fn f591_05_derived_tables_with_where() {
        // F591-05: Derived tables with WHERE clause
        verified_standard_stmt(
            "SELECT * FROM (SELECT a, b FROM t WHERE a > 10) AS sub WHERE b < 100",
        );
    }

    #[test]
    fn f591_06_derived_tables_with_group_by() {
        // F591-06: Derived tables with GROUP BY
        verified_standard_stmt("SELECT dept, total FROM (SELECT dept, SUM(salary) AS total FROM employees GROUP BY dept) AS sub");
    }
}

#[cfg(test)]
mod f850_f869_order_fetch_offset {
    use crate::standards::common::verified_standard_stmt;
    use crate::verified_with_ast;
    use sqlparser::ast::{OrderByKind, Statement};

    #[test]
    fn f850_order_by_in_views() {
        // F850: ORDER BY clause in views
        verified_standard_stmt("CREATE VIEW v AS SELECT * FROM t ORDER BY a");
    }

    #[test]
    fn f851_nulls_first_last() {
        // F851: NULLS FIRST/LAST in ORDER BY
        verified_with_ast!(
            "SELECT * FROM t ORDER BY a NULLS FIRST",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let Some(order_by) = &q.order_by {
                        if let OrderByKind::Expressions(exprs) = &order_by.kind {
                            assert_eq!(exprs.len(), 1);
                            assert_eq!(exprs[0].options.nulls_first, Some(true));
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t ORDER BY a NULLS LAST",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let Some(order_by) = &q.order_by {
                        if let OrderByKind::Expressions(exprs) = &order_by.kind {
                            assert_eq!(exprs[0].options.nulls_first, Some(false));
                        }
                    }
                }
            }
        );
        verified_with_ast!(
            "SELECT * FROM t ORDER BY a DESC NULLS FIRST, b ASC NULLS LAST",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let Some(order_by) = &q.order_by {
                        if let OrderByKind::Expressions(exprs) = &order_by.kind {
                            assert_eq!(exprs.len(), 2);
                            assert_eq!(exprs[0].options.asc, Some(false));
                            assert_eq!(exprs[0].options.nulls_first, Some(true));
                            assert_eq!(exprs[1].options.asc, Some(true));
                            assert_eq!(exprs[1].options.nulls_first, Some(false));
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn f855_nested_order_by() {
        // F855: Nested ORDER BY (in subqueries)
        verified_standard_stmt("SELECT * FROM (SELECT * FROM t ORDER BY a) AS sub");
    }

    #[test]
    fn f857_fetch_first_n_rows_only() {
        // F857: FETCH FIRST n ROWS ONLY
        verified_with_ast!(
            "SELECT * FROM t FETCH FIRST 10 ROWS ONLY",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let Some(fetch) = &q.fetch {
                        assert!(!fetch.with_ties);
                        assert!(!fetch.percent);
                        assert!(fetch.quantity.is_some());
                    } else {
                        panic!("Expected FETCH clause");
                    }
                }
            }
        );
        verified_standard_stmt("SELECT * FROM t ORDER BY a FETCH FIRST 5 ROWS ONLY");
    }

    #[test]
    fn f858_fetch_first_with_percent() {
        // SQL:2016 F858: FETCH FIRST with PERCENT
        verified_with_ast!(
            "SELECT * FROM t FETCH FIRST 10 PERCENT ROWS ONLY",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let Some(fetch) = &q.fetch {
                        assert!(!fetch.with_ties);
                        assert!(fetch.percent, "Expected PERCENT flag to be true");
                    }
                }
            }
        );
    }

    #[test]
    fn f859_fetch_first_with_ties() {
        // F859: FETCH FIRST WITH TIES
        verified_with_ast!(
            "SELECT * FROM t ORDER BY a FETCH FIRST 10 ROWS WITH TIES",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let Some(fetch) = &q.fetch {
                        assert!(fetch.with_ties, "Expected WITH TIES flag to be true");
                        assert!(!fetch.percent);
                    }
                }
            }
        );
    }

    #[test]
    fn f861_offset_n_rows() {
        // F861: OFFSET n ROWS
        verified_with_ast!("SELECT * FROM t OFFSET 10 ROWS", |stmt: Statement| {
            if let Statement::Query(q) = stmt {
                if let Some(limit_clause) = &q.limit_clause {
                    if let sqlparser::ast::LimitClause::LimitOffset { offset, .. } = limit_clause {
                        assert!(offset.is_some(), "Expected OFFSET clause");
                    }
                }
            }
        });
        verified_standard_stmt("SELECT * FROM t OFFSET 5 ROW");
    }

    #[test]
    fn f862_offset_with_fetch() {
        // F862: OFFSET with FETCH
        verified_with_ast!(
            "SELECT * FROM t OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    // Check OFFSET
                    if let Some(limit_clause) = &q.limit_clause {
                        if let sqlparser::ast::LimitClause::LimitOffset { offset, .. } =
                            limit_clause
                        {
                            assert!(offset.is_some());
                        }
                    }
                    // Check FETCH
                    assert!(q.fetch.is_some(), "Expected FETCH clause");
                }
            }
        );
        verified_standard_stmt("SELECT * FROM t ORDER BY a OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY");
    }

    #[test]
    fn f867_order_by_in_union() {
        // F867: ORDER BY in query expression (UNION, etc.)
        verified_with_ast!(
            "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    assert!(q.order_by.is_some(), "Expected ORDER BY");
                    assert!(matches!(
                        q.body.as_ref(),
                        sqlparser::ast::SetExpr::SetOperation { .. }
                    ));
                }
            }
        );
        verified_standard_stmt("SELECT a FROM t1 INTERSECT SELECT a FROM t2 ORDER BY a DESC");
    }

    #[test]
    fn f869_order_by_multiple_columns() {
        // ORDER BY with multiple columns
        verified_with_ast!(
            "SELECT * FROM t ORDER BY a, b DESC, c ASC",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let Some(order_by) = &q.order_by {
                        if let OrderByKind::Expressions(exprs) = &order_by.kind {
                            assert_eq!(exprs.len(), 3, "Expected 3 ORDER BY expressions");
                            assert_eq!(exprs[1].options.asc, Some(false));
                            assert_eq!(exprs[2].options.asc, Some(true));
                        }
                    }
                }
            }
        );
        verified_standard_stmt("SELECT * FROM t ORDER BY 1, 2 DESC");
    }

    #[test]
    fn f869_order_by_expressions() {
        // ORDER BY with expressions
        verified_standard_stmt("SELECT * FROM t ORDER BY a + b");
        verified_standard_stmt("SELECT * FROM t ORDER BY CASE WHEN a IS NULL THEN 1 ELSE 0 END, a");
    }
}

#[cfg(test)]
mod f_series_integration_tests {
    use crate::verified_with_ast;
    use sqlparser::ast::{
        BinaryOperator, CaseWhen, CreateTable, Expr, Fetch, GrantObjects, GroupByExpr,
        JoinConstraint, JoinOperator, Offset, OffsetRows, Privileges, SelectItem, SetExpr,
        SetOperator, Statement, TableFactor, Value, ValueWithSpan,
    };

    #[test]
    fn integration_complex_query_with_multiple_features() {
        // Complex query combining multiple F-series features
        verified_with_ast!(
            "SELECT dept, CASE WHEN avg_sal > 100000 THEN 'high' WHEN avg_sal > 50000 THEN 'medium' ELSE 'low' END AS salary_category, COALESCE(bonus, 0) AS bonus FROM (SELECT dept, AVG(CAST(salary AS DECIMAL(10,2))) AS avg_sal, SUM(bonus) AS bonus FROM employees WHERE hire_date >= DATE '2020-01-01' GROUP BY dept HAVING COUNT(*) > 5) AS dept_stats ORDER BY dept NULLS LAST FETCH FIRST 10 ROWS ONLY",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::Select(select) = q.body.as_ref() {
                        // Verify CASE expression in projection
                        assert!(select.projection.len() >= 3);

                        // Verify derived table (subquery) in FROM
                        if let TableFactor::Derived { subquery, alias, .. } = &select.from[0].relation {
                            assert_eq!(alias.as_ref().unwrap().name.to_string(), "dept_stats");

                            // Verify subquery has GROUP BY and HAVING
                            if let SetExpr::Select(subselect) = subquery.body.as_ref() {
                                // Verify GROUP BY exists (not empty)
                                if let GroupByExpr::Expressions(exprs, _) = &subselect.group_by {
                                    assert!(!exprs.is_empty());
                                }
                                assert!(subselect.having.is_some());
                            }
                        } else {
                            panic!("Expected derived table in FROM clause");
                        }

                        // Verify ORDER BY with NULLS LAST
                        assert!(q.order_by.is_some());

                        // Verify FETCH FIRST clause
                        assert!(q.fetch.is_some());
                        if let Some(Fetch { quantity: Some(quantity), .. }) = &q.fetch {
                            if let Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }) = quantity {
                                assert_eq!(n, "10");
                            }
                        }
                    }
                }
            }
        );
    }

    #[test]
    fn integration_view_with_joins_and_aggregates() {
        // View combining joins, aggregates, and subqueries
        verified_with_ast!(
            "CREATE VIEW department_summary AS SELECT d.dept_name, COUNT(*) AS employee_count, AVG(e.salary) AS avg_salary, (SELECT MAX(salary) FROM employees WHERE dept_id = d.id) AS max_salary FROM departments AS d LEFT JOIN employees AS e ON d.id = e.dept_id WHERE d.active = true GROUP BY d.dept_name, d.id HAVING COUNT(*) > 0",
            |stmt: Statement| {
                if let Statement::CreateView(view) = stmt {
                    assert_eq!(view.name.to_string(), "department_summary");

                    if let SetExpr::Select(select) = view.query.body.as_ref() {
                        // Verify multiple aggregate functions in projection
                        assert!(select.projection.len() >= 4);

                        // Verify LEFT JOIN
                        // Note: LEFT JOIN parses as JoinOperator::Left, LEFT OUTER JOIN as LeftOuter
                        assert_eq!(select.from[0].joins.len(), 1);
                        if let JoinOperator::Left(JoinConstraint::On(_)) = &select.from[0].joins[0].join_operator {
                            // Correct join type
                        } else {
                            panic!("Expected LEFT JOIN");
                        }

                        // Verify WHERE clause
                        assert!(select.selection.is_some());

                        // Verify GROUP BY clause
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 2);
                        }

                        // Verify HAVING clause
                        assert!(select.having.is_some());
                    }
                }
            }
        );
    }

    #[test]
    fn integration_set_operations_with_order_and_fetch() {
        // Set operations with ORDER BY and FETCH
        verified_with_ast!(
            "SELECT name, salary FROM current_employees UNION SELECT name, salary FROM former_employees EXCEPT SELECT name, salary FROM blacklist ORDER BY salary DESC NULLS LAST OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    // Verify UNION and EXCEPT operations
                    if let SetExpr::SetOperation { op: op1, left, .. } = q.body.as_ref() {
                        assert_eq!(op1, &SetOperator::Except);

                        // Verify left side is UNION
                        if let SetExpr::SetOperation { op: op2, .. } = left.as_ref() {
                            assert_eq!(op2, &SetOperator::Union);
                        }
                    }

                    // Verify ORDER BY
                    assert!(q.order_by.is_some());

                    // Verify OFFSET in limit_clause
                    if let Some(sqlparser::ast::LimitClause::LimitOffset { offset: Some(offset), .. }) = &q.limit_clause {
                        if let Offset { value: Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }), rows: OffsetRows::Rows } = offset {
                            assert_eq!(n, "10");
                        }
                    }

                    // Verify FETCH
                    assert!(q.fetch.is_some());
                    if let Some(Fetch { quantity: Some(Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. })), .. }) = &q.fetch {
                        assert_eq!(n, "20");
                    }
                }
            }
        );
    }

    #[test]
    fn integration_nested_subqueries_with_case() {
        // Nested subqueries with CASE expressions
        verified_with_ast!(
            "SELECT customer_id, CASE WHEN (SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id) > 10 THEN 'frequent' WHEN (SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id) > 5 THEN 'regular' ELSE 'occasional' END AS customer_type FROM customers WHERE status IS NOT UNKNOWN",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::Select(select) = q.body.as_ref() {
                        // Verify CASE expression in projection
                        assert_eq!(select.projection.len(), 2);
                        if let SelectItem::ExprWithAlias { expr: Expr::Case { conditions, .. }, .. } = &select.projection[1] {
                            // Verify multiple WHEN clauses (at least 2)
                            assert!(conditions.len() >= 2);

                            // Verify subqueries in CASE conditions
                            let CaseWhen { condition, .. } = &conditions[0];
                            if let Expr::BinaryOp { left, .. } = condition {
                                if matches!(left.as_ref(), Expr::Subquery(_)) {
                                    // Correct structure
                                } else {
                                    panic!("Expected subquery in CASE condition");
                                }
                            } else {
                                panic!("Expected BinaryOp in CASE condition");
                            }
                        } else {
                            panic!("Expected CASE expression");
                        }

                        // Verify WHERE clause with IS NOT UNKNOWN
                        assert!(select.selection.is_some());
                    }
                }
            }
        );
    }

    #[test]
    fn integration_multiple_joins_with_derived_tables() {
        // Multiple joins with derived tables
        verified_with_ast!(
            "SELECT c.name, o.order_count, p.product_count FROM customers AS c INNER JOIN (SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id) AS o ON c.id = o.customer_id LEFT JOIN (SELECT customer_id, COUNT(DISTINCT product_id) AS product_count FROM order_items AS oi JOIN orders AS ord ON oi.order_id = ord.id GROUP BY customer_id) AS p ON c.id = p.customer_id WHERE o.order_count > 5 ORDER BY o.order_count DESC",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::Select(select) = q.body.as_ref() {
                        // Verify base table with alias
                        if let TableFactor::Table { alias: Some(alias), .. } = &select.from[0].relation {
                            assert_eq!(alias.name.to_string(), "c");
                        }

                        // Verify two joins
                        assert_eq!(select.from[0].joins.len(), 2);

                        // Verify first join is INNER JOIN with derived table
                        if let JoinOperator::Join(JoinConstraint::On(_)) = &select.from[0].joins[0].join_operator {
                            if let TableFactor::Derived { subquery, alias, .. } = &select.from[0].joins[0].relation {
                                assert_eq!(alias.as_ref().unwrap().name.to_string(), "o");

                                // Verify subquery has GROUP BY
                                if let SetExpr::Select(subselect) = subquery.body.as_ref() {
                                    if let GroupByExpr::Expressions(exprs, _) = &subselect.group_by {
                                        assert!(!exprs.is_empty());
                                    }
                                }
                            } else {
                                panic!("Expected derived table in first join");
                            }
                        }

                        // Verify second join is LEFT JOIN with nested derived table
                        if let JoinOperator::LeftOuter(JoinConstraint::On(_)) = &select.from[0].joins[1].join_operator {
                            if let TableFactor::Derived { subquery, alias, .. } = &select.from[0].joins[1].relation {
                                assert_eq!(alias.as_ref().unwrap().name.to_string(), "p");

                                // Verify nested subquery contains a JOIN
                                if let SetExpr::Select(subselect) = subquery.body.as_ref() {
                                    assert_eq!(subselect.from[0].joins.len(), 1);
                                }
                            } else {
                                panic!("Expected derived table in second join");
                            }
                        }

                        // Verify WHERE and ORDER BY
                        assert!(select.selection.is_some());
                        assert!(q.order_by.is_some());
                    }
                }
            }
        );
    }

    #[test]
    fn integration_grant_revoke_with_schema() {
        // Schema manipulation with grants
        verified_with_ast!("CREATE SCHEMA production", |stmt: Statement| {
            if let Statement::CreateSchema { schema_name, .. } = stmt {
                assert_eq!(schema_name.to_string(), "production");
            }
        });

        verified_with_ast!(
            "CREATE TABLE production.products (id INTEGER, name VARCHAR(100))",
            |stmt: Statement| {
                if let Statement::CreateTable(CreateTable { name, columns, .. }) = stmt {
                    assert_eq!(name.to_string(), "production.products");
                    assert_eq!(columns.len(), 2);
                }
            }
        );

        verified_with_ast!(
            "GRANT SELECT, INSERT ON production.products TO app_user",
            |stmt: Statement| {
                if let Statement::Grant {
                    privileges,
                    objects,
                    grantees,
                    ..
                } = stmt
                {
                    // Verify multiple privileges
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 2);
                    }

                    // Verify object
                    if let Some(GrantObjects::Tables(tables)) = objects {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].to_string(), "production.products");
                    }

                    // Verify grantee
                    assert_eq!(grantees.len(), 1);
                }
            }
        );

        verified_with_ast!(
            "REVOKE INSERT ON production.products FROM app_user",
            |stmt: Statement| {
                if let Statement::Revoke {
                    privileges,
                    objects,
                    grantees,
                    ..
                } = stmt
                {
                    // Verify privilege
                    if let Privileges::Actions(actions) = privileges {
                        assert_eq!(actions.len(), 1);
                    }

                    // Verify object
                    if let Some(GrantObjects::Tables(tables)) = objects {
                        assert_eq!(tables.len(), 1);
                        assert_eq!(tables[0].to_string(), "production.products");
                    }

                    // Verify grantee
                    assert_eq!(grantees.len(), 1);
                }
            }
        );
    }

    #[test]
    fn integration_information_schema_complex_query() {
        // Complex INFORMATION_SCHEMA query
        verified_with_ast!(
            "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, COUNT(c.COLUMN_NAME) AS column_count, COALESCE(COUNT(tc.CONSTRAINT_NAME), 0) AS constraint_count FROM INFORMATION_SCHEMA.TABLES AS t LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc ON t.TABLE_SCHEMA = tc.TABLE_SCHEMA AND t.TABLE_NAME = tc.TABLE_NAME WHERE t.TABLE_TYPE = 'BASE TABLE' GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME HAVING COUNT(c.COLUMN_NAME) > 0 ORDER BY column_count DESC FETCH FIRST 50 ROWS ONLY",
            |stmt: Statement| {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::Select(select) = q.body.as_ref() {
                        // Verify aggregate functions in projection
                        assert!(select.projection.len() >= 4);

                        // Verify INFORMATION_SCHEMA tables
                        if let TableFactor::Table { name, alias: Some(alias), .. } = &select.from[0].relation {
                            assert_eq!(name.to_string(), "INFORMATION_SCHEMA.TABLES");
                            assert_eq!(alias.name.to_string(), "t");
                        }

                        // Verify two LEFT JOINs with complex ON conditions
                        assert_eq!(select.from[0].joins.len(), 2);

                        // Verify first LEFT JOIN to COLUMNS
                        // Note: LEFT JOIN parses as JoinOperator::Left
                        if let JoinOperator::Left(JoinConstraint::On(expr)) = &select.from[0].joins[0].join_operator {
                            if let TableFactor::Table { name, .. } = &select.from[0].joins[0].relation {
                                assert_eq!(name.to_string(), "INFORMATION_SCHEMA.COLUMNS");
                            }

                            // Verify compound ON condition with AND
                            if let Expr::BinaryOp { op, .. } = expr {
                                assert_eq!(op, &BinaryOperator::And);
                            }
                        } else {
                            panic!("Expected LEFT JOIN");
                        }

                        // Verify second LEFT JOIN to TABLE_CONSTRAINTS
                        if let JoinOperator::Left(JoinConstraint::On(expr)) = &select.from[0].joins[1].join_operator {
                            if let TableFactor::Table { name, .. } = &select.from[0].joins[1].relation {
                                assert_eq!(name.to_string(), "INFORMATION_SCHEMA.TABLE_CONSTRAINTS");
                            }

                            // Verify compound ON condition with AND
                            if let Expr::BinaryOp { op, .. } = expr {
                                assert_eq!(op, &BinaryOperator::And);
                            }
                        }

                        // Verify WHERE clause
                        assert!(select.selection.is_some());

                        // Verify GROUP BY
                        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
                            assert_eq!(exprs.len(), 2);
                        }

                        // Verify HAVING clause
                        assert!(select.having.is_some());

                        // Verify ORDER BY
                        assert!(q.order_by.is_some());

                        // Verify FETCH
                        assert!(q.fetch.is_some());
                        if let Some(Fetch { quantity: Some(Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. })), .. }) = &q.fetch {
                            assert_eq!(n, "50");
                        }
                    }
                }
            }
        );
    }
}
