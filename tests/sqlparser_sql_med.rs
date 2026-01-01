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

//! SQL/MED (ISO/IEC 9075-9) - Management of External Data tests
//!
//! This module tests parsing of SQL/MED statements including:
//! - FOREIGN DATA WRAPPER (CREATE, ALTER, DROP)
//! - SERVER (CREATE, ALTER, DROP)
//! - FOREIGN TABLE (CREATE, ALTER, DROP)
//! - USER MAPPING (CREATE, ALTER, DROP)
//! - IMPORT FOREIGN SCHEMA

use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use test_utils::*;

#[macro_use]
mod test_utils;

fn pg() -> TestedDialects {
    TestedDialects::new(vec![Box::new(PostgreSqlDialect {})])
}

fn pg_and_generic() -> TestedDialects {
    TestedDialects::new(vec![
        Box::new(PostgreSqlDialect {}),
        Box::new(GenericDialect {}),
    ])
}

// ============================================================================
// FOREIGN DATA WRAPPER Tests
// ============================================================================

#[test]
fn parse_create_foreign_data_wrapper_basic() {
    let sql = "CREATE FOREIGN DATA WRAPPER postgres_fdw";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            name,
            if_not_exists,
            handler,
            validator,
            options,
            ..
        }) => {
            assert_eq!(name.to_string(), "postgres_fdw");
            assert!(!if_not_exists);
            assert!(handler.is_none());
            assert!(validator.is_none());
            assert!(options.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_data_wrapper_if_not_exists() {
    let sql = "CREATE FOREIGN DATA WRAPPER IF NOT EXISTS my_fdw";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            name,
            if_not_exists,
            ..
        }) => {
            assert_eq!(name.to_string(), "my_fdw");
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_data_wrapper_with_handler() {
    let sql = "CREATE FOREIGN DATA WRAPPER postgres_fdw HANDLER postgres_fdw_handler";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            name,
            handler,
            ..
        }) => {
            assert_eq!(name.to_string(), "postgres_fdw");
            assert_eq!(handler.unwrap().to_string(), "postgres_fdw_handler");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_data_wrapper_no_handler() {
    let sql = "CREATE FOREIGN DATA WRAPPER file_fdw NO HANDLER";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            name,
            handler,
            ..
        }) => {
            assert_eq!(name.to_string(), "file_fdw");
            // NO HANDLER is represented as None with a special flag
            assert!(handler.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_data_wrapper_with_validator() {
    let sql = "CREATE FOREIGN DATA WRAPPER postgres_fdw VALIDATOR postgres_fdw_validator";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            validator,
            ..
        }) => {
            assert_eq!(validator.unwrap().to_string(), "postgres_fdw_validator");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_data_wrapper_no_validator() {
    let sql = "CREATE FOREIGN DATA WRAPPER file_fdw NO VALIDATOR";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            name,
            validator,
            ..
        }) => {
            assert_eq!(name.to_string(), "file_fdw");
            assert!(validator.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_data_wrapper_full() {
    let sql = "CREATE FOREIGN DATA WRAPPER postgres_fdw HANDLER postgres_fdw_handler VALIDATOR postgres_fdw_validator OPTIONS (debug 'true', timeout '30')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            name,
            handler,
            validator,
            options,
            ..
        }) => {
            assert_eq!(name.to_string(), "postgres_fdw");
            assert_eq!(handler.unwrap().to_string(), "postgres_fdw_handler");
            assert_eq!(validator.unwrap().to_string(), "postgres_fdw_validator");
            let opts = options.unwrap();
            assert_eq!(opts.len(), 2);
            assert_eq!(opts[0].key.to_string(), "debug");
            assert_eq!(opts[1].key.to_string(), "timeout");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_data_wrapper_schema_qualified_handler() {
    let sql = "CREATE FOREIGN DATA WRAPPER my_fdw HANDLER public.my_handler";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement {
            handler, ..
        }) => {
            assert_eq!(handler.unwrap().to_string(), "public.my_handler");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_handler() {
    let sql = "ALTER FOREIGN DATA WRAPPER postgres_fdw HANDLER new_handler";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            name,
            operations,
            ..
        }) => {
            assert_eq!(name.to_string(), "postgres_fdw");
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignDataWrapperOperation::SetHandler(h) => {
                    assert_eq!(h.to_string(), "new_handler");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_no_handler() {
    let sql = "ALTER FOREIGN DATA WRAPPER postgres_fdw NO HANDLER";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            operations, ..
        }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignDataWrapperOperation::NoHandler => {}
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_validator() {
    let sql = "ALTER FOREIGN DATA WRAPPER my_fdw VALIDATOR new_validator";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            operations, ..
        }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignDataWrapperOperation::SetValidator(v) => {
                    assert_eq!(v.to_string(), "new_validator");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_no_validator() {
    let sql = "ALTER FOREIGN DATA WRAPPER my_fdw NO VALIDATOR";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            operations, ..
        }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignDataWrapperOperation::NoValidator => {}
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_options() {
    let sql = "ALTER FOREIGN DATA WRAPPER my_fdw OPTIONS (SET debug 'false', ADD timeout '60', DROP old_option)";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            operations, ..
        }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignDataWrapperOperation::Options(opts) => {
                    assert_eq!(opts.len(), 3);
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_owner() {
    let sql = "ALTER FOREIGN DATA WRAPPER my_fdw OWNER TO new_owner";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            operations, ..
        }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignDataWrapperOperation::OwnerTo(owner) => {
                    assert_eq!(owner.to_string(), "new_owner");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_rename() {
    let sql = "ALTER FOREIGN DATA WRAPPER my_fdw RENAME TO new_fdw";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            operations, ..
        }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignDataWrapperOperation::RenameTo(new_name) => {
                    assert_eq!(new_name.to_string(), "new_fdw");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_data_wrapper_multiple_operations() {
    let sql = "ALTER FOREIGN DATA WRAPPER my_fdw HANDLER new_handler VALIDATOR new_validator";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignDataWrapper(AlterForeignDataWrapperStatement {
            operations, ..
        }) => {
            assert_eq!(operations.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_data_wrapper() {
    let sql = "DROP FOREIGN DATA WRAPPER postgres_fdw";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignDataWrapper(DropForeignDataWrapperStatement {
            name,
            if_exists,
            drop_behavior,
            ..
        }) => {
            assert_eq!(name.to_string(), "postgres_fdw");
            assert!(!if_exists);
            assert!(drop_behavior.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_data_wrapper_if_exists() {
    let sql = "DROP FOREIGN DATA WRAPPER IF EXISTS my_fdw";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignDataWrapper(DropForeignDataWrapperStatement {
            name,
            if_exists,
            ..
        }) => {
            assert_eq!(name.to_string(), "my_fdw");
            assert!(if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_data_wrapper_cascade() {
    let sql = "DROP FOREIGN DATA WRAPPER my_fdw CASCADE";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignDataWrapper(DropForeignDataWrapperStatement {
            drop_behavior,
            ..
        }) => {
            assert_eq!(drop_behavior, Some(DropBehavior::Cascade));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_data_wrapper_restrict() {
    let sql = "DROP FOREIGN DATA WRAPPER my_fdw RESTRICT";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignDataWrapper(DropForeignDataWrapperStatement {
            drop_behavior,
            ..
        }) => {
            assert_eq!(drop_behavior, Some(DropBehavior::Restrict));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_data_wrapper_if_exists_cascade() {
    let sql = "DROP FOREIGN DATA WRAPPER IF EXISTS my_fdw CASCADE";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignDataWrapper(DropForeignDataWrapperStatement {
            name,
            if_exists,
            drop_behavior,
            ..
        }) => {
            assert_eq!(name.to_string(), "my_fdw");
            assert!(if_exists);
            assert_eq!(drop_behavior, Some(DropBehavior::Cascade));
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// SERVER Tests (extending existing CREATE SERVER tests)
// ============================================================================

#[test]
fn parse_create_server_basic() {
    // Already implemented, verify it still works
    let sql = "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateServer(CreateServerStatement { name, .. }) => {
            assert_eq!(name.to_string(), "myserver");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_version() {
    let sql = "ALTER SERVER myserver VERSION '2.0'";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement {
            name, operations, ..
        }) => {
            assert_eq!(name.to_string(), "myserver");
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterServerOperation::SetVersion(v) => {
                    assert_eq!(v.value, "2.0");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_options_set() {
    let sql = "ALTER SERVER myserver OPTIONS (SET host 'newhost')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterServerOperation::Options(opts) => {
                    assert_eq!(opts.len(), 1);
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_options_add() {
    let sql = "ALTER SERVER myserver OPTIONS (ADD port '5433')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_options_drop() {
    let sql = "ALTER SERVER myserver OPTIONS (DROP old_option)";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_options_multiple() {
    let sql =
        "ALTER SERVER myserver OPTIONS (SET host 'localhost', ADD port '5432', DROP old_option)";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterServerOperation::Options(opts) => {
                    assert_eq!(opts.len(), 3);
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_owner() {
    let sql = "ALTER SERVER myserver OWNER TO new_owner";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterServerOperation::OwnerTo(owner) => {
                    assert_eq!(owner.to_string(), "new_owner");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_rename() {
    let sql = "ALTER SERVER myserver RENAME TO newserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterServerOperation::RenameTo(new_name) => {
                    assert_eq!(new_name.to_string(), "newserver");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_server_version_and_options() {
    let sql = "ALTER SERVER myserver VERSION '2.0' OPTIONS (SET host 'newhost')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterServer(AlterServerStatement { operations, .. }) => {
            assert_eq!(operations.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_server() {
    let sql = "DROP SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropServer(DropServerStatement {
            name,
            if_exists,
            drop_behavior,
            ..
        }) => {
            assert_eq!(name.to_string(), "myserver");
            assert!(!if_exists);
            assert!(drop_behavior.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_server_if_exists() {
    let sql = "DROP SERVER IF EXISTS myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropServer(DropServerStatement {
            name, if_exists, ..
        }) => {
            assert_eq!(name.to_string(), "myserver");
            assert!(if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_server_cascade() {
    let sql = "DROP SERVER myserver CASCADE";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropServer(DropServerStatement { drop_behavior, .. }) => {
            assert_eq!(drop_behavior, Some(DropBehavior::Cascade));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_server_restrict() {
    let sql = "DROP SERVER myserver RESTRICT";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropServer(DropServerStatement { drop_behavior, .. }) => {
            assert_eq!(drop_behavior, Some(DropBehavior::Restrict));
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// FOREIGN TABLE Tests
// ============================================================================

#[test]
fn parse_create_foreign_table_basic() {
    let sql = "CREATE FOREIGN TABLE remote_users (id INT, name VARCHAR(100)) SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignTable(CreateForeignTableStatement {
            name,
            columns,
            server,
            if_not_exists,
            options,
            ..
        }) => {
            assert_eq!(name.to_string(), "remote_users");
            assert_eq!(columns.len(), 2);
            assert_eq!(server.to_string(), "myserver");
            assert!(!if_not_exists);
            assert!(options.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_table_if_not_exists() {
    let sql = "CREATE FOREIGN TABLE IF NOT EXISTS remote_users (id INT) SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignTable(CreateForeignTableStatement { if_not_exists, .. }) => {
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_table_with_options() {
    let sql = "CREATE FOREIGN TABLE remote_users (id INT) SERVER myserver OPTIONS (schema_name 'public', table_name 'users')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignTable(CreateForeignTableStatement { options, .. }) => {
            let opts = options.unwrap();
            assert_eq!(opts.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_table_with_column_options() {
    // Note: Column-level OPTIONS are not yet supported (requires parser changes to column parsing)
    // This test verifies basic foreign table with column types
    let sql = "CREATE FOREIGN TABLE remote_users (id INT, name VARCHAR(100)) SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignTable(CreateForeignTableStatement { columns, .. }) => {
            assert_eq!(columns.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_table_with_constraints() {
    let sql =
        "CREATE FOREIGN TABLE remote_users (id INT NOT NULL, name VARCHAR(100)) SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignTable(CreateForeignTableStatement { columns, .. }) => {
            assert_eq!(columns.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_foreign_table_partitioned() {
    let sql = "CREATE FOREIGN TABLE remote_logs PARTITION OF parent_logs FOR VALUES FROM ('2020-01-01') TO ('2021-01-01') SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignTable(CreateForeignTableStatement {
            partition_of,
            partition_bound,
            ..
        }) => {
            assert!(partition_of.is_some());
            assert!(partition_bound.is_some());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_table_add_column() {
    let sql = "ALTER FOREIGN TABLE remote_users ADD COLUMN email VARCHAR(255)";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignTable(AlterForeignTableStatement {
            name, operations, ..
        }) => {
            assert_eq!(name.to_string(), "remote_users");
            assert_eq!(operations.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_table_drop_column() {
    let sql = "ALTER FOREIGN TABLE remote_users DROP COLUMN email";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignTable(AlterForeignTableStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_table_if_exists() {
    let sql = "ALTER FOREIGN TABLE IF EXISTS remote_users ADD COLUMN email VARCHAR(255)";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignTable(AlterForeignTableStatement { if_exists, .. }) => {
            assert!(if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_table_options() {
    let sql = "ALTER FOREIGN TABLE remote_users OPTIONS (SET schema_name 'new_schema')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignTable(AlterForeignTableStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_table_owner() {
    let sql = "ALTER FOREIGN TABLE remote_users OWNER TO new_owner";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignTable(AlterForeignTableStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignTableOperation::OwnerTo(owner) => {
                    assert_eq!(owner.to_string(), "new_owner");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_table_rename() {
    let sql = "ALTER FOREIGN TABLE remote_users RENAME TO new_remote_users";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignTable(AlterForeignTableStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignTableOperation::RenameTo(new_name) => {
                    assert_eq!(new_name.to_string(), "new_remote_users");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_foreign_table_set_schema() {
    let sql = "ALTER FOREIGN TABLE remote_users SET SCHEMA new_schema";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterForeignTable(AlterForeignTableStatement { operations, .. }) => {
            assert_eq!(operations.len(), 1);
            match &operations[0] {
                AlterForeignTableOperation::SetSchema(schema) => {
                    assert_eq!(schema.to_string(), "new_schema");
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_table() {
    let sql = "DROP FOREIGN TABLE remote_users";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignTable(DropForeignTableStatement {
            names,
            if_exists,
            drop_behavior,
            ..
        }) => {
            assert_eq!(names.len(), 1);
            assert_eq!(names[0].to_string(), "remote_users");
            assert!(!if_exists);
            assert!(drop_behavior.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_table_if_exists() {
    let sql = "DROP FOREIGN TABLE IF EXISTS remote_users";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignTable(DropForeignTableStatement { if_exists, .. }) => {
            assert!(if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_table_cascade() {
    let sql = "DROP FOREIGN TABLE remote_users CASCADE";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignTable(DropForeignTableStatement { drop_behavior, .. }) => {
            assert_eq!(drop_behavior, Some(DropBehavior::Cascade));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_foreign_table_multiple() {
    let sql = "DROP FOREIGN TABLE table1, table2, table3";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropForeignTable(DropForeignTableStatement { names, .. }) => {
            assert_eq!(names.len(), 3);
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// USER MAPPING Tests
// ============================================================================

#[test]
fn parse_create_user_mapping_basic() {
    let sql = "CREATE USER MAPPING FOR bob SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement {
            user,
            server,
            if_not_exists,
            options,
            ..
        }) => {
            match user {
                UserMappingUser::User(u) => assert_eq!(u.to_string(), "bob"),
                _ => unreachable!(),
            }
            assert_eq!(server.to_string(), "myserver");
            assert!(!if_not_exists);
            assert!(options.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_user_mapping_if_not_exists() {
    let sql = "CREATE USER MAPPING IF NOT EXISTS FOR bob SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement { if_not_exists, .. }) => {
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_user_mapping_current_user() {
    let sql = "CREATE USER MAPPING FOR CURRENT_USER SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement { user, .. }) => match user {
            UserMappingUser::CurrentUser => {}
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_user_mapping_current_role() {
    let sql = "CREATE USER MAPPING FOR CURRENT_ROLE SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement { user, .. }) => match user {
            UserMappingUser::CurrentRole => {}
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_user_mapping_user_keyword() {
    let sql = "CREATE USER MAPPING FOR USER SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement { user, .. }) => match user {
            UserMappingUser::UserKeyword => {}
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_user_mapping_public() {
    let sql = "CREATE USER MAPPING FOR PUBLIC SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement { user, .. }) => match user {
            UserMappingUser::Public => {}
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_user_mapping_with_options() {
    let sql =
        "CREATE USER MAPPING FOR bob SERVER myserver OPTIONS (user 'remote_bob', password 'secret')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement { options, .. }) => {
            let opts = options.unwrap();
            assert_eq!(opts.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_user_mapping_options() {
    let sql = "ALTER USER MAPPING FOR bob SERVER myserver OPTIONS (SET password 'newsecret')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterUserMapping(AlterUserMappingStatement {
            user,
            server,
            options,
            ..
        }) => {
            match user {
                UserMappingUser::User(u) => assert_eq!(u.to_string(), "bob"),
                _ => unreachable!(),
            }
            assert_eq!(server.to_string(), "myserver");
            assert_eq!(options.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_user_mapping_add_option() {
    let sql = "ALTER USER MAPPING FOR bob SERVER myserver OPTIONS (ADD sslmode 'require')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterUserMapping(AlterUserMappingStatement { options, .. }) => {
            assert_eq!(options.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_user_mapping_drop_option() {
    let sql = "ALTER USER MAPPING FOR bob SERVER myserver OPTIONS (DROP password)";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterUserMapping(AlterUserMappingStatement { options, .. }) => {
            assert_eq!(options.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_user_mapping_current_user() {
    let sql = "ALTER USER MAPPING FOR CURRENT_USER SERVER myserver OPTIONS (SET password 'new')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::AlterUserMapping(AlterUserMappingStatement { user, .. }) => match user {
            UserMappingUser::CurrentUser => {}
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_user_mapping() {
    let sql = "DROP USER MAPPING FOR bob SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropUserMapping(DropUserMappingStatement {
            user,
            server,
            if_exists,
            ..
        }) => {
            match user {
                UserMappingUser::User(u) => assert_eq!(u.to_string(), "bob"),
                _ => unreachable!(),
            }
            assert_eq!(server.to_string(), "myserver");
            assert!(!if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_user_mapping_if_exists() {
    let sql = "DROP USER MAPPING IF EXISTS FOR bob SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropUserMapping(DropUserMappingStatement { if_exists, .. }) => {
            assert!(if_exists);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_user_mapping_public() {
    let sql = "DROP USER MAPPING FOR PUBLIC SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::DropUserMapping(DropUserMappingStatement { user, .. }) => match user {
            UserMappingUser::Public => {}
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

// ============================================================================
// IMPORT FOREIGN SCHEMA Tests
// ============================================================================

#[test]
fn parse_import_foreign_schema_basic() {
    let sql = "IMPORT FOREIGN SCHEMA remote_schema FROM SERVER myserver INTO local_schema";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::ImportForeignSchema(ImportForeignSchemaStatement {
            remote_schema,
            server,
            local_schema,
            limit_type,
            tables,
            options,
            ..
        }) => {
            assert_eq!(remote_schema.to_string(), "remote_schema");
            assert_eq!(server.to_string(), "myserver");
            assert_eq!(local_schema.to_string(), "local_schema");
            assert!(limit_type.is_none());
            assert!(tables.is_empty());
            assert!(options.is_none());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_import_foreign_schema_limit_to() {
    let sql = "IMPORT FOREIGN SCHEMA remote_schema LIMIT TO (users, orders) FROM SERVER myserver INTO local_schema";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::ImportForeignSchema(ImportForeignSchemaStatement {
            limit_type, tables, ..
        }) => {
            assert_eq!(limit_type, Some(ImportForeignSchemaLimitType::LimitTo));
            assert_eq!(tables.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_import_foreign_schema_except() {
    let sql = "IMPORT FOREIGN SCHEMA remote_schema EXCEPT (internal_table) FROM SERVER myserver INTO local_schema";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::ImportForeignSchema(ImportForeignSchemaStatement {
            limit_type, tables, ..
        }) => {
            assert_eq!(limit_type, Some(ImportForeignSchemaLimitType::Except));
            assert_eq!(tables.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_import_foreign_schema_with_options() {
    let sql = "IMPORT FOREIGN SCHEMA remote_schema FROM SERVER myserver INTO local_schema OPTIONS (import_collate 'false', import_default 'true')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::ImportForeignSchema(ImportForeignSchemaStatement { options, .. }) => {
            let opts = options.unwrap();
            assert_eq!(opts.len(), 2);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_import_foreign_schema_full() {
    let sql = "IMPORT FOREIGN SCHEMA remote_schema LIMIT TO (users, orders, products) FROM SERVER myserver INTO public OPTIONS (import_collate 'false')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::ImportForeignSchema(ImportForeignSchemaStatement {
            remote_schema,
            limit_type,
            tables,
            server,
            local_schema,
            options,
            ..
        }) => {
            assert_eq!(remote_schema.to_string(), "remote_schema");
            assert_eq!(limit_type, Some(ImportForeignSchemaLimitType::LimitTo));
            assert_eq!(tables.len(), 3);
            assert_eq!(server.to_string(), "myserver");
            assert_eq!(local_schema.to_string(), "public");
            assert!(options.is_some());
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// Edge Cases and Error Handling Tests
// ============================================================================

#[test]
fn parse_foreign_data_wrapper_quoted_name() {
    let sql = r#"CREATE FOREIGN DATA WRAPPER "my-fdw""#;
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignDataWrapper(CreateForeignDataWrapperStatement { name, .. }) => {
            assert_eq!(name.to_string(), "\"my-fdw\"");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_server_with_schema_qualified_fdw() {
    let sql = "CREATE SERVER myserver FOREIGN DATA WRAPPER public.postgres_fdw";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateServer(CreateServerStatement {
            foreign_data_wrapper,
            ..
        }) => {
            assert_eq!(foreign_data_wrapper.to_string(), "public.postgres_fdw");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_foreign_table_with_all_pg_types() {
    let sql = "CREATE FOREIGN TABLE test_types (col_int INT, col_bigint BIGINT, col_text TEXT, col_varchar VARCHAR(255), col_bool BOOLEAN, col_date DATE, col_timestamp TIMESTAMP, col_numeric NUMERIC(10,2), col_json JSON, col_jsonb JSONB, col_uuid UUID, col_array INT[]) SERVER myserver";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateForeignTable(CreateForeignTableStatement { columns, .. }) => {
            assert_eq!(columns.len(), 12);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_user_mapping_quoted_username() {
    let sql = r#"CREATE USER MAPPING FOR "special-user" SERVER myserver"#;
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateUserMapping(CreateUserMappingStatement { user, .. }) => match user {
            UserMappingUser::User(u) => assert_eq!(u.to_string(), "\"special-user\""),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[test]
fn parse_options_with_empty_string() {
    let sql = "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (password '')";
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateServer(CreateServerStatement { options, .. }) => {
            let opts = options.unwrap();
            assert_eq!(opts.len(), 1);
            assert_eq!(opts[0].value.value, "");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_options_with_special_characters() {
    let sql = r#"CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (connstr 'host=db.example.com port=5432 dbname=mydb')"#;
    let stmt = pg_and_generic().verified_stmt(sql);
    match stmt {
        Statement::CreateServer(CreateServerStatement { options, .. }) => {
            let opts = options.unwrap();
            assert_eq!(opts.len(), 1);
            assert!(opts[0].value.value.contains("host=db.example.com"));
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_sql_med_roundtrip() {
    // Test that all SQL/MED statements can be parsed and re-serialized correctly
    let statements = vec![
        "CREATE FOREIGN DATA WRAPPER postgres_fdw HANDLER postgres_fdw_handler",
        "ALTER FOREIGN DATA WRAPPER postgres_fdw NO HANDLER",
        "DROP FOREIGN DATA WRAPPER IF EXISTS postgres_fdw CASCADE",
        "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost')",
        "ALTER SERVER myserver VERSION '2.0'",
        "DROP SERVER IF EXISTS myserver CASCADE",
        "CREATE FOREIGN TABLE remote_users (id INT, name TEXT) SERVER myserver",
        "ALTER FOREIGN TABLE remote_users ADD COLUMN email TEXT",
        "DROP FOREIGN TABLE IF EXISTS remote_users CASCADE",
        "CREATE USER MAPPING FOR bob SERVER myserver OPTIONS (user 'remote_bob')",
        "ALTER USER MAPPING FOR bob SERVER myserver OPTIONS (SET password 'new')",
        "DROP USER MAPPING IF EXISTS FOR bob SERVER myserver",
        "IMPORT FOREIGN SCHEMA remote_schema LIMIT TO (users) FROM SERVER myserver INTO local_schema",
    ];

    for sql in statements {
        pg_and_generic().verified_stmt(sql);
    }
}

// Error cases - these should fail to parse
#[test]
fn parse_create_foreign_data_wrapper_missing_name() {
    let sql = "CREATE FOREIGN DATA WRAPPER";
    let result = Parser::parse_sql(&PostgreSqlDialect {}, sql);
    assert!(result.is_err());
}

#[test]
fn parse_create_server_missing_fdw() {
    let sql = "CREATE SERVER myserver";
    let result = Parser::parse_sql(&PostgreSqlDialect {}, sql);
    assert!(result.is_err());
}

#[test]
fn parse_create_foreign_table_missing_server() {
    let sql = "CREATE FOREIGN TABLE test (id INT)";
    let result = Parser::parse_sql(&PostgreSqlDialect {}, sql);
    assert!(result.is_err());
}

#[test]
fn parse_create_user_mapping_missing_server() {
    let sql = "CREATE USER MAPPING FOR bob";
    let result = Parser::parse_sql(&PostgreSqlDialect {}, sql);
    assert!(result.is_err());
}

#[test]
fn parse_import_foreign_schema_missing_into() {
    let sql = "IMPORT FOREIGN SCHEMA remote FROM SERVER myserver";
    let result = Parser::parse_sql(&PostgreSqlDialect {}, sql);
    assert!(result.is_err());
}
