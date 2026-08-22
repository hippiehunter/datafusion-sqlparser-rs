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

//! Tests for the PostgreSQL `ALTER <object>` statements that target neither a
//! table nor a foreign object.
//!
//! Reference: <https://www.postgresql.org/docs/current/sql-commands.html>

use sqlparser::ast::{
    AggregateArgs, AllInTablespaceObjectType, AlterCollationAction,
    AlterConfigurationOperation, AlterDatabaseOption, AlterDomainAction, AlterEventTriggerAction,
    AlterGroupAction, AlterIndexOperation, AlterMaterializedViewAction,
    AlterMaterializedViewOperation, AlterObjectAction, AlterObjectTarget, AlterOperatorAction,
    AlterPublicationAction, AlterRoutineAction, AlterSequenceOperation, AlterStatisticsAction,
    AlterSubscriptionAction, AlterTextSearchConfigurationAction, AlterTextSearchDictionaryAction,
    AlterTriggerAction, AlterTypeAction, AlterTypeOperation, AlterViewOperation, DataType,
    DatabaseOptionValue, DefinitionValue, DropBehavior, EventTriggerEnableMode, Expr,
    FunctionBehavior, FunctionCalledOnNull, FunctionParallel, Ident, ObjectName, Owner,
    ProcedureSecurity, ResetConfig, RoutineKind, RoutineOption, SetConfigValue, SqlOption,
    Statement, StatisticsTarget, TableConstraint,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::test_utils::TestedDialects;

fn pg() -> TestedDialects {
    TestedDialects::new(vec![Box::new(PostgreSqlDialect {})])
}

/// Parse `sql`, verifying that it round-trips through [`std::fmt::Display`],
/// and return the [`AlterObjectTarget`] it produced.
fn alter_object_target(sql: &str) -> AlterObjectTarget {
    match pg().verified_stmt(sql) {
        Statement::AlterObject(alter_object) => alter_object.target,
        other => panic!("Expected Statement::AlterObject, got {other:?}"),
    }
}

fn ident_owner(name: &str) -> Owner {
    Owner::Ident(name.into())
}

fn object_name(name: &str) -> ObjectName {
    ObjectName::from(vec![Ident::new(name)])
}

// =============================================================================
// ALTER AGGREGATE
// =============================================================================

#[test]
fn parse_alter_aggregate_rename() {
    // https://www.postgresql.org/docs/current/sql-alteraggregate.html
    match alter_object_target("ALTER AGGREGATE myagg(INT) RENAME TO youragg") {
        AlterObjectTarget::Aggregate {
            name,
            signature,
            action,
        } => {
            assert_eq!(name.to_string(), "myagg");
            match signature {
                AggregateArgs::Args(args) => {
                    assert_eq!(args.len(), 1);
                    assert_eq!(args[0].data_type, DataType::Int(None));
                    assert!(args[0].name.is_none());
                }
                other => panic!("Expected Args, got {other:?}"),
            }
            assert_eq!(
                action,
                AlterObjectAction::RenameTo {
                    new_name: "youragg".into()
                }
            );
        }
        other => panic!("Expected Aggregate, got {other:?}"),
    }
}

#[test]
fn parse_alter_aggregate_star_signature() {
    match alter_object_target("ALTER AGGREGATE mycount(*) OWNER TO CURRENT_USER") {
        AlterObjectTarget::Aggregate {
            signature, action, ..
        } => {
            assert_eq!(signature, AggregateArgs::Star);
            assert_eq!(
                action,
                AlterObjectAction::OwnerTo {
                    new_owner: Owner::CurrentUser
                }
            );
        }
        other => panic!("Expected Aggregate, got {other:?}"),
    }
}

#[test]
fn parse_alter_aggregate_ordered_set_signature() {
    match alter_object_target("ALTER AGGREGATE pct(FLOAT8 ORDER BY FLOAT8) SET SCHEMA s") {
        AlterObjectTarget::Aggregate { signature, .. } => match signature {
            AggregateArgs::OrderedSet {
                direct,
                ordered,
            } => {
                assert_eq!(direct.len(), 1);
                assert_eq!(ordered.len(), 1);
            }
            other => panic!("Expected OrderBy, got {other:?}"),
        },
        other => panic!("Expected Aggregate, got {other:?}"),
    }
}

#[test]
fn parse_alter_aggregate_hypothetical_set_signature() {
    pg().verified_stmt("ALTER AGGREGATE rank(ORDER BY INT) RENAME TO myrank");
}

#[test]
fn parse_alter_aggregate_named_and_variadic_args() {
    match alter_object_target("ALTER AGGREGATE myagg(VARIADIC arr ANYARRAY) RENAME TO youragg") {
        AlterObjectTarget::Aggregate { signature, .. } => match signature {
            AggregateArgs::Args(args) => {
                assert_eq!(args.len(), 1);
                assert_eq!(
                    args[0].name.as_ref().map(ToString::to_string),
                    Some("arr".to_string())
                );
            }
            other => panic!("Expected Args, got {other:?}"),
        },
        other => panic!("Expected Aggregate, got {other:?}"),
    }
}

// =============================================================================
// ALTER COLLATION / CONVERSION / LANGUAGE
// =============================================================================

#[test]
fn parse_alter_collation_refresh_version() {
    // https://www.postgresql.org/docs/current/sql-altercollation.html
    match alter_object_target("ALTER COLLATION c REFRESH VERSION") {
        AlterObjectTarget::Collation { name, action } => {
            assert_eq!(name.to_string(), "c");
            assert_eq!(action, AlterCollationAction::RefreshVersion);
        }
        other => panic!("Expected Collation, got {other:?}"),
    }
}

#[test]
fn parse_alter_collation_object_actions() {
    pg().verified_stmt("ALTER COLLATION c RENAME TO d");
    pg().verified_stmt("ALTER COLLATION s.c OWNER TO CURRENT_ROLE");
    match alter_object_target("ALTER COLLATION c SET SCHEMA s") {
        AlterObjectTarget::Collation { action, .. } => assert_eq!(
            action,
            AlterCollationAction::Object(AlterObjectAction::SetSchema {
                new_schema: object_name("s")
            })
        ),
        other => panic!("Expected Collation, got {other:?}"),
    }
}

#[test]
fn parse_alter_conversion() {
    // https://www.postgresql.org/docs/current/sql-alterconversion.html
    pg().verified_stmt("ALTER CONVERSION c RENAME TO d");
    pg().verified_stmt("ALTER CONVERSION c SET SCHEMA s");
    match alter_object_target("ALTER CONVERSION c OWNER TO u") {
        AlterObjectTarget::Conversion { name, action } => {
            assert_eq!(name.to_string(), "c");
            assert_eq!(
                action,
                AlterObjectAction::OwnerTo {
                    new_owner: ident_owner("u")
                }
            );
        }
        other => panic!("Expected Conversion, got {other:?}"),
    }
}

#[test]
fn parse_alter_language() {
    // https://www.postgresql.org/docs/current/sql-alterlanguage.html
    match alter_object_target("ALTER LANGUAGE plpgsql RENAME TO pl") {
        AlterObjectTarget::Language {
            procedural,
            name,
            action,
        } => {
            assert!(!procedural);
            assert_eq!(name.to_string(), "plpgsql");
            assert_eq!(
                action,
                AlterObjectAction::RenameTo {
                    new_name: "pl".into()
                }
            );
        }
        other => panic!("Expected Language, got {other:?}"),
    }
}

#[test]
fn parse_alter_procedural_language() {
    match alter_object_target("ALTER PROCEDURAL LANGUAGE plpgsql OWNER TO u") {
        AlterObjectTarget::Language { procedural, .. } => assert!(procedural),
        other => panic!("Expected Language, got {other:?}"),
    }
}

#[test]
fn alter_language_rejects_set_schema() {
    // PostgreSQL has no ALTER LANGUAGE ... SET SCHEMA form.
    assert!(pg()
        .parse_sql_statements("ALTER LANGUAGE plpgsql SET SCHEMA s")
        .is_err());
}

// =============================================================================
// ALTER STATISTICS
// =============================================================================

#[test]
fn parse_alter_statistics_set_statistics() {
    // https://www.postgresql.org/docs/current/sql-alterstatistics.html
    match alter_object_target("ALTER STATISTICS st SET STATISTICS 100") {
        AlterObjectTarget::Statistics {
            if_exists,
            name,
            action,
        } => {
            assert!(!if_exists);
            assert_eq!(name.to_string(), "st");
            match action {
                AlterStatisticsAction::SetStatistics {
                    target: StatisticsTarget::Value(value),
                } => assert_eq!(value.to_string(), "100"),
                other => panic!("Expected SetStatistics, got {other:?}"),
            }
        }
        other => panic!("Expected Statistics, got {other:?}"),
    }
}

#[test]
fn parse_alter_statistics_set_statistics_default() {
    match alter_object_target("ALTER STATISTICS st SET STATISTICS DEFAULT") {
        AlterObjectTarget::Statistics { action, .. } => assert_eq!(
            action,
            AlterStatisticsAction::SetStatistics {
                target: StatisticsTarget::Default
            }
        ),
        other => panic!("Expected Statistics, got {other:?}"),
    }
}

#[test]
fn parse_alter_statistics_if_exists_negative_target() {
    match alter_object_target("ALTER STATISTICS IF EXISTS st SET STATISTICS -1") {
        AlterObjectTarget::Statistics {
            if_exists, action, ..
        } => {
            assert!(if_exists);
            match action {
                AlterStatisticsAction::SetStatistics {
                    target: StatisticsTarget::Value(value),
                } => assert_eq!(value.to_string(), "-1"),
                other => panic!("Expected SetStatistics, got {other:?}"),
            }
        }
        other => panic!("Expected Statistics, got {other:?}"),
    }
}

#[test]
fn parse_alter_statistics_object_actions() {
    pg().verified_stmt("ALTER STATISTICS st RENAME TO st2");
    pg().verified_stmt("ALTER STATISTICS st OWNER TO u");
    pg().verified_stmt("ALTER STATISTICS st SET SCHEMA s");
}

// =============================================================================
// ALTER TEXT SEARCH ...
// =============================================================================

#[test]
fn parse_alter_text_search_configuration_add_mapping() {
    // https://www.postgresql.org/docs/current/sql-altertsconfig.html
    match alter_object_target(
        "ALTER TEXT SEARCH CONFIGURATION cfg ADD MAPPING FOR word, asciiword WITH simple, english_stem",
    ) {
        AlterObjectTarget::TextSearchConfiguration { name, action } => {
            assert_eq!(name.to_string(), "cfg");
            match action {
                AlterTextSearchConfigurationAction::AddMapping {
                    token_types,
                    dictionaries,
                } => {
                    assert_eq!(token_types.len(), 2);
                    assert_eq!(dictionaries.len(), 2);
                    assert_eq!(dictionaries[1].to_string(), "english_stem");
                }
                other => panic!("Expected AddMapping, got {other:?}"),
            }
        }
        other => panic!("Expected TextSearchConfiguration, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_configuration_alter_mapping() {
    match alter_object_target(
        "ALTER TEXT SEARCH CONFIGURATION cfg ALTER MAPPING FOR word WITH simple",
    ) {
        AlterObjectTarget::TextSearchConfiguration { action, .. } => match action {
            AlterTextSearchConfigurationAction::AlterMapping {
                token_types,
                dictionaries,
            } => {
                assert_eq!(token_types.len(), 1);
                assert_eq!(dictionaries.len(), 1);
            }
            other => panic!("Expected AlterMapping, got {other:?}"),
        },
        other => panic!("Expected TextSearchConfiguration, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_configuration_replace_dictionary() {
    match alter_object_target(
        "ALTER TEXT SEARCH CONFIGURATION cfg ALTER MAPPING REPLACE simple WITH english_stem",
    ) {
        AlterObjectTarget::TextSearchConfiguration { action, .. } => match action {
            AlterTextSearchConfigurationAction::ReplaceDictionary {
                token_types,
                old_dictionary,
                new_dictionary,
            } => {
                assert!(token_types.is_none());
                assert_eq!(old_dictionary.to_string(), "simple");
                assert_eq!(new_dictionary.to_string(), "english_stem");
            }
            other => panic!("Expected ReplaceDictionary, got {other:?}"),
        },
        other => panic!("Expected TextSearchConfiguration, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_configuration_replace_dictionary_for_tokens() {
    match alter_object_target(
        "ALTER TEXT SEARCH CONFIGURATION cfg ALTER MAPPING FOR word, hword REPLACE simple WITH english_stem",
    ) {
        AlterObjectTarget::TextSearchConfiguration { action, .. } => match action {
            AlterTextSearchConfigurationAction::ReplaceDictionary { token_types, .. } => {
                assert_eq!(token_types.map(|t| t.len()), Some(2));
            }
            other => panic!("Expected ReplaceDictionary, got {other:?}"),
        },
        other => panic!("Expected TextSearchConfiguration, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_configuration_drop_mapping() {
    match alter_object_target("ALTER TEXT SEARCH CONFIGURATION cfg DROP MAPPING IF EXISTS FOR word")
    {
        AlterObjectTarget::TextSearchConfiguration { action, .. } => match action {
            AlterTextSearchConfigurationAction::DropMapping {
                if_exists,
                token_types,
            } => {
                assert!(if_exists);
                assert_eq!(token_types.len(), 1);
            }
            other => panic!("Expected DropMapping, got {other:?}"),
        },
        other => panic!("Expected TextSearchConfiguration, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_configuration_object_actions() {
    pg().verified_stmt("ALTER TEXT SEARCH CONFIGURATION cfg RENAME TO cfg2");
    pg().verified_stmt("ALTER TEXT SEARCH CONFIGURATION cfg OWNER TO u");
    pg().verified_stmt("ALTER TEXT SEARCH CONFIGURATION cfg SET SCHEMA s");
}

#[test]
fn parse_alter_text_search_dictionary_options() {
    // https://www.postgresql.org/docs/current/sql-altertsdictionary.html
    match alter_object_target(
        "ALTER TEXT SEARCH DICTIONARY d (stopwords = english, accept = false)",
    ) {
        AlterObjectTarget::TextSearchDictionary { name, action } => {
            assert_eq!(name.to_string(), "d");
            match action {
                AlterTextSearchDictionaryAction::SetOptions { options } => {
                    assert_eq!(options.len(), 2);
                    assert_eq!(options[0].name.to_string(), "stopwords");
                    assert_eq!(
                        options[0].value,
                        Some(DefinitionValue::Name(object_name("english")))
                    );
                }
                other => panic!("Expected SetOptions, got {other:?}"),
            }
        }
        other => panic!("Expected TextSearchDictionary, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_dictionary_option_without_value() {
    match alter_object_target("ALTER TEXT SEARCH DICTIONARY d (stopwords)") {
        AlterObjectTarget::TextSearchDictionary { action, .. } => match action {
            AlterTextSearchDictionaryAction::SetOptions { options } => {
                assert_eq!(options.len(), 1);
                assert!(options[0].value.is_none());
            }
            other => panic!("Expected SetOptions, got {other:?}"),
        },
        other => panic!("Expected TextSearchDictionary, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_dictionary_numeric_option() {
    match alter_object_target("ALTER TEXT SEARCH DICTIONARY d (accept = 2)") {
        AlterObjectTarget::TextSearchDictionary { action, .. } => match action {
            AlterTextSearchDictionaryAction::SetOptions { options } => assert!(matches!(
                options[0].value,
                Some(DefinitionValue::Literal(Expr::Value(_)))
            )),
            other => panic!("Expected SetOptions, got {other:?}"),
        },
        other => panic!("Expected TextSearchDictionary, got {other:?}"),
    }
}

#[test]
fn parse_alter_text_search_dictionary_object_actions() {
    pg().verified_stmt("ALTER TEXT SEARCH DICTIONARY d RENAME TO d2");
    pg().verified_stmt("ALTER TEXT SEARCH DICTIONARY d OWNER TO u");
    pg().verified_stmt("ALTER TEXT SEARCH DICTIONARY d SET SCHEMA s");
}

#[test]
fn parse_alter_text_search_parser_and_template() {
    // https://www.postgresql.org/docs/current/sql-altertsparser.html
    match alter_object_target("ALTER TEXT SEARCH PARSER p RENAME TO p2") {
        AlterObjectTarget::TextSearchParser { name, .. } => assert_eq!(name.to_string(), "p"),
        other => panic!("Expected TextSearchParser, got {other:?}"),
    }
    pg().verified_stmt("ALTER TEXT SEARCH PARSER p SET SCHEMA s");
    match alter_object_target("ALTER TEXT SEARCH TEMPLATE t SET SCHEMA s") {
        AlterObjectTarget::TextSearchTemplate { action, .. } => assert_eq!(
            action,
            AlterObjectAction::SetSchema {
                new_schema: object_name("s")
            }
        ),
        other => panic!("Expected TextSearchTemplate, got {other:?}"),
    }
}

#[test]
fn alter_text_search_parser_rejects_owner_to() {
    // PostgreSQL has no ALTER TEXT SEARCH PARSER ... OWNER TO form.
    assert!(pg()
        .parse_sql_statements("ALTER TEXT SEARCH PARSER p OWNER TO u")
        .is_err());
}

// =============================================================================
// ALTER DOMAIN
// =============================================================================

#[test]
fn parse_alter_domain_default() {
    // https://www.postgresql.org/docs/current/sql-alterdomain.html
    match alter_object_target("ALTER DOMAIN d SET DEFAULT 5") {
        AlterObjectTarget::Domain { name, action } => {
            assert_eq!(name.to_string(), "d");
            match action {
                AlterDomainAction::SetDefault { value } => assert_eq!(value.to_string(), "5"),
                other => panic!("Expected SetDefault, got {other:?}"),
            }
        }
        other => panic!("Expected Domain, got {other:?}"),
    }
    match alter_object_target("ALTER DOMAIN d DROP DEFAULT") {
        AlterObjectTarget::Domain { action, .. } => {
            assert_eq!(action, AlterDomainAction::DropDefault)
        }
        other => panic!("Expected Domain, got {other:?}"),
    }
}

#[test]
fn parse_alter_domain_not_null() {
    match alter_object_target("ALTER DOMAIN d SET NOT NULL") {
        AlterObjectTarget::Domain { action, .. } => {
            assert_eq!(action, AlterDomainAction::SetNotNull)
        }
        other => panic!("Expected Domain, got {other:?}"),
    }
    match alter_object_target("ALTER DOMAIN d DROP NOT NULL") {
        AlterObjectTarget::Domain { action, .. } => {
            assert_eq!(action, AlterDomainAction::DropNotNull)
        }
        other => panic!("Expected Domain, got {other:?}"),
    }
}

#[test]
fn parse_alter_domain_add_check_constraint() {
    match alter_object_target("ALTER DOMAIN d ADD CONSTRAINT c CHECK (value > 0)") {
        AlterObjectTarget::Domain { action, .. } => match action {
            AlterDomainAction::AddConstraint {
                constraint,
                not_valid,
            } => {
                assert!(!not_valid);
                assert!(matches!(constraint, TableConstraint::Check(_)));
            }
            other => panic!("Expected AddConstraint, got {other:?}"),
        },
        other => panic!("Expected Domain, got {other:?}"),
    }
}

#[test]
fn parse_alter_domain_add_check_constraint_not_valid() {
    match alter_object_target("ALTER DOMAIN d ADD CHECK (value > 0) NOT VALID") {
        AlterObjectTarget::Domain { action, .. } => match action {
            AlterDomainAction::AddConstraint { not_valid, .. } => assert!(not_valid),
            other => panic!("Expected AddConstraint, got {other:?}"),
        },
        other => panic!("Expected Domain, got {other:?}"),
    }
}

#[test]
fn parse_alter_domain_add_not_null_constraint() {
    match alter_object_target("ALTER DOMAIN d ADD NOT NULL") {
        AlterObjectTarget::Domain { action, .. } => assert_eq!(
            action,
            AlterDomainAction::AddNotNull {
                constraint_name: None,
                not_valid: false
            }
        ),
        other => panic!("Expected Domain, got {other:?}"),
    }
    match alter_object_target("ALTER DOMAIN d ADD CONSTRAINT c NOT NULL") {
        AlterObjectTarget::Domain { action, .. } => assert_eq!(
            action,
            AlterDomainAction::AddNotNull {
                constraint_name: Some("c".into()),
                not_valid: false
            }
        ),
        other => panic!("Expected Domain, got {other:?}"),
    }
}

#[test]
fn parse_alter_domain_drop_constraint() {
    match alter_object_target("ALTER DOMAIN d DROP CONSTRAINT IF EXISTS c CASCADE") {
        AlterObjectTarget::Domain { action, .. } => assert_eq!(
            action,
            AlterDomainAction::DropConstraint {
                if_exists: true,
                name: "c".into(),
                drop_behavior: Some(DropBehavior::Cascade)
            }
        ),
        other => panic!("Expected Domain, got {other:?}"),
    }
    pg().verified_stmt("ALTER DOMAIN d DROP CONSTRAINT c RESTRICT");
    pg().verified_stmt("ALTER DOMAIN d DROP CONSTRAINT c");
}

#[test]
fn parse_alter_domain_rename_and_validate_constraint() {
    match alter_object_target("ALTER DOMAIN d RENAME CONSTRAINT c TO c2") {
        AlterObjectTarget::Domain { action, .. } => assert_eq!(
            action,
            AlterDomainAction::RenameConstraint {
                old_name: "c".into(),
                new_name: "c2".into()
            }
        ),
        other => panic!("Expected Domain, got {other:?}"),
    }
    match alter_object_target("ALTER DOMAIN d VALIDATE CONSTRAINT c") {
        AlterObjectTarget::Domain { action, .. } => assert_eq!(
            action,
            AlterDomainAction::ValidateConstraint { name: "c".into() }
        ),
        other => panic!("Expected Domain, got {other:?}"),
    }
}

#[test]
fn parse_alter_domain_object_actions() {
    pg().verified_stmt("ALTER DOMAIN d OWNER TO u");
    pg().verified_stmt("ALTER DOMAIN d RENAME TO d2");
    pg().verified_stmt("ALTER DOMAIN s.d SET SCHEMA s2");
}

// =============================================================================
// ALTER EVENT TRIGGER
// =============================================================================

#[test]
fn parse_alter_event_trigger_enable_disable() {
    // https://www.postgresql.org/docs/current/sql-altereventtrigger.html
    match alter_object_target("ALTER EVENT TRIGGER t DISABLE") {
        AlterObjectTarget::EventTrigger { name, action } => {
            assert_eq!(name.to_string(), "t");
            assert_eq!(action, AlterEventTriggerAction::Disable);
        }
        other => panic!("Expected EventTrigger, got {other:?}"),
    }
    match alter_object_target("ALTER EVENT TRIGGER t ENABLE") {
        AlterObjectTarget::EventTrigger { action, .. } => {
            assert_eq!(action, AlterEventTriggerAction::Enable { mode: None })
        }
        other => panic!("Expected EventTrigger, got {other:?}"),
    }
    match alter_object_target("ALTER EVENT TRIGGER t ENABLE REPLICA") {
        AlterObjectTarget::EventTrigger { action, .. } => assert_eq!(
            action,
            AlterEventTriggerAction::Enable {
                mode: Some(EventTriggerEnableMode::Replica)
            }
        ),
        other => panic!("Expected EventTrigger, got {other:?}"),
    }
    pg().verified_stmt("ALTER EVENT TRIGGER t ENABLE ALWAYS");
}

#[test]
fn parse_alter_event_trigger_object_actions() {
    pg().verified_stmt("ALTER EVENT TRIGGER t OWNER TO u");
    pg().verified_stmt("ALTER EVENT TRIGGER t RENAME TO t2");
}

// =============================================================================
// ALTER FUNCTION / PROCEDURE / ROUTINE
// =============================================================================

fn routine_options(sql: &str) -> Vec<RoutineOption> {
    match alter_object_target(sql) {
        AlterObjectTarget::Routine { action, .. } => match action {
            AlterRoutineAction::Options { options, .. } => options,
            other => panic!("Expected Options, got {other:?}"),
        },
        other => panic!("Expected Routine, got {other:?}"),
    }
}

#[test]
fn parse_alter_function_null_input_actions() {
    // https://www.postgresql.org/docs/current/sql-alterfunction.html
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) CALLED ON NULL INPUT"),
        vec![RoutineOption::CalledOnNull(
            FunctionCalledOnNull::CalledOnNullInput
        )]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) RETURNS NULL ON NULL INPUT"),
        vec![RoutineOption::CalledOnNull(
            FunctionCalledOnNull::ReturnsNullOnNullInput
        )]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) STRICT"),
        vec![RoutineOption::CalledOnNull(FunctionCalledOnNull::Strict)]
    );
}

#[test]
fn parse_alter_function_volatility_actions() {
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) IMMUTABLE"),
        vec![RoutineOption::Behavior(FunctionBehavior::Immutable)]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) STABLE"),
        vec![RoutineOption::Behavior(FunctionBehavior::Stable)]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) VOLATILE"),
        vec![RoutineOption::Behavior(FunctionBehavior::Volatile)]
    );
}

#[test]
fn parse_alter_function_leakproof_actions() {
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) LEAKPROOF"),
        vec![RoutineOption::Leakproof(true)]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) NOT LEAKPROOF"),
        vec![RoutineOption::Leakproof(false)]
    );
}

#[test]
fn parse_alter_function_security_actions() {
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) SECURITY DEFINER"),
        vec![RoutineOption::Security {
            external: false,
            security: ProcedureSecurity::Definer
        }]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) EXTERNAL SECURITY INVOKER"),
        vec![RoutineOption::Security {
            external: true,
            security: ProcedureSecurity::Invoker
        }]
    );
}

#[test]
fn parse_alter_function_parallel_actions() {
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) PARALLEL SAFE"),
        vec![RoutineOption::Parallel(FunctionParallel::Safe)]
    );
    pg().verified_stmt("ALTER FUNCTION f(INT) PARALLEL RESTRICTED");
    pg().verified_stmt("ALTER FUNCTION f(INT) PARALLEL UNSAFE");
}

#[test]
fn parse_alter_function_cost_rows_support() {
    let options = routine_options("ALTER FUNCTION f(INT) COST 10 ROWS 100 SUPPORT my_support");
    assert_eq!(options.len(), 3);
    assert!(matches!(options[0], RoutineOption::Cost(_)));
    assert!(matches!(options[1], RoutineOption::Rows(_)));
    assert!(matches!(options[2], RoutineOption::Support(_)));
}

#[test]
fn parse_alter_function_set_and_reset_actions() {
    match routine_options("ALTER FUNCTION f(INT) SET search_path TO pg_catalog").as_slice() {
        [RoutineOption::Set(config)] => {
            assert_eq!(config.config_name.to_string(), "search_path");
            assert!(matches!(config.config_value, SetConfigValue::Value(_)));
        }
        other => panic!("Expected a single Set, got {other:?}"),
    }
    match routine_options("ALTER FUNCTION f(INT) SET search_path TO a, b").as_slice() {
        [RoutineOption::Set(config)] => match &config.config_value {
            SetConfigValue::Values(values) => assert_eq!(values.len(), 2),
            other => panic!("Expected Values, got {other:?}"),
        },
        other => panic!("Expected a single Set, got {other:?}"),
    }
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) SET a TO DEFAULT"),
        vec![RoutineOption::Set(sqlparser::ast::ProcedureSetConfig {
            config_name: object_name("a"),
            config_value: SetConfigValue::Default
        })]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) SET a FROM CURRENT"),
        vec![RoutineOption::Set(sqlparser::ast::ProcedureSetConfig {
            config_name: object_name("a"),
            config_value: SetConfigValue::FromCurrent
        })]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) RESET ALL"),
        vec![RoutineOption::Reset(ResetConfig::ALL)]
    );
    assert_eq!(
        routine_options("ALTER FUNCTION f(INT) RESET a"),
        vec![RoutineOption::Reset(ResetConfig::ConfigName(object_name(
            "a"
        )))]
    );
}

#[test]
fn parse_alter_function_set_equals_normalizes_to_to() {
    pg().one_statement_parses_to(
        "ALTER FUNCTION f(INT) SET work_mem = '256MB'",
        "ALTER FUNCTION f(INT) SET work_mem TO '256MB'",
    );
}

#[test]
fn parse_alter_function_multiple_actions_with_restrict() {
    match alter_object_target("ALTER FUNCTION f(INT) IMMUTABLE LEAKPROOF PARALLEL SAFE RESTRICT") {
        AlterObjectTarget::Routine { kind, action, .. } => {
            assert_eq!(kind, RoutineKind::Function);
            match action {
                AlterRoutineAction::Options { options, restrict } => {
                    assert_eq!(options.len(), 3);
                    assert!(restrict);
                }
                other => panic!("Expected Options, got {other:?}"),
            }
        }
        other => panic!("Expected Routine, got {other:?}"),
    }
}

#[test]
fn parse_alter_function_object_actions() {
    match alter_object_target("ALTER FUNCTION f RENAME TO g") {
        AlterObjectTarget::Routine { desc, action, .. } => {
            assert_eq!(desc.name.to_string(), "f");
            assert!(desc.args.is_none());
            assert_eq!(
                action,
                AlterRoutineAction::Object(AlterObjectAction::RenameTo {
                    new_name: "g".into()
                })
            );
        }
        other => panic!("Expected Routine, got {other:?}"),
    }
    match alter_object_target("ALTER FUNCTION f() OWNER TO CURRENT_USER") {
        AlterObjectTarget::Routine { desc, .. } => {
            assert_eq!(desc.args.as_ref().map(Vec::len), Some(0))
        }
        other => panic!("Expected Routine, got {other:?}"),
    }
    pg().verified_stmt("ALTER FUNCTION f(IN a INT, OUT b TEXT) SET SCHEMA s");
}

#[test]
fn parse_alter_function_depends_on_extension() {
    match alter_object_target("ALTER FUNCTION f(INT) DEPENDS ON EXTENSION ext") {
        AlterObjectTarget::Routine { action, .. } => assert_eq!(
            action,
            AlterRoutineAction::DependsOnExtension {
                no: false,
                extension_name: "ext".into()
            }
        ),
        other => panic!("Expected Routine, got {other:?}"),
    }
    match alter_object_target("ALTER FUNCTION f(INT) NO DEPENDS ON EXTENSION ext") {
        AlterObjectTarget::Routine { action, .. } => assert_eq!(
            action,
            AlterRoutineAction::DependsOnExtension {
                no: true,
                extension_name: "ext".into()
            }
        ),
        other => panic!("Expected Routine, got {other:?}"),
    }
}

#[test]
fn parse_alter_procedure_and_routine_kinds() {
    // https://www.postgresql.org/docs/current/sql-alterprocedure.html
    match alter_object_target("ALTER PROCEDURE p(INT) SECURITY DEFINER") {
        AlterObjectTarget::Routine { kind, .. } => assert_eq!(kind, RoutineKind::Procedure),
        other => panic!("Expected Routine, got {other:?}"),
    }
    // https://www.postgresql.org/docs/current/sql-alterroutine.html
    match alter_object_target("ALTER ROUTINE r(INT) IMMUTABLE") {
        AlterObjectTarget::Routine { kind, .. } => assert_eq!(kind, RoutineKind::Routine),
        other => panic!("Expected Routine, got {other:?}"),
    }
    pg().verified_stmt("ALTER PROCEDURE p RENAME TO q");
    pg().verified_stmt("ALTER ROUTINE r(INT) OWNER TO u");
}

// =============================================================================
// ALTER GROUP
// =============================================================================

#[test]
fn parse_alter_group() {
    // https://www.postgresql.org/docs/current/sql-altergroup.html
    match alter_object_target("ALTER GROUP g ADD USER a, b") {
        AlterObjectTarget::Group { name, action } => {
            assert_eq!(name, ident_owner("g"));
            assert_eq!(
                action,
                AlterGroupAction::AddUser {
                    members: vec![ident_owner("a"), ident_owner("b")]
                }
            );
        }
        other => panic!("Expected Group, got {other:?}"),
    }
    match alter_object_target("ALTER GROUP g DROP USER a") {
        AlterObjectTarget::Group { action, .. } => assert_eq!(
            action,
            AlterGroupAction::DropUser {
                members: vec![ident_owner("a")]
            }
        ),
        other => panic!("Expected Group, got {other:?}"),
    }
    match alter_object_target("ALTER GROUP g RENAME TO g2") {
        AlterObjectTarget::Group { action, .. } => assert_eq!(
            action,
            AlterGroupAction::RenameTo {
                new_name: "g2".into()
            }
        ),
        other => panic!("Expected Group, got {other:?}"),
    }
}

// =============================================================================
// ALTER OPERATOR
// =============================================================================

#[test]
fn parse_alter_operator_owner_and_schema() {
    // https://www.postgresql.org/docs/current/sql-alteroperator.html
    match alter_object_target("ALTER OPERATOR + (INT, INT) OWNER TO u") {
        AlterObjectTarget::Operator { name, args, action } => {
            assert_eq!(name.to_string(), "+");
            assert_eq!(args.left, Some(DataType::Int(None)));
            assert_eq!(args.right, Some(DataType::Int(None)));
            assert_eq!(
                action,
                AlterOperatorAction::Object(AlterObjectAction::OwnerTo {
                    new_owner: ident_owner("u")
                })
            );
        }
        other => panic!("Expected Operator, got {other:?}"),
    }
    pg().verified_stmt("ALTER OPERATOR @@ (TEXT, TEXT) SET SCHEMA s");
}

#[test]
fn parse_alter_operator_unary_none_argument() {
    match alter_object_target("ALTER OPERATOR - (NONE, INT) SET SCHEMA s") {
        AlterObjectTarget::Operator { args, .. } => {
            assert_eq!(args.left, None);
            assert_eq!(args.right, Some(DataType::Int(None)));
        }
        other => panic!("Expected Operator, got {other:?}"),
    }
}

#[test]
fn parse_alter_operator_set_options() {
    match alter_object_target(
        "ALTER OPERATOR @@ (TEXT, TEXT) SET (restrict = NONE, join = scalarltjoinsel, hashes, merges)",
    ) {
        AlterObjectTarget::Operator { action, .. } => match action {
            AlterOperatorAction::SetOptions { options } => {
                assert_eq!(options.len(), 4);
                assert_eq!(options[0].value, Some(DefinitionValue::None));
                assert_eq!(
                    options[1].value,
                    Some(DefinitionValue::Name(object_name("scalarltjoinsel")))
                );
                assert!(options[2].value.is_none());
                assert!(options[3].value.is_none());
            }
            other => panic!("Expected SetOptions, got {other:?}"),
        },
        other => panic!("Expected Operator, got {other:?}"),
    }
}

#[test]
fn parse_alter_operator_operator_valued_option() {
    match alter_object_target("ALTER OPERATOR <|| (BIGINT, BIGINT) SET (commutator = ||>)") {
        AlterObjectTarget::Operator { name, action, .. } => {
            assert_eq!(name.to_string(), "<||");
            match action {
                AlterOperatorAction::SetOptions { options } => assert_eq!(
                    options[0].value,
                    Some(DefinitionValue::Operator(object_name("||>")))
                ),
                other => panic!("Expected SetOptions, got {other:?}"),
            }
        }
        other => panic!("Expected Operator, got {other:?}"),
    }
}

#[test]
fn parse_alter_operator_quoted_option_name_keeps_case() {
    match alter_object_target(
        "ALTER OPERATOR <<| (BIGINT, BIGINT) SET (\"Restrict\" = scalarltsel)",
    ) {
        AlterObjectTarget::Operator { action, .. } => match action {
            AlterOperatorAction::SetOptions { options } => {
                assert_eq!(options[0].name.value, "Restrict");
                assert_eq!(options[0].name.quote_style, Some('"'));
            }
            other => panic!("Expected SetOptions, got {other:?}"),
        },
        other => panic!("Expected Operator, got {other:?}"),
    }
}

#[test]
fn parse_alter_operator_multi_fragment_symbol() {
    // `@+@` tokenizes as three adjacent operator fragments.
    match alter_object_target("ALTER OPERATOR @+@ (INT4, INT4) SET SCHEMA s") {
        AlterObjectTarget::Operator { name, .. } => assert_eq!(name.to_string(), "@+@"),
        other => panic!("Expected Operator, got {other:?}"),
    }
}

// =============================================================================
// ALTER TRIGGER
// =============================================================================

#[test]
fn parse_alter_trigger() {
    // https://www.postgresql.org/docs/current/sql-altertrigger.html
    match alter_object_target("ALTER TRIGGER t ON tbl RENAME TO t2") {
        AlterObjectTarget::Trigger {
            name,
            table_name,
            action,
        } => {
            assert_eq!(name.to_string(), "t");
            assert_eq!(table_name.to_string(), "tbl");
            assert_eq!(
                action,
                AlterTriggerAction::RenameTo {
                    new_name: "t2".into()
                }
            );
        }
        other => panic!("Expected Trigger, got {other:?}"),
    }
    match alter_object_target("ALTER TRIGGER t ON tbl NO DEPENDS ON EXTENSION ext") {
        AlterObjectTarget::Trigger { action, .. } => assert_eq!(
            action,
            AlterTriggerAction::DependsOnExtension {
                no: true,
                extension_name: "ext".into()
            }
        ),
        other => panic!("Expected Trigger, got {other:?}"),
    }
    pg().verified_stmt("ALTER TRIGGER t ON s.tbl DEPENDS ON EXTENSION ext");
}

// =============================================================================
// ALTER INDEX
// =============================================================================

fn alter_index_operation(sql: &str) -> AlterIndexOperation {
    match pg().verified_stmt(sql) {
        Statement::AlterIndex { operation, .. } => operation,
        other => panic!("Expected Statement::AlterIndex, got {other:?}"),
    }
}

#[test]
fn parse_alter_index_if_exists_rename() {
    // https://www.postgresql.org/docs/current/sql-alterindex.html
    match pg().verified_stmt("ALTER INDEX IF EXISTS i RENAME TO j") {
        Statement::AlterIndex {
            name,
            if_exists,
            operation,
        } => {
            assert_eq!(name.to_string(), "i");
            assert!(if_exists);
            assert_eq!(
                operation,
                AlterIndexOperation::RenameIndex {
                    index_name: object_name("j")
                }
            );
        }
        other => panic!("Expected Statement::AlterIndex, got {other:?}"),
    }
}

#[test]
fn parse_alter_index_set_tablespace_and_attach_partition() {
    assert_eq!(
        alter_index_operation("ALTER INDEX i SET TABLESPACE ts"),
        AlterIndexOperation::SetTablespace {
            tablespace_name: "ts".into()
        }
    );
    assert_eq!(
        alter_index_operation("ALTER INDEX i ATTACH PARTITION p"),
        AlterIndexOperation::AttachPartition {
            partition_index: object_name("p")
        }
    );
}

#[test]
fn parse_alter_index_depends_on_extension() {
    assert_eq!(
        alter_index_operation("ALTER INDEX i DEPENDS ON EXTENSION ext"),
        AlterIndexOperation::DependsOnExtension {
            no: false,
            extension_name: "ext".into()
        }
    );
    assert_eq!(
        alter_index_operation("ALTER INDEX i NO DEPENDS ON EXTENSION ext"),
        AlterIndexOperation::DependsOnExtension {
            no: true,
            extension_name: "ext".into()
        }
    );
}

#[test]
fn parse_alter_index_set_and_reset_storage_parameters() {
    match alter_index_operation("ALTER INDEX i SET (fillfactor = 70, deduplicate_items = true)") {
        AlterIndexOperation::SetOptions { options } => {
            assert_eq!(options.len(), 2);
            assert!(matches!(options[0], SqlOption::KeyValue { .. }));
        }
        other => panic!("Expected SetOptions, got {other:?}"),
    }
    assert_eq!(
        alter_index_operation("ALTER INDEX i RESET (fillfactor)"),
        AlterIndexOperation::ResetOptions {
            options: vec!["fillfactor".into()]
        }
    );
}

#[test]
fn parse_alter_index_bare_storage_parameter() {
    match alter_index_operation("ALTER INDEX i SET (fillfactor)") {
        AlterIndexOperation::SetOptions { options } => {
            assert_eq!(options, vec![SqlOption::Ident("fillfactor".into())])
        }
        other => panic!("Expected SetOptions, got {other:?}"),
    }
}

#[test]
fn parse_alter_index_alter_column_set_statistics() {
    match alter_index_operation("ALTER INDEX i ALTER COLUMN 1 SET STATISTICS -1") {
        AlterIndexOperation::AlterColumnSetStatistics {
            column_number,
            statistics,
        } => {
            assert_eq!(column_number.to_string(), "1");
            assert_eq!(statistics.to_string(), "-1");
        }
        other => panic!("Expected AlterColumnSetStatistics, got {other:?}"),
    }
    pg().one_statement_parses_to(
        "ALTER INDEX i ALTER 1 SET STATISTICS 500",
        "ALTER INDEX i ALTER COLUMN 1 SET STATISTICS 500",
    );
}

#[test]
fn parse_alter_index_all_in_tablespace() {
    match alter_object_target(
        "ALTER INDEX ALL IN TABLESPACE ts OWNED BY a, b SET TABLESPACE ts2 NOWAIT",
    ) {
        AlterObjectTarget::AllInTablespace {
            object_type,
            tablespace_name,
            owned_by,
            new_tablespace,
            nowait,
        } => {
            assert_eq!(object_type, AllInTablespaceObjectType::Index);
            assert_eq!(tablespace_name.to_string(), "ts");
            assert_eq!(owned_by, vec![ident_owner("a"), ident_owner("b")]);
            assert_eq!(new_tablespace.to_string(), "ts2");
            assert!(nowait);
        }
        other => panic!("Expected AllInTablespace, got {other:?}"),
    }
    pg().verified_stmt("ALTER INDEX ALL IN TABLESPACE ts SET TABLESPACE ts2");
}

// =============================================================================
// ALTER MATERIALIZED VIEW
// =============================================================================

fn alter_materialized_view_operation(sql: &str) -> AlterMaterializedViewOperation {
    match pg().verified_stmt(sql) {
        Statement::AlterMaterializedView { operation, .. } => operation,
        other => panic!("Expected Statement::AlterMaterializedView, got {other:?}"),
    }
}

#[test]
fn parse_alter_materialized_view_rename_and_schema() {
    // https://www.postgresql.org/docs/current/sql-altermaterializedview.html
    assert_eq!(
        alter_materialized_view_operation("ALTER MATERIALIZED VIEW m RENAME TO m2"),
        AlterMaterializedViewOperation::RenameTo {
            new_name: "m2".into()
        }
    );
    match pg().verified_stmt("ALTER MATERIALIZED VIEW IF EXISTS m SET SCHEMA s") {
        Statement::AlterMaterializedView {
            if_exists,
            operation,
            ..
        } => {
            assert!(if_exists);
            assert_eq!(
                operation,
                AlterMaterializedViewOperation::SetSchema {
                    new_schema: object_name("s")
                }
            );
        }
        other => panic!("Expected Statement::AlterMaterializedView, got {other:?}"),
    }
}

#[test]
fn parse_alter_materialized_view_rename_column() {
    assert_eq!(
        alter_materialized_view_operation("ALTER MATERIALIZED VIEW m RENAME COLUMN a TO b"),
        AlterMaterializedViewOperation::RenameColumn {
            old_column_name: "a".into(),
            new_column_name: "b".into()
        }
    );
    pg().one_statement_parses_to(
        "ALTER MATERIALIZED VIEW m RENAME a TO b",
        "ALTER MATERIALIZED VIEW m RENAME COLUMN a TO b",
    );
}

#[test]
fn parse_alter_materialized_view_column_actions() {
    match alter_materialized_view_operation(
        "ALTER MATERIALIZED VIEW m ALTER COLUMN a SET STATISTICS 100",
    ) {
        AlterMaterializedViewOperation::Actions(actions) => assert_eq!(
            actions,
            vec![AlterMaterializedViewAction::AlterColumnSetStatistics {
                column_name: "a".into(),
                statistics: Expr::Value(sqlparser::ast::Value::Number("100".into(), false).into())
            }]
        ),
        other => panic!("Expected Actions, got {other:?}"),
    }
    pg().verified_stmt("ALTER MATERIALIZED VIEW m ALTER COLUMN a SET (n_distinct = 5)");
    pg().verified_stmt("ALTER MATERIALIZED VIEW m ALTER COLUMN a RESET (n_distinct)");
    pg().verified_stmt("ALTER MATERIALIZED VIEW m ALTER COLUMN a SET STORAGE plain");
    pg().verified_stmt("ALTER MATERIALIZED VIEW m ALTER COLUMN a SET COMPRESSION lz4");
}

#[test]
fn parse_alter_materialized_view_cluster_and_storage_actions() {
    assert_eq!(
        alter_materialized_view_operation("ALTER MATERIALIZED VIEW m CLUSTER ON idx"),
        AlterMaterializedViewOperation::Actions(vec![AlterMaterializedViewAction::ClusterOn {
            index_name: "idx".into()
        }])
    );
    pg().verified_stmt("ALTER MATERIALIZED VIEW m SET WITHOUT CLUSTER");
    pg().verified_stmt("ALTER MATERIALIZED VIEW m SET ACCESS METHOD heap");
    pg().verified_stmt("ALTER MATERIALIZED VIEW m SET TABLESPACE ts");
    pg().verified_stmt("ALTER MATERIALIZED VIEW m SET (fillfactor = 70)");
    pg().verified_stmt("ALTER MATERIALIZED VIEW m RESET (fillfactor)");
}

#[test]
fn parse_alter_materialized_view_owner_to_keeps_dedicated_variant() {
    assert_eq!(
        alter_materialized_view_operation("ALTER MATERIALIZED VIEW m OWNER TO u"),
        AlterMaterializedViewOperation::OwnerTo(ident_owner("u"))
    );
}

#[test]
fn parse_alter_materialized_view_multiple_actions() {
    match alter_materialized_view_operation("ALTER MATERIALIZED VIEW m CLUSTER ON idx, OWNER TO u")
    {
        AlterMaterializedViewOperation::Actions(actions) => assert_eq!(actions.len(), 2),
        other => panic!("Expected Actions, got {other:?}"),
    }
}

#[test]
fn parse_alter_materialized_view_depends_on_extension() {
    assert_eq!(
        alter_materialized_view_operation("ALTER MATERIALIZED VIEW m NO DEPENDS ON EXTENSION ext"),
        AlterMaterializedViewOperation::DependsOnExtension {
            no: true,
            extension_name: "ext".into()
        }
    );
}

#[test]
fn parse_alter_materialized_view_all_in_tablespace() {
    match alter_object_target(
        "ALTER MATERIALIZED VIEW ALL IN TABLESPACE ts SET TABLESPACE ts2 NOWAIT",
    ) {
        AlterObjectTarget::AllInTablespace { object_type, .. } => {
            assert_eq!(object_type, AllInTablespaceObjectType::MaterializedView)
        }
        other => panic!("Expected AllInTablespace, got {other:?}"),
    }
}

// =============================================================================
// ALTER VIEW
// =============================================================================

fn alter_view_operation(sql: &str) -> AlterViewOperation {
    match pg().verified_stmt(sql) {
        Statement::AlterView { operation, .. } => operation,
        other => panic!("Expected Statement::AlterView, got {other:?}"),
    }
}

#[test]
fn parse_alter_view_column_default() {
    // https://www.postgresql.org/docs/current/sql-alterview.html
    match alter_view_operation("ALTER VIEW v ALTER COLUMN b SET DEFAULT 'x'") {
        AlterViewOperation::AlterColumnSetDefault {
            column_name,
            default,
        } => {
            assert_eq!(column_name.to_string(), "b");
            assert_eq!(default.to_string(), "'x'");
        }
        other => panic!("Expected AlterColumnSetDefault, got {other:?}"),
    }
    assert_eq!(
        alter_view_operation("ALTER VIEW v ALTER COLUMN b DROP DEFAULT"),
        AlterViewOperation::AlterColumnDropDefault {
            column_name: "b".into()
        }
    );
    pg().one_statement_parses_to(
        "ALTER VIEW IF EXISTS v ALTER b DROP DEFAULT",
        "ALTER VIEW IF EXISTS v ALTER COLUMN b DROP DEFAULT",
    );
}

#[test]
fn parse_alter_view_rename_column() {
    assert_eq!(
        alter_view_operation("ALTER VIEW v RENAME COLUMN b TO label"),
        AlterViewOperation::RenameColumn {
            old_column_name: "b".into(),
            new_column_name: "label".into()
        }
    );
    pg().one_statement_parses_to(
        "ALTER VIEW v RENAME b TO label",
        "ALTER VIEW v RENAME COLUMN b TO label",
    );
}

#[test]
fn parse_alter_view_if_exists() {
    match pg().verified_stmt("ALTER VIEW IF EXISTS v RENAME TO v2") {
        Statement::AlterView {
            if_exists,
            operation,
            ..
        } => {
            assert!(if_exists);
            assert_eq!(
                operation,
                AlterViewOperation::Rename {
                    new_name: "v2".into()
                }
            );
        }
        other => panic!("Expected Statement::AlterView, got {other:?}"),
    }
}

// =============================================================================
// ALTER TYPE
// =============================================================================

fn alter_type_operation(sql: &str) -> AlterTypeOperation {
    match pg().verified_stmt(sql) {
        Statement::AlterType(alter_type) => alter_type.operation,
        other => panic!("Expected Statement::AlterType, got {other:?}"),
    }
}

#[test]
fn parse_alter_type_owner_and_schema() {
    // https://www.postgresql.org/docs/current/sql-altertype.html
    assert_eq!(
        alter_type_operation("ALTER TYPE t OWNER TO u"),
        AlterTypeOperation::OwnerTo {
            new_owner: ident_owner("u")
        }
    );
    assert_eq!(
        alter_type_operation("ALTER TYPE t SET SCHEMA s"),
        AlterTypeOperation::SetSchema {
            new_schema: object_name("s")
        }
    );
}

#[test]
fn parse_alter_type_rename_attribute() {
    assert_eq!(
        alter_type_operation("ALTER TYPE t RENAME ATTRIBUTE a TO b"),
        AlterTypeOperation::RenameAttribute {
            old_name: "a".into(),
            new_name: "b".into(),
            drop_behavior: None
        }
    );
    assert_eq!(
        alter_type_operation("ALTER TYPE t RENAME ATTRIBUTE a TO b CASCADE"),
        AlterTypeOperation::RenameAttribute {
            old_name: "a".into(),
            new_name: "b".into(),
            drop_behavior: Some(DropBehavior::Cascade)
        }
    );
}

#[test]
fn parse_alter_type_add_attribute() {
    match alter_type_operation("ALTER TYPE t ADD ATTRIBUTE b TEXT COLLATE \"C\" RESTRICT") {
        AlterTypeOperation::Actions(actions) => match actions.as_slice() {
            [AlterTypeAction::AddAttribute {
                name,
                data_type,
                collation,
                drop_behavior,
            }] => {
                assert_eq!(name.to_string(), "b");
                assert_eq!(*data_type, DataType::Text);
                assert_eq!(
                    collation.as_ref().map(ToString::to_string),
                    Some("\"C\"".to_string())
                );
                assert_eq!(*drop_behavior, Some(DropBehavior::Restrict));
            }
            other => panic!("Expected a single AddAttribute, got {other:?}"),
        },
        other => panic!("Expected Actions, got {other:?}"),
    }
}

#[test]
fn parse_alter_type_drop_attribute() {
    match alter_type_operation("ALTER TYPE t DROP ATTRIBUTE IF EXISTS b CASCADE") {
        AlterTypeOperation::Actions(actions) => assert_eq!(
            actions,
            vec![AlterTypeAction::DropAttribute {
                if_exists: true,
                name: "b".into(),
                drop_behavior: Some(DropBehavior::Cascade)
            }]
        ),
        other => panic!("Expected Actions, got {other:?}"),
    }
}

#[test]
fn parse_alter_type_alter_attribute() {
    match alter_type_operation("ALTER TYPE t ALTER ATTRIBUTE b SET DATA TYPE VARCHAR") {
        AlterTypeOperation::Actions(actions) => match actions.as_slice() {
            [AlterTypeAction::AlterAttribute { had_set_data, .. }] => assert!(*had_set_data),
            other => panic!("Expected a single AlterAttribute, got {other:?}"),
        },
        other => panic!("Expected Actions, got {other:?}"),
    }
    match alter_type_operation("ALTER TYPE t ALTER ATTRIBUTE b TYPE VARCHAR") {
        AlterTypeOperation::Actions(actions) => match actions.as_slice() {
            [AlterTypeAction::AlterAttribute { had_set_data, .. }] => assert!(!*had_set_data),
            other => panic!("Expected a single AlterAttribute, got {other:?}"),
        },
        other => panic!("Expected Actions, got {other:?}"),
    }
}

#[test]
fn parse_alter_type_multiple_attribute_actions() {
    match alter_type_operation("ALTER TYPE t DROP ATTRIBUTE a, ADD ATTRIBUTE d BOOLEAN") {
        AlterTypeOperation::Actions(actions) => {
            assert_eq!(actions.len(), 2);
            assert!(matches!(actions[0], AlterTypeAction::DropAttribute { .. }));
            assert!(matches!(actions[1], AlterTypeAction::AddAttribute { .. }));
        }
        other => panic!("Expected Actions, got {other:?}"),
    }
}

#[test]
fn parse_alter_type_set_properties() {
    match alter_type_operation("ALTER TYPE t SET (receive = my_receive, send = my_send)") {
        AlterTypeOperation::SetProperties { properties } => assert_eq!(properties.len(), 2),
        other => panic!("Expected SetProperties, got {other:?}"),
    }
}

#[test]
fn parse_alter_type_enum_forms_still_parse() {
    pg().verified_stmt("ALTER TYPE t ADD VALUE IF NOT EXISTS 'x' BEFORE 'y'");
    pg().verified_stmt("ALTER TYPE t RENAME VALUE 'x' TO 'y'");
    pg().verified_stmt("ALTER TYPE t RENAME TO t2");
}

// =============================================================================
// ALTER SEQUENCE
// =============================================================================

fn alter_sequence_operation(sql: &str) -> Option<AlterSequenceOperation> {
    match pg().verified_stmt(sql) {
        Statement::AlterSequence { operation, .. } => operation,
        other => panic!("Expected Statement::AlterSequence, got {other:?}"),
    }
}

#[test]
fn parse_alter_sequence_object_actions() {
    // https://www.postgresql.org/docs/current/sql-altersequence.html
    assert_eq!(
        alter_sequence_operation("ALTER SEQUENCE IF EXISTS s RENAME TO s2"),
        Some(AlterSequenceOperation::RenameTo {
            new_name: "s2".into()
        })
    );
    assert_eq!(
        alter_sequence_operation("ALTER SEQUENCE s OWNER TO u"),
        Some(AlterSequenceOperation::OwnerTo {
            new_owner: ident_owner("u")
        })
    );
    assert_eq!(
        alter_sequence_operation("ALTER SEQUENCE s SET SCHEMA sc"),
        Some(AlterSequenceOperation::SetSchema {
            new_schema: object_name("sc")
        })
    );
    assert_eq!(
        alter_sequence_operation("ALTER SEQUENCE s SET LOGGED"),
        Some(AlterSequenceOperation::SetLogged)
    );
    assert_eq!(
        alter_sequence_operation("ALTER SEQUENCE s SET UNLOGGED"),
        Some(AlterSequenceOperation::SetUnlogged)
    );
}

#[test]
fn parse_alter_sequence_as_data_type() {
    match pg().verified_stmt("ALTER SEQUENCE s AS SMALLINT") {
        Statement::AlterSequence {
            sequence_options,
            operation,
            ..
        } => {
            assert_eq!(
                sequence_options,
                vec![sqlparser::ast::SequenceOptions::As(DataType::SmallInt(
                    None
                ))]
            );
            assert!(operation.is_none());
        }
        other => panic!("Expected Statement::AlterSequence, got {other:?}"),
    }
    pg().verified_stmt("ALTER SEQUENCE s AS SMALLINT MAXVALUE 20000");
}

#[test]
fn parse_alter_sequence_full_option_list() {
    pg().verified_stmt(
        "ALTER SEQUENCE s AS BIGINT INCREMENT BY 2 MINVALUE 1 MAXVALUE 100 CYCLE START WITH 5 RESTART WITH 7 CACHE 3 OWNED BY t.c",
    );
}

// =============================================================================
// ALTER DATABASE
// =============================================================================

fn alter_database_operation(sql: &str) -> AlterConfigurationOperation {
    match pg().verified_stmt(sql) {
        Statement::AlterDatabase { operation, .. } => operation,
        other => panic!("Expected Statement::AlterDatabase, got {other:?}"),
    }
}

#[test]
fn parse_alter_database_rename_owner_tablespace() {
    // https://www.postgresql.org/docs/current/sql-alterdatabase.html
    assert_eq!(
        alter_database_operation("ALTER DATABASE db RENAME TO db2"),
        AlterConfigurationOperation::RenameTo {
            new_name: "db2".into()
        }
    );
    assert_eq!(
        alter_database_operation("ALTER DATABASE db OWNER TO CURRENT_ROLE"),
        AlterConfigurationOperation::OwnerTo {
            new_owner: Owner::CurrentRole
        }
    );
    assert_eq!(
        alter_database_operation("ALTER DATABASE db SET TABLESPACE ts"),
        AlterConfigurationOperation::SetTablespace {
            tablespace_name: "ts".into()
        }
    );
    assert_eq!(
        alter_database_operation("ALTER DATABASE db REFRESH COLLATION VERSION"),
        AlterConfigurationOperation::RefreshCollationVersion
    );
}

#[test]
fn parse_alter_database_options() {
    match alter_database_operation("ALTER DATABASE db CONNECTION LIMIT 5") {
        AlterConfigurationOperation::WithOptions { with, options } => {
            assert!(!with);
            assert_eq!(
                options,
                vec![AlterDatabaseOption::ConnectionLimit(
                    DatabaseOptionValue::Value(Expr::Value(
                        sqlparser::ast::Value::Number("5".into(), false).into()
                    ))
                )]
            );
        }
        other => panic!("Expected WithOptions, got {other:?}"),
    }
    match alter_database_operation(
        "ALTER DATABASE db WITH ALLOW_CONNECTIONS false IS_TEMPLATE true",
    ) {
        AlterConfigurationOperation::WithOptions { with, options } => {
            assert!(with);
            assert_eq!(options.len(), 2);
        }
        other => panic!("Expected WithOptions, got {other:?}"),
    }
    pg().verified_stmt("ALTER DATABASE db CONNECTION LIMIT -1");
}

#[test]
fn parse_alter_database_set_and_reset_still_parse() {
    pg().verified_stmt("ALTER DATABASE db SET search_path TO DEFAULT");
    pg().verified_stmt("ALTER DATABASE db SET search_path FROM CURRENT");
    pg().verified_stmt("ALTER DATABASE db RESET ALL");
    match alter_database_operation("ALTER DATABASE db SET search_path TO a, b") {
        AlterConfigurationOperation::Set { config_value, .. } => match config_value {
            SetConfigValue::Values(values) => assert_eq!(values.len(), 2),
            other => panic!("Expected Values, got {other:?}"),
        },
        other => panic!("Expected Set, got {other:?}"),
    }
}

// =============================================================================
// ALTER PUBLICATION / SUBSCRIPTION / ROLE
// =============================================================================

#[test]
fn parse_alter_publication_rename_and_owner() {
    // https://www.postgresql.org/docs/current/sql-alterpublication.html
    match pg().verified_stmt("ALTER PUBLICATION p RENAME TO p2") {
        Statement::AlterPublication { action, .. } => {
            assert_eq!(action, AlterPublicationAction::RenameTo("p2".into()))
        }
        other => panic!("Expected Statement::AlterPublication, got {other:?}"),
    }
    match pg().verified_stmt("ALTER PUBLICATION p OWNER TO CURRENT_USER") {
        Statement::AlterPublication { action, .. } => {
            assert_eq!(action, AlterPublicationAction::OwnerTo(Owner::CurrentUser))
        }
        other => panic!("Expected Statement::AlterPublication, got {other:?}"),
    }
}

#[test]
fn parse_alter_subscription_rename_and_owner() {
    // https://www.postgresql.org/docs/current/sql-altersubscription.html
    match pg().verified_stmt("ALTER SUBSCRIPTION sub RENAME TO sub2") {
        Statement::AlterSubscription { action, .. } => {
            assert_eq!(action, AlterSubscriptionAction::RenameTo("sub2".into()))
        }
        other => panic!("Expected Statement::AlterSubscription, got {other:?}"),
    }
    match pg().verified_stmt("ALTER SUBSCRIPTION sub OWNER TO u") {
        Statement::AlterSubscription { action, .. } => {
            assert_eq!(action, AlterSubscriptionAction::OwnerTo(ident_owner("u")))
        }
        other => panic!("Expected Statement::AlterSubscription, got {other:?}"),
    }
}

#[test]
fn parse_alter_role_negative_connection_limit() {
    // https://www.postgresql.org/docs/current/sql-alterrole.html
    match pg().verified_stmt("ALTER ROLE r WITH CONNECTION LIMIT -1") {
        Statement::AlterRole { operation, .. } => match operation {
            sqlparser::ast::AlterRoleOperation::WithOptions { options } => match options.as_slice()
            {
                [sqlparser::ast::RoleOption::ConnectionLimit(limit)] => {
                    assert_eq!(limit.to_string(), "-1")
                }
                other => panic!("Expected a single ConnectionLimit, got {other:?}"),
            },
            other => panic!("Expected WithOptions, got {other:?}"),
        },
        other => panic!("Expected Statement::AlterRole, got {other:?}"),
    }
    pg().one_statement_parses_to(
        "ALTER ROLE r CONNECTION LIMIT -1",
        "ALTER ROLE r WITH CONNECTION LIMIT -1",
    );
}

#[test]
fn parse_alter_role_full_option_list() {
    pg().verified_stmt(
        "ALTER ROLE r WITH SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN REPLICATION BYPASSRLS CONNECTION LIMIT 5",
    );
    pg().verified_stmt(
        "ALTER ROLE r WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN NOREPLICATION NOBYPASSRLS",
    );
    pg().verified_stmt("ALTER ROLE r WITH PASSWORD NULL");
    pg().verified_stmt("ALTER ROLE r WITH VALID UNTIL '2030-01-01'");
}
