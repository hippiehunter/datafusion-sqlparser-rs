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

//! PostgreSQL object DDL: `COMMENT ON`, the `CREATE` forms of miscellaneous
//! database objects, every `DROP` form, ownership transfer, and role grants.
//!
//! References:
//! - <https://www.postgresql.org/docs/current/sql-comment.html>
//! - <https://www.postgresql.org/docs/current/sql-createcollation.html>
//! - <https://www.postgresql.org/docs/current/sql-createconversion.html>
//! - <https://www.postgresql.org/docs/current/sql-createlanguage.html>
//! - <https://www.postgresql.org/docs/current/sql-createeventtrigger.html>
//! - <https://www.postgresql.org/docs/current/sql-createoperator.html>
//! - <https://www.postgresql.org/docs/current/sql-createaggregate.html>
//! - <https://www.postgresql.org/docs/current/sql-createstatistics.html>
//! - <https://www.postgresql.org/docs/current/sql-drop-owned.html>
//! - <https://www.postgresql.org/docs/current/sql-reassign-owned.html>
//! - <https://www.postgresql.org/docs/current/sql-grant.html>

use crate::postgres_compat::common::*;
use sqlparser::ast::{
    AggregateArgs, CollationDefinition, CommentObject, CommentObjectDetail, DropBehavior,
    GrantObjects, ObjectType, RoleGrantOptionValue, SqlOption, Statement, TriggerExecBodyType,
    Value,
};

/// Parse `sql`, assert it renders back to exactly `sql`, and assert that the
/// rendered text parses to an equal AST.
fn roundtrip(sql: &str) -> Statement {
    let statement = verified_pg_stmt(sql);
    assert_eq!(statement.to_string(), sql, "rendering differs from source");
    assert_eq!(
        verified_pg_stmt(&statement.to_string()),
        statement,
        "rendered SQL does not parse back to the same AST"
    );
    statement
}

/// Parse `sql` and assert it renders to `canonical`, which must itself parse to
/// the same AST. Used where PostgreSQL identifier folding or a canonical clause
/// order makes the rendering differ from the source.
fn renders_to(sql: &str, canonical: &str) -> Statement {
    let statement = verified_pg_stmt(sql);
    assert_eq!(statement.to_string(), canonical);
    assert_eq!(verified_pg_stmt(canonical), statement);
    statement
}

fn comment_parts(
    statement: &Statement,
) -> (
    &CommentObject,
    String,
    &Option<CommentObjectDetail>,
    &Option<String>,
) {
    match statement {
        Statement::Comment {
            object_type,
            object_name,
            comment,
            object_detail,
            ..
        } => (object_type, object_name.to_string(), object_detail, comment),
        other => panic!("Expected Statement::Comment, got {other:?}"),
    }
}

// =============================================================================
// COMMENT ON
// =============================================================================

#[test]
fn comment_on_name_only_objects() {
    for (sql, object_type) in [
        (
            "COMMENT ON ACCESS METHOD heap IS 'am'",
            CommentObject::AccessMethod,
        ),
        (
            "COMMENT ON COLLATION my_coll IS 'coll'",
            CommentObject::Collation,
        ),
        ("COMMENT ON COLUMN t.c IS 'col'", CommentObject::Column),
        (
            "COMMENT ON CONVERSION my_conv IS 'conv'",
            CommentObject::Conversion,
        ),
        ("COMMENT ON DATABASE my_db IS 'db'", CommentObject::Database),
        (
            "COMMENT ON DOMAIN my_domain IS 'domain'",
            CommentObject::Domain,
        ),
        (
            "COMMENT ON EVENT TRIGGER my_trig IS 'evt'",
            CommentObject::EventTrigger,
        ),
        (
            "COMMENT ON EXTENSION hstore IS 'ext'",
            CommentObject::Extension,
        ),
        (
            "COMMENT ON FOREIGN DATA WRAPPER my_fdw IS 'fdw'",
            CommentObject::ForeignDataWrapper,
        ),
        (
            "COMMENT ON FOREIGN TABLE my_ft IS 'ft'",
            CommentObject::ForeignTable,
        ),
        ("COMMENT ON INDEX my_index IS 'index'", CommentObject::Index),
        (
            "COMMENT ON LANGUAGE plpgsql IS 'lang'",
            CommentObject::Language,
        ),
        (
            "COMMENT ON MATERIALIZED VIEW my_mv IS 'mv'",
            CommentObject::MaterializedView,
        ),
        (
            "COMMENT ON PUBLICATION my_pub IS 'pub'",
            CommentObject::Publication,
        ),
        ("COMMENT ON ROLE my_role IS 'role'", CommentObject::Role),
        (
            "COMMENT ON SCHEMA my_schema IS 'schema'",
            CommentObject::Schema,
        ),
        (
            "COMMENT ON SEQUENCE my_seq IS 'seq'",
            CommentObject::Sequence,
        ),
        (
            "COMMENT ON SERVER my_server IS 'server'",
            CommentObject::Server,
        ),
        (
            "COMMENT ON STATISTICS my_stats IS 'stats'",
            CommentObject::Statistics,
        ),
        (
            "COMMENT ON SUBSCRIPTION my_sub IS 'sub'",
            CommentObject::Subscription,
        ),
        ("COMMENT ON TABLE my_table IS 'table'", CommentObject::Table),
        (
            "COMMENT ON TABLESPACE my_ts IS 'ts'",
            CommentObject::Tablespace,
        ),
        (
            "COMMENT ON TEXT SEARCH CONFIGURATION my_cfg IS 'cfg'",
            CommentObject::TextSearchConfiguration,
        ),
        (
            "COMMENT ON TEXT SEARCH DICTIONARY my_dict IS 'dict'",
            CommentObject::TextSearchDictionary,
        ),
        (
            "COMMENT ON TEXT SEARCH PARSER my_parser IS 'parser'",
            CommentObject::TextSearchParser,
        ),
        (
            "COMMENT ON TEXT SEARCH TEMPLATE my_tmpl IS 'tmpl'",
            CommentObject::TextSearchTemplate,
        ),
        ("COMMENT ON TYPE my_type IS 'type'", CommentObject::Type),
        ("COMMENT ON VIEW my_view IS 'view'", CommentObject::View),
    ] {
        let statement = roundtrip(sql);
        let (parsed_type, _, detail, comment) = comment_parts(&statement);
        assert_eq!(parsed_type, &object_type, "{sql}");
        assert!(detail.is_none(), "{sql}");
        assert!(comment.is_some(), "{sql}");
    }
}

#[test]
fn comment_on_is_null_removes_the_comment() {
    let statement = roundtrip("COMMENT ON TABLE my_table IS NULL");
    let (_, _, _, comment) = comment_parts(&statement);
    assert!(comment.is_none());
}

#[test]
fn comment_on_procedural_language_keeps_the_language_target() {
    let statement =
        one_statement_parses_to_pg("COMMENT ON PROCEDURAL LANGUAGE plpgsql IS 'lang'", "");
    let (object_type, name, _, _) = comment_parts(&statement);
    assert_eq!(object_type, &CommentObject::Language);
    assert_eq!(name, "plpgsql");
}

#[test]
fn comment_on_constraint_names_its_relation() {
    let statement = roundtrip("COMMENT ON CONSTRAINT my_check ON my_table IS 'ck'");
    let (object_type, name, detail, _) = comment_parts(&statement);
    assert_eq!(object_type, &CommentObject::Constraint);
    assert_eq!(name, "my_check");
    match detail {
        Some(CommentObjectDetail::On(table)) => assert_eq!(table.to_string(), "my_table"),
        other => panic!("Expected an ON clause, got {other:?}"),
    }
}

#[test]
fn comment_on_domain_constraint_names_its_domain() {
    let statement = roundtrip("COMMENT ON CONSTRAINT my_check ON DOMAIN my_domain IS 'ck'");
    let (_, _, detail, _) = comment_parts(&statement);
    match detail {
        Some(CommentObjectDetail::OnDomain(domain)) => {
            assert_eq!(domain.to_string(), "my_domain")
        }
        other => panic!("Expected an ON DOMAIN clause, got {other:?}"),
    }
}

#[test]
fn comment_on_relation_attached_objects() {
    for (sql, object_type) in [
        (
            "COMMENT ON POLICY my_policy ON my_table IS 'p'",
            CommentObject::Policy,
        ),
        (
            "COMMENT ON RULE my_rule ON my_table IS 'r'",
            CommentObject::Rule,
        ),
        (
            "COMMENT ON TRIGGER my_trigger ON my_table IS 't'",
            CommentObject::Trigger,
        ),
    ] {
        let statement = roundtrip(sql);
        let (parsed_type, _, detail, _) = comment_parts(&statement);
        assert_eq!(parsed_type, &object_type, "{sql}");
        assert!(matches!(detail, Some(CommentObjectDetail::On(_))), "{sql}");
    }
}

#[test]
fn comment_on_routine_takes_an_optional_argument_list() {
    let with_args = roundtrip("COMMENT ON FUNCTION my_func(INT) IS 'f'");
    let (object_type, name, detail, _) = comment_parts(&with_args);
    assert_eq!(object_type, &CommentObject::Function);
    assert_eq!(name, "my_func");
    match detail {
        Some(CommentObjectDetail::Arguments(args)) => assert_eq!(args.len(), 1),
        other => panic!("Expected an argument list, got {other:?}"),
    }

    let empty_args = roundtrip("COMMENT ON PROCEDURE my_proc() IS 'p'");
    let (_, _, detail, _) = comment_parts(&empty_args);
    assert!(matches!(detail, Some(CommentObjectDetail::Arguments(args)) if args.is_empty()));

    let no_args = roundtrip("COMMENT ON ROUTINE my_routine IS 'r'");
    let (object_type, _, detail, _) = comment_parts(&no_args);
    assert_eq!(object_type, &CommentObject::Routine);
    assert!(detail.is_none());
}

#[test]
fn comment_on_aggregate_takes_an_aggregate_signature() {
    let statement = roundtrip("COMMENT ON AGGREGATE my_agg(INT8) IS 'a'");
    let (object_type, name, detail, _) = comment_parts(&statement);
    assert_eq!(object_type, &CommentObject::Aggregate);
    assert_eq!(name, "my_agg");
    assert!(matches!(
        detail,
        Some(CommentObjectDetail::AggregateArguments(AggregateArgs::Args(args))) if args.len() == 1
    ));

    let star = roundtrip("COMMENT ON AGGREGATE my_agg(*) IS 'a'");
    let (_, _, detail, _) = comment_parts(&star);
    assert!(matches!(
        detail,
        Some(CommentObjectDetail::AggregateArguments(AggregateArgs::Star))
    ));

    let ordered = roundtrip("COMMENT ON AGGREGATE my_rank(FLOAT8 ORDER BY FLOAT8) IS 'a'");
    let (_, _, detail, _) = comment_parts(&ordered);
    assert!(matches!(
        detail,
        Some(CommentObjectDetail::AggregateArguments(AggregateArgs::OrderedSet {
            direct,
            ordered,
        })) if direct.len() == 1 && ordered.len() == 1
    ));
}

#[test]
fn comment_on_operator_takes_operand_types() {
    let statement = roundtrip("COMMENT ON OPERATOR ==@ (BIGINT, BIGINT) IS 'op'");
    let (object_type, name, detail, _) = comment_parts(&statement);
    assert_eq!(object_type, &CommentObject::Operator);
    assert_eq!(name, "==@");
    match detail {
        Some(CommentObjectDetail::OperatorArguments(args)) => {
            assert!(args.left.is_some());
            assert!(args.right.is_some());
        }
        other => panic!("Expected operand types, got {other:?}"),
    }
}

#[test]
fn comment_on_unary_operator_spells_the_missing_operand_none() {
    let statement = roundtrip("COMMENT ON OPERATOR - (NONE, INT4) IS 'negate'");
    let (_, _, detail, _) = comment_parts(&statement);
    match detail {
        Some(CommentObjectDetail::OperatorArguments(args)) => {
            assert!(args.left.is_none());
            assert!(args.right.is_some());
        }
        other => panic!("Expected operand types, got {other:?}"),
    }
}

#[test]
fn comment_on_operator_class_and_family_name_an_index_method() {
    for (sql, object_type) in [
        (
            "COMMENT ON OPERATOR CLASS int4_ops USING btree IS 'oc'",
            CommentObject::OperatorClass,
        ),
        (
            "COMMENT ON OPERATOR FAMILY integer_ops USING btree IS 'of'",
            CommentObject::OperatorFamily,
        ),
    ] {
        let statement = roundtrip(sql);
        let (parsed_type, _, detail, _) = comment_parts(&statement);
        assert_eq!(parsed_type, &object_type, "{sql}");
        match detail {
            Some(CommentObjectDetail::Using(method)) => assert_eq!(method.value, "btree"),
            other => panic!("Expected a USING clause, got {other:?}"),
        }
    }
}

#[test]
fn comment_on_cast_names_no_object() {
    let statement = roundtrip("COMMENT ON CAST (INT4 AS TEXT) IS 'cast'");
    let (object_type, name, detail, _) = comment_parts(&statement);
    assert_eq!(object_type, &CommentObject::Cast);
    assert_eq!(name, "");
    assert!(matches!(detail, Some(CommentObjectDetail::Cast(_))));
}

#[test]
fn comment_on_transform_names_a_type_and_a_language() {
    let statement = roundtrip("COMMENT ON TRANSFORM FOR hstore LANGUAGE plpython3u IS 'x'");
    let (object_type, name, detail, _) = comment_parts(&statement);
    assert_eq!(object_type, &CommentObject::Transform);
    assert_eq!(name, "");
    match detail {
        Some(CommentObjectDetail::Transform { language, .. }) => {
            assert_eq!(language.value, "plpython3u")
        }
        other => panic!("Expected a TRANSFORM target, got {other:?}"),
    }
}

#[test]
fn comment_on_large_object_names_an_oid() {
    let statement = roundtrip("COMMENT ON LARGE OBJECT 47270 IS 'lo'");
    let (object_type, name, detail, _) = comment_parts(&statement);
    assert_eq!(object_type, &CommentObject::LargeObject);
    assert_eq!(name, "");
    match detail {
        Some(CommentObjectDetail::LargeObject(oid)) => {
            assert_eq!(oid.value, Value::Number("47270".to_string(), false))
        }
        other => panic!("Expected a large object oid, got {other:?}"),
    }
}

// =============================================================================
// CREATE COLLATION
// =============================================================================

#[test]
fn create_collation_from_properties() {
    let statement = roundtrip(
        "CREATE COLLATION my_coll (locale = 'fr_FR', provider = icu, deterministic = false, rules = '&a < g', version = '153.14')",
    );
    match statement {
        Statement::CreateCollation(collation) => {
            assert!(!collation.if_not_exists);
            assert_eq!(collation.name.to_string(), "my_coll");
            match collation.definition {
                CollationDefinition::Options(options) => assert_eq!(options.len(), 5),
                other => panic!("Expected a property list, got {other:?}"),
            }
        }
        other => panic!("Expected CreateCollation, got {other:?}"),
    }
}

#[test]
fn create_collation_accepts_quoted_identifier_property_values() {
    let statement = renders_to(
        r#"CREATE COLLATION my_coll (LC_COLLATE = "POSIX", LC_CTYPE = "POSIX")"#,
        r#"CREATE COLLATION my_coll (lc_collate = "POSIX", lc_ctype = "POSIX")"#,
    );
    match statement {
        Statement::CreateCollation(collation) => match collation.definition {
            CollationDefinition::Options(options) => assert_eq!(options.len(), 2),
            other => panic!("Expected a property list, got {other:?}"),
        },
        other => panic!("Expected CreateCollation, got {other:?}"),
    }
}

#[test]
fn create_collation_from_existing_collation() {
    let statement = roundtrip(r#"CREATE COLLATION IF NOT EXISTS my_coll FROM "C""#);
    match statement {
        Statement::CreateCollation(collation) => {
            assert!(collation.if_not_exists);
            match collation.definition {
                CollationDefinition::From(source) => assert_eq!(source.to_string(), r#""C""#),
                other => panic!("Expected a FROM clause, got {other:?}"),
            }
        }
        other => panic!("Expected CreateCollation, got {other:?}"),
    }
}

#[test]
fn create_collation_accepts_a_valueless_property() {
    let statement = roundtrip("CREATE COLLATION my_coll (locale)");
    match statement {
        Statement::CreateCollation(collation) => match collation.definition {
            CollationDefinition::Options(options) => {
                assert!(matches!(options.as_slice(), [SqlOption::Ident(_)]))
            }
            other => panic!("Expected a property list, got {other:?}"),
        },
        other => panic!("Expected CreateCollation, got {other:?}"),
    }
}

// =============================================================================
// CREATE CONVERSION
// =============================================================================

#[test]
fn create_conversion() {
    let statement =
        roundtrip("CREATE CONVERSION my_conv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8");
    match statement {
        Statement::CreateConversion(conversion) => {
            assert!(!conversion.default);
            assert_eq!(conversion.name.to_string(), "my_conv");
            assert_eq!(conversion.function.to_string(), "iso8859_1_to_utf8");
        }
        other => panic!("Expected CreateConversion, got {other:?}"),
    }
}

#[test]
fn create_default_conversion() {
    let statement = roundtrip(
        "CREATE DEFAULT CONVERSION public.my_conv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8",
    );
    match statement {
        Statement::CreateConversion(conversion) => {
            assert!(conversion.default);
            assert_eq!(conversion.name.to_string(), "public.my_conv");
        }
        other => panic!("Expected CreateConversion, got {other:?}"),
    }
}

// =============================================================================
// CREATE LANGUAGE
// =============================================================================

#[test]
fn create_language_with_every_handler() {
    let statement = roundtrip(
        "CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE plsample HANDLER plsample_call_handler INLINE plsample_inline_handler VALIDATOR plsample_validator",
    );
    match statement {
        Statement::CreateLanguage(language) => {
            assert!(language.or_replace);
            assert!(language.trusted);
            assert!(language.procedural);
            assert_eq!(language.name.value, "plsample");
            assert_eq!(
                language.handler.map(|h| h.to_string()),
                Some("plsample_call_handler".to_string())
            );
            assert!(language.inline.is_some());
            assert!(language.validator.is_some());
        }
        other => panic!("Expected CreateLanguage, got {other:?}"),
    }
}

#[test]
fn create_language_without_a_handler() {
    let statement = roundtrip("CREATE LANGUAGE plsample");
    match statement {
        Statement::CreateLanguage(language) => {
            assert!(!language.trusted);
            assert!(!language.procedural);
            assert!(language.handler.is_none());
        }
        other => panic!("Expected CreateLanguage, got {other:?}"),
    }
}

// =============================================================================
// CREATE EVENT TRIGGER
// =============================================================================

#[test]
fn create_event_trigger() {
    let statement =
        roundtrip("CREATE EVENT TRIGGER my_trig ON sql_drop EXECUTE FUNCTION my_report()");
    match statement {
        Statement::CreateEventTrigger(trigger) => {
            assert_eq!(trigger.name.value, "my_trig");
            assert_eq!(trigger.event.value, "sql_drop");
            assert!(trigger.conditions.is_empty());
            assert_eq!(trigger.exec_body.exec_type, TriggerExecBodyType::Function);
            assert_eq!(trigger.exec_body.func_desc.name.to_string(), "my_report");
        }
        other => panic!("Expected CreateEventTrigger, got {other:?}"),
    }
}

#[test]
fn create_event_trigger_with_conjoined_when_filters() {
    let statement = renders_to(
        "CREATE EVENT TRIGGER my_trig ON ddl_command_start WHEN TAG IN ('CREATE TABLE', 'CREATE VIEW') AND TAG IN ('DROP TABLE') EXECUTE PROCEDURE my_report()",
        "CREATE EVENT TRIGGER my_trig ON ddl_command_start WHEN tag IN ('CREATE TABLE', 'CREATE VIEW') AND tag IN ('DROP TABLE') EXECUTE PROCEDURE my_report()",
    );
    match statement {
        Statement::CreateEventTrigger(trigger) => {
            assert_eq!(trigger.conditions.len(), 2);
            assert_eq!(trigger.conditions[0].variable.value, "tag");
            assert_eq!(trigger.conditions[0].values.len(), 2);
            assert_eq!(trigger.exec_body.exec_type, TriggerExecBodyType::Procedure);
        }
        other => panic!("Expected CreateEventTrigger, got {other:?}"),
    }
}

// =============================================================================
// CREATE OPERATOR
// =============================================================================

#[test]
fn create_operator_accepts_the_full_operator_character_set() {
    for (sql, canonical, name) in [
        (
            "CREATE OPERATOR @+@ (LEFTARG = INT4, RIGHTARG = INT4, PROCEDURE = int4pl)",
            "CREATE OPERATOR @+@ (PROCEDURE = int4pl, LEFTARG = INT4, RIGHTARG = INT4)",
            "@+@",
        ),
        (
            "CREATE OPERATOR #@%# (RIGHTARG = BIGINT)",
            "CREATE OPERATOR #@%# (RIGHTARG = BIGINT)",
            "#@%#",
        ),
        (
            "CREATE OPERATOR my_schema.=== (LEFTARG = INT4, RIGHTARG = INT4, FUNCTION = int4eq)",
            "CREATE OPERATOR my_schema.=== (FUNCTION = int4eq, LEFTARG = INT4, RIGHTARG = INT4)",
            "my_schema.===",
        ),
    ] {
        let statement = renders_to(sql, canonical);
        match statement {
            Statement::CreateOperator(operator) => assert_eq!(operator.name.to_string(), name),
            other => panic!("Expected CreateOperator, got {other:?}"),
        }
    }
}

#[test]
fn create_operator_without_a_function_parses() {
    // PostgreSQL raises 42P13 when the statement runs, not while parsing.
    let statement = roundtrip("CREATE OPERATOR #@%# (RIGHTARG = BIGINT)");
    match statement {
        Statement::CreateOperator(operator) => {
            assert!(operator.function.is_none());
            assert!(operator.right_arg.is_some());
        }
        other => panic!("Expected CreateOperator, got {other:?}"),
    }
}

#[test]
fn create_operator_procedure_is_a_spelling_of_function() {
    let statement = renders_to(
        "CREATE OPERATOR === (LEFTARG = INT4, PROCEDURE = int4eq)",
        "CREATE OPERATOR === (PROCEDURE = int4eq, LEFTARG = INT4)",
    );
    match statement {
        Statement::CreateOperator(operator) => {
            assert!(operator.is_procedure);
            assert_eq!(
                operator.function.map(|f| f.to_string()),
                Some("int4eq".to_string())
            );
        }
        other => panic!("Expected CreateOperator, got {other:?}"),
    }
}

// =============================================================================
// CREATE AGGREGATE
// =============================================================================

#[test]
fn create_aggregate_old_syntax_has_no_argument_list() {
    let statement =
        roundtrip("CREATE AGGREGATE my_agg (basetype = int4, sfunc = int4pl, stype = int4)");
    match statement {
        Statement::CreateAggregate(aggregate) => {
            assert!(aggregate.signature.is_none());
            assert!(aggregate.args.is_empty());
            assert_eq!(aggregate.options.len(), 3);
        }
        other => panic!("Expected CreateAggregate, got {other:?}"),
    }
}

#[test]
fn create_aggregate_argument_list_forms() {
    let plain = roundtrip("CREATE AGGREGATE my_agg (INT4) (sfunc = int4pl, stype = int4)");
    match plain {
        Statement::CreateAggregate(aggregate) => {
            assert!(
                matches!(aggregate.signature, Some(AggregateArgs::Args(ref args)) if args.len() == 1)
            );
            assert_eq!(aggregate.args.len(), 1);
        }
        other => panic!("Expected CreateAggregate, got {other:?}"),
    }

    let star = roundtrip("CREATE AGGREGATE my_agg (*) (sfunc = int8inc, stype = int8)");
    match star {
        Statement::CreateAggregate(aggregate) => {
            assert!(matches!(aggregate.signature, Some(AggregateArgs::Star)));
            assert!(aggregate.args.is_empty());
        }
        other => panic!("Expected CreateAggregate, got {other:?}"),
    }

    let ordered = roundtrip(
        "CREATE AGGREGATE my_rank (FLOAT8 ORDER BY FLOAT8) (sfunc = ordered_set_transition, stype = internal)",
    );
    match ordered {
        Statement::CreateAggregate(aggregate) => assert!(matches!(
            aggregate.signature,
            Some(AggregateArgs::OrderedSet { ref direct, ref ordered })
                if direct.len() == 1 && ordered.len() == 1
        )),
        other => panic!("Expected CreateAggregate, got {other:?}"),
    }

    let hypothetical = roundtrip(
        "CREATE AGGREGATE my_mode (ORDER BY anyelement) (sfunc = ordered_set_transition, stype = internal)",
    );
    match hypothetical {
        Statement::CreateAggregate(aggregate) => assert!(matches!(
            aggregate.signature,
            Some(AggregateArgs::OrderedSet { ref direct, ref ordered })
                if direct.is_empty() && ordered.len() == 1
        )),
        other => panic!("Expected CreateAggregate, got {other:?}"),
    }
}

#[test]
fn create_aggregate_accepts_an_operator_property_value() {
    roundtrip("CREATE AGGREGATE my_agg (INT4) (sfunc = int4smaller, stype = int4, sortop = <)");
}

// =============================================================================
// CREATE STATISTICS
// =============================================================================

#[test]
fn create_statistics_name_is_optional() {
    let unnamed = roundtrip("CREATE STATISTICS ON a, b FROM my_table");
    match unnamed {
        Statement::CreateStatistics(statistics) => {
            assert!(statistics.name.is_none());
            assert!(statistics.kinds.is_empty());
            assert_eq!(statistics.expressions.len(), 2);
        }
        other => panic!("Expected CreateStatistics, got {other:?}"),
    }

    let unnamed_with_kinds = roundtrip("CREATE STATISTICS (ndistinct, mcv) ON a, b FROM my_table");
    match unnamed_with_kinds {
        Statement::CreateStatistics(statistics) => {
            assert!(statistics.name.is_none());
            assert_eq!(statistics.kinds.len(), 2);
        }
        other => panic!("Expected CreateStatistics, got {other:?}"),
    }

    let named = roundtrip("CREATE STATISTICS IF NOT EXISTS my_stats (mcv) ON a, b FROM my_table");
    match named {
        Statement::CreateStatistics(statistics) => {
            assert_eq!(
                statistics.name.map(|name| name.to_string()),
                Some("my_stats".to_string())
            );
            assert!(statistics.if_not_exists);
        }
        other => panic!("Expected CreateStatistics, got {other:?}"),
    }
}

// =============================================================================
// CREATE ROLE / GROUP
// =============================================================================

#[test]
fn create_role_accepts_the_obsolete_sysid_clause() {
    let statement = roundtrip("CREATE ROLE my_role SYSID 12345");
    match statement {
        Statement::CreateRole(role) => assert!(role.sysid.is_some()),
        other => panic!("Expected CreateRole, got {other:?}"),
    }
}

#[test]
fn create_role_records_the_password_encryption_spelling() {
    let encrypted = roundtrip("CREATE ROLE my_role ENCRYPTED PASSWORD 'secret'");
    match encrypted {
        Statement::CreateRole(role) => assert!(role.password_encryption.is_some()),
        other => panic!("Expected CreateRole, got {other:?}"),
    }

    // PostgreSQL parses UNENCRYPTED and rejects it when the statement runs.
    let unencrypted = roundtrip("CREATE ROLE my_role UNENCRYPTED PASSWORD 'secret'");
    match unencrypted {
        Statement::CreateRole(role) => assert!(role.password_encryption.is_some()),
        other => panic!("Expected CreateRole, got {other:?}"),
    }
}

#[test]
fn create_group_is_a_spelling_of_create_role() {
    let statement = one_statement_parses_to_pg(
        "CREATE GROUP my_group WITH NOLOGIN",
        "CREATE ROLE my_group NOLOGIN",
    );
    match statement {
        Statement::CreateRole(role) => assert_eq!(role.login, Some(false)),
        other => panic!("Expected CreateRole, got {other:?}"),
    }
}

// =============================================================================
// DROP
// =============================================================================

#[test]
fn drop_name_only_objects() {
    for (sql, object_type) in [
        (
            "DROP ACCESS METHOD IF EXISTS my_am CASCADE",
            ObjectType::AccessMethod,
        ),
        (
            "DROP COLLATION IF EXISTS my_coll RESTRICT",
            ObjectType::Collation,
        ),
        ("DROP CONVERSION my_conv CASCADE", ObjectType::Conversion),
        (
            "DROP EVENT TRIGGER IF EXISTS my_trig",
            ObjectType::EventTrigger,
        ),
        (
            "DROP LANGUAGE IF EXISTS plsample CASCADE",
            ObjectType::Language,
        ),
        ("DROP STATISTICS IF EXISTS a, b", ObjectType::Statistics),
    ] {
        let statement = roundtrip(sql);
        match statement {
            Statement::Drop {
                object_type: parsed,
                ..
            } => assert_eq!(parsed, object_type, "{sql}"),
            other => panic!("Expected Statement::Drop, got {other:?}"),
        }
    }
}

#[test]
fn drop_procedural_language_is_a_spelling_of_drop_language() {
    one_statement_parses_to_pg(
        "DROP PROCEDURAL LANGUAGE plsample",
        "DROP LANGUAGE plsample",
    );
}

#[test]
fn drop_group_is_a_spelling_of_drop_role() {
    let statement =
        one_statement_parses_to_pg("DROP GROUP IF EXISTS a, b", "DROP ROLE IF EXISTS a, b");
    match statement {
        Statement::Drop {
            object_type, names, ..
        } => {
            assert_eq!(object_type, ObjectType::Role);
            assert_eq!(names.len(), 2);
        }
        other => panic!("Expected Statement::Drop, got {other:?}"),
    }
}

#[test]
fn drop_index_concurrently() {
    let statement = roundtrip("DROP INDEX CONCURRENTLY IF EXISTS a, b");
    match statement {
        Statement::Drop {
            object_type,
            concurrently,
            if_exists,
            ..
        } => {
            assert_eq!(object_type, ObjectType::Index);
            assert!(concurrently);
            assert!(if_exists);
        }
        other => panic!("Expected Statement::Drop, got {other:?}"),
    }
}

#[test]
fn drop_database_force() {
    let statement = roundtrip("DROP DATABASE IF EXISTS my_db WITH (FORCE)");
    match statement {
        Statement::Drop { force, .. } => assert!(force),
        other => panic!("Expected Statement::Drop, got {other:?}"),
    }

    // The WITH keyword is noise.
    one_statement_parses_to_pg(
        "DROP DATABASE my_db (FORCE)",
        "DROP DATABASE my_db WITH (FORCE)",
    );
}

#[test]
fn drop_aggregate() {
    let statement = roundtrip("DROP AGGREGATE IF EXISTS my_agg(*), my_other(INT4) CASCADE");
    match statement {
        Statement::DropAggregate(drop) => {
            assert!(drop.if_exists);
            assert_eq!(drop.aggregates.len(), 2);
            assert!(matches!(drop.aggregates[0].args, AggregateArgs::Star));
            assert_eq!(drop.drop_behavior, Some(DropBehavior::Cascade));
        }
        other => panic!("Expected DropAggregate, got {other:?}"),
    }
}

#[test]
fn drop_operator() {
    let statement = roundtrip("DROP OPERATOR IF EXISTS <#> (INT4, INT8), <#> (NONE, INT4) CASCADE");
    match statement {
        Statement::DropOperator(drop) => {
            assert!(drop.if_exists);
            assert_eq!(drop.operators.len(), 2);
            assert_eq!(drop.operators[0].name.to_string(), "<#>");
            assert!(drop.operators[1].args.left.is_none());
        }
        other => panic!("Expected DropOperator, got {other:?}"),
    }
}

#[test]
fn drop_operator_class_and_family() {
    let class = roundtrip("DROP OPERATOR CLASS IF EXISTS my_opclass USING btree CASCADE");
    match class {
        Statement::DropOperatorClass(drop) => {
            assert!(!drop.family);
            assert!(drop.if_exists);
            assert_eq!(drop.using.value, "btree");
        }
        other => panic!("Expected DropOperatorClass, got {other:?}"),
    }

    let family = roundtrip("DROP OPERATOR FAMILY my_opfamily USING btree RESTRICT");
    match family {
        Statement::DropOperatorClass(drop) => {
            assert!(drop.family);
            assert_eq!(drop.drop_behavior, Some(DropBehavior::Restrict));
        }
        other => panic!("Expected DropOperatorClass, got {other:?}"),
    }
}

#[test]
fn drop_cast() {
    let statement = roundtrip("DROP CAST IF EXISTS (INT4 AS TEXT) CASCADE");
    match statement {
        Statement::DropCast(drop) => {
            assert!(drop.if_exists);
            assert_eq!(drop.signature.source_type.to_string(), "INT4");
        }
        other => panic!("Expected DropCast, got {other:?}"),
    }
}

#[test]
fn drop_routine() {
    let statement = roundtrip("DROP ROUTINE IF EXISTS my_routine(INT), my_other CASCADE");
    match statement {
        Statement::DropRoutine(drop) => {
            assert_eq!(drop.routines.len(), 2);
            assert!(drop.routines[0].args.is_some());
            assert!(drop.routines[1].args.is_none());
        }
        other => panic!("Expected DropRoutine, got {other:?}"),
    }
}

#[test]
fn drop_transform() {
    let statement = roundtrip("DROP TRANSFORM IF EXISTS FOR hstore LANGUAGE plpython3u CASCADE");
    match statement {
        Statement::DropTransform(drop) => {
            assert!(drop.if_exists);
            assert_eq!(drop.language.value, "plpython3u");
        }
        other => panic!("Expected DropTransform, got {other:?}"),
    }
}

#[test]
fn drop_publication_takes_several_names() {
    let statement = roundtrip("DROP PUBLICATION IF EXISTS a, b, c");
    match statement {
        Statement::DropPublication {
            name,
            additional_names,
            ..
        } => {
            assert_eq!(name.value, "a");
            assert_eq!(additional_names.len(), 2);
        }
        other => panic!("Expected DropPublication, got {other:?}"),
    }
}

// =============================================================================
// Ownership
// =============================================================================

#[test]
fn drop_owned_by() {
    let statement = roundtrip("DROP OWNED BY a, b CASCADE");
    match statement {
        Statement::DropOwned(drop) => {
            assert_eq!(drop.roles.len(), 2);
            assert_eq!(drop.drop_behavior, Some(DropBehavior::Cascade));
        }
        other => panic!("Expected DropOwned, got {other:?}"),
    }
}

#[test]
fn reassign_owned_by() {
    let statement = roundtrip("REASSIGN OWNED BY a, b TO c");
    match statement {
        Statement::ReassignOwned(reassign) => {
            assert_eq!(reassign.roles.len(), 2);
            assert_eq!(reassign.new_role.value, "c");
        }
        other => panic!("Expected ReassignOwned, got {other:?}"),
    }
}

// =============================================================================
// Role grants
// =============================================================================

#[test]
fn grant_role_with_option_list() {
    let statement = renders_to(
        "GRANT a, b TO c WITH ADMIN TRUE, INHERIT FALSE, SET OPTION",
        "GRANT a, b TO c WITH admin TRUE, inherit FALSE, set OPTION",
    );
    match statement {
        Statement::GrantRole {
            role_options,
            with_admin_option,
            ..
        } => {
            assert_eq!(role_options.len(), 3);
            assert_eq!(role_options[0].value, RoleGrantOptionValue::True);
            assert_eq!(role_options[1].value, RoleGrantOptionValue::False);
            assert_eq!(role_options[2].value, RoleGrantOptionValue::Option);
            assert!(!with_admin_option);
        }
        other => panic!("Expected GrantRole, got {other:?}"),
    }
}

#[test]
fn grant_role_with_admin_option_still_sets_the_admin_flag() {
    let statement = renders_to(
        "GRANT a TO b WITH ADMIN OPTION GRANTED BY c",
        "GRANT a TO b WITH admin OPTION GRANTED BY c",
    );
    match statement {
        Statement::GrantRole {
            with_admin_option,
            granted_by,
            role_options,
            ..
        } => {
            assert!(with_admin_option);
            assert_eq!(granted_by.map(|g| g.value), Some("c".to_string()));
            assert_eq!(role_options.len(), 1);
        }
        other => panic!("Expected GrantRole, got {other:?}"),
    }
}

#[test]
fn revoke_role_option() {
    for (sql, option, admin) in [
        ("REVOKE admin OPTION FOR a FROM b", "admin", true),
        ("REVOKE inherit OPTION FOR a FROM b", "inherit", false),
        ("REVOKE set OPTION FOR a FROM b", "set", false),
    ] {
        let statement = roundtrip(sql);
        match statement {
            Statement::RevokeRole {
                option_for,
                admin_option_for,
                ..
            } => {
                assert_eq!(
                    option_for.map(|o| o.value),
                    Some(option.to_string()),
                    "{sql}"
                );
                assert_eq!(admin_option_for, admin, "{sql}");
            }
            other => panic!("Expected RevokeRole, got {other:?}"),
        }
    }
}

#[test]
fn revoke_grant_option_for_stays_a_privilege_revocation() {
    let statement = roundtrip("REVOKE GRANT OPTION FOR SELECT ON my_table FROM b CASCADE");
    match statement {
        Statement::Revoke {
            grant_option_for, ..
        } => assert!(grant_option_for),
        other => panic!("Expected Revoke, got {other:?}"),
    }
}

#[test]
fn grant_on_large_object() {
    let statement = roundtrip("GRANT SELECT, UPDATE ON LARGE OBJECT 1, 2 TO my_role");
    match statement {
        Statement::Grant { objects, .. } => match objects {
            Some(GrantObjects::LargeObjects(oids)) => assert_eq!(oids.len(), 2),
            other => panic!("Expected LARGE OBJECT targets, got {other:?}"),
        },
        other => panic!("Expected Grant, got {other:?}"),
    }
}
