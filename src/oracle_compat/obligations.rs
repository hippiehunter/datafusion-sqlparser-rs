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

#[derive(Debug, Clone, Copy)]
pub struct GrammarObligation {
    pub production: &'static str,
    pub scope: GrammarScope,
    pub positive_cases: &'static [&'static str],
    pub negative_cases: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrammarScope {
    OracleSpecific { isolation_case: &'static str },
    Shared,
}

pub const GRAMMAR_OBLIGATIONS: &[GrammarObligation] = &[
    GrammarObligation {
        production: "alternative_quoting",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "lex.string.alternative_quote",
        },
        positive_cases: &["lex.string.alternative_quote"],
        negative_cases: &["lex.alternative_quote.unclosed"],
    },
    GrammarObligation {
        production: "hierarchical_query_clause",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "select.hierarchical.start_first",
        },
        positive_cases: &["select.hierarchical.start_first"],
        negative_cases: &["query.hierarchy.missing_condition"],
    },
    GrammarObligation {
        production: "dml_returning_into_clause",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "insert.returning",
        },
        positive_cases: &["insert.returning", "update.returning", "delete.returning"],
        negative_cases: &["dml.returning.missing_target"],
    },
    GrammarObligation {
        production: "multitable_insert",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "insert.multitable_all",
        },
        positive_cases: &["insert.multitable_all", "insert.multitable_conditional"],
        negative_cases: &["dml.insert.multitable.missing_branch"],
    },
    GrammarObligation {
        production: "plsql_if_statement",
        scope: GrammarScope::Shared,
        positive_cases: &["plsql.if"],
        negative_cases: &["plsql.if.mismatched_end"],
    },
    GrammarObligation {
        production: "plsql_loop_statement",
        scope: GrammarScope::Shared,
        positive_cases: &["plsql.loop.basic", "plsql.loop.while"],
        negative_cases: &["plsql.loop.mismatched_end"],
    },
    GrammarObligation {
        production: "plsql_declaration",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "plsql.block.declare",
        },
        positive_cases: &["plsql.block.declare"],
        negative_cases: &["plsql.declaration.missing_terminator"],
    },
    GrammarObligation {
        production: "lock_table_statement",
        scope: GrammarScope::Shared,
        positive_cases: &["lock.table"],
        negative_cases: &["lock.invalid_mode"],
    },
    GrammarObligation {
        production: "drop_object_options",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "drop.domain",
        },
        positive_cases: &["drop.domain", "drop.tablespace", "drop.user.if_exists"],
        negative_cases: &[
            "drop.domain.option_order",
            "drop.tablespace.option_order",
            "drop.user.if_not_exists",
        ],
    },
    GrammarObligation {
        production: "audit_outcome_clause",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "audit.unified",
        },
        positive_cases: &["audit.unified"],
        negative_cases: &["audit.invalid_outcome"],
    },
    GrammarObligation {
        production: "create_analytic_view",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "create.analytic_view",
        },
        positive_cases: &["create.analytic_view"],
        negative_cases: &["create.missing_definition", "create.unknown_clause"],
    },
    GrammarObligation {
        production: "recursive_search_cycle",
        scope: GrammarScope::Shared,
        positive_cases: &["select.with.search_cycle", "select.with.search_breadth"],
        negative_cases: &[
            "query.cycle.missing_cycle_value",
            "query.cycle.missing_default_value",
        ],
    },
    GrammarObligation {
        production: "model_clause",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "select.model.for_list",
        },
        positive_cases: &[
            "select.model",
            "select.model.options_aliases",
            "select.model.iterate",
            "select.model.iterate_until_unparenthesized",
            "select.model.reference",
            "select.model.symbolic_multicell",
            "select.model.ordered_rule",
            "select.model.for_list",
            "select.model.for_range",
            "select.model.for_subquery",
            "select.model.for_multicolumn_list",
            "select.model.for_multicolumn_subquery",
        ],
        negative_cases: &[
            "query.model.missing_dimension",
            "query.model.empty_rules",
            "query.model.automatic_with_iterate",
            "query.model.reference_missing_query",
            "query.model.for_missing_dimension",
            "query.model.for_empty_list",
            "query.model.for_range_missing_direction",
            "query.model.for_multicolumn_empty_rows",
            "query.model.for_multicolumn_mixed_selector",
            "query.model.for_with_order_by",
        ],
    },
    GrammarObligation {
        production: "pivot_xml_clause",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "select.pivot.xml",
        },
        positive_cases: &["select.pivot.xml", "select.pivot.xml.subquery"],
        negative_cases: &["query.pivot_xml.static_values", "query.pivot_non_xml.any"],
    },
    GrammarObligation {
        production: "approximate_fetch_clause",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "select.fetch_approximate.next",
        },
        positive_cases: &["select.fetch_approximate.next"],
        negative_cases: &[
            "query.fetch_approximate.percent",
            "query.fetch_approximate.with_ties",
        ],
    },
    GrammarObligation {
        production: "group_by_vector",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "select.group_by.vector",
        },
        positive_cases: &[
            "select.group_by.vector",
            "select.group_by.vector.multicolumn",
        ],
        negative_cases: &[
            "query.group_by_vector.empty",
            "query.group_by_vector.unwrapped_member",
        ],
    },
    GrammarObligation {
        production: "json_exists_clauses",
        // PostgreSQL 18 accepts the same PASSING and ON ERROR clauses.
        scope: GrammarScope::Shared,
        positive_cases: &["select.json_exists", "select.json_exists.passing_unknown"],
        negative_cases: &[
            "query.json_exists.passing_missing_alias",
            "query.json_exists.passing_missing_expression",
        ],
    },
    GrammarObligation {
        production: "dml_error_logging",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "insert.error_logging",
        },
        positive_cases: &[
            "insert.error_logging",
            "update.error_logging",
            "merge.error_logging",
        ],
        negative_cases: &[
            "dml.error_logging.missing_errors",
            "dml.error_logging.invalid_clause_order",
        ],
    },
    GrammarObligation {
        production: "merge_action_conditions",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "merge.action_conditions",
        },
        positive_cases: &["merge.full", "merge.action_conditions"],
        negative_cases: &[
            "merge.update.delete_missing_where",
            "merge.insert.where_missing_condition",
        ],
    },
    GrammarObligation {
        production: "plsql_open_for",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "plsql.cursor.open_dynamic_using",
        },
        positive_cases: &[
            "plsql.cursor.open_for",
            "plsql.cursor.open_dynamic_using",
            "plsql.cursor.open_static_using",
        ],
        negative_cases: &[
            "plsql.open_dynamic.missing_using_argument",
            "plsql.open_static.missing_using_argument",
        ],
    },
    GrammarObligation {
        production: "trigger_referencing_clause",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "create.trigger.disabled_ordered_crossedition",
        },
        positive_cases: &[
            "create.trigger.referencing_aliases",
            "create.trigger.if_not_exists",
            "create.trigger.disabled_ordered_crossedition",
            "create.trigger.reverse_precedes",
            "create.trigger.call",
            "create.trigger.ddl_events",
            "create.trigger.database_after_events",
            "create.trigger.database_before_events",
            "create.trigger.named_schema_set_container",
        ],
        negative_cases: &[
            "plsql.trigger.referencing_empty",
            "plsql.trigger.referencing_duplicate_old",
            "plsql.trigger.when_without_row",
            "plsql.trigger.or_replace_if_not_exists",
            "plsql.trigger.crossedition_missing_kind",
            "plsql.trigger.referencing_with_call",
            "plsql.trigger.call_missing_routine",
            "plsql.trigger.associate_missing_statistics",
            "plsql.trigger.set_missing_container",
            "plsql.trigger.pluggable_missing_database",
            "plsql.trigger.unknown_event",
        ],
    },
    GrammarObligation {
        production: "external_table",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "create.table.external",
        },
        positive_cases: &[
            "create.table.external",
            "create.table.external.multilocation",
            "select.inline_external",
        ],
        negative_cases: &[
            "create.table.external.missing_directory",
            "create.table.external.missing_location",
            "select.inline_external.missing_columns",
        ],
    },
    GrammarObligation {
        production: "materialized_view_options",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "create.materialized_view.deferred",
        },
        positive_cases: &[
            "create.materialized_view",
            "create.materialized_view.deferred",
            "create.materialized_view_log.rowid",
        ],
        negative_cases: &[
            "create.materialized_view.build_missing_mode",
            "create.materialized_view_log.duplicate_option",
        ],
    },
    GrammarObligation {
        production: "special_table_kinds",
        scope: GrammarScope::OracleSpecific {
            isolation_case: "create.table.blockchain",
        },
        positive_cases: &[
            "create.table.private_temporary",
            "create.table.blockchain",
            "create.table.immutable",
        ],
        negative_cases: &[
            "create.table.private.missing_definition",
            "create.table.blockchain.missing_hashing",
            "create.table.immutable.invalid_hashing",
        ],
    },
];
