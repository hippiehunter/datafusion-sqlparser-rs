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

//! A deterministic PostgreSQL corpus spanning OLTP, OLAP, and size curves.
//!
//! Literal values and identifiers are intentionally stable. Changing the
//! corpus changes the macro-benchmark, so corpus edits should not be mixed
//! with parser optimizations when comparing baselines.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    OltpRead,
    OltpWrite,
    Olap,
    PostgresNative,
    Tpch,
    Scaling,
}

impl Family {
    pub const ALL: [Self; 6] = [
        Self::OltpRead,
        Self::OltpWrite,
        Self::Olap,
        Self::PostgresNative,
        Self::Tpch,
        Self::Scaling,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OltpRead => "oltp_read",
            Self::OltpWrite => "oltp_write",
            Self::Olap => "olap",
            Self::PostgresNative => "postgres_native",
            Self::Tpch => "tpch",
            Self::Scaling => "scaling",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    Small,
    Medium,
    Large,
}

impl Tier {
    pub const ALL: [Self; 3] = [Self::Small, Self::Medium, Self::Large];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

#[derive(Debug)]
pub struct QueryCase {
    pub id: String,
    pub family: Family,
    pub tier: Tier,
    pub sql: String,
    pub sentinel: bool,
}

impl QueryCase {
    fn new(
        id: impl Into<String>,
        family: Family,
        tier: Tier,
        sql: impl Into<String>,
        sentinel: bool,
    ) -> Self {
        Self {
            id: id.into(),
            family,
            tier,
            sql: sql.into(),
            sentinel,
        }
    }
}

/// Build the fixed corpus. Construction happens outside every timed region.
pub fn postgres_corpus() -> Vec<QueryCase> {
    let mut cases = Vec::new();
    add_oltp_reads(&mut cases);
    add_oltp_writes(&mut cases);
    add_olap(&mut cases);
    add_postgres_native(&mut cases);
    add_tpch(&mut cases);
    add_scaling_curves(&mut cases);
    cases
}

fn push(
    cases: &mut Vec<QueryCase>,
    id: impl Into<String>,
    family: Family,
    tier: Tier,
    sql: impl Into<String>,
    sentinel: bool,
) {
    cases.push(QueryCase::new(id, family, tier, sql, sentinel));
}

fn add_oltp_reads(cases: &mut Vec<QueryCase>) {
    let projections = [
        ("all", "*"),
        ("narrow", "id, status, updated_at"),
        (
            "json_projection",
            "id, payload ->> 'kind' AS kind, payload #>> '{customer,id}' AS customer_id",
        ),
        (
            "computed_projection",
            "id, total_cents / 100.0 AS total, coalesce(discount_cents, 0) AS discount",
        ),
    ];
    let predicates = [
        ("primary_key", "id = $1"),
        ("tenant_key", "tenant_id = $1 AND id = $2"),
        ("natural_key", "email = $1"),
        ("any_array", "id = ANY($1::bigint[])"),
        ("null_safe", "external_id IS NOT DISTINCT FROM $1"),
        ("row_key", "(tenant_id, id) = ($1, $2)"),
        ("json_contains", "payload @> $1::jsonb"),
        ("time_range", "created_at >= $1 AND created_at < $2"),
    ];

    for (projection_id, projection) in projections {
        for (predicate_id, predicate) in predicates {
            push(
                cases,
                format!("oltp_read/point/{projection_id}/{predicate_id}"),
                Family::OltpRead,
                Tier::Small,
                format!("SELECT {projection} FROM app.orders WHERE {predicate} LIMIT 1"),
                projection_id == "narrow" && predicate_id == "tenant_key",
            );
        }
    }

    let reads = [
        (
            "keyset_pagination",
            Tier::Small,
            "SELECT id, created_at FROM app.orders WHERE tenant_id = $1 AND (created_at, id) < ($2, $3) ORDER BY created_at DESC, id DESC LIMIT 50",
            true,
        ),
        (
            "join_customer",
            Tier::Small,
            "SELECT o.id, o.status, c.display_name FROM app.orders AS o JOIN app.customers AS c ON c.id = o.customer_id WHERE o.tenant_id = $1 AND o.id = $2",
            false,
        ),
        (
            "exists_child",
            Tier::Medium,
            "SELECT o.id FROM app.orders AS o WHERE o.tenant_id = $1 AND EXISTS (SELECT 1 FROM app.order_items AS i WHERE i.order_id = o.id AND i.sku = $2)",
            false,
        ),
        (
            "not_exists_child",
            Tier::Medium,
            "SELECT o.id FROM app.orders AS o WHERE o.status = 'pending' AND NOT EXISTS (SELECT 1 FROM app.payments AS p WHERE p.order_id = o.id AND p.state = 'captured') LIMIT 100",
            false,
        ),
        (
            "lateral_latest",
            Tier::Medium,
            "SELECT o.id, event.kind FROM app.orders AS o LEFT JOIN LATERAL (SELECT e.kind FROM app.order_events AS e WHERE e.order_id = o.id ORDER BY e.created_at DESC LIMIT 1) AS event ON true WHERE o.tenant_id = $1",
            true,
        ),
        (
            "scalar_subquery",
            Tier::Medium,
            "SELECT o.id, (SELECT count(*) FROM app.order_items AS i WHERE i.order_id = o.id) AS item_count FROM app.orders AS o WHERE o.tenant_id = $1 AND o.id = $2",
            false,
        ),
        (
            "distinct_on",
            Tier::Medium,
            "SELECT DISTINCT ON (customer_id) customer_id, id, created_at FROM app.orders WHERE tenant_id = $1 ORDER BY customer_id, created_at DESC",
            false,
        ),
        (
            "queue_lock",
            Tier::Small,
            "SELECT id FROM app.jobs WHERE queue = $1 AND run_at <= now() ORDER BY priority DESC, run_at LIMIT 1 FOR UPDATE SKIP LOCKED",
            true,
        ),
        (
            "share_lock",
            Tier::Small,
            "SELECT id, version FROM app.documents WHERE tenant_id = $1 AND id = $2 FOR KEY SHARE NOWAIT",
            false,
        ),
        (
            "union_sources",
            Tier::Medium,
            "SELECT id, 'order' AS kind, created_at FROM app.orders WHERE customer_id = $1 UNION ALL SELECT id, 'refund' AS kind, created_at FROM app.refunds WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 25",
            false,
        ),
        (
            "cte_authorization",
            Tier::Medium,
            "WITH allowed AS (SELECT resource_id FROM auth.grants WHERE user_id = $1 AND permission = 'read') SELECT d.id, d.title FROM app.documents AS d JOIN allowed AS a ON a.resource_id = d.id WHERE d.tenant_id = $2",
            false,
        ),
        (
            "case_sort",
            Tier::Medium,
            "SELECT id, status FROM app.orders WHERE tenant_id = $1 ORDER BY CASE status WHEN 'failed' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END, created_at DESC LIMIT 100",
            false,
        ),
    ];
    for (id, tier, sql, sentinel) in reads {
        push(
            cases,
            format!("oltp_read/{id}"),
            Family::OltpRead,
            tier,
            sql,
            sentinel,
        );
    }
}

fn add_oltp_writes(cases: &mut Vec<QueryCase>) {
    let writes = [
        ("insert_one", Tier::Small, "INSERT INTO app.orders (tenant_id, customer_id, status, total_cents) VALUES ($1, $2, 'pending', $3) RETURNING id, created_at", true),
        ("insert_defaults", Tier::Small, "INSERT INTO app.jobs (queue, payload) VALUES ($1, $2::jsonb) RETURNING *", false),
        ("insert_multi", Tier::Medium, "INSERT INTO app.order_items (order_id, sku, quantity, unit_cents) VALUES ($1, $2, $3, $4), ($1, $5, $6, $7), ($1, $8, $9, $10) RETURNING id", false),
        ("insert_select", Tier::Medium, "INSERT INTO archive.orders SELECT * FROM app.orders WHERE tenant_id = $1 AND created_at < $2 ON CONFLICT DO NOTHING", false),
        ("upsert_replace", Tier::Medium, "INSERT INTO app.settings AS s (tenant_id, key, value, updated_at) VALUES ($1, $2, $3::jsonb, now()) ON CONFLICT (tenant_id, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at RETURNING s.*", true),
        ("upsert_constraint", Tier::Medium, "INSERT INTO app.users (tenant_id, email, name) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT users_tenant_email_key DO UPDATE SET name = excluded.name RETURNING id", false),
        ("update_point", Tier::Small, "UPDATE app.orders SET status = $3, updated_at = now() WHERE tenant_id = $1 AND id = $2 RETURNING id, version", false),
        ("update_arithmetic", Tier::Small, "UPDATE app.inventory SET available = available - $3, version = version + 1 WHERE warehouse_id = $1 AND sku = $2 AND available >= $3 RETURNING available", true),
        ("update_from", Tier::Medium, "UPDATE app.orders AS o SET customer_name = c.display_name FROM app.customers AS c WHERE c.id = o.customer_id AND o.tenant_id = $1 AND c.updated_at > o.updated_at RETURNING o.id", false),
        ("update_json", Tier::Medium, "UPDATE app.documents SET payload = jsonb_set(payload, '{status}', to_jsonb($3::text), true), version = version + 1 WHERE tenant_id = $1 AND id = $2 RETURNING payload", false),
        ("delete_point", Tier::Small, "DELETE FROM app.sessions WHERE tenant_id = $1 AND token_hash = $2 RETURNING id", false),
        ("delete_using", Tier::Medium, "DELETE FROM app.order_items AS i USING app.orders AS o WHERE o.id = i.order_id AND o.tenant_id = $1 AND o.created_at < $2 RETURNING i.id", false),
        ("soft_delete", Tier::Small, "UPDATE app.users SET deleted_at = now(), email = email || '.deleted.' || id::text WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL RETURNING id", false),
        ("cte_move", Tier::Medium, "WITH moved AS (DELETE FROM app.jobs WHERE queue = $1 AND finished_at < $2 RETURNING *) INSERT INTO archive.jobs SELECT * FROM moved RETURNING id", true),
        ("cte_claim", Tier::Large, "WITH candidate AS (SELECT id FROM app.jobs WHERE queue = $1 AND run_at <= now() ORDER BY priority DESC, run_at LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE app.jobs AS j SET worker_id = $2, started_at = now(), attempts = attempts + 1 FROM candidate AS c WHERE j.id = c.id RETURNING j.*", false),
        ("merge_inventory", Tier::Large, "MERGE INTO app.inventory AS target USING staging.inventory_delta AS source ON target.warehouse_id = source.warehouse_id AND target.sku = source.sku WHEN MATCHED THEN UPDATE SET available = target.available + source.delta WHEN NOT MATCHED THEN INSERT (warehouse_id, sku, available) VALUES (source.warehouse_id, source.sku, source.delta)", false),
        ("truncate_partition", Tier::Small, "TRUNCATE TABLE staging.events_2025_01 RESTART IDENTITY", false),
        ("transaction_batch", Tier::Medium, "BEGIN; SET LOCAL statement_timeout = '2s'; UPDATE app.accounts SET balance = balance - $1 WHERE id = $2; UPDATE app.accounts SET balance = balance + $1 WHERE id = $3; COMMIT", false),
    ];
    for (id, tier, sql, sentinel) in writes {
        push(
            cases,
            format!("oltp_write/{id}"),
            Family::OltpWrite,
            tier,
            sql,
            sentinel,
        );
    }
}

fn add_olap(cases: &mut Vec<QueryCase>) {
    let aggregates = [
        ("basic", "count(*) AS rows, sum(revenue) AS revenue"),
        ("filtered", "count(*) FILTER (WHERE status = 'paid') AS paid, sum(revenue) FILTER (WHERE channel = 'web') AS web_revenue"),
        ("distinct", "count(DISTINCT customer_id) AS customers, count(DISTINCT (customer_id, product_id)) AS customer_products"),
        ("statistics", "avg(revenue) AS mean, stddev_samp(revenue) AS stddev, variance(revenue) AS variance"),
        ("ordered_set", "percentile_cont(0.5) WITHIN GROUP (ORDER BY revenue) AS median, percentile_disc(0.95) WITHIN GROUP (ORDER BY revenue) AS p95"),
    ];
    let groups = [
        ("one_dimension", "tenant_id", "GROUP BY tenant_id"),
        (
            "time_bucket",
            "date_trunc('day', occurred_at) AS day",
            "GROUP BY date_trunc('day', occurred_at)",
        ),
        (
            "two_dimensions",
            "tenant_id, region",
            "GROUP BY tenant_id, region",
        ),
        (
            "rollup",
            "tenant_id, region",
            "GROUP BY ROLLUP (tenant_id, region)",
        ),
        (
            "grouping_sets",
            "tenant_id, region, channel",
            "GROUP BY GROUPING SETS ((tenant_id, region), (tenant_id, channel), ())",
        ),
    ];
    for (aggregate_id, aggregate) in aggregates {
        for (group_id, dimensions, group_by) in groups {
            push(
                cases,
                format!("olap/aggregate/{aggregate_id}/{group_id}"),
                Family::Olap,
                Tier::Medium,
                format!("SELECT {dimensions}, {aggregate} FROM analytics.events WHERE occurred_at >= $1 AND occurred_at < $2 {group_by} HAVING count(*) > 10 ORDER BY 1"),
                aggregate_id == "filtered" && group_id == "grouping_sets",
            );
        }
    }

    let window_functions = [
        ("ranking", "row_number() OVER ({window}) AS row_number, dense_rank() OVER ({window}) AS dense_rank"),
        ("running", "sum(revenue) OVER ({window} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue"),
        ("moving", "avg(revenue) OVER ({window} ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_average"),
        ("navigation", "lag(revenue, 1, 0) OVER ({window}) AS previous_revenue, lead(revenue) OVER ({window}) AS next_revenue"),
        ("distribution", "percent_rank() OVER ({window}) AS percentile, cume_dist() OVER ({window}) AS cumulative_distribution"),
    ];
    let windows = [
        ("global", "ORDER BY occurred_at"),
        ("tenant", "PARTITION BY tenant_id ORDER BY occurred_at"),
        (
            "tenant_product",
            "PARTITION BY tenant_id, product_id ORDER BY occurred_at",
        ),
        (
            "region_revenue",
            "PARTITION BY region ORDER BY revenue DESC",
        ),
        (
            "daily",
            "PARTITION BY date_trunc('day', occurred_at) ORDER BY occurred_at, id",
        ),
    ];
    for (function_id, function_template) in window_functions {
        for (window_id, window) in windows {
            let functions = function_template.replace("{window}", window);
            push(
                cases,
                format!("olap/window/{function_id}/{window_id}"),
                Family::Olap,
                Tier::Medium,
                format!("SELECT tenant_id, product_id, occurred_at, revenue, {functions} FROM analytics.events WHERE occurred_at >= $1"),
                function_id == "running" && window_id == "tenant_product",
            );
        }
    }

    let analytics = [
        ("star_join", Tier::Large, "SELECT d.year, d.month, p.category, c.region, sum(f.revenue) AS revenue FROM warehouse.fact_sales AS f JOIN warehouse.dim_date AS d ON d.date_key = f.date_key JOIN warehouse.dim_product AS p ON p.product_key = f.product_key JOIN warehouse.dim_customer AS c ON c.customer_key = f.customer_key WHERE d.year BETWEEN 2020 AND 2025 GROUP BY d.year, d.month, p.category, c.region", true),
        ("multi_cte", Tier::Large, "WITH daily AS (SELECT tenant_id, date_trunc('day', occurred_at) AS day, sum(revenue) AS revenue FROM analytics.events GROUP BY 1, 2), ranked AS (SELECT *, dense_rank() OVER (PARTITION BY tenant_id ORDER BY revenue DESC) AS rank FROM daily), totals AS (SELECT tenant_id, sum(revenue) AS total FROM daily GROUP BY tenant_id) SELECT r.tenant_id, r.day, r.revenue, r.revenue / nullif(t.total, 0) AS share FROM ranked AS r JOIN totals AS t USING (tenant_id) WHERE r.rank <= 10", true),
        ("correlated_subqueries", Tier::Large, "SELECT c.id, c.region, (SELECT sum(o.total) FROM sales.orders AS o WHERE o.customer_id = c.id) AS lifetime_value FROM sales.customers AS c WHERE EXISTS (SELECT 1 FROM sales.orders AS recent WHERE recent.customer_id = c.id AND recent.created_at >= current_date - interval '30 days') AND c.id IN (SELECT customer_id FROM sales.payments GROUP BY customer_id HAVING count(*) > 5)", false),
        ("recursive_graph", Tier::Large, "WITH RECURSIVE descendants(id, depth, path) AS (SELECT id, 0, ARRAY[id] FROM graph.nodes WHERE id = $1 UNION ALL SELECT edge.child_id, parent.depth + 1, parent.path || edge.child_id FROM descendants AS parent JOIN graph.edges AS edge ON edge.parent_id = parent.id WHERE NOT edge.child_id = ANY(parent.path)) SELECT id, depth, path FROM descendants ORDER BY depth, id", true),
        ("set_operations", Tier::Medium, "(SELECT customer_id FROM sales.orders WHERE created_at >= $1 INTERSECT SELECT customer_id FROM sales.payments WHERE state = 'captured') EXCEPT SELECT customer_id FROM compliance.blocked_customers ORDER BY customer_id", false),
        ("nested_derived", Tier::Large, "SELECT region, avg(customer_revenue) FROM (SELECT c.region, o.customer_id, sum(o.total) AS customer_revenue FROM sales.orders AS o JOIN sales.customers AS c ON c.id = o.customer_id GROUP BY c.region, o.customer_id) AS customer_totals WHERE customer_revenue > (SELECT avg(total) FROM sales.orders) GROUP BY region", false),
        ("qualify_emulation", Tier::Large, "SELECT * FROM (SELECT tenant_id, product_id, revenue, row_number() OVER (PARTITION BY tenant_id ORDER BY revenue DESC) AS rank FROM analytics.product_revenue) AS ranked WHERE rank <= 5 ORDER BY tenant_id, rank", false),
        ("named_windows", Tier::Medium, "SELECT tenant_id, occurred_at, sum(revenue) OVER w AS running, avg(revenue) OVER w7 AS moving FROM analytics.events WINDOW w AS (PARTITION BY tenant_id ORDER BY occurred_at), w7 AS (w ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)", false),
        ("cube", Tier::Medium, "SELECT region, channel, product_type, sum(revenue) FROM analytics.events GROUP BY CUBE (region, channel, product_type)", false),
        ("array_aggregate", Tier::Medium, "SELECT tenant_id, array_agg(DISTINCT product_id ORDER BY product_id) FILTER (WHERE product_id IS NOT NULL) AS products, jsonb_agg(payload ORDER BY occurred_at) AS events FROM analytics.events GROUP BY tenant_id", false),
        ("funnel", Tier::Large, "WITH steps AS (SELECT user_id, min(occurred_at) FILTER (WHERE event_name = 'view') AS viewed_at, min(occurred_at) FILTER (WHERE event_name = 'cart') AS carted_at, min(occurred_at) FILTER (WHERE event_name = 'purchase') AS purchased_at FROM analytics.events GROUP BY user_id) SELECT count(*) AS users, count(*) FILTER (WHERE carted_at > viewed_at) AS carted, count(*) FILTER (WHERE purchased_at > carted_at) AS purchased FROM steps", false),
        ("sessionization", Tier::Large, "WITH marked AS (SELECT user_id, occurred_at, CASE WHEN occurred_at - lag(occurred_at) OVER (PARTITION BY user_id ORDER BY occurred_at) > interval '30 minutes' THEN 1 ELSE 0 END AS new_session FROM analytics.events), numbered AS (SELECT *, sum(new_session) OVER (PARTITION BY user_id ORDER BY occurred_at) AS session_id FROM marked) SELECT user_id, session_id, min(occurred_at), max(occurred_at), count(*) FROM numbered GROUP BY user_id, session_id", false),
    ];
    for (id, tier, sql, sentinel) in analytics {
        push(
            cases,
            format!("olap/{id}"),
            Family::Olap,
            tier,
            sql,
            sentinel,
        );
    }
}

fn add_postgres_native(cases: &mut Vec<QueryCase>) {
    let queries = [
        ("json_path", Tier::Medium, "SELECT id, payload @@ '$.items[*] ? (@.price > 100)'::jsonpath AS has_expensive_item FROM app.orders WHERE payload @? '$.customer.address'", true),
        ("json_recordset", Tier::Medium, "SELECT o.id, item.sku, item.quantity FROM app.orders AS o CROSS JOIN LATERAL jsonb_to_recordset(o.payload -> 'items') AS item(sku text, quantity integer) WHERE o.tenant_id = $1", false),
        ("array_ops", Tier::Small, "SELECT id FROM app.products WHERE tags @> ARRAY['featured']::text[] AND category_ids && $1::bigint[] AND $2 = ANY(region_ids)", false),
        ("range_ops", Tier::Small, "SELECT id FROM app.bookings WHERE room_id = $1 AND occupied_during && tstzrange($2, $3, '[)') AND active_during @> now()", false),
        ("full_text", Tier::Medium, "SELECT id, ts_rank_cd(search_vector, websearch_to_tsquery('english', $1)) AS rank FROM app.documents WHERE search_vector @@ websearch_to_tsquery('english', $1) ORDER BY rank DESC LIMIT 20", true),
        ("regex_ilike", Tier::Small, "SELECT id FROM app.users WHERE display_name ILIKE $1 ESCAPE '\\' OR email ~* '^[a-z0-9._%+-]+@example\\.com$'", false),
        ("operators", Tier::Medium, "SELECT id, point <-> $1::point AS distance FROM geo.places WHERE labels ?| ARRAY['park', 'museum'] AND metadata ?& ARRAY['name', 'address'] ORDER BY point <-> $1::point LIMIT 10", false),
        ("generate_series", Tier::Medium, "SELECT bucket, count(e.id) FROM generate_series($1::timestamptz, $2::timestamptz, interval '1 hour') AS bucket LEFT JOIN analytics.events AS e ON e.occurred_at >= bucket AND e.occurred_at < bucket + interval '1 hour' GROUP BY bucket ORDER BY bucket", false),
        ("unnest_ordinality", Tier::Medium, "SELECT input.ordinality, input.id, p.name FROM unnest($1::bigint[]) WITH ORDINALITY AS input(id, ordinality) LEFT JOIN app.products AS p ON p.id = input.id ORDER BY input.ordinality", false),
        ("table_sample", Tier::Small, "SELECT id, payload FROM analytics.events TABLESAMPLE SYSTEM (1) REPEATABLE (42) WHERE occurred_at >= current_date - interval '7 days'", false),
        ("distinct_on_native", Tier::Medium, "SELECT DISTINCT ON (tenant_id, customer_id) tenant_id, customer_id, id, created_at FROM app.orders ORDER BY tenant_id, customer_id, created_at DESC", false),
        ("materialized_cte", Tier::Large, "WITH recent AS MATERIALIZED (SELECT * FROM analytics.events WHERE occurred_at >= now() - interval '1 day'), summary AS NOT MATERIALIZED (SELECT tenant_id, count(*) AS events FROM recent GROUP BY tenant_id) SELECT * FROM summary WHERE events > 100", false),
        ("aggregate_order", Tier::Medium, "SELECT tenant_id, string_agg(name, ', ' ORDER BY name) FILTER (WHERE active), array_agg(id ORDER BY created_at DESC) FROM app.users GROUP BY tenant_id", false),
        ("fetch_with_ties", Tier::Small, "SELECT id, score FROM search.results WHERE query_id = $1 ORDER BY score DESC FETCH FIRST 10 ROWS WITH TIES", false),
        ("row_constructor", Tier::Small, "SELECT id FROM app.versions WHERE (major, minor, patch) >= ROW($1, $2, $3) AND ROW(tenant_id, id) IN (ROW($4, $5), ROW($4, $6))", false),
        ("cast_timezone", Tier::Small, "SELECT id, created_at::date, created_at AT TIME ZONE 'UTC' AS utc_created_at FROM app.users WHERE created_at::date >= $1::date ORDER BY created_at AT TIME ZONE 'UTC'", false),
    ];
    for (id, tier, sql, sentinel) in queries {
        push(
            cases,
            format!("postgres_native/{id}"),
            Family::PostgresNative,
            tier,
            sql,
            sentinel,
        );
    }
}

fn add_tpch(cases: &mut Vec<QueryCase>) {
    const TPCH: [&str; 22] = [
        include_str!("../../tests/queries/tpch/1.sql"),
        include_str!("../../tests/queries/tpch/2.sql"),
        include_str!("../../tests/queries/tpch/3.sql"),
        include_str!("../../tests/queries/tpch/4.sql"),
        include_str!("../../tests/queries/tpch/5.sql"),
        include_str!("../../tests/queries/tpch/6.sql"),
        include_str!("../../tests/queries/tpch/7.sql"),
        include_str!("../../tests/queries/tpch/8.sql"),
        include_str!("../../tests/queries/tpch/9.sql"),
        include_str!("../../tests/queries/tpch/10.sql"),
        include_str!("../../tests/queries/tpch/11.sql"),
        include_str!("../../tests/queries/tpch/12.sql"),
        include_str!("../../tests/queries/tpch/13.sql"),
        include_str!("../../tests/queries/tpch/14.sql"),
        include_str!("../../tests/queries/tpch/15.sql"),
        include_str!("../../tests/queries/tpch/16.sql"),
        include_str!("../../tests/queries/tpch/17.sql"),
        include_str!("../../tests/queries/tpch/18.sql"),
        include_str!("../../tests/queries/tpch/19.sql"),
        include_str!("../../tests/queries/tpch/20.sql"),
        include_str!("../../tests/queries/tpch/21.sql"),
        include_str!("../../tests/queries/tpch/22.sql"),
    ];
    for (index, sql) in TPCH.into_iter().enumerate() {
        let number = index + 1;
        push(
            cases,
            format!("tpch/q{number:02}"),
            Family::Tpch,
            Tier::Large,
            postgres_tpch_sql(sql),
            number == 9,
        );
    }
}

/// Remove repository license preambles and translate the few TPC-H reference
/// interval literals to the unambiguous PostgreSQL input form. This work is
/// deliberately done while constructing the corpus, never in a timed region.
fn postgres_tpch_sql(source: &str) -> String {
    let mut sql = source.trim_start();
    while sql.starts_with("--") {
        sql = sql
            .split_once('\n')
            .map_or("", |(_, remainder)| remainder)
            .trim_start();
    }

    sql.replace("interval '90' day (3)", "interval '90 days'")
        .replace("interval '1' month", "interval '1 month'")
        .replace("interval '3' month", "interval '3 months'")
        .replace("interval '1' year", "interval '1 year'")
}

fn add_scaling_curves(cases: &mut Vec<QueryCase>) {
    for width in [8, 32, 128, 512] {
        let projection = (0..width)
            .map(|n| format!("coalesce(metric_{n}, 0) AS metric_{n}"))
            .collect::<Vec<_>>()
            .join(", ");
        push(
            cases,
            format!("scaling/wide_projection/{width:04}"),
            Family::Scaling,
            if width <= 32 {
                Tier::Medium
            } else {
                Tier::Large
            },
            format!("SELECT {projection} FROM analytics.wide_events WHERE tenant_id = $1"),
            width == 128,
        );
    }

    for terms in [8, 32, 128, 512] {
        let predicate = (0..terms)
            .map(|n| format!("attribute_{n} = {n}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        push(
            cases,
            format!("scaling/boolean_terms/{terms:04}"),
            Family::Scaling,
            if terms <= 32 {
                Tier::Medium
            } else {
                Tier::Large
            },
            format!("SELECT id FROM analytics.events WHERE {predicate}"),
            false,
        );
    }

    for joins in [2, 8, 32] {
        let mut sql = String::from("SELECT t0.id FROM warehouse.table_0 AS t0");
        for n in 1..=joins {
            sql.push_str(&format!(
                " JOIN warehouse.table_{n} AS t{n} ON t{n}.parent_id = t{}.id",
                n - 1
            ));
        }
        sql.push_str(" WHERE t0.tenant_id = $1");
        push(
            cases,
            format!("scaling/join_chain/{joins:04}"),
            Family::Scaling,
            if joins <= 2 {
                Tier::Medium
            } else {
                Tier::Large
            },
            sql,
            false,
        );
    }

    for depth in [2, 4, 8] {
        let mut expression = String::from("value");
        for n in 0..depth {
            expression = format!("coalesce(nullif({expression}, {n}), {n})");
        }
        push(
            cases,
            format!("scaling/expression_depth/{depth:04}"),
            Family::Scaling,
            Tier::Medium,
            format!("SELECT {expression} FROM analytics.events"),
            false,
        );
    }

    for rows in [8, 64, 512] {
        let values = (0..rows)
            .map(|n| format!("($1, 'sku-{n}', {n}, {n}00)"))
            .collect::<Vec<_>>()
            .join(", ");
        push(
            cases,
            format!("scaling/insert_rows/{rows:04}"),
            Family::Scaling,
            if rows <= 8 { Tier::Medium } else { Tier::Large },
            format!(
                "INSERT INTO app.order_items (order_id, sku, quantity, unit_cents) VALUES {values}"
            ),
            false,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    use super::*;

    #[test]
    fn every_corpus_entry_is_unique_and_parses_as_postgres() {
        let corpus = postgres_corpus();
        let mut ids = HashSet::new();

        assert!(corpus.len() >= 150, "corpus unexpectedly shrank");
        for case in &corpus {
            assert!(ids.insert(case.id.as_str()), "duplicate id: {}", case.id);
            assert!(!case.sql.trim().is_empty(), "empty SQL: {}", case.id);
        }

        // Rust's test harness gives each test worker a smaller stack than the
        // benchmark executable. The large scaling cases intentionally exercise
        // recursion, so validate on an explicit stack rather than making their
        // validity depend on the test runner's implementation detail.
        std::thread::Builder::new()
            .name("postgres-corpus-validation".into())
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                let dialect = PostgreSqlDialect {};
                for case in corpus {
                    if let Err(error) = Parser::parse_sql(&dialect, &case.sql) {
                        panic!(
                            "{} did not parse as PostgreSQL: {error}\n{}",
                            case.id, case.sql
                        );
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn every_family_tier_and_sentinel_set_is_populated() {
        let corpus = postgres_corpus();
        for family in Family::ALL {
            assert!(corpus.iter().any(|case| case.family == family));
        }
        for tier in Tier::ALL {
            assert!(corpus.iter().any(|case| case.tier == tier));
        }
        assert!(corpus.iter().filter(|case| case.sentinel).count() >= 10);
    }
}
