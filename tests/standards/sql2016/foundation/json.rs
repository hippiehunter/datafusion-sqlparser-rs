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

//! SQL:2016 JSON Features (T801-T882) Tests
//!
//! JSON support introduced in SQL:2016 and enhanced in SQL:2023.
//!
//! ## Feature Coverage
//!
//! - T801: JSON data type
//! - T803: String-based JSON
//! - T811-T814: JSON constructor functions (JSON_OBJECT, JSON_ARRAY, JSON_OBJECTAGG, JSON_ARRAYAGG)
//! - T821-T829: JSON query operators (JSON_TABLE, JSON_QUERY, IS JSON)
//! - T830-T840: JSON path language features
//! - T851: Optional keywords for default syntax
//! - T865-T878: JSON item methods (bigint, boolean, date, decimal, etc.)
//! - T879-T882: JSON comparison operations

use crate::standards::common::{
    one_statement_parses_to_std, verified_standard_stmt,
};

// =============================================================================
// T801: JSON Data Type
// =============================================================================

mod t801_json_type {
    use super::*;

    #[test]
    fn t801_01_json_column() {
        // SQL:2016 T801-01: JSON data type
        verified_standard_stmt("CREATE TABLE t (data JSON)");
        verified_standard_stmt("CREATE TABLE docs (id INT, content JSON)");
    }

    #[test]
    fn t801_02_json_cast() {
        // SQL:2016 T801-02: CAST to JSON
        verified_standard_stmt("SELECT CAST('{}' AS JSON)");
        verified_standard_stmt("SELECT CAST('{\"key\": \"value\"}' AS JSON)");
    }

    #[test]
    fn t801_03_json_in_select() {
        // SQL:2016 T801: JSON in SELECT
        verified_standard_stmt("SELECT data FROM t WHERE id = 1");
        verified_standard_stmt("SELECT CAST(column AS JSON) FROM t");
    }
}

// =============================================================================
// T803: String-Based JSON
// =============================================================================

mod t803_string_json {
    use super::*;

    #[test]
    fn t803_01_json_from_string() {
        // SQL:2016 T803-01: String-based JSON values
        verified_standard_stmt("SELECT CAST('null' AS JSON)");
        verified_standard_stmt("SELECT CAST('true' AS JSON)");
        verified_standard_stmt("SELECT CAST('false' AS JSON)");
        verified_standard_stmt("SELECT CAST('123' AS JSON)");
        verified_standard_stmt("SELECT CAST('\"string\"' AS JSON)");
    }

    #[test]
    fn t803_02_json_objects_from_string() {
        // SQL:2016 T803-02: JSON objects from strings
        verified_standard_stmt("SELECT CAST('{}' AS JSON)");
        verified_standard_stmt("SELECT CAST('{\"a\": 1, \"b\": 2}' AS JSON)");
    }

    #[test]
    fn t803_03_json_arrays_from_string() {
        // SQL:2016 T803-03: JSON arrays from strings
        verified_standard_stmt("SELECT CAST('[]' AS JSON)");
        verified_standard_stmt("SELECT CAST('[1, 2, 3]' AS JSON)");
        verified_standard_stmt("SELECT CAST('[\"a\", \"b\", \"c\"]' AS JSON)");
    }
}

// =============================================================================
// T811: JSON_OBJECT Constructor
// =============================================================================

mod t811_json_object {
    use super::*;

    #[test]
    fn t811_01_json_object_empty() {
        // SQL:2016 T811-01: Empty JSON_OBJECT
        verified_standard_stmt("SELECT JSON_OBJECT()");
    }

    #[test]
    fn t811_02_json_object_key_value() {
        // SQL:2016 T811-02: JSON_OBJECT with key-value pairs
        verified_standard_stmt("SELECT JSON_OBJECT('key', 'value')");
        verified_standard_stmt("SELECT JSON_OBJECT('a', 1, 'b', 2)");
        verified_standard_stmt("SELECT JSON_OBJECT('name', name, 'age', age) FROM users");
    }

    #[test]
    fn t811_03_json_object_null_on_null() {
        // SQL:2016 T811-03: JSON_OBJECT with NULL ON NULL
        verified_standard_stmt("SELECT JSON_OBJECT('key', value NULL ON NULL)");
        verified_standard_stmt("SELECT JSON_OBJECT('a', a, 'b', b NULL ON NULL) FROM t");
    }

    #[test]
    fn t811_04_json_object_absent_on_null() {
        // SQL:2016 T811-04: JSON_OBJECT with ABSENT ON NULL
        verified_standard_stmt("SELECT JSON_OBJECT('key', value ABSENT ON NULL)");
        verified_standard_stmt("SELECT JSON_OBJECT('a', a, 'b', b ABSENT ON NULL) FROM t");
    }

    #[test]
    fn t811_05_json_object_with_check() {
        // SQL:2016 T811-05: JSON_OBJECT with WITH UNIQUE KEYS - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_OBJECT('a', 1 WITH UNIQUE KEYS)");
        verified_standard_stmt("SELECT JSON_OBJECT('a', 1, 'b', 2 WITH UNIQUE KEYS)");
    }

    #[test]
    fn t811_06_json_object_without_check() {
        // SQL:2016 T811-06: JSON_OBJECT with WITHOUT UNIQUE KEYS - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_OBJECT('a', 1 WITHOUT UNIQUE KEYS)");
    }

    #[test]
    fn t811_07_json_object_returning() {
        // SQL:2016 T811-07: JSON_OBJECT with RETURNING clause
        verified_standard_stmt("SELECT JSON_OBJECT('key', 'value' RETURNING JSON)");
        verified_standard_stmt("SELECT JSON_OBJECT('a', 1 RETURNING VARCHAR)");
    }
}

// =============================================================================
// T812: JSON_ARRAY Constructor
// =============================================================================

mod t812_json_array {
    use super::*;

    #[test]
    fn t812_01_json_array_empty() {
        // SQL:2016 T812-01: Empty JSON_ARRAY
        verified_standard_stmt("SELECT JSON_ARRAY()");
    }

    #[test]
    fn t812_02_json_array_values() {
        // SQL:2016 T812-02: JSON_ARRAY with values
        verified_standard_stmt("SELECT JSON_ARRAY(1)");
        verified_standard_stmt("SELECT JSON_ARRAY(1, 2, 3)");
        verified_standard_stmt("SELECT JSON_ARRAY('a', 'b', 'c')");
        verified_standard_stmt("SELECT JSON_ARRAY(name, age, city) FROM users");
    }

    #[test]
    fn t812_03_json_array_null_on_null() {
        // SQL:2016 T812-03: JSON_ARRAY with NULL ON NULL
        verified_standard_stmt("SELECT JSON_ARRAY(1, NULL, 3 NULL ON NULL)");
        verified_standard_stmt("SELECT JSON_ARRAY(a, b, c NULL ON NULL) FROM t");
    }

    #[test]
    fn t812_04_json_array_absent_on_null() {
        // SQL:2016 T812-04: JSON_ARRAY with ABSENT ON NULL
        verified_standard_stmt("SELECT JSON_ARRAY(1, NULL, 3 ABSENT ON NULL)");
        verified_standard_stmt("SELECT JSON_ARRAY(x, y, z ABSENT ON NULL) FROM t");
    }

    #[test]
    fn t812_05_json_array_returning() {
        // SQL:2016 T812-05: JSON_ARRAY with RETURNING clause
        verified_standard_stmt("SELECT JSON_ARRAY(1, 2, 3 RETURNING JSON)");
        verified_standard_stmt("SELECT JSON_ARRAY('a', 'b' RETURNING VARCHAR)");
    }
}

// =============================================================================
// T813: JSON_OBJECTAGG Aggregate
// =============================================================================

mod t813_json_objectagg {
    use super::*;

    #[test]
    fn t813_01_json_objectagg_basic() {
        // SQL:2016 T813-01: JSON_OBJECTAGG aggregate
        verified_standard_stmt("SELECT JSON_OBJECTAGG(key, value) FROM t");
        verified_standard_stmt("SELECT JSON_OBJECTAGG(name, score) FROM students GROUP BY class");
    }

    #[test]
    fn t813_02_json_objectagg_null_on_null() {
        // SQL:2016 T813-02: JSON_OBJECTAGG with NULL ON NULL
        verified_standard_stmt("SELECT JSON_OBJECTAGG(key, value NULL ON NULL) FROM t");
    }

    #[test]
    fn t813_03_json_objectagg_absent_on_null() {
        // SQL:2016 T813-03: JSON_OBJECTAGG with ABSENT ON NULL
        verified_standard_stmt("SELECT JSON_OBJECTAGG(key, value ABSENT ON NULL) FROM t");
    }

    #[test]
    fn t813_04_json_objectagg_with_unique() {
        // SQL:2016 T813-04: JSON_OBJECTAGG with WITH UNIQUE KEYS - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_OBJECTAGG(key, value WITH UNIQUE KEYS) FROM t");
    }

    #[test]
    fn t813_05_json_objectagg_returning() {
        // SQL:2016 T813-05: JSON_OBJECTAGG with RETURNING
        verified_standard_stmt("SELECT JSON_OBJECTAGG(key, value RETURNING JSON) FROM t");
    }
}

// =============================================================================
// T814: JSON_ARRAYAGG Aggregate
// =============================================================================

mod t814_json_arrayagg {
    use super::*;

    #[test]
    fn t814_01_json_arrayagg_basic() {
        // SQL:2016 T814-01: JSON_ARRAYAGG aggregate
        verified_standard_stmt("SELECT JSON_ARRAYAGG(value) FROM t");
        verified_standard_stmt(
            "SELECT category, JSON_ARRAYAGG(name) FROM products GROUP BY category",
        );
    }

    #[test]
    fn t814_02_json_arrayagg_order_by() {
        // SQL:2016 T814-02: JSON_ARRAYAGG with ORDER BY
        verified_standard_stmt("SELECT JSON_ARRAYAGG(value ORDER BY value) FROM t");
        verified_standard_stmt("SELECT JSON_ARRAYAGG(name ORDER BY name DESC) FROM employees");
    }

    #[test]
    fn t814_03_json_arrayagg_null_on_null() {
        // SQL:2016 T814-03: JSON_ARRAYAGG with NULL ON NULL
        verified_standard_stmt("SELECT JSON_ARRAYAGG(value NULL ON NULL) FROM t");
    }

    #[test]
    fn t814_04_json_arrayagg_absent_on_null() {
        // SQL:2016 T814-04: JSON_ARRAYAGG with ABSENT ON NULL
        verified_standard_stmt("SELECT JSON_ARRAYAGG(value ABSENT ON NULL) FROM t");
    }

    #[test]
    fn t814_05_json_arrayagg_returning() {
        // SQL:2016 T814-05: JSON_ARRAYAGG with RETURNING
        verified_standard_stmt("SELECT JSON_ARRAYAGG(value RETURNING JSON) FROM t");
    }
}

// =============================================================================
// T821: IS JSON Predicate
// =============================================================================

mod t821_is_json {
    use super::*;

    #[test]
    fn t821_01_is_json_basic() {
        // SQL:2016 T821-01: IS JSON predicate
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON");
        verified_standard_stmt("SELECT * FROM t WHERE content IS NOT JSON");
    }

    #[test]
    fn t821_02_is_json_value() {
        // SQL:2016 T821-02: IS JSON VALUE
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON VALUE");
        verified_standard_stmt("SELECT * FROM t WHERE data IS NOT JSON VALUE");
    }

    #[test]
    fn t821_03_is_json_object() {
        // SQL:2016 T821-03: IS JSON OBJECT
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON OBJECT");
        verified_standard_stmt("SELECT * FROM t WHERE data IS NOT JSON OBJECT");
    }

    #[test]
    fn t821_04_is_json_array() {
        // SQL:2016 T821-04: IS JSON ARRAY
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON ARRAY");
        verified_standard_stmt("SELECT * FROM t WHERE data IS NOT JSON ARRAY");
    }

    #[test]
    fn t821_05_is_json_scalar() {
        // SQL:2016 T821-05: IS JSON SCALAR
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON SCALAR");
        verified_standard_stmt("SELECT * FROM t WHERE data IS NOT JSON SCALAR");
    }

    #[test]
    fn t821_06_is_json_with_unique_keys() {
        // SQL:2016 T821-06: IS JSON with WITH UNIQUE KEYS
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON WITH UNIQUE KEYS");
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON OBJECT WITH UNIQUE KEYS");
    }

    #[test]
    fn t821_07_is_json_without_unique_keys() {
        // SQL:2016 T821-07: IS JSON with WITHOUT UNIQUE KEYS
        verified_standard_stmt("SELECT * FROM t WHERE data IS JSON WITHOUT UNIQUE KEYS");
    }
}

// =============================================================================
// T826: JSON_TABLE - Complex Table Function
// =============================================================================

mod t826_json_table {
    use super::*;

    #[test]
    fn t826_01_json_table_basic() {
        // SQL:2016 T826-01: JSON_TABLE
        // Note: COLUMNS formats without space before parentheses
        one_statement_parses_to_std(
            "SELECT * FROM JSON_TABLE(doc, '$.items[*]' COLUMNS (id INT PATH '$.id'))",
            "SELECT * FROM JSON_TABLE(doc, '$.items[*]' COLUMNS(id INT PATH '$.id'))",
        );
    }

    #[test]
    fn t826_02_json_table_multiple_columns() {
        // SQL:2016 T826-02: JSON_TABLE with multiple columns
        // Note: COLUMNS formats without space before parentheses
        one_statement_parses_to_std(
            "SELECT * FROM JSON_TABLE(data, '$.users[*]' COLUMNS (name VARCHAR(100) PATH '$.name', age INT PATH '$.age', city VARCHAR(50) PATH '$.city'))",
            "SELECT * FROM JSON_TABLE(data, '$.users[*]' COLUMNS(name VARCHAR(100) PATH '$.name', age INT PATH '$.age', city VARCHAR(50) PATH '$.city'))"
        );
    }

    #[test]
    fn t826_03_json_table_nested() {
        // SQL:2016 T826-03: JSON_TABLE with nested paths
        // Note: Outer COLUMNS has no space, but nested COLUMNS keeps space
        one_statement_parses_to_std(
            "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS (NESTED PATH '$.items[*]' COLUMNS (id INT PATH '$.id')))",
            "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS(NESTED PATH '$.items[*]' COLUMNS (id INT PATH '$.id')))"
        );
    }
}

// =============================================================================
// T828: JSON_QUERY Function
// =============================================================================

mod t828_json_query {
    use super::*;

    #[test]
    fn t828_01_json_query_basic() {
        // SQL:2016 T828-01: JSON_QUERY function
        verified_standard_stmt("SELECT JSON_QUERY(data, '$.items')");
    }

    #[test]
    fn t828_02_json_query_with_wrapper() {
        // SQL:2016 T828-02: JSON_QUERY with array wrapper - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_QUERY(data, '$.items' WITH WRAPPER)");
    }

    #[test]
    fn t828_03_json_query_without_wrapper() {
        // SQL:2016 T828-03: JSON_QUERY without wrapper - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_QUERY(data, '$.items' WITHOUT WRAPPER)");
    }

    #[test]
    fn t828_04_json_query_on_error() {
        // SQL:2016 T828-04: JSON_QUERY with error handling - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_QUERY(data, '$.items' NULL ON ERROR)");
    }

    #[test]
    fn t828_05_json_query_on_empty() {
        // SQL:2016 T828-05: JSON_QUERY with empty handling - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_QUERY(data, '$.items' NULL ON EMPTY)");
    }
}

// =============================================================================
// T829: JSON_VALUE Function
// =============================================================================

mod t829_json_value {
    use super::*;

    #[test]
    fn t829_01_json_value_basic() {
        // SQL:2016 T829-01: JSON_VALUE function
        verified_standard_stmt("SELECT JSON_VALUE(data, '$.name')");
    }

    #[test]
    fn t829_02_json_value_returning() {
        // SQL:2016 T829-02: JSON_VALUE with RETURNING
        verified_standard_stmt("SELECT JSON_VALUE(data, '$.age' RETURNING INT)");
    }

    #[test]
    fn t829_03_json_value_on_error() {
        // SQL:2016 T829-03: JSON_VALUE with error handling - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_VALUE(data, '$.name' DEFAULT 'unknown' ON ERROR)");
    }

    #[test]
    fn t829_04_json_value_on_empty() {
        // SQL:2016 T829-04: JSON_VALUE with empty handling - NOT YET IMPLEMENTED
        verified_standard_stmt("SELECT JSON_VALUE(data, '$.name' DEFAULT 'N/A' ON EMPTY)");
    }
}

// =============================================================================
// T830-T840: JSON Path Language
// =============================================================================

mod t830_json_path {
    use super::*;

    #[test]
    fn t830_01_simple_path() {
        // SQL:2016 T830: Simple JSON path expressions
        // JSON path is typically part of JSON_VALUE, JSON_QUERY, etc.
        // These are shown in the context where they would be used
        verified_standard_stmt("SELECT JSON_VALUE(data, '$')");
    }

    #[test]
    fn t830_02_member_access() {
        // SQL:2016 T830: Member access in path
        verified_standard_stmt("SELECT JSON_VALUE(data, '$.key')");
    }

    #[test]
    fn t830_03_array_index() {
        // SQL:2016 T830: Array indexing in path
        verified_standard_stmt("SELECT JSON_VALUE(data, '$.items[0]')");
    }

    #[test]
    fn t830_04_array_wildcard() {
        // SQL:2016 T830: Array wildcard in path
        verified_standard_stmt("SELECT JSON_QUERY(data, '$.items[*]')");
    }

    #[test]
    fn t830_05_recursive_descent() {
        // SQL:2016 T830: Recursive descent in path
        verified_standard_stmt("SELECT JSON_QUERY(data, '$..name')");
    }

    #[test]
    fn t830_06_filter_expression() {
        // SQL:2016 T830: Filter expressions in path
        verified_standard_stmt("SELECT JSON_QUERY(data, '$.items[?(@.price > 100)]')");
    }
}

// =============================================================================
// T865-T878: JSON Item Methods
// =============================================================================

mod t865_json_methods {
    use super::*;

    #[test]
    fn t865_01_json_bigint() {
        // SQL:2016 T865: JSON bigint method
        // Note: Parses as function call data.bigint()
        verified_standard_stmt("SELECT data.bigint() FROM t");
    }

    #[test]
    fn t870_01_json_boolean() {
        // SQL:2016 T870: JSON boolean method
        // Note: Parses as function call data.boolean()
        verified_standard_stmt("SELECT data.boolean() FROM t");
    }

    #[test]
    fn t872_01_json_decimal() {
        // SQL:2016 T872: JSON decimal method
        // Note: Parses as function call data.decimal()
        verified_standard_stmt("SELECT data.decimal() FROM t");
    }

    #[test]
    fn t876_01_json_string() {
        // SQL:2016 T876: JSON string method
        // Note: Parses as function call data.string()
        verified_standard_stmt("SELECT data.string() FROM t");
    }
}

// =============================================================================
// T879-T882: JSON Comparison
// =============================================================================

mod t879_json_comparison {
    use super::*;

    #[test]
    fn t879_01_json_equals() {
        // SQL:2016 T879: JSON equality comparison
        // Basic equality should work with JSON type
        verified_standard_stmt("SELECT * FROM t WHERE data = CAST('{}' AS JSON)");
    }

    #[test]
    fn t879_02_json_not_equals() {
        // SQL:2016 T879: JSON inequality comparison
        verified_standard_stmt("SELECT * FROM t WHERE data <> CAST('null' AS JSON)");
    }

    #[test]
    fn t879_03_json_ordering() {
        // SQL:2016 T879: JSON ordering comparison - Implementation-specific
        // Ordering of JSON values may not be supported in all databases
        verified_standard_stmt("SELECT * FROM t ORDER BY data");
    }
}

// =============================================================================
// Integration Tests
// =============================================================================

mod integration_tests {
    use super::*;

    #[test]
    fn json_constructors_combined() {
        // Combined JSON constructor functions
        verified_standard_stmt(
            "SELECT JSON_OBJECT('name', name, 'tags', JSON_ARRAY(tag1, tag2, tag3)) FROM products",
        );
        verified_standard_stmt(
            "SELECT JSON_OBJECT('user', name, 'data', JSON_OBJECT('age', age, 'city', city)) FROM users"
        );
    }

    #[test]
    fn json_aggregates_with_group_by() {
        // JSON aggregates in GROUP BY queries
        verified_standard_stmt(
            "SELECT category, JSON_ARRAYAGG(name ORDER BY name) AS names, JSON_OBJECTAGG(id, price) AS prices FROM products GROUP BY category"
        );
    }

    #[test]
    fn json_with_null_handling() {
        // Various NULL handling options
        // Note: JSON_OBJECT currently only supports NULL handling at the end of all arguments
        verified_standard_stmt("SELECT JSON_OBJECT('a', a, 'b', b NULL ON NULL) FROM t");
        verified_standard_stmt("SELECT JSON_OBJECT('a', a, 'b', b ABSENT ON NULL) FROM t");
        verified_standard_stmt(
            "SELECT JSON_ARRAY(x, y, z NULL ON NULL), JSON_ARRAY(x, y, z ABSENT ON NULL) FROM t",
        );
    }

    #[test]
    fn json_with_returning_clauses() {
        // RETURNING clauses in JSON functions
        verified_standard_stmt(
            "SELECT JSON_OBJECT('key', 'value' RETURNING JSON), JSON_ARRAY(1, 2, 3 RETURNING VARCHAR) FROM t"
        );
    }

    #[test]
    fn json_predicates_combined() {
        // Multiple IS JSON predicates
        verified_standard_stmt(
            "SELECT * FROM t WHERE data1 IS JSON OBJECT AND data2 IS JSON ARRAY",
        );
        verified_standard_stmt(
            "SELECT * FROM t WHERE data IS JSON OBJECT WITH UNIQUE KEYS OR data IS JSON ARRAY",
        );
    }

    #[test]
    fn json_in_complex_query() {
        // JSON functions in complex queries
        verified_standard_stmt(
            "WITH user_data AS (SELECT id, JSON_OBJECT('name', name, 'email', email) AS profile FROM users) SELECT ud.id, ud.profile FROM user_data AS ud WHERE ud.profile IS JSON OBJECT"
        );
    }

    #[test]
    fn json_cast_and_type() {
        // JSON type usage with CAST
        verified_standard_stmt("CREATE TABLE logs (id INT, event JSON, metadata JSON)");
        verified_standard_stmt("INSERT INTO logs VALUES (1, CAST('{\"type\": \"login\"}' AS JSON), CAST('null' AS JSON))");
        verified_standard_stmt("SELECT id, event, metadata FROM logs WHERE event IS JSON OBJECT");
    }

    // NOTE: JSON_ARRAYAGG as window function is not clearly defined in SQL:2016.
    // Removed test as it may not reflect standard behavior.

    #[test]
    fn nested_json_constructors() {
        // Deeply nested JSON construction
        verified_standard_stmt(
            "SELECT JSON_OBJECT('data', JSON_OBJECT('items', JSON_ARRAY(JSON_OBJECT('id', 1), JSON_OBJECT('id', 2))))"
        );
    }
}
