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

use sqlparser::dialect::OracleDialect;
use sqlparser::parser::Parser;

#[test]
fn anonymous_is_labels_do_not_consume_a_variable() {
    for pattern in [
        "(IS vertex)-[IS edge]->(IS vertex)",
        "(a IS vertex WHERE a.id = 1)-[IS edge]->{2,4}(b IS vertex)",
        "(\"IS\" IS vertex)-[\"IS\" IS edge]->(b IS vertex)",
    ] {
        let sql = format!("SELECT * FROM GRAPH_TABLE(g MATCH {pattern} COLUMNS (1 AS value))");
        let parsed = Parser::parse_sql(&OracleDialect {}, &sql).unwrap();
        assert_eq!(parsed.len(), 1, "{sql}");
        let rendered = parsed[0].to_string();
        let reparsed = Parser::parse_sql(&OracleDialect {}, &rendered).unwrap();
        assert_eq!(reparsed[0].to_string(), rendered);
    }
}
