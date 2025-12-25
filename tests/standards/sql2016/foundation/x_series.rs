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

//! SQL:2016 X-Series (XML Support) Tests
//!
//! X010-X400: XML-related features (SQL/XML).
//!
//! ## Feature Coverage
//!
//! - X010-X016: XML type and attributes
//! - X020-X038: XMLConcat, XMLElement, XMLForest, XMLAgg, XMLComment, XMLPI, XMLText
//! - X040-X059: Table mapping (basic and advanced)
//! - X060-X072: XMLParse and XMLSerialize
//! - X090: XML document predicate
//! - X120-X121: XML parameters in routines
//! - X221: XML passing mechanism BY VALUE
//! - X301-X304: XMLTable
//! - X400: Name and identifier mapping
//! - X410: Alter column data type: XML type

use crate::standards::common::{one_statement_parses_to_std, verified_standard_stmt};

// ==================== X010-X016: XML Type ====================

#[test]
fn x010_01_xml_data_type() {
    // SQL:2016 X010: XML data type
    // Note: Parsed as Custom type, not a dedicated XML type
    verified_standard_stmt("CREATE TABLE t (doc XML)");
}

#[test]
fn x010_02_xml_column_definition() {
    // SQL:2016 X010: XML column in CREATE TABLE
    // Note: Parsed as Custom type
    verified_standard_stmt("CREATE TABLE docs (id INT, content XML)");
}

#[test]
fn x010_03_xml_type_with_constraints() {
    // SQL:2016 X010: XML type with constraints
    // Note: Parsed as Custom type
    verified_standard_stmt("CREATE TABLE t (doc XML NOT NULL)");
}

#[test]
fn x011_01_xml_sequence_type() {
    // SQL:2016 X011: XML sequence type
    // Note: Parsed as Custom type with modifiers
    verified_standard_stmt("CREATE TABLE t (doc XML(SEQUENCE))");
}

#[test]
fn x014_01_xml_document_type() {
    // SQL:2016 X014: XML(DOCUMENT) type
    // Note: Parsed as Custom type with modifiers
    verified_standard_stmt("CREATE TABLE t (doc XML(DOCUMENT))");
}

#[test]
fn x015_01_xml_content_type() {
    // SQL:2016 X015: XML(CONTENT) type
    // Note: Parsed as Custom type with modifiers
    verified_standard_stmt("CREATE TABLE t (doc XML(CONTENT))");
}

#[test]
fn x016_01_xml_schema_type() {
    // SQL:2016 X016: XML with schema
    // Note: Parsed as Custom type with modifiers, normalizes to comma-separated
    verified_standard_stmt("CREATE TABLE t (doc XML(DOCUMENT, XMLSCHEMA, schema_uri))");
}

// ==================== X020-X038: XML Constructor Functions ====================

#[test]
fn x020_01_xmlconcat_basic() {
    // SQL:2016 X020: XMLCONCAT function
    // Note: Parsed as a regular function
    verified_standard_stmt("SELECT XMLCONCAT(xml1, xml2) FROM t");
}

#[test]
fn x020_02_xmlconcat_multiple() {
    // SQL:2016 X020: XMLCONCAT with multiple arguments
    // Note: Parsed as a regular function
    verified_standard_stmt("SELECT XMLCONCAT(xml1, xml2, xml3, xml4)");
}

#[test]
fn x025_01_xmlelement_basic() {
    // SQL:2016 X025: XMLELEMENT function - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLELEMENT(NAME 'customer', name) FROM t");
}

#[test]
fn x025_02_xmlelement_with_attributes() {
    // SQL:2016 X025: XMLELEMENT with attributes - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLELEMENT(NAME 'customer', XMLATTRIBUTES(id AS 'id'), name) FROM t",
    );
}

#[test]
fn x025_03_xmlelement_nested() {
    // SQL:2016 X025: Nested XMLELEMENT - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLELEMENT(NAME 'order', XMLELEMENT(NAME 'item', product_name)) FROM t",
    );
}

#[test]
fn x030_01_xmlforest_basic() {
    // SQL:2016 X030: XMLFOREST function
    // Note: Parsed as a regular function
    verified_standard_stmt("SELECT XMLFOREST(name, address, city) FROM t");
}

#[test]
fn x030_02_xmlforest_with_aliases() {
    // SQL:2016 X030: XMLFOREST with AS aliases - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLFOREST(name AS 'customer_name', id AS 'customer_id') FROM t");
}

#[test]
fn x031_01_xmlagg_basic() {
    // SQL:2016 X031: XMLAGG aggregate function - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLAGG(XMLELEMENT(NAME 'item', name)) FROM t");
}

#[test]
fn x031_02_xmlagg_order_by() {
    // SQL:2016 X031: XMLAGG with ORDER BY - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLAGG(XMLELEMENT(NAME 'item', name) ORDER BY name) FROM t");
}

#[test]
fn x032_01_xmlcomment_basic() {
    // SQL:2016 X032: XMLCOMMENT function
    // Note: Parsed as a regular function
    verified_standard_stmt("SELECT XMLCOMMENT('This is a comment')");
}

#[test]
fn x032_02_xmlcomment_from_column() {
    // SQL:2016 X032: XMLCOMMENT from column
    // Note: Parsed as a regular function
    verified_standard_stmt("SELECT XMLCOMMENT(comment_text) FROM t");
}

#[test]
fn x034_01_xmlpi_basic() {
    // SQL:2016 X034: XMLPI (processing instruction) - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLPI(NAME 'php', 'echo \"Hello\";')");
}

#[test]
fn x034_02_xmlpi_no_value() {
    // SQL:2016 X034: XMLPI without value - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLPI(NAME 'xml-stylesheet')");
}

#[test]
fn x038_01_xmltext_basic() {
    // SQL:2016 X038: XMLTEXT function
    // Note: Parsed as a regular function
    verified_standard_stmt("SELECT XMLTEXT('Some text content')");
}

#[test]
fn x038_02_xmltext_from_column() {
    // SQL:2016 X038: XMLTEXT from column
    // Note: Parsed as a regular function
    verified_standard_stmt("SELECT XMLTEXT(description) FROM t");
}

// ==================== X060-X072: XMLParse and XMLSerialize ====================

#[test]
fn x060_01_xmlparse_document() {
    // SQL:2016 X060: XMLPARSE with DOCUMENT - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLPARSE(DOCUMENT '<root>data</root>')");
}

#[test]
fn x060_02_xmlparse_content() {
    // SQL:2016 X060: XMLPARSE with CONTENT - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLPARSE(CONTENT '<item>value</item>')");
}

#[test]
fn x060_03_xmlparse_from_column() {
    // SQL:2016 X060: XMLPARSE from column - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLPARSE(DOCUMENT xml_string) FROM t");
}

#[test]
fn x060_04_xmlparse_preserve_whitespace() {
    // SQL:2016 X060: XMLPARSE with PRESERVE WHITESPACE - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLPARSE(DOCUMENT '<root>  </root>' PRESERVE WHITESPACE)");
}

#[test]
fn x060_05_xmlparse_strip_whitespace() {
    // SQL:2016 X060: XMLPARSE with STRIP WHITESPACE - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLPARSE(DOCUMENT '<root>  </root>' STRIP WHITESPACE)");
}

#[test]
fn x065_01_xmlserialize_document() {
    // SQL:2016 X065: XMLSERIALIZE - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLSERIALIZE(DOCUMENT xml_col AS VARCHAR(1000)) FROM t");
}

#[test]
fn x065_02_xmlserialize_content() {
    // SQL:2016 X065: XMLSERIALIZE CONTENT - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLSERIALIZE(CONTENT xml_col AS CLOB) FROM t");
}

#[test]
fn x065_03_xmlserialize_with_encoding() {
    // SQL:2016 X065: XMLSERIALIZE with encoding - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLSERIALIZE(DOCUMENT xml_col AS VARCHAR(1000) ENCODING UTF8) FROM t",
    );
}

#[test]
fn x065_04_xmlserialize_version() {
    // SQL:2016 X065: XMLSERIALIZE with version - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLSERIALIZE(DOCUMENT xml_col AS VARCHAR(1000) VERSION '1.0') FROM t",
    );
}

#[test]
fn x070_01_xmlserialize_indent() {
    // SQL:2016 X070: XMLSERIALIZE with INDENT - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLSERIALIZE(DOCUMENT xml_col AS VARCHAR(1000) INDENT) FROM t");
}

#[test]
fn x070_02_xmlserialize_no_indent() {
    // SQL:2016 X070: XMLSERIALIZE with NO INDENT - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLSERIALIZE(DOCUMENT xml_col AS VARCHAR(1000) NO INDENT) FROM t",
    );
}

// ==================== X090: XML Document Predicate ====================

#[test]
fn x090_01_is_document() {
    // SQL:2016 X090: IS DOCUMENT predicate - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM t WHERE xml_col IS DOCUMENT");
}

#[test]
fn x090_02_is_not_document() {
    // SQL:2016 X090: IS NOT DOCUMENT - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM t WHERE xml_col IS NOT DOCUMENT");
}

#[test]
fn x090_03_is_content() {
    // SQL:2016 X090: IS CONTENT predicate - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM t WHERE xml_col IS CONTENT");
}

// ==================== X120-X121: XML Parameters in Routines ====================

#[test]
fn x120_01_xml_function_parameter() {
    // SQL:2016 X120: XML parameter in function
    // Note: XML parsed as Custom type
    verified_standard_stmt("CREATE FUNCTION process_xml(doc XML) RETURNS INTEGER RETURN 0");
}

#[test]
fn x120_02_xml_function_return() {
    // SQL:2016 X120: Function returning XML - NOT YET IMPLEMENTED
    verified_standard_stmt("CREATE FUNCTION get_xml() RETURNS XML RETURN XMLELEMENT(NAME 'root')");
}

#[test]
fn x121_01_xml_procedure_parameter() {
    // SQL:2016 X121: XML in procedure
    // Note: XML parsed as Custom type
    verified_standard_stmt("CREATE PROCEDURE proc(doc XML) AS BEGIN SELECT 1; END");
}

// ==================== X221: XML Passing Mechanism ====================

#[test]
fn x221_01_xml_by_value() {
    // SQL:2016 X221: XML passing BY VALUE - NOT YET IMPLEMENTED
    verified_standard_stmt("CREATE FUNCTION f(doc XML BY VALUE) RETURNS INTEGER RETURN 0");
}

#[test]
fn x221_02_xml_by_ref() {
    // SQL:2016 X221: XML passing BY REF - NOT YET IMPLEMENTED
    verified_standard_stmt("CREATE FUNCTION f(doc XML BY REF) RETURNS INTEGER RETURN 0");
}

// ==================== X301-X304: XMLTable ====================

#[test]
fn x301_01_xmltable_basic() {
    // SQL:2016 X301: XMLTABLE
    verified_standard_stmt(
        "SELECT * FROM XMLTABLE('/root/item' PASSING xml_doc COLUMNS name VARCHAR(50) PATH 'name')",
    );
}

#[test]
fn x301_02_xmltable_multiple_columns() {
    // SQL:2016 X301: XMLTABLE with multiple columns
    // Note: DECIMAL normalizes to remove space after comma
    verified_standard_stmt(
        "SELECT * FROM XMLTABLE('/root/item' PASSING xml_doc COLUMNS id INT PATH '@id', name VARCHAR(50) PATH 'name', price DECIMAL(10,2) PATH 'price')",
    );
}

#[test]
fn x301_03_xmltable_namespaces() {
    // SQL:2016 X301: XMLTABLE with namespaces
    verified_standard_stmt(
        "SELECT * FROM XMLTABLE(XMLNAMESPACES('http://example.com' AS ex), '/ex:root/ex:item' PASSING xml_doc COLUMNS name VARCHAR(50) PATH 'ex:name')",
    );
}

#[test]
fn x301_04_xmltable_default_on_empty() {
    // SQL:2016 X301: XMLTABLE with DEFAULT on empty - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM XMLTABLE('/root/item' PASSING xml_doc COLUMNS name VARCHAR(50) PATH 'name' DEFAULT 'N/A' ON EMPTY)");
}

#[test]
fn x301_05_xmltable_null_on_error() {
    // SQL:2016 X301: XMLTABLE with NULL on error - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM XMLTABLE('/root/item' PASSING xml_doc COLUMNS price DECIMAL PATH 'price' NULL ON ERROR)");
}

#[test]
fn x302_01_xmltable_ordinality() {
    // SQL:2016 X302: XMLTABLE with FOR ORDINALITY
    verified_standard_stmt(
        "SELECT * FROM XMLTABLE('/root/item' PASSING xml_doc COLUMNS seq FOR ORDINALITY, name VARCHAR(50) PATH 'name')",
    );
}

#[test]
fn x303_01_xmltable_nested_path() {
    // SQL:2016 X303: XMLTABLE with nested path
    // Note: XMLTYPE is parsed as a custom type
    verified_standard_stmt(
        "SELECT * FROM XMLTABLE('/root' PASSING xml_doc COLUMNS items XMLTYPE PATH 'items')",
    );
}

// ==================== X400: Name and Identifier Mapping ====================

#[test]
fn x400_01_xml_name_mapping() {
    // SQL:2016 X400: Name and identifier mapping in XML
    // This is typically handled implicitly by XML constructor functions
    // Testing with XMLELEMENT which should handle name mapping
    verified_standard_stmt("SELECT XMLELEMENT(NAME 'my-element', column_name) FROM t");
}

// ==================== X410: Alter Column Data Type (XML) ====================

#[test]
fn x410_01_alter_column_to_xml() {
    // SQL:2016 X410: ALTER COLUMN to XML type
    // Note: XML parsed as Custom type
    verified_standard_stmt("ALTER TABLE t ALTER COLUMN doc SET DATA TYPE XML");
}

#[test]
fn x410_02_alter_xml_column() {
    // SQL:2016 X410: Modify XML column
    // Note: XML parsed as Custom type with modifiers
    verified_standard_stmt("ALTER TABLE t ALTER COLUMN doc TYPE XML(DOCUMENT)");
}

// ==================== Comprehensive XML Tests ====================

#[test]
fn x_series_xml_in_select() {
    // SQL:2016 X-series: XML in SELECT statement - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT xml_col FROM t WHERE xml_col IS DOCUMENT");
}

#[test]
fn x_series_xml_construction() {
    // SQL:2016 X-series: Complex XML construction - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLELEMENT(NAME 'customer', XMLFOREST(id, name, address)) FROM t",
    );
}

#[test]
fn x_series_xml_aggregation() {
    // SQL:2016 X-series: XML aggregation - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLAGG(XMLELEMENT(NAME 'item', name) ORDER BY id) FROM t");
}

#[test]
fn x_series_xml_parse_serialize_chain() {
    // SQL:2016 X-series: Parse and serialize chain - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLSERIALIZE(CONTENT XMLPARSE(CONTENT xml_string) AS VARCHAR(1000)) FROM t",
    );
}

#[test]
fn x_series_xml_with_cte() {
    // SQL:2016 X-series: XML with CTE - NOT YET IMPLEMENTED
    verified_standard_stmt("WITH xml_data AS (SELECT XMLPARSE(DOCUMENT '<root/>') AS doc) SELECT XMLSERIALIZE(CONTENT doc AS VARCHAR) FROM xml_data");
}

#[test]
fn x_series_xml_subquery() {
    // SQL:2016 X-series: XML in subquery - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM docs WHERE doc IS DOCUMENT)",
    );
}

#[test]
fn x_series_xmltable_join() {
    // SQL:2016 X-series: XMLTABLE in JOIN
    verified_standard_stmt(
        "SELECT t.id, x.name FROM t JOIN XMLTABLE('/items/item' PASSING t.xml_data COLUMNS name VARCHAR(50) PATH 'name') AS x ON true",
    );
}

#[test]
fn x_series_xmltable_lateral() {
    // SQL:2016 X-series: XMLTABLE with LATERAL - NOT YET IMPLEMENTED
    one_statement_parses_to_std(
        "SELECT t.id, x.* FROM t, LATERAL XMLTABLE('/items/item' PASSING t.xml_data COLUMNS name VARCHAR(50) PATH 'name') x",
        "SELECT t.id, x.* FROM t, LATERAL XMLTABLE('/items/item' PASSING t.xml_data COLUMNS name VARCHAR(50) PATH 'name') AS x"
    );
}

#[test]
fn x_series_xml_insert() {
    // SQL:2016 X-series: XML in INSERT - NOT YET IMPLEMENTED
    verified_standard_stmt("INSERT INTO t (doc) VALUES (XMLPARSE(DOCUMENT '<root/>'))");
}

#[test]
fn x_series_xml_update() {
    // SQL:2016 X-series: XML in UPDATE - NOT YET IMPLEMENTED
    verified_standard_stmt("UPDATE t SET doc = XMLELEMENT(NAME 'updated', content) WHERE id = 1");
}

#[test]
fn x_series_nested_xml_construction() {
    // SQL:2016 X-series: Deeply nested XML construction - NOT YET IMPLEMENTED
    one_statement_parses_to_std(
        "SELECT XMLELEMENT(NAME 'order', XMLELEMENT(NAME 'customer', XMLFOREST(c.name, c.address)), XMLAGG(XMLELEMENT(NAME 'item', i.product))) FROM customers c JOIN items i ON c.id = i.customer_id GROUP BY c.id",
        "SELECT XMLELEMENT(NAME 'order', XMLELEMENT(NAME 'customer', XMLFOREST(c.name, c.address)), XMLAGG(XMLELEMENT(NAME 'item', i.product))) FROM customers AS c JOIN items AS i ON c.id = i.customer_id GROUP BY c.id"
    );
}

#[test]
fn x_series_xml_cast() {
    // SQL:2016 X-series: CAST to/from XML
    // Note: XML parsed as Custom type
    verified_standard_stmt("SELECT CAST(doc AS XML) FROM t");
}

#[test]
fn x_series_xml_null_handling() {
    // SQL:2016 X-series: XML with NULL handling - NOT YET IMPLEMENTED
    verified_standard_stmt(
        "SELECT XMLELEMENT(NAME 'item', XMLATTRIBUTES(NULL AS 'attr'), name) FROM t",
    );
}

#[test]
fn x_series_xml_comment_pi_combination() {
    // SQL:2016 X-series: Combining comments and processing instructions - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT XMLCONCAT(XMLCOMMENT('header'), XMLPI(NAME 'version', '1.0'), XMLELEMENT(NAME 'root', data)) FROM t");
}

#[test]
fn x_series_xmltable_complex() {
    // SQL:2016 X-series: Complex XMLTABLE query - NOT YET IMPLEMENTED
    verified_standard_stmt("SELECT * FROM XMLTABLE('/catalog/book' PASSING XMLPARSE(DOCUMENT xml_string) COLUMNS book_id INT PATH '@id', title VARCHAR(100) PATH 'title', author VARCHAR(100) PATH 'author', price DECIMAL(10,2) PATH 'price' DEFAULT 0.00 ON EMPTY, seq FOR ORDINALITY) AS books");
}

#[test]
fn x_series_xml_xpath_predicates() {
    // SQL:2016 X-series: XMLTABLE with XPath predicates
    verified_standard_stmt(
        "SELECT * FROM XMLTABLE('/root/item[@type=\"premium\"]' PASSING xml_doc COLUMNS name VARCHAR(50) PATH 'name')",
    );
}
