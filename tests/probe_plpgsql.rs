use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn try_parse(name: &str, sql: &str) -> bool {
    match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
        Ok(stmts) => {
            let display = stmts[0].to_string();
            match Parser::parse_sql(&PostgreSqlDialect {}, &display) {
                Ok(_) => {
                    eprintln!("PASS: {}", name);
                    true
                }
                Err(e) => {
                    eprintln!("ROUNDTRIP_FAIL: {} -> {}", name, e);
                    eprintln!("  DISPLAY: {}", display);
                    false
                }
            }
        }
        Err(e) => {
            eprintln!("FAIL: {} -> {}", name, e);
            false
        }
    }
}

#[test]
fn probe_roundtrip_debug() {
    try_parse("cf::while_basic", r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER := 0;
BEGIN
    WHILE i < 10 LOOP
        i := i + 1;
    END LOOP;
END $$ LANGUAGE plpgsql"#);

    try_parse("cf::for_range", r#"CREATE FUNCTION test() RETURNS void AS $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..10 LOOP
        RAISE NOTICE 'i = %', i;
    END LOOP;
END $$ LANGUAGE plpgsql"#);
}
