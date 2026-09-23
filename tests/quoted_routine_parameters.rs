use sqlparser::ast::{ArgMode, Ident, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn assert_identifier(name: &Ident, spelling: &str, quoted: bool) {
    assert_eq!(name.value, spelling);
    assert_eq!(name.quote_style, quoted.then_some('"'));
    assert!(name.span.start.column > 0);
}

#[test]
fn function_parameter_names_preserve_token_spelling_quotes_and_modes() {
    for (declaration, spelling, quoted, mode) in [
        ("MixedName integer", "mixedname", false, None),
        (r#""A Arg" integer"#, "A Arg", true, None),
        (
            r#"INOUT "A Arg" integer"#,
            "A Arg",
            true,
            Some(ArgMode::InOut),
        ),
        (
            r#""A Arg" INOUT integer"#,
            "A Arg",
            true,
            Some(ArgMode::InOut),
        ),
        (r#"OUT "A""B" integer"#, "A\"B", true, Some(ArgMode::Out)),
        (
            r#""SELECT" OUT integer"#,
            "SELECT",
            true,
            Some(ArgMode::Out),
        ),
    ] {
        let sql =
            format!("CREATE FUNCTION f({declaration}) RETURNS integer LANGUAGE sql AS 'SELECT 1'");
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, &sql).unwrap();
        let Statement::CreateFunction(function) = &statements[0] else {
            panic!("expected function")
        };
        let argument = &function.args.as_ref().unwrap()[0];
        assert_identifier(argument.name.as_ref().unwrap(), spelling, quoted);
        assert_eq!(argument.mode, mode);
        let rendered = statements[0].to_string();
        let replay = Parser::parse_sql(&PostgreSqlDialect {}, &rendered).unwrap();
        let Statement::CreateFunction(function) = &replay[0] else {
            panic!("expected function")
        };
        assert_identifier(
            function.args.as_ref().unwrap()[0].name.as_ref().unwrap(),
            spelling,
            quoted,
        );
    }
}

#[test]
fn procedure_parameter_names_preserve_token_spelling_quotes_and_modes() {
    for (declaration, spelling, quoted, mode) in [
        ("MixedName integer", "mixedname", false, None),
        (r#""A Arg" integer"#, "A Arg", true, None),
        (
            r#"INOUT "A Arg" integer"#,
            "A Arg",
            true,
            Some(ArgMode::InOut),
        ),
        (
            r#""A Arg" INOUT integer"#,
            "A Arg",
            true,
            Some(ArgMode::InOut),
        ),
        (r#"OUT "A""B" integer"#, "A\"B", true, Some(ArgMode::Out)),
        (
            r#""SELECT" OUT integer"#,
            "SELECT",
            true,
            Some(ArgMode::Out),
        ),
    ] {
        let sql =
            format!("CREATE PROCEDURE p({declaration}) LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, &sql).unwrap();
        let Statement::CreateProcedure { params, .. } = &statements[0] else {
            panic!("expected procedure")
        };
        let argument = &params.as_ref().unwrap()[0];
        assert_identifier(&argument.name, spelling, quoted);
        assert_eq!(argument.mode, mode);
        let rendered = statements[0].to_string();
        let replay = Parser::parse_sql(&PostgreSqlDialect {}, &rendered).unwrap();
        let Statement::CreateProcedure { params, .. } = &replay[0] else {
            panic!("expected procedure")
        };
        assert_identifier(&params.as_ref().unwrap()[0].name, spelling, quoted);
    }
}
