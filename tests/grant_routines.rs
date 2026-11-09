use sqlparser::ast::{ArgMode, GrantObjects, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn parse_statement(sql: &str) -> Statement {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    assert_eq!(statements.len(), 1);
    statements.pop().unwrap()
}

#[test]
fn parses_all_procedures_and_routines_in_schema() {
    let Statement::Grant { objects, .. } =
        parse_statement("GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA app, admin TO runner")
    else {
        panic!("expected GRANT statement");
    };
    assert!(matches!(
        objects,
        Some(GrantObjects::AllProceduresInSchema { schemas })
            if schemas.iter().map(ToString::to_string).collect::<Vec<_>>()
                == vec!["app".to_string(), "admin".to_string()]
    ));

    let Statement::Revoke { objects, .. } =
        parse_statement("REVOKE EXECUTE ON ALL ROUTINES IN SCHEMA app FROM runner")
    else {
        panic!("expected REVOKE statement");
    };
    assert!(matches!(
        objects,
        Some(GrantObjects::AllRoutinesInSchema { schemas })
            if schemas.iter().map(ToString::to_string).collect::<Vec<_>>()
                == vec!["app".to_string()]
    ));
}

#[test]
fn grant_and_revoke_on_function_procedure_routine_with_argument_names_and_modes() {
    // https://www.postgresql.org/docs/current/sql-grant.html
    for sql in [
        "GRANT ALL ON FUNCTION app.add(a INTEGER, b INTEGER) TO r",
        "GRANT ALL ON PROCEDURE p.pr(INOUT x INTEGER, IN y INTEGER) TO r",
        "GRANT EXECUTE ON ROUTINE p.f(INTEGER, TEXT) TO r",
        "GRANT EXECUTE ON FUNCTION app.add(INTEGER, INTEGER), app.neg(INTEGER) TO r",
        "GRANT EXECUTE ON FUNCTION app.nullary() TO r",
        "GRANT EXECUTE ON FUNCTION app.unique_name TO r",
        "REVOKE ALL ON FUNCTION app.add(a INTEGER, b INTEGER) FROM r",
        "REVOKE EXECUTE ON PROCEDURE p.pr(INOUT x INTEGER, IN y INTEGER) FROM r",
        "REVOKE EXECUTE ON ROUTINE p.f(INTEGER, TEXT) FROM r",
    ] {
        assert_eq!(parse_statement(sql).to_string(), sql);
    }

    let Statement::Grant { objects, .. } =
        parse_statement("GRANT ALL ON PROCEDURE p.pr(INOUT x INTEGER, IN y INTEGER) TO r")
    else {
        panic!("expected GRANT statement");
    };
    let Some(GrantObjects::Procedure(procedures)) = objects else {
        panic!("expected a procedure target");
    };
    let [procedure] = procedures.as_slice() else {
        panic!("expected one procedure");
    };
    assert_eq!(procedure.name.to_string(), "p.pr");
    let arguments = procedure.args.as_deref().unwrap_or_default();
    assert_eq!(
        arguments
            .iter()
            .map(|argument| (
                argument.mode.clone(),
                argument.name.as_ref().map(|name| name.value.clone())
            ))
            .collect::<Vec<_>>(),
        vec![
            (Some(ArgMode::InOut), Some("x".to_string())),
            (Some(ArgMode::In), Some("y".to_string())),
        ]
    );

    let Statement::Revoke { objects, .. } =
        parse_statement("REVOKE EXECUTE ON ROUTINE p.f(INTEGER, TEXT) FROM r")
    else {
        panic!("expected REVOKE statement");
    };
    assert!(matches!(
        objects,
        Some(GrantObjects::Routine(routines))
            if routines.len() == 1 && routines[0].args.as_ref().map(Vec::len) == Some(2)
    ));

    // Without an argument list the name stands alone, and an empty list
    // names the routine with no arguments.
    let Statement::Grant { objects, .. } =
        parse_statement("GRANT EXECUTE ON FUNCTION app.unique_name TO r")
    else {
        panic!("expected GRANT statement");
    };
    assert!(matches!(
        objects,
        Some(GrantObjects::Function(functions)) if functions[0].args.is_none()
    ));
}

#[test]
fn routine_keyword_followed_by_to_names_a_table() {
    // PostgreSQL reads `routine` here as a table name.
    let Statement::Grant { objects, .. } = parse_statement("GRANT SELECT ON routine TO r") else {
        panic!("expected GRANT statement");
    };
    assert!(matches!(
        objects,
        Some(GrantObjects::Tables(tables))
            if tables.iter().map(ToString::to_string).collect::<Vec<_>>() == vec!["routine"]
    ));
}
