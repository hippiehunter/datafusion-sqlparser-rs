use sqlparser::ast::{GrantObjects, Statement};
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
