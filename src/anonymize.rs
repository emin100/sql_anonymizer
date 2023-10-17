
use sqlparser::ast::{Expr, Query, Select, SetExpr, Statement, Offset, Value, FunctionArg};
use log::{debug};
use sqlparser::ast::FunctionArgExpr::Expr as OtherExpr;
use crate::parser::Command;


fn selection_changer(selection: &mut Expr) -> &mut Expr {
    debug!("Selection Changer: {:?}", selection);
    match selection {
        Expr::BinaryOp { left, right, .. } => {
            *left = Box::new(selection_changer(left).to_owned());
            *right= Box::new(selection_changer(right).to_owned());
        },
        Expr::Like {  pattern, .. } => {
            *pattern = Box::new(selection_changer(pattern).to_owned());
        }
       Expr::Value(value) => {
            *value = Value::Placeholder("?".to_string());
        }
        Expr::InList { list , .. } => {
            *list = vec![Expr::Value(Value::Placeholder("?".to_string()))];
        }
        Expr::Between { low, high, .. } => {
            *low = Box::new(selection_changer(low).to_owned());
            *high = Box::new(selection_changer(high).to_owned());
        }
        Expr::Subquery(query) => {
            *query = Box::new(matcher(query).to_owned());
        }
        Expr::Nested(nested) => {
            *nested = Box::new(selection_changer(nested).to_owned());
        }
        Expr::Function(function) => {
            if function.args.len() > 1 {
                function.args = function.args.drain(0..2).collect();
            }

            for arg in function.args.iter_mut() {
                match arg {
                    FunctionArg::Unnamed(ref mut f_arg) => {
                        let OtherExpr(f_arg_expr) = f_arg else { panic!("{}", f_arg) };
                        {
                            *f_arg_expr = selection_changer(f_arg_expr).to_owned();
                        }
                    },
                    FunctionArg::Named { ref mut arg, .. } => {
                        let OtherExpr(f_arg_expr) = arg else { panic!("{}", arg) };
                        {
                            *f_arg_expr = selection_changer(f_arg_expr).to_owned();
                        }
                    }
                }
            }

        }
        _ => {}
    };
    selection
}

fn matcher(query: &mut Query) -> &mut Query {
    debug!("matcher: {:?}", query);

    match &mut *query.body {
        SetExpr::Values(values) => {
            for xx in values.rows.iter_mut() {
                for yy in xx.iter_mut() {
                    *yy = selection_changer(yy).to_owned();
                }
            };

        }
        SetExpr::Select(select) => {
            let Select { selection, .. } = select.as_mut();
            {
                if !selection.is_none() {
                    *selection = Some(selection_changer(selection.as_mut().unwrap()).to_owned());
                }
            }

        }
        _ => ()
    };
    if query.offset.is_some() {
        let Offset { value, .. } = query.offset.as_mut().unwrap();
        {
            *value = selection_changer(value).to_owned();
        }

    }

    for order_by in query.order_by.iter_mut() {
        order_by.expr = selection_changer(&mut order_by.expr).to_owned()
    }

    if query.limit.is_some() {
        query.limit = Some(selection_changer(query.limit.as_mut().unwrap()).to_owned());
    }

    query
}

#[derive(Debug, Clone)]
pub struct Replaced {
    pub statement_type: Command,
    pub statement: Statement
}

pub fn rec(statement: &mut Statement) -> Replaced {
    debug!("rec: {:?}", statement);
    let typed ;

    match statement {
        Statement::Query(query) => {
            *query = Box::new(matcher(query).to_owned());
            typed = Command::Query;
        },
        Statement::Explain { statement: explain_statement, .. } => {
            *explain_statement = Box::new(rec(explain_statement).statement.clone());
            typed = Command::Explain;
        },
        Statement::Insert {  source,.. } => {
            *source = Box::new(matcher(source).to_owned());
            typed = Command::Insert;
        },
        Statement::Update { selection,assignments, .. } => {

            *selection = Some(selection_changer(selection.as_mut().unwrap()).clone());

            for assigment in assignments.iter_mut() {
                assigment.value = selection_changer(&mut assigment.value).to_owned();
            }

            typed = Command::Update;
        },
        Statement::Delete { selection, .. } => {

            *selection = Some(selection_changer(selection.as_mut().unwrap()).clone());
            typed = Command::Delete;

        },
        _ => {
            typed = Command::Other;
        }
    };
    Replaced {
        statement_type: typed,
        statement: statement.clone(),
    }
}
