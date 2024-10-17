use crate::parser::Command;
use log::debug;
use sqlparser::ast::FunctionArgExpr::Expr as OtherExpr;
use sqlparser::ast::{Expr, FunctionArg, FunctionArguments, Offset, Query, Select, SelectItem, SetExpr, Statement, Value};
use sqlparser::ast::OnInsert::DuplicateKeyUpdate;

fn selection_changer(selection: &mut Expr) -> &mut Expr {
    debug!("Selection Changer: {:?}", selection);
    match selection {
        Expr::UnaryOp {  expr, .. } => {
            return selection_changer(expr)
        }
        Expr::BinaryOp { left, right, .. } => {
            *left = Box::new(selection_changer(left).to_owned());
            *right = Box::new(selection_changer(right).to_owned());
        }
        Expr::Like { pattern, .. } => {
            *pattern = Box::new(selection_changer(pattern).to_owned());
        }
        Expr::Value(value) => {
            *value = Value::Placeholder("?".to_string());
        }
        Expr::InList { list, .. } => {
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
        Expr::Function(function) => match function.args {
            FunctionArguments::Subquery(ref mut query) => {
                *query = Box::new(matcher(query).to_owned());
            }
            FunctionArguments::List(ref mut falist) => {
                for arg in falist.args.iter_mut() {
                    match arg {
                        FunctionArg::Unnamed(ref mut f_arg) => {
                            match f_arg {
                                OtherExpr(f_arg_expr) => {
                                    *f_arg_expr = selection_changer(f_arg_expr).to_owned();
                                }
                                _ => {}
                            }
                            /*let OtherExpr(f_arg_expr) = f_arg else {
                                panic!("{}", f_arg)
                            };
                            {
                                *f_arg_expr = selection_changer(f_arg_expr).to_owned();
                            }*/
                        }
                        FunctionArg::Named { ref mut arg, .. } => {
                            match arg {
                                OtherExpr(f_arg_expr) => {
                                    *f_arg_expr = selection_changer(f_arg_expr).to_owned();
                                }
                                _ => {}
                            }
                            /*let OtherExpr(f_arg_expr) = arg else {
                                panic!("{}", arg)
                            };
                            {
                                *f_arg_expr = selection_changer(f_arg_expr).to_owned();
                            }*/
                        }
                    }
                }
            }
            FunctionArguments::None => {}
        },
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
            }
        }
        SetExpr::Select(select) => {
            let Select { selection, projection, having, .. } = select.as_mut();
            {
                for p in projection {
                    match p {
                        SelectItem::UnnamedExpr(expr) => {
                            *expr = selection_changer(expr).to_owned();
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            *expr = selection_changer(expr).to_owned();
                        }
                        SelectItem::QualifiedWildcard(_, _) => {}
                        SelectItem::Wildcard(_) => {}
                    }

                }
                if !selection.is_none() {
                    *selection = Some(selection_changer(selection.as_mut().unwrap()).to_owned());
                }
                if !having.is_none() {
                    *having = Some(selection_changer(having.as_mut().unwrap()).to_owned());
                }
            }
        }
        _ => (),
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
    pub statement: Statement,
}

pub fn rec(statement: &mut Statement) -> Replaced {
    debug!("rec: {:?}", statement);
    let typed;

    match statement {
        Statement::Query(query) => {
            *query = Box::new(matcher(query).to_owned());
            typed = Command::Query;
        }
        Statement::Explain {
            statement: explain_statement,
            ..
        } => {
            *explain_statement = Box::new(rec(explain_statement).statement.clone());
            typed = Command::Explain;
        }
        Statement::Insert(insert) => {
            insert.source = Some(Box::new(
                matcher(insert.source.to_owned().unwrap().as_mut()).to_owned(),
            ));
            if insert.on.is_some() {
                match insert.on.as_mut().unwrap() {
                    DuplicateKeyUpdate(_0)=> {
                        for ass in _0.iter_mut() {
                            ass.value = selection_changer(&mut ass.value.to_owned()).to_owned()
                        }
                        _0.to_owned()
                    },
                    _ => todo!(),
                };
            }
            typed = Command::Insert;
        }
        Statement::Update {
            selection,
            assignments,
            ..
        } => {
            if selection.is_some() {
                *selection = Some(selection_changer(selection.as_mut().unwrap()).clone());
            }

            for assigment in assignments.iter_mut() {
                assigment.value = selection_changer(&mut assigment.value).to_owned();
            }

            typed = Command::Update;
        }
        Statement::Delete(delete) => {
            if delete.selection.is_some() {
                delete.selection =
                    Some(selection_changer(delete.selection.as_mut().unwrap()).clone());
            }
            if delete.limit.is_some() {
                delete.limit = Some(selection_changer(delete.limit.as_mut().unwrap()).clone());
            }
            typed = Command::Delete;
        }
        _ => {
            typed = Command::Other;
        }
    };
    Replaced {
        statement_type: typed,
        statement: statement.clone(),
    }
}
