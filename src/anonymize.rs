use sqlparser::ast::{Expr, Query, Select, SetExpr, Statement, Value, Values, Offset, OrderByExpr, Function, FunctionArg, FunctionArgExpr};
use log::{debug};

fn selection_changer(selection: Option<Expr>) -> Option<Expr> {
    debug!("Selection Changer: {:?}", selection);
    match selection {
        Some(Expr::BinaryOp { left, op, right }) => {
            Some(Expr::BinaryOp {
                left: selection_changer(Some(*left)).map(Box::new).unwrap(),
                op,
                right: selection_changer(Some(*right)).map(Box::new).unwrap(),
            })
        }
        Some(Expr::Like { negated, expr, pattern, escape_char }) => {
            Some(Expr::Like {
                negated,
                expr,
                pattern: selection_changer(Some(*pattern)).map(Box::new).unwrap(),
                escape_char,
            })
        }
        Some(Expr::Value(_value)) => {
            Some(Expr::Value(Value::Placeholder("?".to_string())))
        }
        Some(Expr::InList { expr, list: _, negated }) => {
            Some(Expr::InList {
                expr,
                list: vec![Expr::Value(Value::Placeholder("?".to_string()))],
                negated,
            })
        }
        Some(Expr::Between { expr, negated, low, high }) => {
            Some(Expr::Between {
                expr,
                negated,
                low: selection_changer(Some(*low.clone())).map(Box::new).unwrap(),
                high: selection_changer(Some(*high.clone())).map(Box::new).unwrap(),
            })
        }
        Some(Expr::Subquery(query)) => {
            Some(Expr::Subquery(
                Box::new(matcher(&query))
            ))
        }
        Some(Expr::Nested(nested)) => {
            Some(Expr::Nested(
                Box::new(selection_changer(Some(*nested)).unwrap())
            ))
        }
        Some(Expr::Function(function)) => {
            let Function { name, mut args, over, distinct, special, order_by } = function;
            {
                if args.len() > 1 {
                    args = args.drain(0..2).collect();
                }


                for arg in args.iter_mut() {
                    *arg = match arg {
                        FunctionArg::Unnamed(f_arg) => {
                            FunctionArg::Unnamed(match f_arg {
                                FunctionArgExpr::Expr(f_arg_expr) => {
                                    FunctionArgExpr::Expr(selection_changer(Some(f_arg_expr.clone())).unwrap())
                                }
                                _ => {
                                    f_arg.clone()
                                }
                            })
                        }
                        FunctionArg::Named { name, arg } => {
                            FunctionArg::Named {
                                name: name.clone(),
                                arg: match arg {
                                    FunctionArgExpr::Expr(f_arg_expr) => {
                                        FunctionArgExpr::Expr(selection_changer(Some(f_arg_expr.clone())).unwrap())
                                    }
                                    _ => {
                                        arg.clone()
                                    }
                                },
                            }
                        }
                    }
                }


                Some(Expr::Function(Function {
                    name,
                    args,
                    over,
                    distinct,
                    special,
                    order_by,
                }))
            }
        }
        _ => {
            selection
        }
    }
}

fn matcher(query: &Query) -> Query {
    debug!("matcher: {:?}", query);
    let mut query = query.clone();
    let replaced_body = match &*query.body {
        SetExpr::Values(values) => {
            let mut replaced_rows = values.rows.clone();

            for xx in replaced_rows.iter_mut() {
                for yy in xx.iter_mut() {
                    *yy = selection_changer(Some(yy.clone())).unwrap();
                }
            };

            SetExpr::Values(Values { explicit_row: false, rows: replaced_rows })
        }
        SetExpr::Select(select) => {
            let select = select.clone();
            SetExpr::Select(Box::new(Select {
                distinct: select.distinct,
                top: None,
                projection: select.projection,
                into: None,
                from: select.from,
                // Add or modify other fields as needed
                // For example, you can add a WHERE clause as follows:
                lateral_views: select.lateral_views,
                selection: selection_changer(select.selection),
                // ...
                group_by: select.group_by,
                cluster_by: select.cluster_by,
                distribute_by: select.distribute_by,
                sort_by: select.sort_by,
                having: None,
                named_window: select.named_window,
                qualify: None,
            }))
        }

        _ => {
            *query.body
        }
    };
    let replaced_offset = match query.offset {
        Some(Offset { value, rows }) => {
            Some(Offset { value: selection_changer(Some(value)).unwrap(), rows })
        }
        _ => {
            query.offset
        }
    };

    for order_by in query.order_by.iter_mut() {
        *order_by = {
                OrderByExpr {
                    expr: selection_changer(Some(order_by.expr.clone())).unwrap(),
                    asc: order_by.asc,
                    nulls_first: order_by.nulls_first,
                }

        }
    }


    Query {
        with: query.with,
        body: Box::new(replaced_body),
        order_by: query.order_by,
        limit: selection_changer(query.limit),
        offset: replaced_offset,
        fetch: query.fetch,
        locks: query.locks,
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Replaced {
    pub statement_type: String,
    pub statement: Statement
}

pub fn rec(statement: &mut Statement) -> Replaced {
    debug!("rec: {:?}", statement);
    let typed;

    match statement {
        Statement::Query(query) => {
            *statement = Statement::Query(Box::new(matcher(query)));
            typed = "query";
        }
        Statement::Explain { describe_alias, analyze, verbose, statement: explain_statement, format } => {

            *statement = Statement::Explain {
                describe_alias: *describe_alias,
                analyze: *analyze,
                verbose: *verbose,
                statement: Box::new(rec(explain_statement).statement.clone()),
                format: *format
            };
            typed = "explain";
        }

        Statement::Insert { or, into, table_name, columns, overwrite, source, partitioned, after_columns, table, on, returning } => {

            *statement = Statement::Insert {
                or: *or,
                into: *into,
                table_name: table_name.clone(),
                columns: columns.to_vec(),
                overwrite: *overwrite,
                source: Box::new(matcher(source)),
                partitioned: partitioned.clone(),
                after_columns: after_columns.to_vec(),
                table: *table,
                on: on.clone(),
                returning: returning.clone(),
            };
            typed = "insert";
        }
        Statement::Update { table, assignments, from, selection, returning } => {
            *statement = Statement::Update {
                table: table.clone(),
                assignments: assignments.to_vec(),
                from: from.clone(),
                selection: selection_changer(selection.clone()),
                returning: returning.clone(),
            };
            typed = "update";
        },
        _ => {
            typed = "other";
        }
    };
    Replaced {
        statement_type: typed.to_string(),
        statement: statement.clone(),
    }
}
