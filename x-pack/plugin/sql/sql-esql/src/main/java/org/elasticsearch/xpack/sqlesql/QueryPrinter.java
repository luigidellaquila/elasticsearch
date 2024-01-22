/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sqlesql;

import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.RLike;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.WildcardLike;
import org.elasticsearch.xpack.esql.plan.logical.EsqlUnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;

import java.util.Stack;
import java.util.stream.Collectors;

public class QueryPrinter {

    public static String toEsqlQueryString(LogicalPlan plan, boolean pretty) {
        Stack<LogicalPlan> stack = new Stack<>();
        while (true) {
            stack.push(plan);
            if (plan instanceof UnaryPlan unaryPlan) {
                plan = unaryPlan.child();
            } else {
                break;
            }
        }
        StringBuilder result = new StringBuilder();
        boolean first = true;
        while (stack.empty() == false) {
            if (first == false) {
                result.append(pretty ? "\n| " : " | ");
            }
            LogicalPlan node = stack.pop();
            nodeToEsql(node, result);
            first = false;
        }

        return result.toString();
    }

    private static void nodeToEsql(LogicalPlan node, StringBuilder output) {
        if (node instanceof UnresolvedRelation ur) {
            nodeToEsql(ur, output);
        } else if (node instanceof Filter f) {
            output.append("WHERE " + toEsqlString(f.condition()));
        } else if (node instanceof Keep k) {
            output.append("KEEP " + k.projections().stream().map(QueryPrinter::toEsqlString).collect(Collectors.joining(", ")));
        } else if (node instanceof Eval e) {
            output.append("EVAL " + e.expressions().stream().map(QueryPrinter::toEsqlString).collect(Collectors.joining(", ")));
        } else if (node instanceof Limit l) {
            output.append("LIMIT " + toEsqlString(l.limit()));
        } else if (node instanceof OrderBy o) {
            output.append("SORT " + o.order().stream().map(x -> toEsqlString(x)).collect(Collectors.joining(", ")));
        } else if (node instanceof Aggregate a) {
            output.append("STATS ");
            output.append(a.aggregates().stream().map(QueryPrinter::toEsqlString).collect(Collectors.joining(", ")));
            if (a.groupings().size() > 0) {
                output.append(" BY ");
                output.append(a.groupings().stream().map(QueryPrinter::toEsqlString).collect(Collectors.joining(", ")));
            }
        } else {
            output.append("TODO: " + node.getClass().getSimpleName());
        }
    }

    private static String toEsqlString(Order x) {
        String result = toEsqlString(x.child()) + " " + x.direction();
        result += switch (x.nullsPosition()) {
            case FIRST -> " NULLS FIRST";
            case LAST -> " NULLS LAST";
            case ANY -> "";
        };
        return result;
    }

    private static String toEsqlString(Expression exp) {

        if (exp instanceof WildcardLike rm) {
            return toEsqlString(rm.field()) + " LIKE \"" + rm.pattern().asLuceneWildcard() + "\"";
        } else if (exp instanceof RLike rm) {
            return toEsqlString(rm.field()) + " RLIKE \"" + rm.pattern().asJavaRegex() + "\"";
        } else if (exp instanceof BinaryPredicate bc) {
            return toEsqlString(bc.left()) + " " + bc.symbol() + " " + toEsqlString(bc.right());
        } else if (exp instanceof ArithmeticOperation ao) {
            return toEsqlString(ao.left()) + " " + ao.symbol() + " " + toEsqlString(ao.right());
        } else if (exp instanceof UnresolvedAttribute ua) {
            return "`" + ua.qualifiedName() + "`";
        } else if (exp instanceof UnresolvedAlias u) {
            return toEsqlString(u.child());
        } else if (exp instanceof UnresolvedStar u) {
            return "*";
        } else if (exp instanceof Alias a) {
            return "`" + a.qualifiedName() + "` = " + toEsqlString(a.child());
        } else if (exp instanceof UnresolvedFunction f) {
            return f.name() + "(" + f.arguments().stream().map(QueryPrinter::toEsqlString).collect(Collectors.joining(", ")) + ")";
        } else if (exp instanceof Literal l && l.fold() instanceof String) {
            return "\"" + l.fold() + "\"";
        }
        return exp.toString();
    }

    private static void nodeToEsql(UnresolvedRelation ur, StringBuilder output) {
        output.append("FROM ");
        output.append(ur.table().qualifiedIndex());
        if (ur instanceof EsqlUnresolvedRelation eur && eur.metadataFields() != null && eur.metadataFields().size() > 0) {
            output.append(" [");
            output.append(eur.metadataFields().stream().map(Attribute::qualifiedName).collect(Collectors.joining(", ")));
            output.append("]");
        }
    }
}
