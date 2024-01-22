/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sqlesql;

import org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.WildcardLike;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.plan.logical.Distinct;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.plan.logical.With;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SqlEsqlTranslator {

    SqlFunctionRegistry registry = new SqlFunctionRegistry();

    public LogicalPlan toEsql(LogicalPlan plan) {
        if (plan instanceof UnresolvedRelation) {
            return plan;
        } else if (plan instanceof With with) {
            return toEsql(with);
        } else if (plan instanceof Filter f) {
            return toEsql(f);
        } else if (plan instanceof Project p) {
            return toEsql(p);
        } else if (plan instanceof SubQueryAlias s) {
            return toEsql(s.child());
        } else if (plan instanceof OrderBy o) {
            return toEsql(o);
        } else if (plan instanceof Aggregate a) {
            return toEsql(a);
        } else if (plan instanceof Distinct d) {
            return toEsql(d);
        } else if (plan instanceof UnaryPlan unary) {
            return unary.replaceChild(toEsql(unary.child()));
        }
        return plan;
    }

    private LogicalPlan toEsql(With with) {
        if (with.subQueries().size() > 0) {
            throw new IllegalArgumentException("Subqueries are currently unsupported: " + with.sourceText());
        }
        return toEsql(with.child());
    }

    private Filter toEsql(Filter filter) {
        return ((Filter) filter.transformExpressionsUp(x -> toEsql(x))).replaceChild(toEsql(filter.child()));
    }

    private UnaryPlan toEsql(Project p) {
        List<Expression> aggregates = resolveAggFunctions(p.projections());
        if (aggregates.size() > 0) {
            List<Expression> groupings = new ArrayList<>(p.projections());
            groupings.removeAll(aggregates);
            Aggregate agg = new Aggregate(
                Source.EMPTY,
                p.child(),
                explicitAliases(groupings),
                (List<? extends NamedExpression>) (List) explicitAliases(aggregates)
            );
            if (groupings.size() == 0) {
                return agg;
            }

            return new Keep(
                Source.EMPTY,
                agg,
                p.projections()
                    .stream()
                    .map(x -> new UnresolvedAttribute(Source.EMPTY, x instanceof Alias al ? al.qualifiedName() : unquote(x.sourceText())))
                    .toList()
            );
        } else {
            List<Alias> aliases = new ArrayList<>();
            for (NamedExpression x : p.projections()) {
                if (x instanceof Alias a) {
                    aliases.add(new Alias(Source.EMPTY, x.name(), toEsql(a.child())));
                } else if (x instanceof UnresolvedAlias u
                    && u.child() instanceof UnresolvedAttribute == false
                    && u.child() instanceof UnresolvedStar == false) {
                        aliases.add(new Alias(Source.EMPTY, x.sourceText(), toEsql(x)));
                    }
            }

            if (aliases.isEmpty()) {
                return new Keep(Source.EMPTY, toEsql(p.child()), p.projections());
            }
            List<NamedExpression> newProjections = new ArrayList<>();
            for (NamedExpression x : p.projections()) {
                if (x instanceof Alias a) {
                    newProjections.add(new UnresolvedAttribute(Source.EMPTY, a.name()));
                } else {
                    newProjections.add(
                        new UnresolvedAttribute(Source.EMPTY, x instanceof Alias al ? al.qualifiedName() : unquote(x.sourceText()))
                    );
                }
            }
            return new Keep(Source.EMPTY, new Eval(Source.EMPTY, toEsql(p.child()), aliases), newProjections);
        }
    }

    private UnaryPlan toEsql(Distinct d) {
        // TODO this doesn't work for multi-values!!!
        Project p = findProject(d.child());
        if (p != null) {
            List<Expression> groupings = new ArrayList<>(
                p.projections().stream().map(x -> new UnresolvedAttribute(Source.EMPTY, x.sourceText())).toList()
            );
            return new Aggregate(Source.EMPTY, toEsql(p), explicitAliases(groupings), List.of());
        } else {
            return d;
        }
    }

    private LogicalPlan toEsql(OrderBy o) {
        LogicalPlan child = toEsql(o.child());
        if (child instanceof Project p) {
            return p.replaceChild(
                new OrderBy(Source.EMPTY, p.child(), o.order().stream().map(x -> x.replaceChildren(List.of(toEsql(x.child())))).toList())
            );
        }
        return new OrderBy(
            Source.EMPTY,
            toEsql(o.child()),
            o.order().stream().map(x -> x.replaceChildren(List.of(toEsql(x.child())))).toList()
        );
    }

    private LogicalPlan toEsql(Aggregate a) {
        List<Expression> aggregates = resolveAggFunctions(a.aggregates());
        ArrayList<NamedExpression> nonAggs = new ArrayList<>(a.aggregates());
        nonAggs.removeAll(aggregates);

        List<Expression> groupings = new ArrayList<>(a.groupings());
        groupings.removeAll(aggregates);
        for (NamedExpression nonAgg : nonAggs) {
            // TODO we can do better than checking source text
            if (groupings.stream().noneMatch(x -> x.sourceText().equals(nonAgg.sourceText()))) {
                groupings.add(nonAgg);
            }
        }
        Aggregate agg = new Aggregate(
            Source.EMPTY,
            toEsql(a.child()),
            explicitAliases(groupings),
            (List<? extends NamedExpression>) (List) explicitAliases(aggregates)
        );
        if (groupings.size() == 0) {
            return agg;
        }

        return new Keep(
            Source.EMPTY,
            agg,
            a.aggregates()
                .stream()
                .map(x -> new UnresolvedAttribute(Source.EMPTY, x instanceof Alias al ? al.qualifiedName() : unquote(x.sourceText())))
                .toList()
        );
    }

    private Expression toEsql(Expression exp) {
        if (exp instanceof Equals x) {
            return new org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals(
                Source.EMPTY,
                toEsql(x.left()),
                toEsql(x.right()),
                x.zoneId()
            );
        } else if (exp instanceof NotEquals x) {
            return new org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEquals(
                Source.EMPTY,
                toEsql(x.left()),
                toEsql(x.right()),
                x.zoneId()
            );
        } else if (exp instanceof GreaterThan x) {
            return new org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThan(
                Source.EMPTY,
                toEsql(x.left()),
                toEsql(x.right()),
                x.zoneId()
            );
        } else if (exp instanceof GreaterThanOrEqual x) {
            return new org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqual(
                Source.EMPTY,
                toEsql(x.left()),
                toEsql(x.right()),
                x.zoneId()
            );
        } else if (exp instanceof LessThan x) {
            return new org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThan(
                Source.EMPTY,
                toEsql(x.left()),
                toEsql(x.right()),
                x.zoneId()
            );
        } else if (exp instanceof LessThanOrEqual x) {
            return new org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqual(
                Source.EMPTY,
                toEsql(x.left()),
                toEsql(x.right()),
                x.zoneId()
            );
        } else if (exp instanceof Like l) {
            return new WildcardLike(Source.EMPTY, toEsql(l.field()), new WildcardPattern(l.pattern().asLuceneWildcard()));
        } else if (exp instanceof RLike l) {
            return new org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex.RLike(
                Source.EMPTY,
                toEsql(l.field()),
                l.pattern(),
                l.caseInsensitive()
            );
        }
        return exp.children().isEmpty() ? exp : exp.replaceChildren(exp.children().stream().map(this::toEsql).toList());
    }

    private List<Expression> explicitAliases(List<? extends Expression> list) {
        List<Expression> result = new ArrayList<>(list.size());
        for (Expression expression : list) {
            if (expression instanceof UnresolvedAlias ua) {
                result.add(new Alias(Source.EMPTY, unquote(ua.sourceText()), ua.child()));
            } else {
                result.add(expression);
            }
        }
        return result;
    }

    private String unquote(String sourceText) {
        if (sourceText.length() > 1 && sourceText.startsWith("\"") && sourceText.endsWith("\"")) {
            return sourceText.substring(1, sourceText.length() - 1);
        }
        return sourceText;
    }

    private Project findProject(LogicalPlan plan) {
        if (plan instanceof Project) {
            return (Project) plan;
        }
        if (plan instanceof Filter || plan instanceof Limit || plan instanceof OrderBy) {
            return (Project) ((UnaryPlan) plan).child();
        }
        return null;
    }

    private List<Expression> resolveAggFunctions(List<? extends NamedExpression> p) {
        // TODO this checks only the root, but aggs could (wrongly) be deeper down the tree
        List<Expression> result = new ArrayList<>();
        for (NamedExpression proj : p) {
            if (proj instanceof UnresolvedAlias a && a.child() instanceof UnresolvedFunction f) {
                FunctionDefinition resolved = registry.resolveFunction(f.name().toUpperCase(Locale.ROOT));
                if (AggregateFunction.class.isAssignableFrom(resolved.clazz())) {
                    result.add(a);
                }
            } else if (proj instanceof Alias a && a.child() instanceof UnresolvedFunction f) {
                FunctionDefinition resolved = registry.resolveFunction(f.name().toUpperCase(Locale.ROOT));
                if (AggregateFunction.class.isAssignableFrom(resolved.clazz())) {
                    result.add(a);
                }
            }
        }
        return result;
    }
}
