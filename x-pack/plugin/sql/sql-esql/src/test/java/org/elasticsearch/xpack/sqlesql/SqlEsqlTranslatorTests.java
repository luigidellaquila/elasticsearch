/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sqlesql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.parser.SqlParser;

import java.util.Locale;

public class SqlEsqlTranslatorTests extends ESTestCase {

    public void testSimple() {
        assertTranslation("select * from idx", "from idx | keep *");
        assertTranslation("select a, b, c from idx", "from idx | keep `a`, `b`, `c`");
        assertTranslation("select *, a, b from idx", "from idx | keep *, `a`, `b`");
    }

    public void testWhere() {
        assertTranslation("SELECT a, b FROM idx WHERE a = 1 AND b = 2", """
            FROM idx
            | WHERE `a` == 1 AND `b` == 2
            | KEEP `a`, `b`
            """);
        assertTranslation("SELECT a, b FROM idx WHERE a = 'foo' AND b = 2", """
            FROM idx
            | WHERE `a` == "foo" AND `b` == 2
            | KEEP `a`, `b`
            """);
        assertTranslation("SELECT a, b FROM idx WHERE a LIKE \'foo%\' AND b = 2", """
            FROM idx
            | WHERE `a` LIKE "foo*" AND `b` == 2
            | KEEP `a`, `b`
            """);
        assertTranslation("SELECT a, b FROM idx WHERE a LIKE \'foo%\' AND b = 2 or c RLIKE 'bla.*'", """
            FROM idx
            | WHERE `a` LIKE "foo*" AND `b` == 2 OR `c` RLIKE "bla.*"
            | KEEP `a`, `b`
            """);
    }

    public void testOrderBy() {
        assertTranslation("SELECT a, b FROM idx WHERE a = 1 AND b = 2 ORDER BY a ASC, b DESC", """
            FROM idx
            | WHERE `a` == 1 AND `b` == 2
            | SORT `a` ASC, `b` DESC
            | KEEP `a`, `b`
            """);
    }

    public void testGroupBy() {
        assertTranslation("select count(*) from person", """
            FROM person
            | STATS `count(*)` = count(*)
            """);
        assertTranslation("select count(*), name from person group by name", """
            FROM person
            | STATS `count(*)` = count(*) BY `name`
            | KEEP `count(*)`, `name`
            """);
        assertTranslation("select surname, count(*), name from person group by name, surname", """
            FROM person
            | STATS `count(*)` = count(*) BY `name`, `surname`
            | KEEP `surname`, `count(*)`, `name`
            """);
    }

    public void testSubqueries() {
        assertTranslation("select \"count(*)\", 1 + 2 from (select count(*) from person) where 1 > 3", """
            FROM person
            | STATS `count(*)` = count(*)
            | WHERE 1 > 3
            | EVAL `1 + 2` = 1 + 2
            | KEEP `count(*)`, `1 + 2`
            """);
        assertTranslation("select \"count(*)\", 1 + 2 as x from (select count(*) from person) where 1 > 3", """
            FROM person
            | STATS `count(*)` = count(*)
            | WHERE 1 > 3
            | EVAL `x` = 1 + 2
            | KEEP `count(*)`, `x`
            """);
        assertTranslation("""
            select age, max_salary from (
            select age, max(salary) as max_salary
            from person where
            job = "foo"
            having max_salary > 10000
            order by country
            ) where age > 30
            limit 10
                """, """
            FROM person
            | WHERE `job` == `foo`
            | STATS `max_salary` = max(`salary`) BY `age` = `age`
            | KEEP `age`, `max_salary`
            | WHERE `max_salary` > 10000
            | SORT `country` ASC
            | WHERE `age` > 30
            | KEEP `age`, `max_salary`
            | LIMIT 10
            """);
        assertTranslation("""
            select name, max_salary, age + 2 as age2 from (
                select name, max(salary) as max_salary, age
                from person
                where age = 40
                group by name, age
                having max_salary < 10000
                limit 10000
            )
            order by age desc, age2
            limit 100
                """, """
            FROM person
            | WHERE `age` == 40
            | STATS `max_salary` = max(`salary`) BY `name`, `age`
            | KEEP `name`, `max_salary`, `age`
            | WHERE `max_salary` < 10000
            | LIMIT 10000
            | EVAL `age2` = `age` + 2
            | SORT `age` DESC, `age2` ASC
            | KEEP `name`, `max_salary`, `age2`
            | LIMIT 100
            """);
    }

    private void assertTranslation(String from, String to) {
        to = to.replaceAll("\n", " ");
        to = to.replaceAll("\s+", " ");
        to = to.toLowerCase(Locale.ROOT).trim();
        LogicalPlan parsed = new SqlParser().createStatement(from);
        LogicalPlan esqlPlan = new SqlEsqlTranslator().toEsql(parsed);
        String result = QueryPrinter.toEsqlQueryString(esqlPlan, false);

        result = result.replaceAll("\n", " ");
        result = result.replaceAll("\s+", " ");
        result = result.toLowerCase(Locale.ROOT).trim();

        assertEquals(to, result);
    }
}
