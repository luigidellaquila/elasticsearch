/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sqlesql;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.parser.SqlParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder builder = new StringBuilder();
        while (true) {
            String input = reader.readLine().trim();
            if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                return;
            }
            if (input.endsWith(";")) {
                builder.append(input, 0, input.length() - 1);
                System.out.println("---------------------------");
                System.out.println(traslate(builder.toString()));
                System.out.println("---------------------------");
                builder = new StringBuilder();
            } else {
                builder.append(input);
                builder.append(" ");
            }

        }

    }

    private static String traslate(String sql) {
        LogicalPlan parsed = new SqlParser().createStatement(sql);
        System.out.println(parsed);
        System.out.println("---------------------------");
        LogicalPlan esqlPlan = new SqlEsqlTranslator().toEsql(parsed);
        return QueryPrinter.toEsqlQueryString(esqlPlan, true);
    }
}
