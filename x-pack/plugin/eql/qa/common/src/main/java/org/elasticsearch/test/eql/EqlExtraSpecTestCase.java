/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.List;

import static org.elasticsearch.test.eql.DataLoader.TEST_EXTRA_INDEX;

public abstract class EqlExtraSpecTestCase extends BaseEqlSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {
        return asArray(EqlSpecLoader.load("/test_extra.toml"));
    }

    // constructor for "local" rest tests
    public EqlExtraSpecTestCase(String query, String name, List<long[]> eventIds, String[] joinKeys) {
        this(TEST_EXTRA_INDEX, query, name, eventIds, joinKeys);
    }

    // constructor for multi-cluster tests
    public EqlExtraSpecTestCase(String index, String query, String name, List<long[]> eventIds, String[] joinKeys) {
        super(index, query, name, eventIds, joinKeys);
    }

    @Override
    protected String tiebreaker() {
        return "sequence";
    }
}
