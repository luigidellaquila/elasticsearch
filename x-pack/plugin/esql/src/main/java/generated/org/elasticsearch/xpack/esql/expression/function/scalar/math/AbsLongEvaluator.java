// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import java.lang.Long;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link Abs}.
 * This class is generated. Do not edit it.
 */
public final class AbsLongEvaluator implements EvalOperator.ExpressionEvaluator {
  private final EvalOperator.ExpressionEvaluator fieldVal;

  public AbsLongEvaluator(EvalOperator.ExpressionEvaluator fieldVal) {
    this.fieldVal = fieldVal;
  }

  static Long fold(Expression fieldVal) {
    Object fieldValVal = fieldVal.fold();
    if (fieldValVal == null) {
      return null;
    }
    return Abs.process((long) fieldValVal);
  }

  @Override
  public Object computeRow(Page page, int position) {
    Object fieldValVal = fieldVal.computeRow(page, position);
    if (fieldValVal == null) {
      return null;
    }
    return Abs.process((long) fieldValVal);
  }

  @Override
  public String toString() {
    return "AbsLongEvaluator[" + "fieldVal=" + fieldVal + "]";
  }
}
