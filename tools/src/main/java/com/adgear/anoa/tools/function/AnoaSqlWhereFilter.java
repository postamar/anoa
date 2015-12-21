package com.adgear.anoa.tools.function;

import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedPredicate;
import org.josql.QueryExecutionException;
import org.josql.utils.ExpressionEvaluator;

import java.util.function.Predicate;

/**
 * A Predicate implementation which tests the specified SQL WHERE clause against the predicate
 * argument. The SQL library used here is JoSQL.
 *
 * @param <T> Value type
 */
public class AnoaSqlWhereFilter<T> implements Predicate<T> {

  final public ExpressionEvaluator expressionEvaluator;

  /**
   * @param expressionEvaluator a JoSQL ExpressionEvaluator instance.
   */
  public AnoaSqlWhereFilter(ExpressionEvaluator expressionEvaluator) {
    this.expressionEvaluator = expressionEvaluator;
  }

  /**
   * @param klazz       record Class object
   * @param whereClause SQL WHERE clause, without the WHERE keyword
   */
  public AnoaSqlWhereFilter(Class<T> klazz, String whereClause) {
    this(Unchecked.supplier(() -> new ExpressionEvaluator(whereClause, klazz)).get());
  }

  public boolean test(T object) {
    try {
      return expressionEvaluator.isTrue(object);
    } catch (QueryExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public CheckedPredicate<T> asChecked() {
    return expressionEvaluator::isTrue;
  }
}
