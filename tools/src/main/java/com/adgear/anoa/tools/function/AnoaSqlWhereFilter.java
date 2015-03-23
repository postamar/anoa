package com.adgear.anoa.tools.function;

import checkers.nullness.quals.NonNull;

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
  public AnoaSqlWhereFilter(@NonNull ExpressionEvaluator expressionEvaluator) {
    this.expressionEvaluator = expressionEvaluator;
  }

  /**
   * @param klazz record Class object
   * @param whereClause SQL WHERE clause, without the WHERE keyword
   */
  public AnoaSqlWhereFilter(@NonNull Class<T> klazz, @NonNull String whereClause) {
    this(Unchecked.supplier(() -> new ExpressionEvaluator(whereClause, klazz)).get());
  }

  public boolean test(@NonNull T object) {
    try {
      return expressionEvaluator.isTrue(object);
    } catch (QueryExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public @NonNull CheckedPredicate<T> asChecked() {
    return expressionEvaluator::isTrue;
  }
}
