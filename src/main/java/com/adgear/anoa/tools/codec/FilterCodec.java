package com.adgear.anoa.tools.codec;

import com.adgear.anoa.codec.base.CodecBase;
import com.adgear.anoa.provider.Provider;

import org.josql.QueryExecutionException;
import org.josql.QueryParseException;
import org.josql.utils.ExpressionEvaluator;

/**
 * Filters each provided record by testing it against a JoSQL WHERE clause, using java reflection.
 */
public class FilterCodec<R> extends CodecBase<R, R, FilterCodec.FilterCounters> {

  protected final ExpressionEvaluator expressionEvaluator;

  /**
   * @param whereClause A JoSQL WHERE clause.
   */
  public FilterCodec(Provider<R> provider, Class<R> klazz, String whereClause) {
    super(provider, FilterCounters.class);
    try {
      expressionEvaluator = new ExpressionEvaluator(whereClause, klazz);
    } catch (QueryParseException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the unchanged input if it satisfies the WHERE clause, null otherwise.
   */
  @Override
  public R transform(R input) {
    final boolean predicateResult;
    try {
      predicateResult = expressionEvaluator.isTrue(input);
    } catch (QueryExecutionException e) {
      throw new RuntimeException(e);
    }
    if (predicateResult) {
      return input;
    } else {
      increment(FilterCounters.FILTERED_OUT);
      return null;
    }
  }

  public static enum FilterCounters {
    /**
     * Counts records which did not satisfy the provided WHERE clause.
     */
    FILTERED_OUT
  }
}
