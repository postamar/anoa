package com.adgear.anoa.tools.function;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaCounted;
import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.impl.AnoaFunctionBase;
import com.adgear.anoa.impl.AnoaRecordImpl;

import org.jooq.lambda.Unchecked;
import org.josql.utils.ExpressionEvaluator;

import java.util.function.Predicate;
import java.util.stream.Stream;

public class AnoaSqlWhereFilter<T> extends AnoaFunctionBase<T, T> {

  final public Predicate<T> predicate;

  public AnoaSqlWhereFilter(@NonNull ExpressionEvaluator expressionEvaluator) {
    this.predicate = Unchecked.predicate(expressionEvaluator::isTrue);
  }

  public AnoaSqlWhereFilter(@NonNull Class<T> klazz, @NonNull String whereClause) {
    this(Unchecked.supplier(() -> new ExpressionEvaluator(whereClause, klazz)).get());
  }

  @Override
  protected AnoaRecord<T> applyPresent(@NonNull AnoaRecord<T> record) {
    return predicate.test(record.asOptional().get())
           ? record
           : AnoaRecordImpl.createEmpty(record, Stream.of(AnoaCounted.get("FILTERED_OUT")));
  }
}
