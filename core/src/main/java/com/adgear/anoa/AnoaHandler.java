package com.adgear.anoa;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import com.adgear.anoa.write.WriteConsumer;

import org.jooq.lambda.fi.util.function.CheckedBiConsumer;
import org.jooq.lambda.fi.util.function.CheckedBiFunction;
import org.jooq.lambda.fi.util.function.CheckedConsumer;
import org.jooq.lambda.fi.util.function.CheckedFunction;
import org.jooq.lambda.fi.util.function.CheckedPredicate;
import org.jooq.lambda.fi.util.function.CheckedSupplier;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * Utility class for generating 'safe' variants of functions which use a provided exception handler.
 *
 * @param <M> Metadata type for decorating {@code Anoa} instances with.
 */
public class AnoaHandler<M> {

  /**
   * this instance's exception handler
   */
  final Function<Throwable, M[]> handler0;
  final BiFunction<Throwable, Object, M[]> handler1;
  final BiFunction<Throwable, Tuple2, M[]> handler2;

  public AnoaHandler(Function<Throwable, M[]> handler0) {
    this(handler0, ((t, __) -> handler0.apply(t)), ((t, __) -> handler0.apply(t)));
  }

  public AnoaHandler(Function<Throwable, M[]> handler0,
                     BiFunction<Throwable, Object, M[]> handler1,
                     BiFunction<Throwable, Tuple2, M[]> handler2) {
    this.handler0 = handler0;
    this.handler1 = handler1;
    this.handler2 = handler2;
  }

  static private final Object[] EMPTY_ARRAY = new Object[0];

  @SuppressWarnings("unchecked")
  static private <M> M[] single(M metadatum) {
    final M[] meta = (M[]) new Object[1];
    meta[0] = metadatum;
    return meta;
  }

  static public final AnoaHandler<?> ERASE
      = new AnoaHandler<>(__ -> EMPTY_ARRAY);
  static public final AnoaHandler<Throwable> NO_OP
      = new AnoaHandler<>(AnoaHandler::single);

  /**
   * @return An {@code AnoaHandler} in which exceptions are handled by applying a function to the
   * exception object and passing the result along as {@code Anoa} metadata.
   */
  static public <M> @NonNull AnoaHandler<M> withFn(
      @NonNull Function<Throwable, M> handler) {
    return new AnoaHandler<>(handler.andThen(AnoaHandler::single));
  }

  /**
   * @return An {@code AnoaHandler} in which exceptions are handled by applying a function to the
   * exception object and passing the result along as {@code Anoa} metadata.
   */
  static public <M> @NonNull AnoaHandler<M> withBiFn(
      @NonNull BiFunction<Throwable, Tuple, M> handler) {
    return new AnoaHandler<>(
        t -> single(handler.apply(t, Tuple.tuple())),
        (t, u) -> single(handler.apply(t, Tuple.tuple(u))),
        (t, t2) -> single(handler.apply(t, t2)));
  }

  public <T> @NonNull Supplier<Anoa<T, M>> supplier(@NonNull Supplier<? extends T> supplier) {
    Objects.requireNonNull(supplier);
    return () -> {
      try {
        return Anoa.of(supplier.get());
      } catch (Throwable throwable) {
        return handle(throwable);
      }
    };
  }

  public <T> @NonNull Supplier<Anoa<T, M>> supplierChecked(
      @NonNull CheckedSupplier<? extends T> supplier) {
    Objects.requireNonNull(supplier);
    return () -> {
      try {
        return Anoa.of(supplier.get());
      } catch (Throwable throwable) {
        return handle(throwable);
      }
    };
  }

  public <T, U> @NonNull Function<Anoa<U, M>, Anoa<T, M>> function(
      @NonNull Function<? super U, ? extends T> function) {
    Objects.requireNonNull(function);
    return (Anoa<U, M> uWrapped) -> (
        uWrapped.flatMap((U u) -> {
          try {
            return Anoa.of(function.apply(u));
          } catch (Throwable throwable) {
            return handle(throwable, u);
          }
        }));
  }

  public <T, U> @NonNull Function<Anoa<U, M>, Anoa<T, M>> functionChecked(
      @NonNull CheckedFunction<? super U, ? extends T> function) {
    Objects.requireNonNull(function);
    return (Anoa<U, M> uWrapped) -> (
        uWrapped.flatMap((U u) -> {
          try {
            return Anoa.of(function.apply(u));
          } catch (Throwable throwable) {
            return handle(throwable, u);
          }
        }));
  }

  public <T, U, V> @NonNull BiFunction<Anoa<U, M>, V, Anoa<T, M>> biFunction(
      @NonNull BiFunction<? super U, ? super V, ? extends T> biFunction) {
    Objects.requireNonNull(biFunction);
    return (Anoa<U, M> uWrapped, V v) -> (
        uWrapped.flatMap((U u) -> {
          try {
            return Anoa.of(biFunction.apply(u, v));
          } catch (Throwable throwable) {
            return handle(throwable, u, v);
          }
        }));
  }

  public <T, U, V> @NonNull BiFunction<Anoa<U, M>, V, Anoa<T, M>> biFunctionChecked(
      @NonNull CheckedBiFunction<? super U, ? super V, ? extends T> biFunction) {
    Objects.requireNonNull(biFunction);
    return (Anoa<U, M> uWrapped, V v) -> (
        uWrapped.flatMap((U u) -> {
          try {
            return Anoa.of(biFunction.apply(u, v));
          } catch (Throwable throwable) {
            return handle(throwable, u, v);
          }
        }));
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> predicate(
      @NonNull Predicate<T> predicate) {
    return predicate(predicate, __ -> Stream.<M>empty());
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> predicate(
      @NonNull Predicate<T> predicate,
      @NonNull Function<T, Stream<M>> failHandler) {
    Objects.requireNonNull(predicate);
    Objects.requireNonNull(failHandler);
    return (Anoa<T, M> tWrapped) -> (
      tWrapped.flatMap((T t) -> {
        final boolean testResult;
        try {
          testResult = predicate.test(t);
        } catch (Throwable throwable) {
          return handle(throwable, t);
        }
        return testResult
               ? Anoa.of(t)
               : Anoa.of(null, failHandler.apply(t));
      }));
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> predicateChecked(
      @NonNull CheckedPredicate<T> predicate) {
    return predicateChecked(predicate, __ -> Stream.<M>empty());
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> predicateChecked(
      @NonNull CheckedPredicate<T> predicate,
      @NonNull Function<T, Stream<M>> failHandler) {
    Objects.requireNonNull(predicate);
    Objects.requireNonNull(failHandler);
    return (Anoa<T, M> tWrapped) -> (
        tWrapped.flatMap((T t) -> {
          final boolean testResult;
          try {
            testResult = predicate.test(t);
          } catch (Throwable throwable) {
            return handle(throwable, t);
          }
          return testResult
                 ? Anoa.of(t)
                 : Anoa.of(null, failHandler.apply(t));
        }));
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> writeConsumer(
      @NonNull WriteConsumer<T> writeConsumer) {
    Objects.requireNonNull(writeConsumer);
    return (Anoa<T, M> tWrapped) -> (
      tWrapped.flatMap((T t) -> {
        try {
          writeConsumer.acceptChecked(t);
        } catch (Throwable throwable) {
          return handle(throwable, t);
        }
        return Anoa.of(t);
      }));
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> consumer(@NonNull Consumer<T> consumer) {
    Objects.requireNonNull(consumer);
    return (Anoa<T, M> tWrapped) -> (
        tWrapped.flatMap((T t) -> {
          try {
            consumer.accept(t);
          } catch (Throwable throwable) {
            return handle(throwable, t);
          }
          return Anoa.of(t);
        }));
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> consumerChecked(
      @NonNull CheckedConsumer<T> consumer) {
    Objects.requireNonNull(consumer);
    return (Anoa<T, M> tWrapped) -> (
        tWrapped.flatMap((T t) -> {
          try {
            consumer.accept(t);
          } catch (Throwable throwable) {
            return handle(throwable, t);
          }
          return Anoa.of(t);
        }));
  }

  public <T, U> @NonNull BiFunction<Anoa<T, M>, U, Anoa<T, M>> biConsumer(
      @NonNull BiConsumer<T, U> biConsumer) {
    Objects.requireNonNull(biConsumer);
    return (Anoa<T, M> tWrapped, U u) -> (
        tWrapped.flatMap((T t) -> {
          try {
            biConsumer.accept(t, u);
          } catch (Throwable throwable) {
            return handle(throwable, t, u);
          }
          return Anoa.of(t);
        }));
  }

  public <T, U> @NonNull BiFunction<Anoa<T, M>, U, Anoa<T, M>> biConsumerChecked(
      @NonNull CheckedBiConsumer<T, U> biConsumer) {
    Objects.requireNonNull(biConsumer);
    return (Anoa<T, M> tWrapped, U u) -> (
        tWrapped.flatMap((T t) -> {
          try {
            biConsumer.accept(t, u);
          } catch (Throwable throwable) {
            return handle(throwable, t, u);
          }
          return Anoa.of(t);
        }));
  }

  public <T> @NonNull Anoa<T, M> wrap(@Nullable T value) {
    return Anoa.of(value);
  }

  public <T> @NonNull Anoa<T, M> handle(Throwable throwable) {
    return new Anoa<>(handler0.apply(throwable));
  }

  public <T, U> @NonNull Anoa<T, M> handle(Throwable throwable, U arg) {
    return new Anoa<>(handler1.apply(throwable, arg));
  }

  public <T, U, V> @NonNull Anoa<T, M> handle(Throwable throwable, U arg, V otherArg) {
    return new Anoa<>(handler1.apply(throwable, Tuple.tuple(arg, otherArg)));
  }

}
