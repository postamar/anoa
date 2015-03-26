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

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
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

  static public final AnoaHandler<?> ERASE
      = new AnoaHandler<>(__ -> AnoaHandler.empty());
  static public final AnoaHandler<Throwable> NO_OP
      = new AnoaHandler<>(t -> AnoaHandler.<Throwable>empty().decorate(t));

  /**
   * this instance's exception handler
   */
  final Function<Throwable, Anoa<?, M>> handler0;
  final BiFunction<Throwable, Object, Anoa<?, M>> handler1;
  final BiFunction<Throwable, Tuple2, Anoa<?, M>> handler2;

  AnoaHandler(Function<Throwable, Anoa<?, M>> handler0) {
    this(handler0, ((t, __) -> handler0.apply(t)), ((t, __) -> handler0.apply(t)));
  }

  AnoaHandler(Function<Throwable, Anoa<?, M>> handler0,
              BiFunction<Throwable, Object, Anoa<?, M>> handler1,
              BiFunction<Throwable, Tuple2, Anoa<?, M>> handler2) {
    this.handler0 = handler0;
    this.handler1 = handler1;
    this.handler2 = handler2;
  }

  /**
   * @return An {@code AnoaHandler} in which exceptions are handled by applying a function to the
   * exception object and passing the result along as {@code Anoa} metadata.
   */
  static public <M> @NonNull AnoaHandler<M> withFn(
      @NonNull Function<Throwable, M> handler) {
    return new AnoaHandler<>(t -> AnoaHandler.<M>empty().decorate(handler.apply(t)));
  }

  /**
   * @return An {@code AnoaHandler} in which exceptions are handled by applying a function to the
   * exception object and passing the result along as {@code Anoa} metadata.
   */
  static public <M> @NonNull AnoaHandler<M> withBiFn(
      @NonNull BiFunction<Throwable, Tuple, M> handler) {
    return new AnoaHandler<>(
        t -> AnoaHandler.<M>empty().decorate(handler.apply(t, Tuple.tuple())),
        (t, u) -> AnoaHandler.<M>empty().decorate(handler.apply(t, Tuple.tuple(u))),
        (t, t2) -> AnoaHandler.<M>empty().decorate(handler.apply(t, t2)));
  }

  public <T> @NonNull Supplier<Anoa<T, M>> supplier(@NonNull Supplier<? extends T> supplier) {
    Objects.requireNonNull(supplier);
    return () -> {
      try {
        return wrap(supplier.get());
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
        return wrap(supplier.get());
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
          return wrap(function.apply(u));
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
            return wrap(function.apply(u));
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
            return wrap(biFunction.apply(u, v));
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
            return wrap(biFunction.apply(u, v));
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
               ? AnoaHandler.<T, M>anoa(t)
               : AnoaHandler.<T, M>anoa(null).decorate(failHandler.apply(t));
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
                 ? AnoaHandler.<T, M>anoa(t)
                 : AnoaHandler.<T, M>anoa(null).decorate(failHandler.apply(t));
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
        return wrap(t);
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
          return wrap(t);
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
          return wrap(t);
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
          return wrap(t);
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
          return wrap(t);
        }));
  }

  public <T> @NonNull Anoa<T, M> handle(Throwable throwable) {
    return handler0.apply(throwable).unsafeCast();
  }

  public <T, U> @NonNull Anoa<T, M> handle(Throwable throwable, U arg) {
    return handler1.apply(throwable, arg).unsafeCast();
  }

  public <T, U, V> @NonNull Anoa<T, M> handle(Throwable throwable, U arg, V otherArg) {
    return handler2.apply(throwable, Tuple.tuple(arg, otherArg)).unsafeCast();
  }

  static <T, M> @NonNull Anoa<T, M> anoa(@Nullable T value) {
    return new Anoa<>(Optional.ofNullable(value), new ArrayList<M>());
  }

  public <T> @NonNull Anoa<T, M> wrap(@Nullable T value) {
    return AnoaHandler.anoa(value);
  }

  static <M> Anoa<?, M> empty() {
    return new Anoa<>(Optional.empty(), new ArrayList<M>());
  }
}
