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

  /**
   * this instance's exception handler
   */
  final public BiFunction<Throwable, Tuple, Stream<M>> biFn;

  /**
   * @param biFn The exception handler to use with this instance.
   */
  public AnoaHandler(BiFunction<Throwable, Tuple, Stream<M>> biFn) {
    Objects.requireNonNull(biFn);
    this.biFn = biFn;
  }

  /**
   * @return An {@code AnoaHandler} in which exceptions are handled by doing nothing.
   */
  static public @NonNull AnoaHandler<?> noOp() {
    return new AnoaHandler<>((throwable, __) -> Stream.empty());
  }

  /**
   * @return An {@code AnoaHandler} in which exceptions are handled by passing them along as
   * {@code Anoa} metadata.
   */
  static public @NonNull AnoaHandler<Throwable> passAlong() {
    return new AnoaHandler<>((throwable, __) -> Stream.of(throwable));
  }

  /**
   * @return An {@code AnoaHandler} in which exceptions are handled by applying a function to the
   * exception object and passing the result along as {@code Anoa} metadata.
   */
  static public <M> @NonNull AnoaHandler<M> mapThrowable(
      @NonNull Function<Throwable, M> handler) {
    return new AnoaHandler<>((throwable, __) -> Stream.of(handler.apply(throwable)));
  }

  public <T> @NonNull Supplier<Anoa<T, M>> supplier(@NonNull Supplier<? extends T> supplier) {
    Objects.requireNonNull(supplier);
    return () -> {
      final T result;
      try {
        result = supplier.get();
      } catch (Throwable throwable) {
        return new Anoa<>(biFn.apply(throwable, Tuple.tuple()));
      }
      return wrap(result);
    };
  }

  public <T> @NonNull Supplier<Anoa<T, M>> supplierChecked(
      @NonNull CheckedSupplier<? extends T> supplier) {
    Objects.requireNonNull(supplier);
    return () -> {
      final T result;
      try {
        result = supplier.get();
      } catch (Throwable throwable) {
        return new Anoa<>(biFn.apply(throwable, Tuple.tuple()));
      }
      return wrap(result);
    };
  }

  public <T, U> @NonNull Function<Anoa<U, M>, Anoa<T, M>> function(
      @NonNull Function<? super U, ? extends T> function) {
    Objects.requireNonNull(function);
    return (Anoa<U, M> uWrapped) -> {
      if (uWrapped.isPresent()) {
        final U u = uWrapped.get();
        final T result;
        try {
          result = function.apply(u);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(u)));
        }
        return wrap(result);
      } else {
        return uWrapped.unsafeCast();
      }
    };
  }

  public <T, U> @NonNull Function<Anoa<U, M>, Anoa<T, M>> functionChecked(
      @NonNull CheckedFunction<? super U, ? extends T> function) {
    Objects.requireNonNull(function);
    return (Anoa<U, M> uWrapped) -> {
      if (uWrapped.isPresent()) {
        final U u = uWrapped.get();
        final T result;
        try {
          result = function.apply(u);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(u)));
        }
        return wrap(result);
      } else {
        return uWrapped.unsafeCast();
      }
    };
  }

  public <T, U, V> @NonNull BiFunction<Anoa<U, M>, V, Anoa<T, M>> biFunction(
      @NonNull BiFunction<? super U, ? super V, ? extends T> biFunction) {
    Objects.requireNonNull(biFunction);
    return (Anoa<U, M> uWrapped, V v) -> {
      if (uWrapped.isPresent()) {
        final U u = uWrapped.get();
        final T result;
        try {
          result = biFunction.apply(u, v);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(u, v)));
        }
        return new Anoa<>(Optional.ofNullable(result), uWrapped.meta());
      } else {
        return uWrapped.unsafeCast();
      }
    };
  }

  public <T, U, V> @NonNull BiFunction<Anoa<U, M>, V, Anoa<T, M>> biFunctionChecked(
      @NonNull CheckedBiFunction<? super U, ? super V, ? extends T> biFunction) {
    Objects.requireNonNull(biFunction);
    return (Anoa<U, M> uWrapped, V v) -> {
      if (uWrapped.isPresent()) {
        final U u = uWrapped.get();
        final T result;
        try {
          result = biFunction.apply(u, v);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(u, v)));
        }
        return new Anoa<>(Optional.ofNullable(result), uWrapped.meta());
      } else {
        return uWrapped.unsafeCast();
      }
    };
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> predicate(
      @NonNull Predicate<T> predicate,
      @NonNull Optional<M> decoratePass,
      @NonNull Optional<M> decorateFail) {
    Objects.requireNonNull(predicate);
    final UnaryOperator<Stream<M>> pass = decoratePass.isPresent()
                                          ? (ms -> Stream.concat(ms, Stream.of(decoratePass.get())))
                                          : UnaryOperator.<Stream<M>>identity();
    final UnaryOperator<Stream<M>> fail = decorateFail.isPresent()
                                          ? (ms -> Stream.concat(ms, Stream.of(decorateFail.get())))
                                          : UnaryOperator.<Stream<M>>identity();
    return (Anoa<T, M> tWrapped) -> {
      if (tWrapped.isPresent()) {
        final T t = tWrapped.get();
        final boolean testResult;
        try {
          testResult = predicate.test(t);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(t)));
        }
        return new Anoa<>(testResult ? tWrapped.asOptional() : Optional.<T>empty(),
                          (testResult ? pass : fail).apply(tWrapped.meta()));
      } else {
        return tWrapped;
      }
    };
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> predicateChecked(
      @NonNull CheckedPredicate<T> predicate,
      @NonNull Optional<M> decoratePass,
      @NonNull Optional<M> decorateFail) {
    Objects.requireNonNull(predicate);
    final UnaryOperator<Stream<M>> pass = decoratePass.isPresent()
                                          ? (ms -> Stream.concat(ms, Stream.of(decoratePass.get())))
                                          : UnaryOperator.<Stream<M>>identity();
    final UnaryOperator<Stream<M>> fail = decorateFail.isPresent()
                                          ? (ms -> Stream.concat(ms, Stream.of(decorateFail.get())))
                                          : UnaryOperator.<Stream<M>>identity();
    return (Anoa<T, M> tWrapped) -> {
      if (tWrapped.isPresent()) {
        final T t = tWrapped.get();
        final boolean testResult;
        try {
          testResult = predicate.test(t);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(t)));
        }
        return new Anoa<>(testResult ? tWrapped.asOptional() : Optional.<T>empty(),
                          (testResult ? pass : fail).apply(tWrapped.meta()));
      } else {
        return tWrapped;
      }
    };
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> writeConsumer(
      @NonNull WriteConsumer<T> writeConsumer) {
    Objects.requireNonNull(writeConsumer);
    return (Anoa<T, M> tWrapped) -> {
      if (tWrapped.isPresent()) {
        final T t = tWrapped.get();
        try {
          writeConsumer.acceptChecked(t);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(t)));
        }
      }
      return tWrapped;
    };
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> consumer(@NonNull Consumer<T> consumer) {
    Objects.requireNonNull(consumer);
    return (Anoa<T, M> tWrapped) -> {
      if (tWrapped.isPresent()) {
        final T t = tWrapped.get();
        try {
          consumer.accept(t);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(t)));
        }
      }
      return tWrapped;
    };
  }

  public <T> @NonNull UnaryOperator<Anoa<T, M>> consumerChecked(
      @NonNull CheckedConsumer<T> consumer) {
    Objects.requireNonNull(consumer);
    return (Anoa<T, M> tWrapped) -> {
      if (tWrapped.isPresent()) {
        final T t = tWrapped.get();
        try {
          consumer.accept(t);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(t)));
        }
      }
      return tWrapped;
    };
  }

  public <T, U> @NonNull BiFunction<Anoa<T, M>, U, Anoa<T, M>> biConsumer(
      @NonNull BiConsumer<T, U> biConsumer) {
    Objects.requireNonNull(biConsumer);
    return (Anoa<T, M> tWrapped, U u) -> {
      if (tWrapped.isPresent()) {
        final T t = tWrapped.get();
        try {
          biConsumer.accept(t, u);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(t, u)));
        }
      }
      return tWrapped;
    };
  }

  public <T, U> @NonNull BiFunction<Anoa<T, M>, U, Anoa<T, M>> biConsumerChecked(
      @NonNull CheckedBiConsumer<T, U> biConsumer) {
    Objects.requireNonNull(biConsumer);
    return (Anoa<T, M> tWrapped, U u) -> {
      if (tWrapped.isPresent()) {
        final T t = tWrapped.get();
        try {
          biConsumer.accept(t, u);
        } catch (Throwable throwable) {
          return new Anoa<>(biFn.apply(throwable, Tuple.tuple(t, u)));
        }
      }
      return tWrapped;
    };
  }

  /**
   *
   * @param value Value to be wrapped (may be null)
   * @param <T> Value type
   * @return The value wrapped in an instance of {@code Anoa}
   */
  public <T> @NonNull Anoa<T, M> wrap(@Nullable T value) {
    return new Anoa<>(Optional.ofNullable(value), Stream.<M>empty());
  }
}
