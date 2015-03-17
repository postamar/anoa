package com.adgear.anoa;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class Anoa<T, M> {

  final private Optional<T> value;
  final private Stream<M> meta;

  public Anoa() {
    this(Optional.<T>empty(), Stream.<M>empty());
  }

  public Anoa(Stream<M> meta) {
    this(Optional.<T>empty(), meta);
  }

  public Anoa(Optional<T> value) {
    this(value, Stream.<M>empty());
  }

  public Anoa(Optional<T> value, Stream<M> meta) {
    this.value = value;
    this.meta = meta;
  }

  /**
   * Returns a stream of metadata elements decorating this {@code AnoaOptional}
   *
   * @return a stream of metadata elements decorating this {@code AnoaOptional}
   */
  public @NonNull Stream<@NonNull M> meta() {
    return meta;
  }

  /**
   * Decorates this {@code AnoaOptional} with metadata element
   *
   * @param metadata a metadata element
   */
  public Anoa<T, M> decorate(@NonNull M metadata) {
    return decorate(Stream.of(metadata));
  }

  /**
   * Decorates this {@code AnoaOptional} with metadata elements
   *
   * @param metadata an stream for metadata elements
   */
  public Anoa<T, M> decorate(@NonNull Stream<@NonNull M> metadata) {
    return new Anoa<>(value, Stream.concat(meta, metadata));
  }

  /**
   * Decorates this {@code AnoaOptional} with metadata elements
   *
   * @param metadata an iterable for metadata elements
   */
  public Anoa<T, M> decorate(@NonNull Iterable<@NonNull M> metadata) {
    final Spliterator<M> spliterator = Spliterators.spliteratorUnknownSize(
        metadata.iterator(),
        Spliterator.NONNULL | Spliterator.ORDERED);
    return new Anoa<>(value, StreamSupport.stream(spliterator, false));
  }

  /**
   * Returns the value held in this {@code AnoaOptional}
   *
   * @return the value held by this {@code AnoaOptional}
   */
  public Optional<T> asOptional() {
    return value;
  }

  /**
   * If a value is present in this {@code AnoaOptional}, returns the value, otherwise throws {@code
   * NoSuchElementException}.
   *
   * @return the non-null value held by this {@code AnoaOptional}
   * @throws java.util.NoSuchElementException if there is no value present
   * @see Anoa#isPresent()
   */
  public T get() {
    return value.get();
  }

  /**
   * Return {@code true} if there is a value present, otherwise {@code false}.
   *
   * @return {@code true} if there is a value present, otherwise {@code false}
   */
  public boolean isPresent() {
    return value.isPresent();
  }

  /**
   * If a value is present, invoke the specified consumer with the value, otherwise do nothing.
   *
   * @param consumer block to be executed if a value is present
   * @throws NullPointerException if value is present and {@code consumer} is null
   */
  public void ifPresent(Consumer<? super T> consumer) {
    value.ifPresent(consumer);
  }

  /**
   * If a value is present, and the value matches the given predicate, return an {@code
   * AnoaOptional} describing the value, otherwise return an empty {@code AnoaOptional}.
   *
   * @param predicate a predicate to apply to the value, if present
   * @return an {@code AnoaOptional} describing the value of this {@code AnoaOptional} if a value is
   * present and the value matches the given predicate, otherwise an empty {@code AnoaOptional}
   * @throws NullPointerException if the predicate is null
   */
  public Anoa<T, M> filter(Predicate<? super T> predicate) {
    if (value.isPresent()) {
      return predicate.test(value.get()) ? this : new Anoa<>(meta);
    } else {
      return this;
    }
  }

  @SuppressWarnings("unchecked")
  <T, M> Anoa<T, M> unsafeCast() {
    return (Anoa<T, M>) this;
  }

  /**
   * If a value is present, apply the provided {@code AnoaOptional}-bearing mapping function to it,
   * return that result, otherwise return an empty {@code AnoaOptional}.
   *
   * @param <U>    The type parameter to the {@code AnoaOptional} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code AnoaOptional}-bearing mapping function to the value of
   * this {@code AnoaOptional}, if a value is present, otherwise an empty {@code AnoaOptional}
   * @throws NullPointerException if the mapping function is null or returns a null result
   */
  public <U> Anoa<U, M> map(Function<? super T, ? extends U> mapper) {
    if (value.isPresent()) {
      return new Anoa<>(Optional.ofNullable(mapper.apply(value.get())), meta);
    } else {
      return unsafeCast();
    }
  }

  /**
   * If a value is present, apply the provided {@code AnoaOptional}-bearing mapping function to it,
   * return that result, otherwise return an empty {@code AnoaOptional}.
   *
   * @param <U>    The type parameter to the {@code AnoaOptional} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code AnoaOptional}-bearing mapping function to the value of
   * this {@code AnoaOptional}, if a value is present, otherwise an empty {@code AnoaOptional}
   * @throws NullPointerException if the mapping function is null or returns a null result
   */
  public <U> Anoa<U, M> flatMap(Function<? super T, Anoa<U, M>> mapper) {
    if (value.isPresent()) {
      final Anoa<U, M> mapperResult = mapper.apply(value.get());
      return new Anoa<>(mapperResult.value, Stream.concat(meta, mapperResult.meta));
    } else {
      return unsafeCast();
    }
  }

  /**
   * Return the value if present, otherwise return {@code other}
   *
   * @param other the value to be returned if there is no value present, may be null
   * @return the value, if present, otherwise {@code other}
   */
  public T orElse(@Nullable T other) {
    return value.orElse(other);
  }

  /**
   * Return the value if present, otherwise invoke {@code other} and return the result of that
   * invocation.
   *
   * @param other a {@code Supplier} whose result is returned if no value is present
   * @return the value if present otherwise the result of {@code other.get()}
   * @throws NullPointerException if value is not present and {@code other} is null
   */
  public T orElseGet(@NonNull Supplier<@Nullable ? extends T> other) {
    return value.orElseGet(other);
  }

  /**
   * Return the contained value, if present, otherwise throw an exception to be created by the
   * provided supplier.
   *
   * @param <X>               Type of the exception to be thrown
   * @param exceptionSupplier The supplier which will return the exception to be thrown
   * @return the present value
   * @throws X                    if there is no value present
   * @throws NullPointerException if no value is present and {@code exceptionSupplier} is null
   */
  public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
    return value.orElseThrow(exceptionSupplier);
  }

  @Override
  public String toString() {
    return value.isPresent() ? ("Anoa(" + value.get() + ")") : "Anoa~empty";
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}

