package com.adgear.anoa;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class Anoa<T, M> {

  static public <T, M> Anoa<T, M> of(@Nullable T value, Collection<M> metadata) {
    return new Anoa<>(Optional.ofNullable(value), new ArrayList<>(metadata));
  }

  final Optional<T> value;
  final ArrayList<M> meta;

  Anoa(Optional<T> value, ArrayList<M> meta) {
    this.value = value;
    this.meta = meta;
  }

  /**
   * Returns a stream of metadata elements decorating this {@code Anoa}
   *
   * @return a stream of metadata elements decorating this {@code Anoa}
   */
  public @NonNull Stream<@NonNull M> meta() {
    return meta.stream();
  }

  /**
   * Decorates this {@code Anoa} with metadata element
   *
   * @param metadata a metadata element
   */
  public Anoa<T, M> decorate(@NonNull M metadata) {
    meta.add(metadata);
    return this;
  }

  /**
   * Decorates this {@code Anoa} with metadata elements
   *
   * @param metadata an stream for metadata elements
   */
  public Anoa<T, M> decorate(@NonNull Stream<@NonNull M> metadata) {
    metadata.forEach(meta::add);
    return this;
  }

  /**
   * Decorates this {@code Anoa} with metadata elements
   *
   * @param metadata an iterable for metadata elements
   */
  public Anoa<T, M> decorate(@NonNull Iterable<@NonNull M> metadata) {
    if (metadata instanceof Collection) {
      meta.addAll((Collection<M>) metadata);
    } else {
      for (M element : metadata) {
        meta.add(element);
      }
    }
    return this;
  }

  /**
   * Returns the value held in this {@code Anoa}
   *
   * @return the value held by this {@code Anoa}
   */
  public Optional<T> asOptional() {
    return value;
  }


  /**
   * Returns the value held in this {@code Anoa} in a stream, if exists
   *
   * @return a stream containing the value held by this {@code Anoa}, if exists
   */
  public Stream<T> asStream() {
    return value.isPresent() ? Stream.of(value.get()) : Stream.<T>empty();
  }

  /**
   * If a value is present in this {@code Anoa}, returns the value, otherwise throws {@code
   * NoSuchElementException}.
   *
   * @return the non-null value held by this {@code Anoa}
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
   * Anoa} describing the value, otherwise return an empty {@code Anoa}.
   *
   * @param predicate a predicate to apply to the value, if present
   * @return an {@code Anoa} describing the value of this {@code Anoa} if a value is
   * present and the value matches the given predicate, otherwise an empty {@code Anoa}
   * @throws NullPointerException if the predicate is null
   */
  public Anoa<T, M> filter(Predicate<? super T> predicate) {
    if (value.isPresent()) {
      return predicate.test(value.get()) ? this : new Anoa<>(Optional.empty(), meta);
    } else {
      return this;
    }
  }

  @SuppressWarnings("unchecked")
  <U> Anoa<U, M> unsafeCast() {
    return (Anoa<U, M>) this;
  }

  /**
   * If a value is present, apply the provided mapping function to it, return that result wrapped
   * in an {@code Anoa}, otherwise return an empty {@code Anoa}.
   *
   * @param <U>    The type parameter to the {@code Anoa} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code Anoa}-bearing mapping function to the value of
   * this {@code Anoa}, if a value is present, otherwise an empty {@code Anoa}
   * @throws NullPointerException if the mapping function is null or returns a null result
   */
  public <U> Anoa<U, M> map(Function<? super T, ? extends U> mapper) {
    return new Anoa<>(value.map(mapper), new ArrayList<>(meta));
  }

  /**
   * If a value is present, apply the provided {@code Anoa}-bearing mapping function to it,
   * return that result, otherwise return an empty {@code Anoa}.
   *
   * @param <U>    The type parameter to the {@code Anoa} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code Anoa}-bearing mapping function to the value of
   * this {@code Anoa}, if a value is present, otherwise an empty {@code Anoa}
   * @throws NullPointerException if the mapping function is null or returns a null result
   */
  public <U> Anoa<U, M> flatMap(Function<? super T, Anoa<U, M>> mapper) {
    final ArrayList<M> meta = new ArrayList<>(this.meta);
    if (value.isPresent()) {
      final Anoa<U, M> mapperResult = mapper.apply(value.get());
      meta.addAll(mapperResult.meta);
      return new Anoa<>(mapperResult.value, meta);
    } else {
      return new Anoa<>(Optional.empty(), meta);
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
}

