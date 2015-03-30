package com.adgear.anoa;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

final public class Anoa<T, M> {

  static private final Object[] EMPTY_ARRAY = new Object[0];

  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> of(@Nullable T value) {
    return new Anoa<>(value, (M[]) EMPTY_ARRAY);
  }

  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> of(@Nullable T value, @NonNull Stream<M> metadata) {
    Objects.requireNonNull(metadata);
    return new Anoa<>(value, (M[]) metadata.toArray());
  }

  final T value;
  final M[] meta;

  Anoa(M[] meta) {
    this(null, meta);
  }

  Anoa(T value, M[] meta) {
    this.value = value;
    this.meta = meta;
  }

  /**
   * Returns a stream of metadata elements decorating this {@code Anoa}
   *
   * @return a stream of metadata elements decorating this {@code Anoa}
   */
  public @NonNull Stream<@NonNull M> meta() {
    return Arrays.stream(meta);
  }

  /**
   * Returns the value held in this {@code Anoa}
   *
   * @return the value held by this {@code Anoa}
   */
  public Optional<T> asOptional() {
    return Optional.ofNullable(value);
  }

  /**
   * Returns the value held in this {@code Anoa} in a stream, if exists
   *
   * @return a stream containing the value held by this {@code Anoa}, if exists
   */
  public Stream<T> asStream() {
    return (value != null) ? Stream.of(value) : Stream.<T>empty();
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
    if (value == null) {
      throw new NoSuchElementException();
    }
    return value;
  }

  /**
   * Return {@code true} if there is a value present, otherwise {@code false}.
   *
   * @return {@code true} if there is a value present, otherwise {@code false}
   */
  public boolean isPresent() {
    return (value != null);
  }

  /**
   * If a value is present, invoke the specified consumer with the value, otherwise do nothing.
   *
   * @param consumer block to be executed if a value is present
   * @throws NullPointerException if value is present and {@code consumer} is null
   */
  public void ifPresent(Consumer<? super T> consumer) {
    if (value != null) {
      consumer.accept(value);
    }
  }

  /**
   * If a value is present, and the value matches the given predicate, return an {@code
   * Anoa} describing the value, otherwise return an empty0 {@code Anoa}.
   *
   * @param predicate a predicate to apply to the value, if present
   * @return an {@code Anoa} describing the value of this {@code Anoa} if a value is
   * present and the value matches the given predicate, otherwise an empty0 {@code Anoa}
   * @throws NullPointerException if the predicate is null
   */
  public Anoa<T, M> filter(Predicate<? super T> predicate) {
    if (value == null) {
      return this;
    } else {
      return predicate.test(value) ? this : new Anoa<>(null, meta);
    }
  }

  /**
   * If a value is present, apply the provided mapping function to it, return that result wrapped
   * in an {@code Anoa}, otherwise return an empty0 {@code Anoa}.
   *
   * @param <U>    The type parameter to the {@code Anoa} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code Anoa}-bearing mapping function to the value of
   * this {@code Anoa}, if a value is present, otherwise an empty0 {@code Anoa}
   * @throws NullPointerException if the mapping function is null or returns a null result
   */
  @SuppressWarnings("unchecked")
  public <U> Anoa<U, M> map(Function<? super T, ? extends U> mapper) {
    if (value == null) {
      return (Anoa<U, M>) this;
    } else {
      final U result = mapper.apply(value);
      Objects.requireNonNull(result);
      return new Anoa<>(result, meta);
    }
  }

  /**
   * If a value is present, apply the provided {@code Anoa}-bearing mapping function to it,
   * return that result, otherwise return an empty0 {@code Anoa}.
   *
   * @param <U>    The type parameter to the {@code Anoa} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code Anoa}-bearing mapping function to the value of
   * this {@code Anoa}, if a value is present, otherwise an empty0 {@code Anoa}
   * @throws NullPointerException if the mapping function is null or returns a null result
   */
  @SuppressWarnings("unchecked")
  public <U> Anoa<U, M> flatMap(Function<? super T, Anoa<U, M>> mapper) {
    if (value == null) {
      return (Anoa<U, M>) this;
    } else {
      final Anoa<U, M> result = mapper.apply(value);
      Objects.requireNonNull(result);
      M[] newMeta = Arrays.copyOf(meta, meta.length + result.meta.length);
      System.arraycopy(result.meta, 0, newMeta, meta.length, newMeta.length);
      return new Anoa<>(result.value, newMeta);
    }
  }

  /**
   * Return the value if present, otherwise return {@code other}
   *
   * @param other the value to be returned if there is no value present, may be null
   * @return the value, if present, otherwise {@code other}
   */
  public T orElse(@Nullable T other) {
    return (value != null) ? value : other;
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
    return (value != null) ? value : other.get();
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
    if (value == null) {
      throw exceptionSupplier.get();
    }
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof Anoa)) {
      return false;
    }
    final Anoa<?, ?> anoa = (Anoa<?, ?>) o;
    return Objects.equals(value, anoa.value) && Arrays.equals(meta, anoa.meta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, meta);
  }

  @Override
  public String toString() {
    return (value != null) ? ("Anoa(" + value + ")") : "Anoa~empty";
  }
}

