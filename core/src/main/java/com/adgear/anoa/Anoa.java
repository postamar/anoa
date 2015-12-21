package com.adgear.anoa;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A container object which may or may not contain a non-null value, much like {@link Optional}: if
 * a value is present, {@code isPresent()} will return {@code true} and {@code get()} will return
 * the value.
 *
 * In addition to the functionalities offered by {@link Optional}, these objects can be decorated by
 * metadata. Possible uses include documenting the presence or absence of value, documenting the
 * value itself, or documenting any exceptions handled during the transformation of the value. For
 * the latter use case, consider using the factory class {@code AnoaHandler}.
 *
 * <p>This is a <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">
 * value-based</a> class; use of identity-sensitive operations (including reference equality ({@code
 * ==}), identity hash code, or synchronization) on instances of {@code Anoa} may have unpredictable
 * results and should be avoided.
 *
 * @param <T> Value type
 * @param <M> Metadata type
 * @see Optional
 * @see AnoaHandler
 */
final public class Anoa<T, M> {

  static final Anoa EMPTY = new Anoa<>(null, new Object[0]);

  final T value;
  final M[] meta;

  private Anoa(T value, M[] meta) {
    this.value = value;
    this.meta = meta;
  }

  /**
   * Factory method
   *
   * @see Optional#empty()
   */
  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> empty() {
    return EMPTY;
  }

  /**
   * Factory method
   *
   * @throws NullPointerException when metadata stream is null
   * @see Optional#empty()
   */
  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> empty(
      Stream<M> metadata) {
    Objects.requireNonNull(metadata);
    return ofNullable(null, metadata);
  }

  /**
   * Factory method
   *
   * @throws NullPointerException when value is null
   * @see Optional#of(Object)
   */
  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> of(
      T value) {
    Objects.requireNonNull(value);
    return ofNullable(value);
  }

  /**
   * Factory method
   *
   * @throws NullPointerException when value or metadata stream is null
   * @see Optional#of(Object)
   */
  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> of(
      T value,
      Stream<M> metadata) {
    Objects.requireNonNull(value);
    Objects.requireNonNull(metadata);
    return ofNullable(value, metadata);
  }

  /**
   * Factory method
   *
   * @see Optional#ofNullable(Object)
   */
  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> ofNullable(
      /*@Nullable*/ T value) {
    return new Anoa<>(value, (M[]) EMPTY.meta);
  }

  /**
   * Factory method
   *
   * @throws NullPointerException when metadata stream is null
   * @see Optional#ofNullable(Object)
   */
  @SuppressWarnings("unchecked")
  static public <T, M> Anoa<T, M> ofNullable(
      /*@Nullable*/ T value,
      Stream<M> metadata) {
    Objects.requireNonNull(metadata);
    return new Anoa<>(value, (M[]) metadata.toArray());
  }

  /**
   * Returns a stream of metadata elements decorating this {@code Anoa}
   */
  public Stream<M> meta() {
    return Arrays.stream(meta);
  }

  /**
   * Returns the value held in this {@code Anoa}
   */
  public Optional<T> asOptional() {
    return Optional.ofNullable(value);
  }

  /**
   * Returns the value held in this {@code Anoa} in a stream, if exists
   */
  public Stream<T> asStream() {
    return (value != null) ? Stream.of(value) : Stream.<T>empty();
  }

  /**
   * If a value is present in this {@code Anoa}, returns the value, otherwise throws {@link
   * NoSuchElementException}.
   *
   * @see Anoa#isPresent()
   * @see Optional#get()
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
   * @see Optional#isPresent()
   */
  public boolean isPresent() {
    return (value != null);
  }

  /**
   * If a value is present, invoke the specified consumer with the value, otherwise do nothing.
   *
   * @param consumer lambda to be invoked with the value, if present
   * @throws NullPointerException if value is present and {@code consumer} is null
   * @see Optional#ifPresent(Consumer)
   */
  public void ifPresent(Consumer<? super T> consumer) {
    if (value != null) {
      consumer.accept(value);
    }
  }

  /**
   * If a value is present, and the value matches the given predicate, return an {@code Anoa}
   * describing the value, otherwise return a valueless {@code Anoa}.
   *
   * @param predicate a predicate to apply to the value, if present
   * @return an {@code Anoa} describing the value of this {@code Anoa} if a value is present and the
   * value matches the given predicate, otherwise a valueless {@code Anoa}
   * @throws NullPointerException if the predicate is null
   * @see Optional#filter(Predicate)
   */
  public Anoa<T, M> filter(Predicate<? super T> predicate) {
    if (value == null) {
      return this;
    } else {
      return predicate.test(value) ? this : new Anoa<>(null, meta);
    }
  }

  /**
   * If a value is present, apply the provided mapping function to it, return that result wrapped in
   * an {@code Anoa}, otherwise return a valueless {@code Anoa}.
   *
   * @param <U>    The type parameter to the {@code Anoa} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code Anoa}-bearing mapping function to the value of this
   * {@code Anoa}, if a value is present, otherwise  a valueless {@code Anoa}
   * @throws NullPointerException if the mapping function is null or returns a null result
   * @see Optional#map(Function)
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
   * If a value is present, apply the provided {@code Anoa}-bearing mapping function to it, return
   * that result, otherwise return  a valueless {@code Anoa}.
   *
   * @param <U>    The type parameter to the {@code Anoa} returned by
   * @param mapper a mapping function to apply to the value, if present
   * @return the result of applying an {@code Anoa}-bearing mapping function to the value of this
   * {@code Anoa}, if a value is present, otherwise  a valueless {@code Anoa}
   * @throws NullPointerException if the mapping function is null or returns a null result
   * @see Optional#flatMap(Function)
   */
  @SuppressWarnings("unchecked")
  public <U> Anoa<U, M> flatMap(Function<? super T, Anoa<U, M>> mapper) {
    if (value == null) {
      return (Anoa<U, M>) this;
    } else {
      final Anoa<U, M> result = mapper.apply(value);
      Objects.requireNonNull(result);
      final M[] newMeta = Arrays.copyOf(meta, meta.length + result.meta.length);
      System.arraycopy(result.meta, 0, newMeta, meta.length, result.meta.length);
      return new Anoa<>(result.value, newMeta);
    }
  }

  /**
   * Return the value if present, otherwise return {@code other}
   *
   * @param other the value to be returned if there is no value present, may be null
   * @return the value, if present, otherwise {@code other}
   * @see Optional#orElse(Object)
   */
  public T orElse(T other) {
    return (value != null) ? value : other;
  }

  /**
   * Return the value if present, otherwise invoke {@code other} and return the result of that
   * invocation.
   *
   * @param other a {@code Supplier} whose result is returned if no value is present
   * @return the value if present otherwise the result of {@code other.get()}
   * @throws NullPointerException if value is not present and {@code other} is null
   * @see Optional#orElseGet(Supplier)
   */
  public T orElseGet(Supplier<? extends T> other) {
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
   * @see Optional#orElseThrow(Supplier)
   */
  public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
    if (value == null) {
      throw exceptionSupplier.get();
    }
    return value;
  }

  /**
   * Indicates whether some other object is "equal to" this Anoa. The other object is considered
   * equal if it is also an {@code Anoa} and: <ul> <li>both instances have no value present, or both
   * instances have values which are "equal to" each other via {@code equals()}, and; <li>both
   * instances have no metadata present, or both instances have metadata which is "equal to" each
   * other via {@code equals()}. </ul>
   *
   * @param obj an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object, otherwise {@code false}
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || !(obj instanceof Anoa)) {
      return false;
    }
    final Anoa<?, ?> anoa = (Anoa<?, ?>) obj;
    return Objects.equals(value, anoa.value) && Arrays.equals(meta, anoa.meta);
  }

  /**
   * Returns a hash code generated solely from the value and the metadata, if present.
   */
  @Override
  public int hashCode() {
    return Objects.hash(value, meta);
  }

  /**
   * Returns a non-empty string representation of this Anoa suitable for debugging. The exact
   * presentation format is unspecified and may vary between implementations and versions.
   *
   * @implSpec If a value is present the result must include its string representation in the
   * result; same for metadata. Objects must be unambiguously differentiable.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("Anoa<").append(value);
    char c = '|';
    for (Object m : meta) {
      sb.append(c).append(m);
      c = ',';
    }
    return sb.append('>').toString();
  }
}

