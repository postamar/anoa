package com.adgear.anoa.read;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A generic {@link Iterator} implementation which performs one-step look-ahead.
 *
 * @param <T> Value type
 */
final public class LookAheadIterator<T> implements Iterator<T> {

  final protected Supplier<Boolean> noNext;
  final protected UnaryOperator<T> next;
  protected long counter = 0;
  private Closeable closeable;
  private boolean isStale;
  private boolean hasNext;
  private T nextValue;

  /**
   * @param noNext      called in hasNext(), will cause it to return false if itself returns true.
   * @param nextFactory a function which builds a function which returns the next element based on
   *                    the previous one, and can trigger the end of the iteration as a
   *                    side-effect.
   * @param closeable   {@link Closeable#close()} will be called at the end of the iteration
   */
  public LookAheadIterator(
      Supplier<Boolean> noNext,
      Function<Consumer<Boolean>, UnaryOperator<T>> nextFactory,
      Closeable closeable) {
    this.next = nextFactory.apply(this::setHasNext);
    this.noNext = noNext;
    this.closeable = closeable;
    reset(null);
  }

  /**
   * Returns a sequential stream wrapping the generated {@code LookAheadIterator} instance.
   */
  static public <T> Stream<T> stream(
      Supplier<Boolean> noNext,
      Function<Consumer<Boolean>, UnaryOperator<T>> nextFactory,
      Closeable closeable) {
    return new LookAheadIterator<>(noNext, nextFactory, closeable).asStream();
  }

  /**
   * Returns a sequential stream wrapping the generated {@code LookAheadIterator} instance.
   */
  static public <T> Stream<T> stream(
      Supplier<Boolean> noNext,
      Function<Consumer<Boolean>, UnaryOperator<T>> nextFactory) {
    return stream(noNext, nextFactory, () -> {
    });
  }

  void reset(T nextValue) {
    this.isStale = true;
    this.hasNext = true;
    this.nextValue = nextValue;
  }

  protected void setHasNext(boolean hasNext) {
    this.hasNext = hasNext;
  }

  @Override
  public boolean hasNext() {
    if (isStale) {
      isStale = false;
      if (noNext.get()) {
        setHasNext(false);
      } else {
        ++counter;
        nextValue = next.apply(nextValue);
      }
    }
    if (!hasNext) {
      try {
        closeable.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      closeable = null;
    }
    return hasNext;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    isStale = true;
    return nextValue;
  }

  Spliterator<T> asSpliterator() {
    return Spliterators.spliteratorUnknownSize(this, Spliterator.NONNULL | Spliterator.ORDERED);
  }

  Stream<T> asStream() {
    return StreamSupport.stream(asSpliterator(), false);
  }
}