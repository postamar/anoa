package com.adgear.anoa.write;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

/**
 * Implemented by consumers which perform write operations, and therefore may need to be flushed or
 * closed.
 *
 * Methods in Anoa Utility classes matching #"\w+Consumer" usually return objects implementing this
 * interface.
 *
 * @param <T> Value type
 * @see AvroConsumers
 * @see JacksonConsumers
 * @see ThriftConsumers
 */
public interface WriteConsumer<T> extends Closeable, Flushable, Consumer<T> {

  /**
   * Write the record somewhere
   *
   * @param record the record to be written
   * @throws IOException raised when write fails
   */
  void acceptChecked(T record) throws IOException;

  /**
   * Default implementation of {@link Consumer#accept} wraps {@link WriteConsumer#acceptChecked} by
   * rethrowing any {@link IOException} as an {@link UncheckedIOException}
   *
   * @param record the record to be written
   */
  default void accept(T record) {
    try {
      acceptChecked(record);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Calls {@link Flushable#flush()}, rethrowing any {@link IOException} as an {@link
   * UncheckedIOException}
   */
  default void flushUnchecked() {
    try {
      flush();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Default implementation of {@link Closeable#close} calls {@link Flushable#flush}
   *
   * @throws IOException raised when close fails
   */
  default void close() throws IOException {
    flush();
  }
}
