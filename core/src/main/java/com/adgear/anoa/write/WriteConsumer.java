package com.adgear.anoa.write;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

public interface WriteConsumer<T> extends Closeable, Flushable, Consumer<T> {

  void acceptChecked(T record) throws IOException;

  default void accept(T record) {
    try {
      acceptChecked(record);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  default void close() throws IOException {
    flush();
  }
}
