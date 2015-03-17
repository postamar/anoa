package com.adgear.anoa.write;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.function.Consumer;

public interface WriteConsumer<T, E extends Throwable>
    extends Closeable, Flushable, Consumer<T> {

  void acceptChecked(T record) throws E;

  default void close() throws IOException {
    flush();
  }
}
