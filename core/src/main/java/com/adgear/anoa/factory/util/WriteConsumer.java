package com.adgear.anoa.factory.util;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.function.Consumer;

public interface WriteConsumer<T> extends Closeable, Flushable, Consumer<T> {

  default void close() throws IOException {
    flush();
  }
}
