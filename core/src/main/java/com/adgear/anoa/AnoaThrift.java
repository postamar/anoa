package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFileTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AnoaThrift {

  static class ThriftReadIterator<T extends TBase<T,?>> implements Iterator<T> {

    final TTransport tTransport;
    final TProtocol tProtocol;
    final Supplier<T> tSupplier;

    ThriftReadIterator(TTransport tTransport, TProtocol tProtocol, Supplier<T> tSupplier) {
      this.tTransport = tTransport;
      this.tProtocol = tProtocol;
      this.tSupplier = tSupplier;
    }

    @Override
    public boolean hasNext() {
      return tTransport.isOpen();
    }

    @Override
    public T next() {
      final T result = tSupplier.get();
      try {
        result.read(tProtocol);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return result;
    }
  }

  static public <T extends TBase<T,?>> @NonNull Function<byte[],T> fromCompactFn(
      @NonNull Supplier<T> supplier) {
    return fromFn(supplier, TCompactProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromCompact(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return fromCompact(supplier, new TIOStreamTransport(inputStream));
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromCompact(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return fromCompact(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromCompact(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return from(supplier, tTransport, TCompactProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<byte[],T> fromBinaryFn(
      @NonNull Supplier<T> supplier) {
    return fromFn(supplier, TBinaryProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromBinary(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return fromBinary(supplier, new TIOStreamTransport(inputStream));
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromBinary(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return fromBinary(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromBinary(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return from(supplier, tTransport, TBinaryProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<byte[],T> fromJsonFn(
      @NonNull Supplier<T> supplier) {
    return fromFn(supplier, TJSONProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromJson(
      @NonNull Supplier<T> supplier,
      @NonNull InputStream inputStream) {
    return fromJson(supplier, new TIOStreamTransport(inputStream));
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromJson(
      @NonNull Supplier<T> supplier,
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return fromJson(supplier, new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T,?>> @NonNull Stream<T> fromJson(
      @NonNull Supplier<T> supplier,
      @NonNull TTransport tTransport) {
    return from(supplier, tTransport, TJSONProtocol::new);
  }

  static protected <T extends TBase<T,?>> Stream<T> from(Supplier<T> supplier,
                                                         TTransport tTransport,
                                                         Function<TTransport,TProtocol> cFn) {
    Iterator<T> it = new ThriftReadIterator<>(tTransport, cFn.apply(tTransport), supplier);
    int characteristics = Spliterator.NONNULL | Spliterator.ORDERED;
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, characteristics), false);
  }

  static protected <T extends TBase<T,?>> Function<byte[],T> fromFn(
      Supplier<T> supplier,
      Function<TTransport,TProtocol> cFn) {
    TMemoryInputTransport tTransport = new TMemoryInputTransport();
    ThriftReadIterator<T> iterator = new ThriftReadIterator<>(tTransport,
                                                              cFn.apply(tTransport),
                                                              supplier);
    return (byte[] bytes) -> {
      tTransport.reset(bytes);
      return iterator.next();
    };
  }

  static class TMemoryOutputTransport extends TTransport {

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void open() throws TTransportException {
    }

    @Override
    public void close() {
    }

    @Override
    public int read(byte[] bytes, int i, int i1) throws TTransportException {
      throw new TTransportException("Read not supported");
    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws TTransportException {
      baos.write(bytes, i, i1);
    }
  }

  static public <T extends TBase<T,?>> @NonNull Function<T,byte[]> toCompactFn() {
    return toFn(TCompactProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toCompact(
      @NonNull OutputStream outputStream) {
    return toCompact(new TIOStreamTransport(outputStream));
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toCompact(
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return toCompact(new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toCompact(
      @NonNull TTransport tTransport) {
    return to(tTransport, TCompactProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<T,byte[]> toBinaryFn() {
    return toFn(TBinaryProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toBinary(
      @NonNull OutputStream outputStream) {
    return toBinary(new TIOStreamTransport(outputStream));
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toBinary(
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return toBinary(new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toBinary(
      @NonNull TTransport tTransport) {
    return to(tTransport, TBinaryProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Function<T,byte[]> toJsonFn() {
    return toFn(TJSONProtocol::new);
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toJson(
      @NonNull OutputStream outputStream) {
    return toJson(new TIOStreamTransport(outputStream));
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toJson(
      @NonNull String fileName,
      @NonNull boolean readOnly) {
    try {
      return toJson(new TFileTransport(fileName, readOnly));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <T extends TBase<T,?>> @NonNull Consumer<T> toJson(
      @NonNull TTransport tTransport) {
    return to(tTransport, TJSONProtocol::new);
  }

  static protected <T extends TBase<T,?>> Consumer<T> to(TTransport tTransport,
                                                         Function<TTransport,TProtocol> cFn) {
    TProtocol tProtocol = cFn.apply(tTransport);
    return (T t) -> {
      try {
        t.write(tProtocol);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    };
  }

  static protected <T extends TBase<T,?>> Function<T,byte[]> toFn(
      Function<TTransport,TProtocol> cFn) {
    TMemoryOutputTransport tTransport = new TMemoryOutputTransport();
    TProtocol tProtocol = cFn.apply(tTransport);
    return (T t) -> {
      tTransport.baos.reset();
      try {
        t.write(tProtocol);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return tTransport.baos.toByteArray();
    };
  }
}
