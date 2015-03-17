package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jooq.lambda.Unchecked;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class ReadIteratorUtils {

  static <T> Stream<T> stream(Iterator<T> iterator) {
    final Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(
        iterator,
        Spliterator.NONNULL | Spliterator.ORDERED);
    return StreamSupport.stream(spliterator, false);
  }

  static <N extends TreeNode> ReadIterator<N> jackson(JsonParser jacksonParser) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> (() -> {
      try {
        final N tree = jacksonParser.readValueAsTree();
        if (tree == null && jacksonParser.getCurrentToken() == null) {
          setHasNext.accept(false);
        }
        return tree;
      } catch (EOFException e) {
        setHasNext.accept(false);
        return null;
      } catch (IOException e) {
        setHasNext.accept(false);
        throw new UncheckedIOException(e);
      }
    }), Unchecked.supplier(jacksonParser::isClosed));
  }

  static <N extends TreeNode, M> ReadIterator<Anoa<N, M>> jackson(AnoaFactory<M> anoaFactory,
                                                                  JsonParser jacksonParser) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> anoaFactory.supplierChecked(() -> {
      try {
        final N tree = jacksonParser.readValueAsTree();
        if (tree == null && jacksonParser.getCurrentToken() == null) {
          setHasNext.accept(false);
        }
        return tree;
      } catch (EOFException e) {
        setHasNext.accept(false);
        return null;
      } catch (Throwable e) {
        setHasNext.accept(false);
        throw e;
      }
    }), Unchecked.supplier(jacksonParser::isClosed));
  }

  static <R extends IndexedRecord> ReadIterator<R> avro(DataFileStream<R> dfs) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> (() -> {
      try {
        return dfs.next(null);
      } catch (IOException e) {
        setHasNext.accept(false);
        throw new UncheckedIOException(e);
      } catch (Throwable e) {
        setHasNext.accept(false);
        throw e;
      }
    }), () -> !dfs.hasNext());
  }

  static <R extends IndexedRecord, M> ReadIterator<Anoa<R, M>> avro(AnoaFactory<M> anoaFactory,
                                                                    DataFileStream<R> dfs) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> anoaFactory.supplierChecked(() -> {
      try {
        return dfs.next(null);
      } catch (Throwable e) {
        setHasNext.accept(false);
        throw e;
      }
    }), () -> !dfs.hasNext());
  }

  static <R extends IndexedRecord> ReadIterator<R> avro(DatumReader<R> reader,
                                                        BinaryDecoder decoder) {
    return avro(reader, decoder, Unchecked.supplier(decoder::isEnd));
  }

  static <R extends IndexedRecord, M> ReadIterator<Anoa<R, M>> avro(
      AnoaFactory<M> anoaFactory,
      DatumReader<R> reader,
      BinaryDecoder decoder) {
    return avro(anoaFactory, reader, decoder, Unchecked.supplier(decoder::isEnd));
  }

  static <R extends IndexedRecord> ReadIterator<R> avro(DatumReader<R> reader,
                                                        JsonDecoder decoder) {
    return avro(reader, decoder, () -> false);
  }

  static <R extends IndexedRecord, M> ReadIterator<Anoa<R, M>> avro(
      AnoaFactory<M> anoaFactory,
      DatumReader<R> reader,
      JsonDecoder decoder) {
    return avro(anoaFactory, reader, decoder, () -> false);
  }

  static <R extends IndexedRecord> ReadIterator<R> avro(DatumReader<R> reader,
                                                        Decoder decoder,
                                                        Supplier<Boolean> eof) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> (() -> {
      try {
        return reader.read(null, decoder);
      } catch (EOFException e) {
        setHasNext.accept(false);
        return null;
      } catch (IOException e) {
        setHasNext.accept(false);
        throw new UncheckedIOException(e);
      }
    }), eof);
  }

  static <R extends IndexedRecord, M> ReadIterator<Anoa<R, M>> avro(
      AnoaFactory<M> anoaFactory,
      DatumReader<R> reader,
      Decoder decoder,
      Supplier<Boolean> eof) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> anoaFactory.supplierChecked(() -> {
      try {
        return reader.read(null, decoder);
      } catch (EOFException e) {
        setHasNext.accept(false);
        return null;
      } catch (Throwable e) {
        setHasNext.accept(false);
        throw e;
      }
    }), eof);
  }

  static <T extends TBase<T, ? extends TFieldIdEnum>> ReadIterator<T> thrift(
      TTransport tTransport,
      TProtocol tProtocol,
      Supplier<T> tSupplier) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> (() -> {
      final T result = tSupplier.get();
      try {
        result.read(tProtocol);
      } catch (TTransportException e) {
        if (TTransportException.END_OF_FILE == e.getType()) {
          setHasNext.accept(false);
          return null;
        } else {
          throw new RuntimeException(e);
        }
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      return result;
    }), () -> !tTransport.isOpen());
  }

  static <T extends TBase<T, ? extends TFieldIdEnum>, M> ReadIterator<Anoa<T, M>> thrift(
      AnoaFactory<M> anoaFactory,
      TTransport tTransport,
      TProtocol tProtocol,
      Supplier<T> tSupplier) {
    return new ReadIterator<>((Consumer<Boolean> setHasNext) -> anoaFactory.supplierChecked(() -> {
      final T result = tSupplier.get();
      try {
        result.read(tProtocol);
      } catch (TTransportException e) {
        setHasNext.accept(false);
        if (TTransportException.END_OF_FILE == e.getType()) {
          return null;
        } else {
          throw e;
        }
      } catch (Throwable e) {
        setHasNext.accept(false);
        throw e;
      }
      return result;
    }), () -> !tTransport.isOpen());
  }
}
