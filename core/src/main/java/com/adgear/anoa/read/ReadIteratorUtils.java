package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedSupplier;
import org.jooq.lambda.tuple.Tuple;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class ReadIteratorUtils {

  static <T> Stream<T> stream(Iterator<T> iterator) {
    final Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(
        iterator,
        Spliterator.NONNULL | Spliterator.ORDERED);
    return StreamSupport.stream(spliterator, false);
  }

  static protected <X, M> ReadIterator<Anoa<X, M>> anoa(
      AnoaFactory<M> anoaFactory,
      Supplier<Boolean> eofClosure,
      CheckedSupplier<X> supplier) {
    Anoa<X, M> anoaInitial = anoaFactory.wrap(null);
    UnaryOperator<Anoa<X, M>> anoaFn = ((Anoa<X, M> anoa) -> {
      final X result;
      try {
        result = supplier.get();
      } catch (Throwable e) {
        return new Anoa<>(Stream.concat(anoa.meta(), anoaFactory.handler.apply(e, Tuple.tuple())));
      }
      return new Anoa<>(Optional.of(result), anoa.meta());
    });
    return new ReadIterator<>(
        eofClosure,
        (Consumer<Boolean> setHasNext) -> ((Anoa<X, M> anoa) -> {
          if (anoa == null) {
            return anoaFn.apply(anoaInitial);
          } else if (anoa.isPresent()) {
            return anoaFn.apply(anoa);
          } else {
            setHasNext.accept(false);
            return null;
          }
        }));
  }


  static <N extends TreeNode> ReadIterator<N> jackson(JsonParser jacksonParser) {
    return new ReadIterator<>(
        Unchecked.supplier(jacksonParser::isClosed),
        (Consumer<Boolean> setHasNext) -> (__ -> {
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
        }));
  }

  static <N extends TreeNode, M> ReadIterator<Anoa<N, M>> jackson(AnoaFactory<M> anoaFactory,
                                                                  JsonParser jacksonParser) {
    return anoa(
        anoaFactory,
        Unchecked.supplier(jacksonParser::isClosed),
        () -> {
          final N tree = jacksonParser.readValueAsTree();
          if (tree == null && jacksonParser.getCurrentToken() == null) {
            throw new IOException("JsonParser::readValueAsTree returned null.");
          }
          return tree;
        });
  }

  static <R extends IndexedRecord> ReadIterator<R> avro(DataFileStream<R> dfs) {
    return new ReadIterator<>(
        () -> !dfs.hasNext(),
        (Consumer<Boolean> setHasNext) -> (__ -> {
          try {
            return dfs.next(null);
          } catch (IOException e) {
            setHasNext.accept(false);
            throw new UncheckedIOException(e);
          }
        }));
  }

  static <R extends IndexedRecord, M> ReadIterator<Anoa<R, M>> avro(AnoaFactory<M> anoaFactory,
                                                                    DataFileStream<R> dfs) {
    return anoa(
        anoaFactory,
        () -> !dfs.hasNext(),
        () -> dfs.next(null));
  }

  static <R extends IndexedRecord> ReadIterator<R> avro(DatumReader<R> reader,
                                                        Decoder decoder,
                                                        Supplier<Boolean> eof) {
    return new ReadIterator<>(
        eof,
        (Consumer<Boolean> setHasNext) -> (__ -> {
          try {
            return reader.read(null, decoder);
          } catch (EOFException e) {
            setHasNext.accept(false);
            return null;
          } catch (IOException e) {
            setHasNext.accept(false);
            throw new UncheckedIOException(e);
          }
        }));
  }

  static <R extends IndexedRecord, M> ReadIterator<Anoa<R, M>> avro(
      AnoaFactory<M> anoaFactory,
      DatumReader<R> reader,
      Decoder decoder,
      Supplier<Boolean> eof) {
    return anoa(anoaFactory, eof, () -> reader.read(null, decoder));
  }

  static <T extends TBase> ReadIterator<T> thrift(
      TProtocol tProtocol,
      Supplier<T> supplier) {
    final TTransport tTransport = tProtocol.getTransport();
    return new ReadIterator<>(
        () -> !tTransport.isOpen(),
        (Consumer<Boolean> setHasNext) -> (__ -> {
          final T record = supplier.get();
          try {
            record.read(tProtocol);
          } catch (TTransportException e) {
            setHasNext.accept(false);
            if (TTransportException.END_OF_FILE == e.getType()) {
              return null;
            } else {
              throw new RuntimeException(e);
            }
          } catch (TException e) {
            setHasNext.accept(false);
            throw new RuntimeException(e);
          }
          return record;
        }));
  }

  static <T extends TBase, M> ReadIterator<Anoa<T, M>> thrift(
      AnoaFactory<M> anoaFactory,
      TProtocol tProtocol,
      Supplier<T> supplier) {
    final TTransport tTransport = tProtocol.getTransport();
    return anoa(
        anoaFactory,
        () -> !tTransport.isOpen(),
        () -> {
          final T record = supplier.get();
          record.read(tProtocol);
          return record;
        });
  }
}