package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
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

  static protected <X, M> ReadIterator<Anoa<X, M>> anoa(
      AnoaHandler<M> anoaHandler,
      Supplier<Boolean> eofClosure,
      CheckedSupplier<X> supplier) {
    final Supplier<Anoa<X, M>> anoaSupplier = anoaHandler.supplierChecked(supplier);
    return new ReadIterator<>(
        eofClosure,
        (Consumer<Boolean> setHasNext) -> (anoa -> {
          if (anoa == null || anoa.isPresent()) {
            return anoaSupplier.get();
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

  static <N extends TreeNode, M> ReadIterator<Anoa<N, M>> jackson(AnoaHandler<M> anoaHandler,
                                                                  JsonParser jacksonParser) {
    return anoa(
        anoaHandler,
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

  static <R extends IndexedRecord, M> ReadIterator<Anoa<R, M>> avro(AnoaHandler<M> anoaHandler,
                                                                    DataFileStream<R> dfs) {
    return anoa(
        anoaHandler,
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
      AnoaHandler<M> anoaHandler,
      DatumReader<R> reader,
      Decoder decoder,
      Supplier<Boolean> eof) {
    return anoa(anoaHandler, eof, () -> reader.read(null, decoder));
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
      AnoaHandler<M> anoaHandler,
      TProtocol tProtocol,
      Supplier<T> supplier) {
    final TTransport tTransport = tProtocol.getTransport();
    return anoa(
        anoaHandler,
        () -> !tTransport.isOpen(),
        () -> {
          final T record = supplier.get();
          record.read(tProtocol);
          return record;
        });
  }
}