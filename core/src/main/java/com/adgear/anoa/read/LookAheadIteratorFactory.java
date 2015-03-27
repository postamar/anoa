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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.function.Consumer;
import java.util.function.Supplier;

class LookAheadIteratorFactory {

  protected LookAheadIteratorFactory() {
  }

  static protected <X, M> LookAheadIterator<Anoa<X, M>> anoa(
      AnoaHandler<M> anoaHandler,
      Supplier<Boolean> noNext,
      CheckedSupplier<X> supplier,
      Closeable closeable) {
    final Supplier<Anoa<X, M>> anoaSupplier = anoaHandler.supplierChecked(supplier);
    return new LookAheadIterator<>(
        noNext,
        (Consumer<Boolean> setHasNext) -> (anoa -> {
          if (anoa == null || anoa.isPresent()) {
            return anoaSupplier.get();
          } else {
            setHasNext.accept(false);
            return null;
          }
        }),
        closeable);
  }

  static <N extends TreeNode> LookAheadIterator<N> jackson(
      JsonParser jacksonParser) {
    return new LookAheadIterator<>(
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
        }),
        jacksonParser);
  }

  static <N extends TreeNode, M> LookAheadIterator<Anoa<N, M>> jackson(
      AnoaHandler<M> anoaHandler,
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
        },
        jacksonParser);
  }

  static <R extends IndexedRecord> LookAheadIterator<R> avro(
      DataFileStream<R> dfs) {
    return new LookAheadIterator<>(
        () -> !dfs.hasNext(),
        (Consumer<Boolean> setHasNext) -> (__ -> {
          try {
            return dfs.next(null);
          } catch (IOException e) {
            setHasNext.accept(false);
            throw new UncheckedIOException(e);
          }
        }),
        dfs);
  }

  static <R extends IndexedRecord, M> LookAheadIterator<Anoa<R, M>> avro(
      AnoaHandler<M> anoaHandler,
      DataFileStream<R> dfs) {
    return anoa(
        anoaHandler,
        () -> !dfs.hasNext(),
        () -> dfs.next(null),
        dfs);
  }

  static <R extends IndexedRecord> LookAheadIterator<R> avro(
      DatumReader<R> reader,
      Decoder decoder,
      Supplier<Boolean> eof,
      Closeable closeable) {
    return new LookAheadIterator<>(
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
        }),
        closeable);
  }

  static <R extends IndexedRecord, M> LookAheadIterator<Anoa<R, M>> avro(
      AnoaHandler<M> anoaHandler,
      DatumReader<R> reader,
      Decoder decoder,
      Supplier<Boolean> eof,
      Closeable closeable) {
    return anoa(anoaHandler, eof, () -> reader.read(null, decoder), closeable);
  }

  static <T extends TBase> LookAheadIterator<T> thrift(
      TProtocol tProtocol,
      Supplier<T> supplier) {
    final TTransport tTransport = tProtocol.getTransport();
    return new LookAheadIterator<>(
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
        }),
        tTransport);
  }

  static <T extends TBase, M> LookAheadIterator<Anoa<T, M>> thrift(
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
        },
        tTransport);
  }
}