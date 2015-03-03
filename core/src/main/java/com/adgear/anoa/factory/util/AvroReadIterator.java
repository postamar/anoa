package com.adgear.anoa.factory.util;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;

public class AvroReadIterator<R extends IndexedRecord> implements ReadIterator<R> {

  final public DatumReader<R> datumReader;
  final public Decoder decoder;

  public AvroReadIterator(DatumReader<R> datumReader, Decoder decoder) {
    this.datumReader = datumReader;
    this.decoder = decoder;
  }

  private R nextValue = null;

  @Override
  public boolean hasNext() {
    if (nextValue != null) {
      return true;
    }
    try {
      nextValue = datumReader.read(null, decoder);
    } catch (EOFException e) {
      nextValue = null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return (nextValue != null);
  }

  @Override
  public R next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final R value = nextValue;
    nextValue = null;
    return value;
  }
}
