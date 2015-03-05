package com.adgear.anoa.factory.util;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

public class AvroReadIterator<R extends IndexedRecord> extends AbstractReadIterator<R> {

  final public DatumReader<R> datumReader;
  final public Decoder decoder;

  public AvroReadIterator(DatumReader<R> datumReader,
                          Decoder decoder,
                          Supplier<Boolean> eofSupplier) {
    super(eofSupplier);
    this.datumReader = datumReader;
    this.decoder = decoder;
  }

  @Override
  protected R doNext() {
    try {
      return datumReader.read(null, decoder);
    } catch (EOFException e) {
      declareNoNext();
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
