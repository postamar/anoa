package com.adgear.anoa;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.jooq.lambda.Unchecked;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AnoaAvro {

  static public <R extends IndexedRecord> @NonNull Function<byte[], R> fromBinaryFn(
      @NonNull DatumReader<R> reader,
      @Nullable Supplier<R> supplier) {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    return fromFn(reader,
                  supplier,
                  bytes -> DecoderFactory.get().binaryDecoder(bytes, decoder));
  }

  static public <R extends IndexedRecord> @NonNull Function<String, R> fromJsonFn(
      @NonNull DatumReader<R> reader,
      @Nullable Supplier<R> supplier) {
    Schema schema = supplier.get().getSchema();
    return fromFn(reader,
                  supplier,
                  Unchecked.function(s -> DecoderFactory.get().jsonDecoder(schema, s)));
  }

  static protected <T, R extends IndexedRecord, D extends Decoder> Function<T, R> fromFn(
      DatumReader<R> reader,
      Supplier<R> supplier,
      Function<T,D> decoderFn) {
    if (supplier == null) {
      return (T in) -> {
        try {
          return reader.read(null, decoderFn.apply(in));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
    } else {
      return (T in) -> {
        try {
          return reader.read(supplier.get(), decoderFn.apply(in));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
    }
  }

  static public <R extends IndexedRecord> @NonNull Function<R, byte[]> toBinaryFn(
      @NonNull DatumWriter<R> writer) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder reuse = EncoderFactory.get().directBinaryEncoder(baos, null);
    return (R record) -> {
      BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, reuse);
      baos.reset();
      try {
        writer.write(record, encoder);
        encoder.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return baos.toByteArray();
    };
  }

  static public <R extends IndexedRecord> @NonNull Function<R, String> toJsonFn(
      @NonNull DatumWriter<R> writer,
      @NonNull Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    return (R record) -> {
      baos.reset();
      try {
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);
        writer.write(record, encoder);
        encoder.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return baos.toString();
    };
  }

  static public @NonNull Stream<GenericRecord> from(@NonNull File file, @NonNull Schema schema) {
    try {
      return from(DataFileReader.<GenericRecord>openReader(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> from(@NonNull InputStream inputStream,
                                                    @NonNull Schema schema) {
    try {
      return from(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> from(@NonNull File file,
                                                                   @NonNull Class<R> recordClass) {
    try {
      return from(DataFileReader.openReader(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> from(@NonNull InputStream inputStream,
                                                                   @NonNull Class<R> recordClass) {
    try {
      return from(new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> fromGeneric(@NonNull File file) {
    try {
      return from(DataFileReader.<GenericRecord>openReader(file, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> fromGeneric(@NonNull InputStream inputStream) {
    try {
      return from(new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> fromSpecific(@NonNull File file) {
    try {
      return from(DataFileReader.<R>openReader(file, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> fromSpecific(
      @NonNull InputStream inputStream) {
    try {
      return from(new DataFileStream<>(inputStream, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static protected  <R extends IndexedRecord> Stream<R> from(Iterable<R> iterable) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                                    iterable.iterator(),
                                    Spliterator.NONNULL | Spliterator.ORDERED),
                                false);
  }

  static public @NonNull DataFileWriter<GenericRecord> to(@NonNull File file,
                                                          @NonNull Schema schema) {
    try {
      return new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
          .create(schema, file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull DataFileWriter<R> to(
      @NonNull File file,
      @NonNull Class<R> recordClass) {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new AvroRuntimeException("No schema found for class " + recordClass);
    }
    try {
      return new DataFileWriter<R>(new SpecificDatumWriter<>(schema)).create(schema, file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull DataFileWriter<GenericRecord> to(@NonNull OutputStream outputStream,
                                                          @NonNull Schema schema) {
    try {
      return new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
          .create(schema, outputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull DataFileWriter<R> to(
      @NonNull OutputStream outputStream,
      @NonNull Class<R> recordClass) {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new AvroRuntimeException("No schema found for class " + recordClass);
    }
    try {
      return new DataFileWriter<R>(new SpecificDatumWriter<>(schema)).create(schema, outputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
