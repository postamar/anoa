package com.adgear.anoa.tools.runnable;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.jooq.lambda.Unchecked;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Runnable for concatenating Avro record batches from multiple input streams.
 */
public class AvroConcatenator implements Runnable {

  final protected List<InputStream> inputStreams;
  final protected OutputStream outputStream;

  /**
   * @param inputStreams Avro batch file input streams. The first stream provides the {@link
   *                     org.apache.avro.Schema} used during processing.
   * @param outputStream Output stream to Avro batch file.
   */
  public AvroConcatenator(List<InputStream> inputStreams, OutputStream outputStream) {
    this.inputStreams = inputStreams;
    this.outputStream = outputStream;
    if (inputStreams.isEmpty()) {
      throw new IllegalArgumentException("Requires at least 1 input stream.");
    }
  }

  @Override
  public void run() {
    try {
      DataFileStream<GenericRecord> dfs0 = new DataFileStream<>(inputStreams.get(0),
                                                                new GenericDatumReader<>());
      Schema schema = dfs0.getSchema();
      Function<InputStream, DataFileStream<GenericRecord>> builder = Unchecked.function(
          stream -> new DataFileStream<>(stream, new GenericDatumReader<>(schema)));

      try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
          new GenericDatumWriter<GenericRecord>(schema))
          .create(schema, outputStream)) {
        writer.appendAllFrom(dfs0, true);
        inputStreams.stream().skip(1).sequential()
            .map(builder)
            .forEach(Unchecked.consumer(dfs -> writer.appendAllFrom(dfs, true)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public void main(String[] args) {
    List<InputStream> inputs = Stream.of(args)
        .map(File::new)
        .peek(f -> {
          if (!f.exists())
            throw new IllegalArgumentException("File '" + f + "' does not exist.");
        })
        .peek(f -> {if (f.isDirectory())
          throw new IllegalArgumentException("File '" + f + "' is a directory.");})
        .map(Unchecked.function(FileInputStream::new))
        .collect(Collectors.toList());
    new AvroConcatenator(inputs, System.out).run();
  }
}
