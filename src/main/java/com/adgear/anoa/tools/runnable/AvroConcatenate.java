package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.sink.avro.AvroSink;
import com.adgear.anoa.source.avro.AvroGenericSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A Runnable for concatenating Avro record batches from multiple input streams.
 */
public class AvroConcatenate extends ToolBase {

  final private Collection<InputStream> inputStreams;
  final private OutputStream outputStream;

  /**
   * @param inputStreams Avro batch file input streams. The first stream provides the {@link
   *                     org.apache.avro.Schema} used during processing.
   * @param outputStream Output stream to Avro batch file.
   */
  public AvroConcatenate(Collection<InputStream> inputStreams, OutputStream outputStream) {
    this.inputStreams = inputStreams;
    this.outputStream = outputStream;
    if (inputStreams.isEmpty()) {
      throw new IllegalArgumentException("inputStreams must not be empty.");
    }
  }

  public static void main(String[] args) throws Exception {
    List<InputStream> streams = new ArrayList<>();
    for (String path : args) {
      File file = new File(path);
      if (!file.exists()) {
        throw new IOException("File '" + path + "' does not exist.");
      }
      if (file.isDirectory()) {
        throw new IOException("File '" + path + "' is a directory.");
      }
      streams.add(new FileInputStream(file));
    }
    new AvroConcatenate(streams, System.out).run();
  }

  @Override
  public void execute() throws IOException {
    Schema schema = null;
    List<AvroGenericSource> sources = new ArrayList<>();
    for (InputStream in : inputStreams) {
      if (schema == null) {
        AvroGenericSource source = new AvroGenericSource(in);
        schema = source.getAvroSchema();
        sources.add(source);
      } else {
        sources.add(new AvroGenericSource(in, schema));
      }
    }
    AvroSink<GenericRecord> sink = new AvroSink<>(outputStream, schema);
    for (AvroGenericSource source : sources) {
      sink.appendAll(source);
      source.close();
    }
    sink.close();
  }
}
