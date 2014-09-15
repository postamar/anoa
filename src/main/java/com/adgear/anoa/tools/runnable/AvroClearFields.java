package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.sink.avro.AvroSink;
import com.adgear.anoa.source.avro.AvroSpecificSource;
import com.adgear.anoa.tools.codec.CleanserCodec;

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Runnable implementation of {@link com.adgear.anoa.tools.codec.CleanserCodec} on Avro {@link
 * org.apache.avro.specific.SpecificRecord}.
 *
 * @param <R> Type of the records to be processed, must extend Avro's {@link
 *            org.apache.avro.specific.SpecificRecord}.
 */
public class AvroClearFields<R extends SpecificRecord> extends ToolBase {

  final private Class<R> recordClass;
  final private String[] fields;
  final private InputStream in;
  final private OutputStream out;

  /**
   * @param fields Names of fields to be nulled.
   * @param in     Input stream to Avro batch file, compatible with the provided record class.
   * @param out    Output stream to Avro batch file.
   */
  public AvroClearFields(Class<R> recordClass, String[] fields, InputStream in, OutputStream out)
      throws IOException {
    this.recordClass = recordClass;
    this.fields = fields;
    this.in = in;
    this.out = out;
  }

  static public void main(String[] args) throws Exception {
    new AvroClearFields<>(getRecordClass(System.getProperty("recordClass")),
                          System.getProperty("fields").split(","),
                          System.in,
                          System.out)
        .run();
  }

  @Override
  public void execute() throws IOException {
    try {
      new AvroSink<>(out, recordClass)
          .appendAll(new CleanserCodec<>(new AvroSpecificSource<>(in, recordClass), fields))
          .close();
    } catch (RuntimeException e) {
      if (e.getCause() != null && e.getCause() instanceof NoSuchFieldException) {
        NoSuchFieldException nsfe = (NoSuchFieldException) e.getCause();
        System.err.println("Unknown field: " + nsfe.getMessage());
        System.exit(1);
      } else {
        throw e;
      }
    }
  }
}
