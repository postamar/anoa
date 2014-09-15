package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.sink.avro.AvroSink;
import com.adgear.anoa.source.avro.AvroSpecificSource;
import com.adgear.anoa.tools.codec.FilterCodec;

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Runnable implementation of {@link com.adgear.anoa.tools.codec.FilterCodec}.
 *
 * @param <R> Type of the records to be processed, must extend Avro's {@link
 *            org.apache.avro.specific.SpecificRecord}.
 */
public class AvroGrep<R extends SpecificRecord> extends ToolBase {

  final private Class<R> recordClass;
  final private String filterExpression;
  final private InputStream in;
  final private OutputStream out;

  /**
   * @param filterExpression A JoSQL WHERE clause.
   * @param in               Input stream to Avro batch file, compatible with the provided record
   *                         class.
   * @param out              Output stream to Avro batch file.
   */
  public AvroGrep(Class<R> recordClass,
                  String filterExpression,
                  InputStream in,
                  OutputStream out) {
    this.recordClass = recordClass;
    this.filterExpression = filterExpression;
    this.in = in;
    this.out = out;
  }

  public static void main(String[] args) throws Exception {
    new AvroGrep<>(getRecordClass(System.getProperty("recordClass")),
                   System.getProperty("filterExpression"),
                   System.in,
                   System.out)
        .run();
  }

  @Override
  public void execute() throws IOException {
    new AvroSink<>(out, recordClass)
        .appendAll(new FilterCodec<>(new AvroSpecificSource<>(in, recordClass),
                                     recordClass,
                                     filterExpression))
        .close();
  }
}
