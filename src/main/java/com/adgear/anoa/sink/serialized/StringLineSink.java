package com.adgear.anoa.sink.serialized;

import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.sink.Sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Collects String records into a newline-separated file.
 *
 * @see com.adgear.anoa.sink.Sink
 * @see com.adgear.anoa.source.serialized.StringLineSource
 * @see com.adgear.anoa.codec.serialized.JsonNodeToString
 */
public class StringLineSink implements Sink<String, StringLineSink> {

  final private BufferedWriter writer;

  public StringLineSink(BufferedWriter writer) {
    this.writer = writer;
  }

  /**
   * @param writer A writer which will be wrapped in a {@link java.io.BufferedWriter}.
   */
  public StringLineSink(Writer writer) {
    this(new BufferedWriter(writer));
  }

  @Override
  public StringLineSink append(String record) throws IOException {
    if (record != null) {
      writer.write(record);
      writer.write('\n');
    }
    return this;
  }

  @Override
  public StringLineSink appendAll(Provider<String> provider) throws IOException {
    for (String element : provider) {
      writer.write(element);
    }
    writer.flush();
    return this;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

}
