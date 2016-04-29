package com.adgear.anoa.library.tools.runnable;

import com.adgear.anoa.AnoaReflectionUtils;
import com.adgear.anoa.library.tools.function.AnoaSqlWhereFilter;
import com.adgear.anoa.library.write.AvroConsumers;
import com.adgear.anoa.library.write.WriteConsumer;
import com.adgear.anoa.read.AvroStreams;

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

public class AvroGrep<R extends SpecificRecord> implements Runnable {

  final protected Class<R> recordClass;
  final protected String filterExpression;
  final protected InputStream inputStream;
  final protected OutputStream outputStream;

  public AvroGrep(Class<R> recordClass,
                  String filterExpression,
                  InputStream inputStream,
                  OutputStream outputStream) {
    this.recordClass = recordClass;
    this.filterExpression = filterExpression;
    this.inputStream = inputStream;
    this.outputStream = outputStream;
  }

  static public void main(String[] args) throws Exception {
    new AvroGrep<>(AnoaReflectionUtils.getAvroClass(System.getProperty("recordClass")),
                   System.getProperty("filterExpression"),
                   System.in,
                   System.out)
        .run();
  }

  @Override
  public void run() {
    AnoaSqlWhereFilter<R> predicate = new AnoaSqlWhereFilter<>(recordClass, filterExpression);
    try (WriteConsumer<R> consumer = AvroConsumers.batch(recordClass, outputStream)) {
      AvroStreams.batch(recordClass, inputStream)
          .filter(predicate)
          .forEach(consumer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
