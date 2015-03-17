package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.AnoaReflectionUtils;
import com.adgear.anoa.read.AvroSpecificStreams;
import com.adgear.anoa.tools.function.AnoaSqlWhereFilter;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.WriteConsumer;

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

  @Override
  public void run() {
    AnoaSqlWhereFilter<R> predicate = new AnoaSqlWhereFilter<>(recordClass, filterExpression);
    try (WriteConsumer<R, IOException> consumer = AvroConsumers.batch(outputStream, recordClass)) {
      AvroSpecificStreams.batch(inputStream, recordClass)
          .filter(predicate)
          .forEach(consumer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public void main(String[] args) throws Exception {
    new AvroGrep<>(AnoaReflectionUtils.getAvroClass(System.getProperty("recordClass")),
                   System.getProperty("filterExpression"),
                   System.in,
                   System.out)
        .run();
  }
}
