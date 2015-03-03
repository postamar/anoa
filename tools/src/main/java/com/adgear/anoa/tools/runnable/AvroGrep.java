package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.factory.AvroConsumers;
import com.adgear.anoa.factory.AvroSpecificStreams;
import com.adgear.anoa.factory.util.ReflectionUtils;
import com.adgear.anoa.factory.util.WriteConsumer;
import com.adgear.anoa.tools.function.AnoaSqlWhereFilter;

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Optional;

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
    AnoaSqlWhereFilter<R> filter = new AnoaSqlWhereFilter<>(recordClass, filterExpression);
    try (WriteConsumer<R> consumer = AvroConsumers.batch(outputStream, recordClass)) {
      AvroSpecificStreams.batch(inputStream, recordClass)
          .map(AnoaRecord::of)
          .map(filter)
          .map(AnoaRecord::asOptional)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .forEach(consumer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public void main(String[] args) throws Exception {
    new AvroGrep<>(ReflectionUtils.getAvroClass(System.getProperty("recordClass")),
                   System.getProperty("filterExpression"),
                   System.in,
                   System.out)
        .run();
  }
}
