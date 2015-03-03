package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.factory.AvroConsumers;
import com.adgear.anoa.factory.AvroSpecificStreams;
import com.adgear.anoa.factory.util.ReflectionUtils;
import com.adgear.anoa.factory.util.WriteConsumer;
import com.adgear.anoa.tools.function.AnoaFieldNuller;

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Optional;

public class AvroClearFields<R extends SpecificRecord> implements Runnable {

  final protected Class<R> recordClass;
  final protected String[] fields;
  final protected InputStream inputStream;
  final protected OutputStream outputStream;

  public AvroClearFields(Class<R> recordClass,
                         InputStream inputStream,
                         OutputStream outputStream,
                         String... fields) {
    this.recordClass = recordClass;
    this.fields = fields;
    this.inputStream = inputStream;
    this.outputStream = outputStream;
  }

  @Override
  public void run() {
    try (WriteConsumer<R> consumer = AvroConsumers.batch(outputStream, recordClass)) {
      AvroSpecificStreams.batch(inputStream, recordClass)
          .map(AnoaRecord::of)
          .map(new AnoaFieldNuller<>(fields))
          .map(AnoaRecord::asOptional)
          .map(Optional::get)
          .forEach(consumer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public void main(String[] args) throws Exception {
    new AvroClearFields<>(ReflectionUtils.getAvroClass(System.getProperty("recordClass")),
                          System.in,
                          System.out,
                          System.getProperty("fields").split(","))
        .run();
  }

}
