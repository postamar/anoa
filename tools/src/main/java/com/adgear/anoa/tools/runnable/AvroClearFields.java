package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.AnoaReflectionUtils;
import com.adgear.anoa.read.AvroSpecificStreams;
import com.adgear.anoa.tools.function.AnoaFieldNuller;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.WriteConsumer;

import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

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
    try (WriteConsumer<R> consumer = AvroConsumers.batch(recordClass, outputStream)) {
      AvroSpecificStreams.batch(recordClass, inputStream)
          .map(new AnoaFieldNuller<>(fields))
          .forEach(consumer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public void main(String[] args) throws Exception {
    new AvroClearFields<>(AnoaReflectionUtils.getAvroClass(System.getProperty("recordClass")),
                          System.in,
                          System.out,
                          System.getProperty("fields").split(","))
        .run();
  }

}
