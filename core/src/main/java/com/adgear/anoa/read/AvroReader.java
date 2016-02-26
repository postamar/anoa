package com.adgear.anoa.read;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.stream.Stream;

abstract class AvroReader<R extends IndexedRecord>
    extends AbstractRecordReader<R, AvroFieldWrapper> {

  @SuppressWarnings("unchecked")
  private AvroReader(Schema schema) {
    super(buildFieldWrappers(schema));
  }

  static private Stream<AvroFieldWrapper> buildFieldWrappers(Schema schema) {
    ArrayList<AvroFieldWrapper> list = new ArrayList<>();
    int index = 0;
    for (Schema.Field field : schema.getFields()) {
      list.add(new AvroFieldWrapper(index++, field));
    }
    return list.stream();
  }


  static class GenericReader extends AvroReader<GenericRecord> {

    final private Schema schema;

    GenericReader(Schema schema) {
      super(schema);
      this.schema = schema;
    }

    @Override
    protected RecordWrapper<GenericRecord, AvroFieldWrapper> newWrappedInstance() {
      return new AvroRecordWrapper<>(new GenericData.Record(schema), fieldWrappers);
    }
  }


  static class SpecificReader<R extends SpecificRecord> extends AvroReader<R> {

    final private Constructor<R> constructor;

    SpecificReader(Class<R> recordClass) {
      super(SpecificData.get().getSchema(recordClass));
      try {
        this.constructor = recordClass.getDeclaredConstructor();
        constructor.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected RecordWrapper<R, AvroFieldWrapper> newWrappedInstance() {
      try {
        return new AvroRecordWrapper<>(constructor.newInstance(), fieldWrappers);
      } catch (InstantiationException | IllegalAccessException |InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
