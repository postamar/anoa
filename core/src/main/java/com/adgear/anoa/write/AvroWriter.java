package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class AvroWriter<R extends IndexedRecord> extends AbstractRecordWriter<R> {

  final Map<Schema.Field, AbstractWriter<Object>> fieldWriters;
  final Map<Schema.Field, Object> fieldDefaults;

  final DefaultValueTester tester;
  final Schema schema;

  AvroWriter(Class<R> recordClass) {
    this(new AvroWriterSpecificData(), SpecificData.get().getSchema(recordClass));
  }

  AvroWriter(Schema schema) {
    this(new AvroWriterGenericData(), schema);
  }

  @SuppressWarnings("unchecked")
  private AvroWriter(DefaultValueTester tester, Schema schema) {
    this.tester = tester;
    this.schema = schema;
    this.fieldWriters = new LinkedHashMap<>();
    this.fieldDefaults = new HashMap<>();
    schema.getFields().stream().forEach(f -> {
      fieldWriters.put(f, (AbstractWriter<Object>) createWriter(f.schema()));
      if (f.defaultValue() != null) {
        fieldDefaults.put(f, tester.getDefaultValue(f));
      }
    });
  }

  private interface DefaultValueTester {
    Object getDefaultValue(Schema.Field field);
    boolean testInequality(Object o1, Object o2, Schema schema);
  }

  static private class AvroWriterGenericData extends GenericData implements DefaultValueTester {

    @Override
    public boolean testInequality(Object o1, Object o2, Schema schema) {
      return compare(o1, o2, schema, true) != 0;
    }
  }

  static private class AvroWriterSpecificData extends SpecificData implements DefaultValueTester {

    @Override
    public boolean testInequality(Object o1, Object o2, Schema schema) {
      return compare(o1, o2, schema, true) != 0;
    }
  }



  protected AbstractWriter<?> createWriter(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return new CollectionWriter<>(createWriter(schema.getElementType()));
      case BOOLEAN:
        return new BooleanWriter();
      case BYTES:
        return new ByteBufferWriter();
      case DOUBLE:
        return new DoubleWriter();
      case ENUM:
        return new EnumWriter();
      case FIXED:
        return new AvroFixedWriter();
      case FLOAT:
        return new FloatWriter();
      case INT:
        return new IntegerWriter();
      case LONG:
        return new LongWriter();
      case MAP:
        return new MapWriter<>(createWriter(schema.getValueType()));
      case RECORD:
        return new AvroWriter<>(tester, schema);
      case STRING:
        return new StringWriter();
      case UNION:
        if (schema.getTypes().size() == 2) {
          return createWriter(schema.getTypes().get(
              (schema.getTypes().get(0).getType() == Schema.Type.NULL) ? 1 : 0));
        }
    }
    throw new RuntimeException("Unsupported Avro schema: " + schema);
  }

  @Override
  protected void write(R record, JsonGenerator jacksonGenerator) throws IOException {
    if (!record.getSchema().equals(schema)) {
      throw new IOException("Record does not have correct Avro schema:\n"
                            + record.getSchema().toString(true));
    }
    jacksonGenerator.writeStartObject();
    for (Map.Entry<Schema.Field, AbstractWriter<Object>> entry : fieldWriters.entrySet()) {
      final Schema.Field field = entry.getKey();
      final Object value = record.get(field.pos());
      if (value != null) {
        final Object defaultValue = fieldDefaults.get(field);
        if (defaultValue == null || tester.testInequality(value, defaultValue, field.schema())) {
          jacksonGenerator.writeFieldName(field.name());
          entry.getValue().write(value, jacksonGenerator);
        }
      }
    }
    jacksonGenerator.writeEndObject();
  }
}
