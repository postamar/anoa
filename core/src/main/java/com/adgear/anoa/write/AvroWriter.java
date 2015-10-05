package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

class AvroWriter<R extends IndexedRecord> extends AbstractWriter<R> {

  final private LinkedHashMap<Schema.Field,AbstractWriter<Object>> fieldMap;

  private Schema schema;

  AvroWriter(Class<R> recordClass) {
    this(SpecificData.get().getSchema(recordClass));
  }

  @SuppressWarnings("unchecked")
  AvroWriter(Schema schema) {
    this.fieldMap = new LinkedHashMap<>();
    this.schema = schema;
    schema.getFields().stream()
        .forEach(f -> fieldMap.put(f, (AbstractWriter<Object>) createWriter(f.schema())));
  }

  @Override
  protected void writeChecked(R record, JsonGenerator jacksonGenerator) throws IOException {
    if (!record.getSchema().equals(schema)) {
      throw new IOException("Record does not have correct Avro schema:\n"
                            + record.getSchema().toString(true));
    }
    jacksonGenerator.writeStartObject();
    for (Map.Entry<Schema.Field,AbstractWriter<Object>> entry : fieldMap.entrySet()) {
      Schema.Field field = entry.getKey();
      Object value = record.get(field.pos());
      if (!(value == null && (field.defaultValue() == null || field.defaultValue().isNull()))) {
        jacksonGenerator.writeFieldName(field.name());
        entry.getValue().writeChecked(value, jacksonGenerator);
      }
    }
    jacksonGenerator.writeEndObject();
  }

  static protected AbstractWriter<?> createWriter(Schema schema) {
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
        return new AvroWriter<>(schema);
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
}
