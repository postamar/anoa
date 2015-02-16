package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

class AvroWriter<R extends IndexedRecord> extends JsonWriter<R> {

  final private LinkedHashMap<Schema.Field,JsonWriter<Object>> fieldMap;

  AvroWriter(Class<R> recordClass) {
    this(SpecificData.get().getSchema(recordClass));
  }

  @SuppressWarnings("unchecked")
  AvroWriter(Schema schema) {
    fieldMap = new LinkedHashMap<>();
    schema.getFields().stream()
        .forEach(f -> fieldMap.put(f, (JsonWriter<Object>) createWriter(f.schema())));
  }

  @Override
  public void write(R record, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();
    for (Map.Entry<Schema.Field,JsonWriter<Object>> entry : fieldMap.entrySet()) {
      Schema.Field field = entry.getKey();
      Object value = record.get(field.pos());
      if (!(value == null && (field.defaultValue() == null || field.defaultValue().isNull()))) {
        jsonGenerator.writeFieldName(field.name());
        entry.getValue().write(value, jsonGenerator);
      }
    }
    jsonGenerator.writeEndObject();
  }

  static protected JsonWriter<?> createWriter(Schema schema) {
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
