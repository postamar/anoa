package com.adgear.anoa.library.write;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.adgear.anoa.AnoaReflectionUtils;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class ProtobufWriter<R extends Message> extends AbstractRecordWriter<R> {

  final Map<Descriptors.FieldDescriptor, AbstractWriter<Object>> fieldWriters;
  final Map<Descriptors.FieldDescriptor, Object> fieldDefaults;

  final boolean isMapEntry;
  final Message.Builder builder;

  @SuppressWarnings("unchecked")
  protected ProtobufWriter(Message.Builder builder) {
    this.builder = builder;
    fieldWriters = new LinkedHashMap<>();
    fieldDefaults = new HashMap<>();
    Descriptors.Descriptor descriptor = builder.getDescriptorForType();
    isMapEntry = descriptor.getOptions().getMapEntry();
    descriptor.getFields().forEach(this::updateMapsWithField);
  }

  ProtobufWriter(Class<R> recordClass) {
    this((Message.Builder) AnoaReflectionUtils.getProtobufBuilder(recordClass).clone().clear());
  }

  private AbstractWriter<?> createWriter(Descriptors.FieldDescriptor field) {
    switch (field.getType()) {
      case BOOL:
        return new BooleanWriter();
      case BYTES:
        return new ProtobufByteStringWriter();
      case DOUBLE:
        return new DoubleWriter();
      case ENUM:
        return new EnumWriter();
      case FIXED32:
      case INT32:
      case SFIXED32:
      case SINT32:
      case UINT32:
        return new IntegerWriter();
      case FIXED64:
      case INT64:
      case SFIXED64:
      case SINT64:
      case UINT64:
        return new LongWriter();
      case FLOAT:
        return new FloatWriter();
      case GROUP:
      case MESSAGE:
        return field.getMessageType().getOptions().getMapEntry()
               ? new ProtobufMapEntryWriter<>(builder.newBuilderForField(field))
               : new ProtobufWriter<>(builder.newBuilderForField(field));
      case STRING:
        return new StringWriter();
    }
    throw new RuntimeException("Unknown type for " + field);
  }

  @SuppressWarnings("unchecked")
  private void updateMapsWithField(Descriptors.FieldDescriptor field) {
    fieldWriters.put(field, (AbstractWriter<Object>) createWriter(field));
    if (!field.isRepeated()) {
      if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
        fieldDefaults.put(field, builder.newBuilderForField(field).getDefaultInstanceForType());
      } else {
        fieldDefaults.put(field, builder.getField(field));
      }
    }
  }

  @Override
  void write(R msg, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeStartObject();
    for (Map.Entry<Descriptors.FieldDescriptor, AbstractWriter<Object>> e : fieldWriters.entrySet()) {
      Descriptors.FieldDescriptor field = e.getKey();
      AbstractWriter<Object> writer = e.getValue();
      if (field.isRepeated()) {
        int n = msg.getRepeatedFieldCount(field);
        if (n > 0) {
          jacksonGenerator.writeFieldName(field.getName());
          boolean isFieldMap = (writer instanceof ProtobufWriter)
                               && ((ProtobufWriter) writer).isMapEntry;
          if (isFieldMap) {
            jacksonGenerator.writeStartObject();
          } else {
            jacksonGenerator.writeStartArray(n);
          }
          for (int i = 0; i < n; i++) {
            Object value = msg.getRepeatedField(field, i);
            if (value == null) {
              jacksonGenerator.writeNull();
            } else {
              writer.write(value, jacksonGenerator);
            }            }
          if (isFieldMap) {
            jacksonGenerator.writeEndObject();
          } else {
            jacksonGenerator.writeEndArray();
          }
        }
      } else if (msg.hasField(field)) {
        Object value = msg.getField(field);
        if (value != null && !value.equals(fieldDefaults.get(field))) {
          jacksonGenerator.writeFieldName(field.getName());
          writer.write(value, jacksonGenerator);
        }
      }
    }
    jacksonGenerator.writeEndObject();
  }

  @Override
  void writeStrict(R msg, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeStartObject();
    for (Map.Entry<Descriptors.FieldDescriptor, AbstractWriter<Object>> e : fieldWriters.entrySet()) {
      Descriptors.FieldDescriptor field = e.getKey();
      AbstractWriter<Object> writer = e.getValue();
      if (field.isRepeated()) {
        int n = msg.getRepeatedFieldCount(field);
        if (n > 0) {
          jacksonGenerator.writeFieldName(field.getName());
          boolean isFieldMap = (writer instanceof ProtobufWriter)
                               && ((ProtobufWriter) writer).isMapEntry;
          if (isFieldMap) {
            jacksonGenerator.writeStartObject();
          } else {
            jacksonGenerator.writeStartArray(n);
          }
          for (int i = 0; i < n; i++) {
            Object value = msg.getRepeatedField(field, i);
            if (value == null) {
              jacksonGenerator.writeNull();
            } else {
              writer.writeStrict(value, jacksonGenerator);
            }
          }
          if (isFieldMap) {
            jacksonGenerator.writeEndObject();
          } else {
            jacksonGenerator.writeEndArray();
          }
        }
      } else if (msg.hasField(field)) {
        Object value = msg.getField(field);
        jacksonGenerator.writeFieldName(field.getName());
        if (value == null
            || (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && value.equals(fieldDefaults.get(field)))) {
          jacksonGenerator.writeNull();
        } else {
          writer.writeStrict(value, jacksonGenerator);
        }
      }
    }
    jacksonGenerator.writeEndObject();  }
}
