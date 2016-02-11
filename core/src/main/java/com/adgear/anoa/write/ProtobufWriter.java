package com.adgear.anoa.write;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.adgear.anoa.AnoaReflectionUtils;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class ProtobufWriter<R extends Message> extends AbstractWriter<R> {

  final private HashMap<Descriptors.FieldDescriptor, AbstractWriter<Object>> fieldMap;

  protected ProtobufWriter(Descriptors.Descriptor descriptor) {
    fieldMap = new HashMap<>();
    descriptor.getFields().forEach(this::updateFieldMap);
    descriptor.getExtensions().forEach(this::updateFieldMap);
  }

  ProtobufWriter(Class<R> recordClass) {
    this(AnoaReflectionUtils.getProtobufDescriptor(recordClass));
  }

  static private AbstractWriter<?> createWriter(
      Descriptors.FieldDescriptor fieldDescriptor) {
    switch (fieldDescriptor.getType()) {
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
        return new ProtobufWriter<>(fieldDescriptor.getMessageType());
      case STRING:
        return new StringWriter();
    }
    throw new RuntimeException("Unknown type for " + fieldDescriptor);
  }

  @SuppressWarnings("unchecked")
  private void updateFieldMap(Descriptors.FieldDescriptor fieldDescriptor) {
    final AbstractWriter<?> fieldWriter = createWriter(fieldDescriptor);
    final AbstractWriter<?> writer = fieldDescriptor.isRepeated()
                                     ? new CollectionWriter<>(fieldWriter)
                                     : fieldWriter;
    fieldMap.put(fieldDescriptor, (AbstractWriter<Object>) writer);
  }

  @Override
  void writeChecked(R msg, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeStartObject();
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : msg.getAllFields().entrySet()) {
      final Descriptors.FieldDescriptor field = entry.getKey();
      jacksonGenerator.writeFieldName(field.getName());
      fieldMap.get(field).writeChecked(entry.getValue(), jacksonGenerator);
    }
    jacksonGenerator.writeEndObject();
  }
}
