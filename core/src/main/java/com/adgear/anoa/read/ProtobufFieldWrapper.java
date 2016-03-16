package com.adgear.anoa.read;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.stream.Stream;

class ProtobufFieldWrapper implements FieldWrapper {

  final Descriptors.FieldDescriptor field;
  final private Object defaultValue;
  final private AbstractReader<?> reader;

  ProtobufFieldWrapper(Descriptors.FieldDescriptor field, Message.Builder parentBuilder) {
    this.field = field;
    this.reader = createReader(field, parentBuilder);
    this.defaultValue = field.isRepeated()
                        ? null
                        : parentBuilder.getDefaultInstanceForType().getField(field);
  }

  @Override
  public Stream<String> getNames() {
    return Stream.of(field.getName());
  }

  @Override
  public AbstractReader<?> getReader() {
    return reader;
  }

  static private AbstractReader<?> createReader(Descriptors.FieldDescriptor field,
                                                Message.Builder parentBuilder) {
    if (!field.isRepeated()) {
      return createBaseReader(field, parentBuilder);
    }
    if (field.getType() != Descriptors.FieldDescriptor.Type.MESSAGE
        || !field.getMessageType().getOptions().getMapEntry()) {
      return new ListReader(createBaseReader(field, parentBuilder));
    }
    Message.Builder mapEntryBuilder = parentBuilder.newBuilderForField(field);
    Descriptors.Descriptor mapEntryDescriptor = field.getMessageType();
    Descriptors.FieldDescriptor keyDescriptor = mapEntryDescriptor.getFields().get(0);
    Descriptors.FieldDescriptor valueDescriptor = mapEntryDescriptor.getFields().get(1);
    return new ProtobufMapReader(keyDescriptor,
                                 valueDescriptor,
                                 mapEntryBuilder,
                                 createReader(valueDescriptor, mapEntryBuilder));
  }

  @SuppressWarnings("unchecked")
  static private AbstractReader<?> createBaseReader(Descriptors.FieldDescriptor field,
                                                    Message.Builder parentBuilder) {
    switch (field.getType()) {
      case BOOL:
        return new BooleanReader();
      case BYTES:
        return new ProtobufByteStringReader();
      case DOUBLE:
        return new DoubleReader();
      case ENUM:
        final Object defaultValue = field.isRepeated() ? null : field.getDefaultValue();
        return new ProtobufEnumReader(field.getEnumType(),
                                      (Descriptors.EnumValueDescriptor) defaultValue);
      case FIXED32:
      case INT32:
      case SFIXED32:
      case SINT32:
      case UINT32:
        return new IntegerReader();
      case FIXED64:
      case INT64:
      case SFIXED64:
      case SINT64:
      case UINT64:
        return new LongReader();
      case FLOAT:
        return new FloatReader();
      case GROUP:
      case MESSAGE:
        return new ProtobufReader(parentBuilder.newBuilderForField(field));
      case STRING:
        return new StringReader();
    }
    throw new RuntimeException("Unknown type for " + field);
  }

  @Override
  public boolean equalsDefaultValue(Object value) {
    return value == null || value.equals(defaultValue);
  }
}
