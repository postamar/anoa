package com.adgear.anoa.read;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

class ProtobufFieldWrapper {

  final Descriptors.FieldDescriptor field;
  final AbstractReader<?> reader;
  final ListReader listReader;

  ProtobufFieldWrapper(Descriptors.FieldDescriptor field,
                       Message.Builder parentBuilder) {
    this.field = field;
    this.reader = createReader(field, parentBuilder);
    this.listReader = new ListReader(reader);
  }

  @SuppressWarnings("unchecked")
  static private AbstractReader<?> createReader(Descriptors.FieldDescriptor field,
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

}
