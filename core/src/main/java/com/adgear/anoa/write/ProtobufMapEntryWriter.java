package com.adgear.anoa.write;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class ProtobufMapEntryWriter<R extends Message> extends ProtobufWriter<R> {

  final Descriptors.FieldDescriptor keyField;
  final Descriptors.FieldDescriptor valueField;
  final AbstractWriter<Object> valueWriter;

  ProtobufMapEntryWriter(Message.Builder builder) {
    super(builder);
    Iterator<Map.Entry<Descriptors.FieldDescriptor, AbstractWriter<Object>>> iterator =
        fieldWriters.entrySet().iterator();
    this.keyField = iterator.next().getKey();
    Map.Entry<Descriptors.FieldDescriptor, AbstractWriter<Object>> valueEntry = iterator.next();
    this.valueField = valueEntry.getKey();
    this.valueWriter = valueEntry.getValue();
  }

  @Override
  void write(R msg, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeFieldName(msg.getField(keyField).toString());
    if (!valueField.isRepeated()) {
      valueWriter.write(msg.getField(valueField), jacksonGenerator);
    } else {
      int n = msg.getRepeatedFieldCount(valueField);
      jacksonGenerator.writeStartArray(n);
      for (int i = 0; i < n; i++) {
        valueWriter.write(msg.getRepeatedField(valueField, i), jacksonGenerator);
      }
      jacksonGenerator.writeEndArray();
    }
  }
}
