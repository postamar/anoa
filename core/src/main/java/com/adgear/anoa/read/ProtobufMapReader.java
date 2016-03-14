package com.adgear.anoa.read;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ProtobufMapReader extends AbstractReader<List<Object>> {

  final Descriptors.FieldDescriptor keyDescriptor;
  final Descriptors.FieldDescriptor valueDescriptor;
  final Message.Builder mapEntryBuilder;
  final MapReader mapReader;

  ProtobufMapReader(Descriptors.FieldDescriptor keyDescriptor,
                    Descriptors.FieldDescriptor valueDescriptor,
                    Message.Builder mapEntryBuilder,
                    AbstractReader<?> valueElementReader) {
    this.mapReader = new MapReader(valueElementReader);
    this.mapEntryBuilder = mapEntryBuilder.clone().clear();
    this.keyDescriptor = keyDescriptor;
    this.valueDescriptor = valueDescriptor;
  }

  private List<Object> toEntryList(Map<String, Object> map) {
    ArrayList<Object> result = new ArrayList<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      mapEntryBuilder.clear();
      mapEntryBuilder.setField(keyDescriptor, entry.getKey());
      if (valueDescriptor.isRepeated()) {
        for (Object element : (Iterable) entry.getValue()) {
          mapEntryBuilder.addRepeatedField(valueDescriptor, element);
        }
      } else {
        mapEntryBuilder.setField(valueDescriptor, entry.getValue());
      }
      result.add(mapEntryBuilder.build());
    }
    return result;
  }

  @Override
  protected List<Object> read(JsonParser jacksonParser) throws IOException {
    return toEntryList(mapReader.read(jacksonParser));
  }

  @Override
  protected List<Object> readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    return toEntryList(mapReader.readStrict(jacksonParser));
  }
}
