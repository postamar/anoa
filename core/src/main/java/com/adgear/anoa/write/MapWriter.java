package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Map;

class MapWriter<V> extends AbstractWriter<Map<CharSequence, V>> {

  final AbstractWriter<V> valueElementWriter;

  MapWriter(AbstractWriter<V> valueElementWriter) {
    this.valueElementWriter = valueElementWriter;
  }

  @Override
  protected void write(Map<CharSequence, V> map, JsonGenerator jacksonGenerator)
      throws IOException {
    jacksonGenerator.writeStartObject();
    for (Map.Entry<CharSequence, V> entry : map.entrySet()) {
      jacksonGenerator.writeFieldName(entry.getKey().toString());
      if (entry.getValue() == null) {
        jacksonGenerator.writeNull();
      } else {
        valueElementWriter.write(entry.getValue(), jacksonGenerator);
      }
    }
    jacksonGenerator.writeEndObject();
  }

  @Override
  void writeStrict(Map<CharSequence, V> map, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeStartObject();
    for (Map.Entry<CharSequence, V> entry : map.entrySet()) {
      jacksonGenerator.writeFieldName(entry.getKey().toString());
      if (entry.getValue() == null) {
        jacksonGenerator.writeNull();
      } else {
        valueElementWriter.writeStrict(entry.getValue(), jacksonGenerator);
      }
    }
    jacksonGenerator.writeEndObject();  }
}
