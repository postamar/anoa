package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Map;

class MapWriter<V> extends JacksonWriter<Map<CharSequence,V>> {

  final JacksonWriter<V> valueElementWriter;

  MapWriter(JacksonWriter<V> valueElementWriter) {
    this.valueElementWriter = valueElementWriter;
  }

  @Override
  public void write(Map<CharSequence, V> map, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();
    for (Map.Entry<CharSequence,V> entry : map.entrySet()) {
      jsonGenerator.writeFieldName(entry.getKey().toString());
      if (entry.getValue() == null) {
        jsonGenerator.writeNull();
      } else {
        valueElementWriter.write(entry.getValue(), jsonGenerator);
      }
    }
    jsonGenerator.writeEndObject();
  }
}
