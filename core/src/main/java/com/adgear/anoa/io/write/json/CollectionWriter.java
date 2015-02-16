package com.adgear.anoa.io.write.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Collection;

class CollectionWriter<E> extends JsonWriter<Collection<E>> {

  final JsonWriter<E> elementWriter;

  CollectionWriter(JsonWriter<E> elementWriter) {
    this.elementWriter = elementWriter;
  }

  @Override
  public void write(Collection<E> array, JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartArray(array.size());
    for (E element : array) {
      if (element == null) {
        jsonGenerator.writeNull();
      } else {
        elementWriter.write(element, jsonGenerator);
      }
    }
    jsonGenerator.writeEndArray();
  }
}
