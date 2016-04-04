package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Collection;

class CollectionWriter<E> extends AbstractWriter<Collection<E>> {

  final AbstractWriter<E> elementWriter;

  CollectionWriter(AbstractWriter<E> elementWriter) {
    this.elementWriter = elementWriter;
  }

  @Override
  protected void write(Collection<E> array, JsonGenerator jacksonGenerator)
      throws IOException {
    jacksonGenerator.writeStartArray(array.size());
    for (E element : array) {
      if (element == null) {
        jacksonGenerator.writeNull();
      } else {
        elementWriter.write(element, jacksonGenerator);
      }
    }
    jacksonGenerator.writeEndArray();
  }

  @Override
  void writeStrict(Collection<E> array, JsonGenerator jacksonGenerator) throws IOException {
    jacksonGenerator.writeStartArray(array.size());
    for (E element : array) {
      if (element == null) {
        jacksonGenerator.writeNull();
      } else {
        elementWriter.writeStrict(element, jacksonGenerator);
      }
    }
    jacksonGenerator.writeEndArray();
  }
}
