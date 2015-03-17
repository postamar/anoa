package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.ArrayList;

class ListReader extends AbstractReader<ArrayList<Object>> {

  final AbstractReader<?> elementReader;

  ListReader(AbstractReader<?> elementReader) {
    this.elementReader = elementReader;
  }

  @Override
  protected ArrayList<Object> read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_ARRAY) {
      ArrayList<Object> result = new ArrayList<>();
      doArray(jacksonParser, p -> result.add(elementReader.read(p)));
      return result;
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  protected ArrayList<Object> readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_ARRAY:
        ArrayList<Object> result = new ArrayList<>();
        doArray(jacksonParser, p -> result.add(elementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaTypeException("Token is not '[': " + jacksonParser.getCurrentToken());
    }
  }
}
