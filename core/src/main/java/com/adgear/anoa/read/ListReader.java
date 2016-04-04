package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

class ListReader extends AbstractReader<List<Object>> {

  final AbstractReader<?> elementReader;
  final Supplier<List<Object>> listSupplier;

  ListReader(AbstractReader<?> elementReader) {
    this(elementReader, ArrayList::new);
  }

  ListReader(AbstractReader<?> elementReader, Supplier<List<Object>> listSupplier) {
    this.elementReader = elementReader;
    this.listSupplier = listSupplier;
  }

  @Override
  protected List<Object> read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_ARRAY) {
      List<Object> result = listSupplier.get();
      doArray(jacksonParser, p -> result.add(elementReader.read(p)));
      return result;
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  protected List<Object> readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_ARRAY:
        List<Object> result = listSupplier.get();
        doArray(jacksonParser, p -> result.add(elementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaJacksonTypeException("Token is not '[': " + jacksonParser.getCurrentToken());
    }
  }
}
