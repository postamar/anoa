package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.HashMap;
import java.util.function.Function;

class MapReader extends AbstractReader<HashMap<CharSequence, Object>> {

  final AbstractReader<?> valueElementReader;
  final Function<String, CharSequence> fn;

  MapReader(AbstractReader<?> valueElementReader) {
    this(valueElementReader, s -> s);
  }

  MapReader(AbstractReader<?> valueElementReader, Function<String, CharSequence> fnKey) {
    this.valueElementReader = valueElementReader;
    this.fn = fnKey;
  }

  @Override
  protected HashMap<CharSequence, Object> read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.START_OBJECT) {
      HashMap<CharSequence, Object> result = new HashMap<>();
      doMap(jacksonParser, (k, p) -> result.put(fn.apply(k), valueElementReader.read(p)));
      return result;
    } else {
      gobbleValue(jacksonParser);
      return null;
    }
  }

  @Override
  protected HashMap<CharSequence, Object> readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_NULL:
        return null;
      case START_OBJECT:
        HashMap<CharSequence, Object> result = new HashMap<>();
        doMap(jacksonParser, (k, p) -> result.put(fn.apply(k), valueElementReader.readStrict(p)));
        return result;
      default:
        throw new AnoaJacksonTypeException("Token is not '{': " + jacksonParser.getCurrentToken());
    }
  }
}
