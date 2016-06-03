package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.function.Function;

class StringReader extends AbstractReader<CharSequence> {

  final Function<String, CharSequence> fn;

  StringReader() {
    this(s -> s);
  }

  StringReader(Function<String, CharSequence> fn) {
    this.fn = fn;
  }

  @Override
  protected CharSequence read(JsonParser jacksonParser) throws IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_STRING:
        return fn.apply(jacksonParser.getText());
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_EMBEDDED_OBJECT:
        return fn.apply(jacksonParser.getValueAsString());
      default:
        gobbleValue(jacksonParser);
        return null;
    }
  }

  @Override
  protected CharSequence readStrict(JsonParser jacksonParser)
      throws AnoaJacksonTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_STRING:
        return fn.apply(jacksonParser.getText());
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaJacksonTypeException(
            "Token is not string: " + jacksonParser.getCurrentToken());
    }
  }
}
