package com.adgear.anoa.read;

import com.adgear.anoa.AnoaJacksonTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.UncheckedIOException;

abstract class AbstractReader<R> {

  final R read(JsonParser jacksonParser, Boolean strict) {
    try {
      return readChecked(jacksonParser, strict);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  final R readChecked(JsonParser jacksonParser, Boolean strict) throws IOException {
    jacksonParser.nextToken();
    return validateTopLevel(Boolean.TRUE.equals(strict)
                            ? readStrict(jacksonParser)
                            : read(jacksonParser));
  }

  protected R validateTopLevel(R record) {
    return record;
  }

  abstract protected R read(JsonParser jacksonParser) throws IOException;

  abstract protected R readStrict(JsonParser jacksonParser) throws AnoaJacksonTypeException, IOException;

  protected interface ValueConsumer<E extends Exception> {
    void accept(JsonParser jacksonParser) throws IOException, E;
  }

  protected interface FieldValueConsumer<E extends Exception> {
    void accept(String fieldName, JsonParser jacksonParser) throws IOException, E;
  }

  static protected <E extends Exception> void doArray(JsonParser jacksonParser,
                                                      ValueConsumer<E> valueConsumer)
      throws IOException, E {
    while (jacksonParser.nextToken() != JsonToken.END_ARRAY) {
      switch (jacksonParser.getCurrentToken()) {
        case END_OBJECT:
        case FIELD_NAME:
        case NOT_AVAILABLE:
          throw new IOException("Expected object value, not "
                                + jacksonParser.getCurrentToken());
      }
      valueConsumer.accept(jacksonParser);
    }
  }

  static protected <E extends Exception> void doMap(JsonParser jacksonParser,
                                                    FieldValueConsumer<E> fieldValueConsumer)
      throws IOException, E {
    while (jacksonParser.nextToken() != JsonToken.END_OBJECT) {
      if (jacksonParser.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw new IOException("Expected object field name, not "
                              + jacksonParser.getCurrentToken());
      }
      final String key = jacksonParser.getCurrentName();
      switch (jacksonParser.nextToken()) {
        case END_ARRAY:
        case END_OBJECT:
        case FIELD_NAME:
        case NOT_AVAILABLE:
          throw new IOException("Expected object value, not " + jacksonParser.getCurrentToken());
      }
      fieldValueConsumer.accept(key, jacksonParser);
    }
  }

  static protected void gobbleValue(JsonParser jacksonParser) throws IOException {
    switch (jacksonParser.getCurrentToken()) {
      case START_ARRAY:
        doArray(jacksonParser, AbstractReader::gobbleValue);
        break;
      case START_OBJECT:
        doMap(jacksonParser, (k, p) -> gobbleValue(p));
        break;
      case END_ARRAY:
      case END_OBJECT:
      case NOT_AVAILABLE:
      case FIELD_NAME:
        throw new IOException("Expected START_ARRAY, START_OBJECT, or value, not "
                              + jacksonParser.getCurrentToken());
    }
  }
}
