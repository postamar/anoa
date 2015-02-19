package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiFunction;

abstract class JacksonReader<R> implements BiFunction<JsonParser,Boolean,R> {

  @Override
  public R apply(JsonParser jp, Boolean strict)
      throws UncheckedIOException, AnoaTypeException {
    try {
      jp.nextToken();
      return Boolean.TRUE.equals(strict) ? readStrict(jp) : read(jp);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  abstract protected R read(JsonParser jp) throws IOException;

  abstract protected R readStrict(JsonParser jp) throws AnoaTypeException, IOException;

  static protected interface ValueConsumer<E extends Exception> {
    public void accept(JsonParser jp) throws IOException, E;
  }

  static protected interface FieldValueConsumer<E extends Exception> {
    public void accept(String fieldName, JsonParser jp) throws IOException, E;
  }

  static protected <E extends Exception> void doArray(JsonParser jp,
                                                      ValueConsumer<E> valueConsumer)
      throws IOException, E {
    while (jp.nextToken() != JsonToken.END_ARRAY) {
      switch (jp.getCurrentToken()) {
        case END_OBJECT:
        case FIELD_NAME:
        case NOT_AVAILABLE:
          throw new IOException("Expected JSON object value, not " + jp.getCurrentToken());
      }
      valueConsumer.accept(jp);
    }
  }

  static protected <E extends Exception> void doMap(JsonParser jp,
                                                    FieldValueConsumer<E> fieldValueConsumer)
      throws IOException, E {
    while (jp.nextToken() != JsonToken.END_OBJECT) {
      if (jp.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw new IOException("Expected JSON object field name, not " + jp.getCurrentToken());
      }
      final String key = jp.getCurrentName();
      switch (jp.nextToken()) {
        case END_ARRAY:
        case END_OBJECT:
        case FIELD_NAME:
        case NOT_AVAILABLE:
          throw new IOException("Expected JSON object value, not " + jp.getCurrentToken());
      }
      fieldValueConsumer.accept(key, jp);
    }
  }

  static protected void gobbleValue(JsonParser jp) throws IOException {
    switch (jp.getCurrentToken()) {
      case START_ARRAY:
        doArray(jp, JacksonReader::gobbleValue);
        break;
      case START_OBJECT:
        doMap(jp, (k, p) -> gobbleValue(p));
        break;
      case END_ARRAY:
      case END_OBJECT:
      case NOT_AVAILABLE:
      case FIELD_NAME:
        throw new IOException("Expected '[', '{', or JSON value, not " + jp.getCurrentToken());
    }
  }

}
