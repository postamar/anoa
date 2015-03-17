package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByteArrayReader extends AbstractReader<byte[]> {

  @Override
  protected byte[] read(JsonParser jacksonParser) throws IOException {
    if (jacksonParser.getCurrentToken() == JsonToken.VALUE_STRING) {
      try {
        return jacksonParser.getBinaryValue();
      } catch (IOException e) {
        return null;
      }
    } else if (jacksonParser.getCurrentToken() == JsonToken.VALUE_EMBEDDED_OBJECT) {
      final Object object;
      try {
        object = jacksonParser.getEmbeddedObject();
      } catch (IOException e) {
        return null;
      }
      if (object instanceof byte[]) {
        return (byte[]) object;
      } else if (object instanceof ByteBuffer) {
        return ((ByteBuffer) object).array();
      } else {
        return null;
      }
    }
    gobbleValue(jacksonParser);
    return null;
  }

  @Override
  protected byte[] readStrict(JsonParser jacksonParser) throws AnoaTypeException, IOException {
    switch (jacksonParser.getCurrentToken()) {
      case VALUE_STRING:
        try {
          return jacksonParser.getBinaryValue();
        } catch (IOException e) {
          throw new AnoaTypeException("String is not base64 encoded bytes: " + jacksonParser.getText());
        }
      case VALUE_EMBEDDED_OBJECT:
        final Object object = jacksonParser.getEmbeddedObject();
        if (object instanceof byte[]) {
          return (byte[]) object;
        } else if (object instanceof ByteBuffer) {
          return ((ByteBuffer) object).array();
        } else {
          throw new AnoaTypeException("Token is not byte[]: " + jacksonParser.getCurrentToken());
        }
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not string or byte[]: " + jacksonParser.getCurrentToken());
    }
  }
}
