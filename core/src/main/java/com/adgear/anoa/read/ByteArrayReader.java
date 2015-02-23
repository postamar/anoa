package com.adgear.anoa.read;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.nio.ByteBuffer;

class ByteArrayReader extends JacksonReader<byte[]> {

  @Override
  public byte[] read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.VALUE_STRING) {
      try {
        return jp.getBinaryValue();
      } catch (IOException e) {
        return null;
      }
    } else if (jp.getCurrentToken() == JsonToken.VALUE_EMBEDDED_OBJECT) {
      final Object object;
      try {
        object = jp.getEmbeddedObject();
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
    gobbleValue(jp);
    return null;
  }

  @Override
  public byte[] readStrict(JsonParser jp) throws AnoaTypeException, IOException {
    switch (jp.getCurrentToken()) {
      case VALUE_STRING:
        try {
          return jp.getBinaryValue();
        } catch (IOException e) {
          throw new AnoaTypeException("String is not base64 encoded bytes: " + jp.getText());
        }
      case VALUE_EMBEDDED_OBJECT:
        final Object object = jp.getEmbeddedObject();
        if (object instanceof byte[]) {
          return (byte[]) object;
        } else if (object instanceof ByteBuffer) {
          return ((ByteBuffer) object).array();
        } else {
          throw new AnoaTypeException("Token is not byte[]: " + jp.getCurrentToken());
        }
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not string or byte[]: " + jp.getCurrentToken());
    }
  }
}
