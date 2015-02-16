package com.adgear.anoa.io.read.json;

import com.adgear.anoa.AnoaTypeException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

class ByteArrayReader extends JsonReader<byte[]> {

  @Override
  public byte[] read(JsonParser jp) throws IOException {
    if (jp.getCurrentToken() == JsonToken.VALUE_STRING) {
      try {
        return jp.getBinaryValue();
      } catch (IOException e) {
        return null;
      }
    } else {
      gobbleValue(jp);
      return null;
    }
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
      case VALUE_NULL:
        return null;
      default:
        throw new AnoaTypeException("Token is not string: " + jp.getCurrentToken());
    }  }
}
