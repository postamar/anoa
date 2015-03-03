package com.adgear.anoa.factory;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonObjects extends JacksonObjects<
    ObjectMapper,
    JsonFactory,
    FormatSchema,
    JsonParser,
    JsonGenerator> {

  public JsonObjects() {
    super(new ObjectMapper());
  }
}
