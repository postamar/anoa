package com.adgear.anoa.factory;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

public class SmileObjects extends JacksonObjects<
    ObjectMapper,
    SmileFactory,
    FormatSchema,
    SmileParser,
    SmileGenerator> {

  public SmileObjects() {
    super(new ObjectMapper(new SmileFactory()));
  }
}
