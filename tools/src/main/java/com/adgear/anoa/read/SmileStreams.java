package com.adgear.anoa.read;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

public class SmileStreams extends JacksonStreamsBase<
    ObjectMapper,
    SmileFactory,
    FormatSchema,
    SmileParser> {

  public SmileStreams() {
    super(new ObjectMapper(new SmileFactory()));
  }
}
