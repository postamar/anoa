package com.adgear.anoa.read;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for streaming Jackson records from SMILE serializations.
 */
public class SmileStreams extends JacksonStreamsBase<
    ObjectMapper,
    SmileFactory,
    FormatSchema,
    SmileParser> {

  public SmileStreams() {
    this(new HashMap<>());
  }

  public SmileStreams(Map<SmileParser.Feature, Boolean> smileFeatures) {
    super(new ObjectMapper(streamFactory(smileFeatures)));
  }

  static private SmileFactory streamFactory(Map<SmileParser.Feature, Boolean> smileFeatures) {
    SmileFactory factory = new SmileFactory();
    smileFeatures.forEach(factory::configure);
    return factory;
  }

}
