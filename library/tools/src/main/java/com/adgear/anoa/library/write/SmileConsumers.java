package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for writing Jackson records in the SMILE format.
 */
public class SmileConsumers extends JacksonConsumersBase<
    ObjectMapper,
    SmileFactory,
    FormatSchema,
    SmileGenerator> {

  public SmileConsumers() {
    this(new HashMap<>());
  }

  public SmileConsumers(Map<SmileGenerator.Feature, Boolean> smileFeatures) {
    super(new ObjectMapper(consumerFactory(smileFeatures)));
  }

  static private SmileFactory consumerFactory(Map<SmileGenerator.Feature, Boolean> smileFeatures) {
    SmileFactory factory = new SmileFactory();
    smileFeatures.forEach(factory::configure);
    return factory;
  }
}
