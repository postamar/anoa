package com.adgear.anoa.write;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;

/**
 * Utility class for writing Jackson records in the SMILE format.
 */
public class SmileConsumers extends JacksonConsumersBase<
    ObjectMapper,
    SmileFactory,
    FormatSchema,
    SmileGenerator> {

  public SmileConsumers() {
    super(new ObjectMapper(new SmileFactory()));
  }
}