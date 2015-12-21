package com.adgear.anoa.write;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

/**
 * Utility class for writing Jackson records in the JSON format.
 */
public class JsonConsumers extends JacksonConsumersBase<
    ObjectMapper,
    JsonFactory,
    FormatSchema,
    JsonGenerator> {

  static final private PrettyPrinter PRETTY_PRINTER = new MinimalPrettyPrinter("\n");

  public JsonConsumers() {
    super(new ObjectMapper());
  }

  @Override
  public JsonGenerator with(JsonGenerator generator) {
    generator = super.with(generator);
    generator.setPrettyPrinter(PRETTY_PRINTER);
    return generator;
  }

  public TokenBuffer generator() {
    return (TokenBuffer) with(new TokenBuffer(objectCodec, false));
  }

  public TokenBufferWriteConsumer to() {
    return new TokenBufferWriteConsumer(generator());
  }

}
