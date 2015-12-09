package com.adgear.anoa.write;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

class JacksonConsumersBase<
    M extends ObjectMapper,
    F extends JsonFactory,
    S extends FormatSchema,
    G extends JsonGenerator>
  extends JacksonConsumers<ObjectNode, M, F, S, G> {

  JacksonConsumersBase(M objectMapper) {
    this(objectMapper, Optional.<S>empty());
  }

  JacksonConsumersBase(M objectMapper, Optional<S> schema) {
    super(objectMapper, schema);
    objectMapper.findAndRegisterModules();
  }
}
