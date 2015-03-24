package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

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

  JacksonConsumersBase(@NonNull M objectMapper) {
    this(objectMapper, Optional.<S>empty());
  }

  JacksonConsumersBase(@NonNull M objectMapper, @NonNull Optional<S> schema) {
    super(objectMapper, schema);
    objectMapper.findAndRegisterModules();
  }
}
