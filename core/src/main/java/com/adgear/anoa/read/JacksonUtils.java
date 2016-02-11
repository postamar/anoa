package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import java.util.function.Function;
import java.util.stream.Stream;

class JacksonUtils {

  static <P extends JsonParser, R> Function<P, R> decoder(
      AbstractReader<R> reader,
      boolean strict) {
    return (P jp) -> reader.read(jp, strict);
  }

  static <P extends JsonParser, R, M> Function<Anoa<P, M>, Anoa<R, M>> decoder(
      AnoaHandler<M> anoaHandler,
      AbstractReader<R> reader,
      boolean strict) {
    return anoaHandler.functionChecked((P jp) -> reader.readChecked(jp, strict));
  }

  static <R> Stream<R> stream(
      AbstractReader<R> reader,
      boolean strict,
      JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(jacksonParser).asStream()
        .map(TreeNode::traverse)
        .map(decoder(reader, strict));
  }

  static <R, M> Stream<Anoa<R, M>> stream(
      AnoaHandler<M> anoaHandler,
      AbstractReader<R> reader,
      boolean strict,
      JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(anoaHandler, jacksonParser).asStream()
        .map(anoaHandler.function(TreeNode::traverse))
        .map(decoder(anoaHandler, reader, strict));
  }
}
