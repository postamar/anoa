package com.adgear.anoa.write;


import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonGenerator;

import java.util.function.Function;
import java.util.function.Supplier;

class JacksonUtils {

  static <R, G extends JsonGenerator> Function<R, G> encoder(
      AbstractWriter<R> writer,
      Supplier<G> supplier) {
    return (R record) -> {
      G jg = supplier.get();
      writer.write(record, jg);
      return jg;
    };
  }

  static <R, G extends JsonGenerator, M> Function<Anoa<R, M>, Anoa<G, M>> encoder(
      AnoaHandler<M> anoaHandler,
      AbstractWriter<R> writer,
      Supplier<G> supplier) {
    return anoaHandler.functionChecked((R record) -> {
      G jg = supplier.get();
      writer.writeChecked(record, jg);
      return jg;
    });
  }
}
