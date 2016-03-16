package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;


abstract class AbstractRecordWriter<R> extends AbstractWriter<R> {

  final <G extends JsonGenerator> Function<R, G> encoder(Supplier<G> supplier, boolean strict) {
    if (strict) {
      return (R record) -> {
        G jacksonGenerator = supplier.get();
        try {
          writeStrict(record, jacksonGenerator);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return jacksonGenerator;
      };
    } else {
      return (R record) -> {
        G jacksonGenerator = supplier.get();
        try {
          write(record, jacksonGenerator);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return jacksonGenerator;
      };
    }
  }

  final <G extends JsonGenerator, M> Function<Anoa<R, M>, Anoa<G, M>> encoder(
      AnoaHandler<M> anoaHandler,
      Supplier<G> supplier,
      boolean strict) {
    if (strict) {
      return anoaHandler.functionChecked((R record) -> {
        G jacksonGenerator = supplier.get();
        writeStrict(record, jacksonGenerator);
        return jacksonGenerator;
      });
    } else {
      return anoaHandler.functionChecked((R record) -> {
        G jacksonGenerator = supplier.get();
        write(record, jacksonGenerator);
        return jacksonGenerator;
      });
    }
  }

  final WriteConsumer<R> writeConsumer(JsonGenerator jacksonGenerator, boolean strict) {
    if (strict) {
      return new WriteConsumer<R>() {
        @Override
        public void acceptChecked(R record) throws IOException {
          writeStrict(record, jacksonGenerator);
        }

        @Override
        public void flush() throws IOException {
          jacksonGenerator.flush();
        }
      };
    } else {
      return new WriteConsumer<R>() {
        @Override
        public void acceptChecked(R record) throws IOException {
          write(record, jacksonGenerator);
        }

        @Override
        public void flush() throws IOException {
          jacksonGenerator.flush();
        }
      };
    }
  }
}
