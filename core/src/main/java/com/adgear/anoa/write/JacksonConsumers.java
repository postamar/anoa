package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Optional;

public class JacksonConsumers<
    N extends TreeNode,
    C extends ObjectCodec,
    F extends JsonFactory,
    S extends FormatSchema,
    G extends JsonGenerator> {

  final public C objectCodec;
  final public F factory;
  final public Optional<S> schema;

  @SuppressWarnings("unchecked")
  public JacksonConsumers(@NonNull C objectCodec, @NonNull Optional<S> schema) {
    this.objectCodec = objectCodec;
    this.factory = (F) objectCodec.getFactory();
    this.factory.setCodec(objectCodec);
    this.schema = schema;
  }

  @SuppressWarnings("unchecked")
  public @NonNull G generator(@NonNull Writer writer) {
    try {
      return with((G) factory.createGenerator(new BufferedWriter(writer)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull G generator(@NonNull OutputStream outputStream) {
    try {
      return with((G) factory.createGenerator(new BufferedOutputStream(outputStream), JsonEncoding.UTF8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull G generator(@NonNull File file) {
    try {
      return with((G) factory.createGenerator(file, JsonEncoding.UTF8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public @NonNull G with(@NonNull G generator) {
    schema.ifPresent(generator::setSchema);
    return generator;
  }

  public @NonNull WriteConsumer<N, IOException> to(@NonNull G generator) {
    return new TreeNodeWriteConsumer<>(generator);
  }

  public @NonNull WriteConsumer<N, IOException> to(@NonNull Writer writer) {
    return to(generator(writer));
  }

  public @NonNull WriteConsumer<N, IOException> to(@NonNull OutputStream outputStream) {
    return to(generator(outputStream));
  }

  public @NonNull WriteConsumer<N, IOException> to(@NonNull File file) {
    return to(generator(file));
  }


}
