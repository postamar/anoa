package com.adgear.anoa.write;

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

/**
 * Utility class for generating {@code WriteConsumer} instances to write Jackson
 * {@link com.fasterxml.jackson.core.TreeNode} instances.
 *
 * Intended to be used as a base class, subclasses should wrap appropriate Jackson databinding types
 * together; see the anoa-tools module for examples.
 *
 * @param <N> Record type
 * @param <C> Mapper type
 * @param <F> Factory type
 * @param <S> Schema type
 * @param <G> Generator type
 *
 */
public class JacksonConsumers<
    N extends TreeNode,
    C extends ObjectCodec,
    F extends JsonFactory,
    S extends FormatSchema,
    G extends JsonGenerator> {

  /**
   * The object mapper used by this instance
   */
  final public C objectCodec;

  /**
   * The factory used by this instance
   */
  final public F factory;

  /**
   * The (optional) format schema used by this instance
   */
  final public Optional<S> schema;

  /**
   *
   * @param objectCodec Jackson object mapper instance
   * @param schema Jackson format schema (optional)
   */
  @SuppressWarnings("unchecked")
  public JacksonConsumers(
      C objectCodec,
      Optional<S> schema) {
    this.objectCodec = objectCodec;
    this.factory = (F) objectCodec.getFactory();
    this.factory.setCodec(objectCodec);
    this.schema = schema;
  }

  @SuppressWarnings("unchecked")
  protected G generatorChecked(OutputStream outputStream) throws IOException {
    return with((G) factory.createGenerator(new BufferedOutputStream(outputStream),
                                            JsonEncoding.UTF8));
  }

  public G generator(
      OutputStream outputStream) {
    try {
      return generatorChecked(outputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  protected G generatorChecked(Writer writer) throws IOException {
    return with((G) factory.createGenerator(new BufferedWriter(writer)));
  }

  public G generator(
      Writer writer) {
    try {
      return generatorChecked(writer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  protected G generatorChecked(File file) throws IOException {
    return with((G) factory.createGenerator(file, JsonEncoding.UTF8));
  }

  public G generator(
      File file) {
    try {
      return generatorChecked(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @return the generator passed as argument, after setting the current format schema
   */
  public G with(G generator) {
    schema.ifPresent(generator::setSchema);
    return generator;
  }

  public WriteConsumer<N> to(G generator) {
    return new TreeNodeWriteConsumer<>(generator);
  }

  public WriteConsumer<N> to(Writer writer) {
    return to(generator(writer));
  }

  public WriteConsumer<N> to(OutputStream outputStream) {
    return to(generator(outputStream));
  }

  public WriteConsumer<N> to(File file) {
    return to(generator(file));
  }


}
