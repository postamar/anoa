package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.URL;

public class AnoaJackson<
    M extends ObjectMapper,
    F extends JsonFactory,
    P extends JsonParser,
    G extends JsonGenerator> {

  static final public AnoaJackson<ObjectMapper, CBORFactory, CBORParser, CBORGenerator> CBOR;
  static final public AnoaJackson<CsvMapper, CsvFactory, CsvParser, CsvGenerator> CSV;
  static final public AnoaJackson<ObjectMapper, JsonFactory, JsonParser, JsonGenerator> JSON;
  static final public AnoaJackson<ObjectMapper, SmileFactory, SmileParser, SmileGenerator> SMILE;
  static final public AnoaJackson<XmlMapper, XmlFactory, FromXmlParser, ToXmlGenerator> XML;
  static final public AnoaJackson<YAMLMapper, YAMLFactory, YAMLParser, YAMLGenerator> YAML;

  static {
    CBOR = new AnoaJackson<>(new ObjectMapper(new CBORFactory()));
    CSV = new AnoaJackson<>(new CsvMapper());
    JSON = new AnoaJackson<>(new ObjectMapper());
    SMILE = new AnoaJackson<>(new ObjectMapper(new SmileFactory()));
    XML = new AnoaJackson<>(new XmlMapper(new XmlFactory()));
    YAML = new AnoaJackson<>(new YAMLMapper());
  }

  @SuppressWarnings("unchecked")
  protected AnoaJackson(M mapper) {
    this.mapper = mapper;
    this.mapper.findAndRegisterModules();
    this.factory = (F) this.mapper.getFactory();
  }

  final protected M mapper;
  final protected F factory;

  @SuppressWarnings("unchecked")
  public @NonNull P from(@NonNull InputStream inputStream) {
    try {
      return (P) factory.createParser(inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P from(@NonNull Reader reader) {
    try {
      return (P) factory.createParser(reader);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P from(@NonNull byte[] bytes) {
    try {
      return (P) factory.createParser(bytes);

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P from(@NonNull String string) {
    try {
      return (P) factory.createParser(string);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P from(@NonNull File file) {
    try {
      return (P) factory.createParser(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull P from(@NonNull URL url) {
    try {
      return (P) factory.createParser(url);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public @NonNull JsonParser from(@NonNull TokenBuffer tokenBuffer) {
    return tokenBuffer.asParser(mapper);
  }

  @SuppressWarnings("unchecked")
  public @NonNull G to(@NonNull Writer writer) {
    try {
      return (G) factory.createGenerator(writer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull G to(@NonNull OutputStream outputStream) {
    try {
      return (G) factory.createGenerator(outputStream, JsonEncoding.UTF8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public @NonNull G to(@NonNull File file) {
    try {
      return (G) factory.createGenerator(file, JsonEncoding.UTF8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public @NonNull TokenBuffer to() {
    return new TokenBuffer(mapper, false);
  }
}
