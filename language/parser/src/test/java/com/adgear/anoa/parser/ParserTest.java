package com.adgear.anoa.parser;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.stream.Stream;


public class ParserTest {

  private Stream<String> readResource(String name) {
    InputStream inputStream = getClass().getResourceAsStream("/" + name);
    return new BufferedReader(new InputStreamReader(inputStream)).lines();
  }

  protected Stream<SchemaGenerator> parse(String... name) {
    return new Parser(ParserTest::errorConsumer, this::readResource).apply(Stream.of(name)).get();
  }

  static void errorConsumer(String error) {
    System.err.println(error);
  }

  @Test
  public void testProto() {
    parse("com.adgear.anoa.browsers.enum", "com.adgear.anoa.event.struct")
        .map(SchemaGenerator::protoSchema)
        .forEach(System.out::println);
  }

  @Test
  public void testAvro() {
    parse("com.adgear.anoa.browsers.enum", "com.adgear.anoa.event.struct")
        .map(SchemaGenerator::avroSchema)
        .forEach(System.out::println);
  }

  @Test
  public void testThrift() {
    parse("com.adgear.anoa.browsers.enum", "com.adgear.anoa.event.struct")
        .map(SchemaGenerator::thriftSchema)
        .forEach(System.out::println);
  }

  @Test
  public void testCsv() {
    parse("com.adgear.anoa.browsers.enum")
        .map(SchemaGenerator::csvSchema)
        .map(Optional::get)
        .forEach(System.out::println);
  }
}
