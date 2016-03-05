package com.adgear.anoa.parser;

import org.apache.avro.Protocol;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;


public class ParserTest {


  protected Protocol parse(String namespace) throws ParseException {
    try {
      return new AnoaParser(namespace, getClass().getClassLoader()).CompilationUnit();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testParserEnums() throws ParseException {
    parse("com.adgear.anoa.test.enums");
  }


  @Test
  public void testParserStructs() throws ParseException {
    parse("com.adgear.anoa.test.structs");
  }

  protected ProtocolFactory factory() {
    try {
      return new ProtocolFactory("com.adgear.anoa.test.structs", getClass().getClassLoader())
          .parse();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testProtocolFactory() {
    factory();
  }

  @Test
  public void testAvro() {
    SchemaGenerator gen = new AvroGenerator(factory());
    System.err.println(gen.getSchema());
  }


  @Test
  public void testProtobuf() {
    SchemaGenerator gen = new ProtobufGenerator(factory());
    System.err.println(gen.getSchema());
  }


  @Test
  public void testThrift() {
    SchemaGenerator gen = new ThriftGenerator(factory());
    System.err.println(gen.getSchema());
  }
}
