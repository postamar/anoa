package com.adgear.anoa.parser;

import com.adgear.anoa.compiler.CompilationUnit;
import com.adgear.anoa.compiler.ParseException;

import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;


public class ParserTest {

  CompilationUnit parse(String namespace) throws ParseException {
    try {
      return new CompilationUnit(namespace, getClass().getClassLoader()).parse(System.err::println);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testEnums() throws ParseException {
    parse("com.adgear.anoa.test.enums");
  }

  @Test
  public void testPrimitive() throws ParseException {
    parse("com.adgear.anoa.test.primitive");
  }

  @Test
  public void testContainer() throws ParseException {
    parse("com.adgear.anoa.test.container");
  }

  @Test
  public void testNested() throws ParseException {
    parse("com.adgear.anoa.test.nested");
  }

  @Test
  public void testAll() throws ParseException {
    parse("com.adgear.anoa.test.all");
  }

}
