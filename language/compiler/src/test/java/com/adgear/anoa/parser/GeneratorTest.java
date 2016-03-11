package com.adgear.anoa.parser;

import com.adgear.anoa.compiler.CompilationUnit;

import org.junit.Assert;
import org.junit.Test;

public class GeneratorTest {

  CompilationUnit f;

  public GeneratorTest() {
    try {
      f = new CompilationUnit("com.adgear.anoa.test.all", getClass().getClassLoader())
          .parse(System.err::println);
    } catch (Exception e) {
      f = null;
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAvro() {
    Assert.assertNotNull(f);
    f.avroGenerator().generateSchema();
  }

  @Test
  public void testProtobuf() {
    Assert.assertNotNull(f);
    f.protobufGenerator().generateSchema();
  }

  @Test
  public void testThrift() {
    Assert.assertNotNull(f);
    f.thriftGenerator().generateSchema();
  }

}
