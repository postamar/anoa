package com.adgear.anoa.compiler.javagen;

import org.apache.avro.Schema;

abstract class AbstractImplJavaGenerator {

  static final String VALUE = "value";

  static String BYTES_SUPPLIER =
      "byte[] b = new byte[bb.remaining()]; "
          + "bb.asReadOnlyBuffer().get(b); "
          + "return new java.util.function.Supplier<byte[]>() { "
          + "public byte[] get() { return java.util.Arrays.copyOf(b, b.length); } };";

  final InterfaceJavaGenerator ijg;
  final Schema schema;

  AbstractImplJavaGenerator(InterfaceJavaGenerator ijg) {
    this.ijg = ijg;
    this.schema = ijg.getSchema();
  }

  final protected String anoaValueType(Schema schema) {
    return ijg.anoaValueType(schema);
  }

  final protected String name(Schema schema) {
    return mangle(schema.getFullName());
  }

  final protected String name(Schema.Field field) {
    return mangle(field.name());
  }

  final protected String mangle(String string) {
    return InterfaceJavaGenerator.mangle(string);
  }

  abstract String wrapperName();

  abstract String className();

  abstract boolean hasExportField(Schema.Field field);

  abstract String exportValue(Schema.Field field);

  abstract String importValue(Schema.Field field);

  String builderClearMethod(Schema.Field field) {
    return InterfaceJavaGenerator.generateClearMethod(schema, field) + "()";
  }
}
