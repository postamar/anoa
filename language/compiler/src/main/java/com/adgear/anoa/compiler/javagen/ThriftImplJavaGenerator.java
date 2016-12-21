package com.adgear.anoa.compiler.javagen;

import com.adgear.anoa.compiler.CompilationUnit;

import org.apache.avro.Schema;

class ThriftImplJavaGenerator extends AbstractImplJavaGenerator {

  ThriftImplJavaGenerator(InterfaceJavaGenerator ijg) {
    super(ijg);
  }

  @Override
  String wrapperName() {
    return "Thrift";
  }

  @Override
  String className() {
    return ijg.anoaInterfaceFullName() + "Thrift";
  }

  @Override
  boolean hasExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case STRING:
      case ENUM:
      case RECORD:
      case ARRAY:
      case MAP:
        return true;
    }
    return false;
  }

  @Override
  String exportValue(Schema.Field field) {
    String value = "wrapped." + (field.schema().getType() == Schema.Type.BOOLEAN ? "is" : "get")
                   + Character.toUpperCase(field.name().charAt(0))
                   + field.name().substring(1) + "()";
    if (!hasExportField(field)) {
      return ((field.schema().getType() == Schema.Type.FLOAT) ? "(float) " : "") + value;
    }
    value = "java.util.Optional.ofNullable(" + value + ")";
    switch (field.schema().getType()) {
      case BYTES:
        return "java.util.Optional.of(" + value + ".orElse(new byte[0]))"
            + ".map(b -> new java.util.function.Supplier<byte[]>() { "
            + "public byte[] get() { return java.util.Arrays.copyOf(b, b.length); } }).get()";
      case STRING:
        return value + ".orElse(\"\")";
      case ENUM:
        return name(field.schema()) + ".thrift(" + value
               + ".map(org.apache.thrift.TEnum::getValue).orElse(0))";
      case RECORD:
        return name(field.schema()) + ".thrift(" + value + ".orElseGet("
               + InterfaceJavaGenerator.anoaInterfaceFullName(field.schema()) + "Thrift::new))";
      case ARRAY:
        return value + ".orElseGet(java.util.Collections::empty"
               + (CompilationUnit.isSet(field.schema()) ? "Set" : "List") + ").stream()"
               + exportArrayMapper(field.schema().getElementType()) + ".collect("
               + "java.util.stream.Collectors.toCollection(() -> new java.util."
               + (CompilationUnit.isSet(field.schema()) ? "TreeSet" : "ArrayList")
               + "<" + anoaValueType(field.schema().getElementType()) + ">()))";
      case MAP:
        return value + ".orElseGet(java.util.Collections::emptyMap).entrySet().stream().collect("
               + "java.util.stream.Collectors.toMap("
               + "e -> e.getKey(), "
               + exportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.TreeMap<java.lang.String,"
               + anoaValueType(field.schema().getValueType()) + ">()))";
      default:
        return value;
    }
  }

  @Override
  public String importValue(Schema.Field field) {
    String setter = "set" + Character.toUpperCase(field.name().charAt(0))
                    + field.name().substring(1);
    String value = VALUE;
    switch (field.schema().getType()) {
      case FLOAT:
        value = "(double) " + value;
        break;
      case INT:
        switch (CompilationUnit.getThriftPrecision(field.schema())) {
          case 8:  value = "(byte) "  + value; break;
          case 16: value = "(short) " + value; break;
          default: value = "(int) "   + value;
        }
        break;
      case LONG:
        value = "(long) " + value;
        break;
      case BYTES:
        value = "java.nio.ByteBuffer.wrap(" +
            "java.util.Arrays.copyOf(" + VALUE + ".get(), " + VALUE + ".get().length))";
        break;
      case ENUM:
      case RECORD:
        value = name(field.schema()) + ".thrift(" + value + ").get()";
        break;
      case ARRAY:
        value += ".stream()" + importArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection(() -> new java.util."
                 + (CompilationUnit.isSet(field.schema()) ? "HashSet" : "ArrayList")
                 + "<>(" + value + ".size())))";
        break;
      case MAP:
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey(), "
                 + importMapValueFunction(field.schema().getValueType()) + ", (u,v) -> {"
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap<>(" + value + ".size())))";
    }
    return setter + "(" + value + ")";
  }

  private String exportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(bb -> {" + BYTES_SUPPLIER + "})";
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return ".map(java.lang.Number::" + s.getType().toString().toLowerCase() + "Value)";
      case ENUM:
      case RECORD:
        return  ".map(" + name(s) + "::thrift)";
    }
    return "";
  }

  private String exportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> { java.nio.ByteBuffer bb = e.getValue(); " + BYTES_SUPPLIER + " }";
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return "e -> e.getValue()." + s.getType().toString().toLowerCase() + "Value()";
      case ENUM:
      case RECORD:
        return "e -> " + name(s) + ".thrift(e.getValue())";
    }
    return "e -> e.getValue()";
  }

  private String importArrayMapper(Schema s) {
    switch (s.getType()) {
      case FLOAT:
        return ".map(java.lang.Float::doubleValue)";
      case LONG:
      case INT:
        switch (CompilationUnit.getThriftPrecision(s)) {
          case 64:
            return ".map(java.lang.Number::longValue)";
          case 32:
            return ".map(java.lang.Number::intValue)";
          case 16:
            return ".map(java.lang.Number::shortValue)";
          case 8:
            return ".map(java.lang.Number::byteValue)";
        }
        break;
      case BYTES:
        return ".map(java.util.function.Supplier::get)"
            + ".map(a -> java.util.Arrays.copyOf(a, a.length))"
            + ".map(java.nio.ByteBuffer::wrap)";
      case ENUM:
      case RECORD:
        return  ".map(" + name(s) + "::thrift).map(java.util.function.Supplier::get)";
    }
    return "";
  }

  private String importMapValueFunction(Schema s) {
    switch (s.getType()) {
      case FLOAT:
        return "e -> e.getValue().doubleValue()";
      case LONG:
      case INT:
        switch (CompilationUnit.getThriftPrecision(s)) {
          case 64:
            return "e -> e.getValue().longValue()";
          case 32:
            return "e -> e.getValue().intValue()";
          case 16:
            return "e -> e.getValue().shortValue()";
          case 8:
            return "e -> e.getValue().byteValue()";
        }
        break;
      case BYTES:
        return "e -> {" +
            "byte[] a = e.getValue().get(); " +
            "return java.nio.ByteBuffer.wrap(java.util.Arrays.copyOf(a, a.length)); }";
      case ENUM:
      case RECORD:
        return "e -> " + name(s) + ".thrift(e.getValue()).get()";
    }
    return "e -> e.getValue()";
  }

  @Override
  String builderClearMethod(Schema.Field field) {
    return "unset"
           + Character.toUpperCase(field.name().charAt(0)) + field.name().substring(1) + "()";
  }
}
