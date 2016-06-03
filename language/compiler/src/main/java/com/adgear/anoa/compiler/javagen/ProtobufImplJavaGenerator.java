package com.adgear.anoa.compiler.javagen;

import com.adgear.anoa.compiler.AnoaParserBase;
import com.adgear.anoa.compiler.CompilationUnit;

import org.apache.avro.Schema;

class ProtobufImplJavaGenerator extends AbstractImplJavaGenerator {

  ProtobufImplJavaGenerator(InterfaceJavaGenerator ijg) {
    super(ijg);
  }

  @Override
  String wrapperName() {
    return "Protobuf";
  }

  @Override
  String className() {
    String ns = schema.getNamespace();
    ns += "." + ns.substring(ns.lastIndexOf('.') + 1) + "_protobuf";
    String protobufClassName = AnoaParserBase.capitalizeQualified(ns);
    return mangle(AnoaParserBase.capitalizeQualified(protobufClassName + "." + schema.getName()));
  }

  @Override
  boolean hasExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case LONG:
        return (CompilationUnit.getPrecision(field.schema()) == 32);
      case BYTES:
      case ENUM:
      case RECORD:
        return true;
      case ARRAY:
        if (CompilationUnit.isSet(field.schema())) {
          return true;
        }
        switch (field.schema().getElementType().getType()) {
          case LONG:
            return (CompilationUnit.getPrecision(field.schema().getElementType()) == 32);
          case BYTES:
          case ENUM:
          case RECORD:
            return true;
        }
        return false;
      case MAP:
        return true;
    }
    return false;
  }

  @Override
  String exportValue(Schema.Field field) {
    String base = "get()." + ijg.getMethod(field).replace("$", "");
    String value = base + ((field.schema().getType() == Schema.Type.ARRAY) ? "List()" : "()");
    if (!hasExportField(field)) {
      if (field.schema().getType() == Schema.Type.ARRAY
          && field.schema().getElementType().getType() == Schema.Type.STRING) {
        value = "(java.util.List<java.lang.String>)(java.util.List<?>) " + value;
      }
      return value;
    }
    switch (field.schema().getType()) {
      case ARRAY:
        if (CompilationUnit.isSet(field.schema())) {
          return "java.util.Collections.unmodifiableSortedSet(" + value + ".stream()"
                 + exportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection("
                 + "() -> new java.util.TreeSet<"
                 + anoaValueType(field.schema().getElementType()) + ">())))";
        } else {
          return "java.util.Collections.unmodifiableList(" + value + ".stream()"
                 + exportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection("
                 + "() -> new java.util.ArrayList<"
                 + anoaValueType(field.schema().getElementType())
                 + ">(" + base + "Count()))))";
        }
      case MAP:
        return "java.util.Collections.unmodifiableSortedMap(" + value + ".entrySet().stream()"
               + ".collect(java.util.stream.Collectors.toMap("
               + "e -> e.getKey(), "
               + exportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.TreeMap<java.lang.String,"
               + anoaValueType(field.schema().getValueType()) + ">())))";
      case LONG:
        return "((long) " + value + ") & 0xFFFFFFFFL";
      case BYTES:
        return "() -> " + value + ".toByteArray()";
      default:
        return name(field.schema()) + ".protobuf(" + value + ")";
    }
  }

  @Override
  String importValue(Schema.Field field) {
    String setter = ijg.setMethod(field).replace("$", "");
    String value = VALUE;
    switch (field.schema().getType()) {
      case LONG:
        if (CompilationUnit.getPrecision(field.schema()) == 32) {
          value = "(int) " + value;
        }
        break;
      case BYTES:
        value = "com.google.protobuf.ByteString.copyFrom(" + value + ".get())";
        break;
      case ENUM:
      case RECORD:
        value = name(field.schema()) + ".protobuf(" + value + ").get()";
        break;
      case ARRAY:
        setter = "addAll" + setter.substring(3);
        String map = importArrayMapper(field.schema().getElementType());
        if (!map.isEmpty()) {
          value += ".stream()" + map + ".collect(java.util.stream.Collectors.toCollection("
                   + "() -> new java.util.ArrayList(" + value + ".size())))";
        }
        break;
      case MAP:
        setter = "putAll" + setter.substring(3);
        String fn = importMapValueFunction(field.schema().getValueType());
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey(), " + fn + ", (u,v) -> { "
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap(" + value + ".size())))";
        break;
    }
    return setter + "(" + value + ")";
  }

  @Override
  String builderClearMethod(Schema.Field field) {
    if (field.schema().getType() == Schema.Type.MAP) {
      return "putAll" + ijg.setMethod(field).substring(3) + "(java.util.Collections.emptyMap())";
    } else {
      return ijg.clearMethod(field).replace("$", "") + "()";
    }
  }

  private String exportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(bs -> new java.util.function.Supplier<byte[]>() { "
               + "public byte[] get() { return bs.toByteArray();} })";
      case ENUM:
      case RECORD:
        return ".map(" + name(s) + "::protobuf)";
      case LONG:
        if (CompilationUnit.getPrecision(s) == 32) {
          return ".map(i -> i.longValue() & 0xFFFFFFFFL)";
        }
      default:
        return "";
    }
  }

  private String exportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> { com.google.protobuf.ByteString bs = e.getValue(); "
               + "return new java.util.function.Supplier<byte[]>() { "
               + "public byte[] get() { return bs.toByteArray();} }; }";
      case ENUM:
      case RECORD:
        return "e -> " + name(s) + ".protobuf(e.getValue())";
      case LONG:
        if (CompilationUnit.getPrecision(s) == 32) {
          return "e -> e.getValue().longValue() & 0xFFFFFFFFL";
        }
      default:
        return "e -> e.getValue()";
    }
  }

  private String importArrayMapper(Schema s) {
    switch (s.getType()) {
      case LONG:
        if (CompilationUnit.getPrecision(s) == 32) {
          return ".map(java.lang.Number::intValue)";
        }
        break;
      case BYTES:
        return ".map(java.util.function.Supplier::get)"
               + ".map(com.google.protobuf.ByteString::copyFrom)";
      case ENUM:
      case RECORD:
        return ".map(" + name(s) + "::protobuf)"
               + ".map(java.util.function.Supplier::get)";
    }
    return "";
  }

  private String importMapValueFunction(Schema s) {
    switch (s.getType()) {
      case LONG:
        if (CompilationUnit.getPrecision(s.getValueType()) == 32) {
          return "e -> e.getValue().intValue()";
        }
        break;
      case BYTES:
        return "e -> com.google.protobuf.ByteString.copyFrom(e.getValue().get())";
      case ENUM:
      case RECORD:
        return "e -> " + name(s) + ".protobuf(e.getValue()).get()";
    }
    return "e -> e.getValue()";
  }
}
