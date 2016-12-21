package com.adgear.anoa.compiler.javagen;

import com.adgear.anoa.compiler.CompilationUnit;

import org.apache.avro.Schema;

class AvroImplJavaGenerator extends AbstractImplJavaGenerator {

  AvroImplJavaGenerator(InterfaceJavaGenerator ijg) {
    super(ijg);
  }

  @Override
  String wrapperName() {
    return "Avro";
  }

  @Override
  String className() {
    return ijg.anoaInterfaceFullName() + "Avro";
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
      default:
        return false;
    }
  }

  @Override
  String exportValue(Schema.Field field) {
    String value = "get()." + name(field);
    final String unmod = "java.util.Collections.unmodifiable";
    switch (field.schema().getType()) {
      case BYTES:
        return "java.util.Optional.of(" + value + ").map(bb -> {" + BYTES_SUPPLIER + "}).get()";
      case STRING:
        return "java.util.Optional.of(" + value + ").map(java.lang.Object::toString).orElse(\"\")";
      case ENUM:
      case RECORD:
        return name(field.schema()) + ".avro(" + value + ")";
      case ARRAY:
        if (CompilationUnit.isSet(field.schema())) {
          return unmod + "SortedSet(" + value + ".stream()"
                 + exportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection("
                 + "() -> new java.util.TreeSet<" + anoaValueType(field.schema().getElementType())
                 + ">())))";
        } else {
          switch (field.schema().getElementType().getType()) {
            case STRING:
            case BYTES:
            case ENUM:
            case RECORD:
              return unmod + "List(" + value + ".stream()"
                     + exportArrayMapper(field.schema().getElementType())
                     + ".collect(java.util.stream.Collectors.toCollection("
                     + "() -> new java.util.ArrayList<"
                     + anoaValueType(field.schema().getElementType())
                     + ">(" + value + ".size()))))";
            default:
              return unmod + "List(" + value + ")";
          }
        }
      case MAP:
        return "java.util.Collections.unmodifiableSortedMap(" + value + ".entrySet().stream()"
               + ".collect(java.util.stream.Collectors.toMap("
               + "e -> e.getKey().toString(), "
               + exportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.TreeMap<java.lang.String,"
               + anoaValueType(field.schema().getValueType()) + ">())))";
      default:
        return value;
    }  }

  @Override
  String importValue(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
        return "java.nio.ByteBuffer.wrap(" +
            "java.util.Arrays.copyOf(" + VALUE + ".get(), " + VALUE + ".get().length))";
      case STRING:
        return "new org.apache.avro.util.Utf8(" + VALUE + ")";
      case ENUM:
      case RECORD:
        return InterfaceJavaGenerator.anoaInterfaceFullName(field.schema())
               + ".avro(" + VALUE + ").get()";
      case ARRAY:
        return VALUE + ".stream()" + importArrayMapper(field.schema().getElementType())
               + ".collect(java.util.stream.Collectors.toCollection(() -> "
               + "new org.apache.avro.generic.GenericData.Array<>(" + VALUE + ".size(), "
               + InterfaceJavaGenerator.anoaInterfaceFullName(schema) + "Avro.SCHEMA$"
               + ".getFields().get(" + field.pos() + ").schema())))";
      case MAP:
        return VALUE + ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
               + "e -> new org.apache.avro.util.Utf8(e.getKey()), "
               + importMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.HashMap<>(" + VALUE + ".size())))";
      default:
        return VALUE;
    }
  }

  private String exportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(bb -> { " + BYTES_SUPPLIER + " })";
      case STRING:
        return ".map(java.lang.Object::toString)";
      case ENUM:
      case RECORD:
        return ".map(" + name(s) + "::avro)";
      default:
        return "";
    }
  }

  private String exportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> { java.nio.ByteBuffer bb = e.getValue(); " + BYTES_SUPPLIER + " }";
      case STRING:
        return "e -> e.getValue().toString()";
      case ENUM:
      case RECORD:
        return "e -> " + name(s) + ".avro(e.getValue())";
      default:
        return "e -> e.getValue()";
    }
  }

  private String importArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(java.util.function.Supplier::get)" +
            ".map(a -> java.util.Arrays.copyOf(a, a.length))" +
            ".map(java.nio.ByteBuffer::wrap)";
      case STRING:
        return ".map(org.apache.avro.util.Utf8::new)";
      case ENUM:
      case RECORD:
        return ".map(" + InterfaceJavaGenerator.anoaInterfaceFullName(s) + "::avro)"
               + ".map(java.util.function.Supplier::get)";
    }
    return "";
  }

  private String importMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> { byte[] a = e.getValue().get(); " +
            "return java.nio.ByteBuffer.wrap(java.util.Arrays.copyOf(a, a.length)); }";
      case STRING:
        return "e -> new org.apache.avro.util.Utf8(e.getValue())";
      case ENUM:
      case RECORD:
        return "e -> " + InterfaceJavaGenerator.anoaInterfaceFullName(s) + ".avro(e.getValue()).get()";
    }
    return "e -> e.getValue()";
  }

}
