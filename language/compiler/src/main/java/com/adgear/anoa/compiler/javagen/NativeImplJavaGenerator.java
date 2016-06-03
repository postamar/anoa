package com.adgear.anoa.compiler.javagen;

import com.adgear.anoa.compiler.AnoaBinaryNode;
import com.adgear.anoa.compiler.CompilationUnit;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

class NativeImplJavaGenerator extends AbstractImplJavaGenerator {

  NativeImplJavaGenerator(InterfaceJavaGenerator ijg) {
    super(ijg);
  }

  @Override
  String className() {
    return "NativeImpl";
  }

  @Override
  String wrapperName() {
    return className();
  }

  @Override
  boolean hasExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case ARRAY:
      case MAP:
        return true;
    }
    return false;
  }

  String nativeFieldType(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
        return "byte[]";
      case ARRAY:
        if (field.schema().getElementType().getType() == Schema.Type.BYTES) {
          return "java.util." + (CompilationUnit.isSet(field.schema()) ? "SortedSet<" : "List<")
                 + "byte[]>";
        }
        break;
      case MAP:
        if (field.schema().getValueType().getType() == Schema.Type.BYTES) {
          return "java.util.SortedMap<java.lang.String,byte[]>";
        }
        break;
    }
    return ijg.exportType(field);
  }

  String nativeFieldName(Schema.Field field) {
    return "__" + mangle(field.name());
  }

  String nativeToString(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
        return "\"\\\"\" + java.util.Base64.getEncoder().encodeToString("
               + nativeFieldName(field) + ") + '\\\"'";
      case STRING:
        return "\"\\\"\" + " + nativeFieldName(field) + ".replace(\"\\\"\",\"\\\\\\\"\") + '\\\"'";
      case ARRAY:
        final String elementFn;
        switch (field.schema().getElementType().getType()) {
          case BYTES:
            elementFn = "v -> \"\\\"\" + java.util.Base64.getEncoder().encodeToString(v) + '\\\"'";
            break;
          case STRING:
            elementFn = "v -> \"\\\"\" + v.replace(\"\\\"\",\"\\\\\\\"\") + '\\\"'";
            break;
          default:
            elementFn = "java.lang.Object::toString";
        }
        return nativeFieldName(field) + ".stream().map(" + elementFn
               + ").collect(java.util.stream.Collectors.joining(\", \", \"[\", \"]\"))";
      case MAP:
        final String value;
        switch (field.schema().getValueType().getType()) {
          case BYTES:
            value = "'\\\"' + java.util.Base64.getEncoder().encodeToString(e.getValue()) + '\\\"'";
            break;
          case STRING:
            value = "'\\\"' + e.getValue().replace(\"\\\"\",\"\\\\\\\"\") + '\\\"'";
            break;
          default:
            value = "e.getValue()";
        }
        return nativeFieldName(field) + ".entrySet().stream()"
               + ".map(e -> \"\\\"\" + e.getKey() + \"\\\": \" + " + value + ").collect("
               + "java.util.stream.Collectors.joining(\", \", \"{\", \"}\"))";
      default:
        return nativeFieldName(field);
    }
  }


  @Override
  String exportValue(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
        return "(java.util.function.Supplier<byte[]>) " + nativeFieldName(field) + "::clone";
      case ARRAY:
        if (field.schema().getElementType().getType() == Schema.Type.BYTES) {
          return  "java.util.Collections.unmodifiable"
                  + (CompilationUnit.isSet(field.schema()) ? "SortedSet(" : "List(")
                  + nativeFieldName(field) + ".stream()"
                  + ".map(v -> (java.util.function.Supplier<byte[]>) v::clone)"
                  + ".collect(java.util.stream.Collectors.toCollection(() -> new java.util."
                  + (CompilationUnit.isSet(field.schema())
                     ? "TreeSet<>()"
                     : ("ArrayList<>(" + nativeFieldName(field) + ".size())"))
                  + ")))";
        } else {
          return  "java.util.Collections.unmodifiable"
                  + (CompilationUnit.isSet(field.schema()) ? "SortedSet(" : "List(")
                  + nativeFieldName(field) + ")";
        }
      case MAP:
        if (field.schema().getValueType().getType() == Schema.Type.BYTES) {
          return  "java.util.Collections.unmodifiableSortedMap(" + nativeFieldName(field)
                  + ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                  + "e -> e.getKey(), "
                  + "e -> (java.util.function.Supplier<byte[]>) e.getValue()::clone, "
                  + "(u,v) -> {throw new java.lang.IllegalStateException(\"Duplicate key \" + u);},"
                  + "() -> new java.util.TreeMap<>())))";
        } else {
          return  "java.util.Collections.unmodifiableSortedMap(" + nativeFieldName(field) + ")";
        }
    }
    throw new IllegalStateException();
  }

  String nativeDefaultValue(Schema.Field field) {
    JsonNode node = field.defaultValue();
    switch (field.schema().getType()) {
      case ARRAY:
        return CompilationUnit.isSet(field.schema())
               ? "java.util.Collections.emptySortedSet()"
               : "java.util.Collections.emptyList()";
      case MAP:
        return "java.util.Collections.emptySortedMap()";
      case ENUM:
        return InterfaceJavaGenerator.anoaInterfaceFullName(field.schema()) + ".nativeImpl(0)";
      case RECORD:
        return InterfaceJavaGenerator.anoaInterfaceFullName(field.schema())
               + ".newNativeImplBuilder().build()";
      case BOOLEAN:
        return Boolean.toString(node.getBooleanValue());
      case INT:
        return Integer.toString(node.getIntValue(), 16);
      case LONG:
        return Long.toString(node.getLongValue(), 16) + "L";
      case FLOAT:
        return Float.toHexString((float) node.getDoubleValue()) + "f";
      case DOUBLE:
        return Double.toHexString(node.getDoubleValue());
      case STRING:
        return node.toString();
      case BYTES:
        return ((AnoaBinaryNode) field.defaultValue()).toOctalString() + ".getBytes()";
    }
    throw new IllegalStateException();
  }

  @Override
  String importValue(Schema.Field field) {
    String value = VALUE;
    switch (field.schema().getType()) {
      case BYTES:
        value += ".get().clone()";
        break;
      case ENUM:
      case RECORD:
        value = name(field.schema()) + ".nativeImpl(" + value + ")";
        break;
      case ARRAY:
        value += ".stream()" + importArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection(() -> new java.util."
                 + (CompilationUnit.isSet(field.schema())
                    ? "TreeSet<>()"
                    : ("ArrayList<>(" + value + ".size())"))
                 + "))";
        break;
      case MAP:
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey(), "
                 + importMapValueFunction(field.schema().getValueType()) + ", (u,v) -> {"
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.TreeMap<>()))";
    }
    return value;
  }

  private String importArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(v -> v.get().clone())";
      case ENUM:
      case RECORD:
        return  ".map(" + name(s) + "::nativeImpl)";
    }
    return "";
  }

  private String importMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> e.getValue().get().clone()";
      case ENUM:
      case RECORD:
        return "e -> " + name(s) + ".nativeImpl(e.getValue())";
    }
    return "e -> e.getValue()";
  }


}
