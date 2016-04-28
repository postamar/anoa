package com.adgear.anoa.compiler.javagen;

import com.adgear.anoa.compiler.AnoaParserBase;
import com.adgear.anoa.compiler.CompilationUnit;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

/**
 * Base class for custom java source code generator.
 */
abstract class AbstractJavaGenerator extends SpecificCompiler {

  final protected String protocolFullName;

  AbstractJavaGenerator(Protocol protocol) {
    super(protocol);
    setStringType(GenericData.StringType.Utf8);
    setFieldVisibility(FieldVisibility.PRIVATE);
    setCreateSetters(false);
    setOutputCharacterEncoding("UTF-8");
    this.protocolFullName = Optional.ofNullable(protocol.getNamespace())
                                .map(ns -> ns + ".")
                                .orElse("")
                            + protocol.getName();
    try {
      Field protocolField = SpecificCompiler.class.getDeclaredField("protocol");
      protocolField.setAccessible(true);
      protocolField.set(this, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Schema.Field> fields(Schema schema) {
    return CompilationUnit.modifySchema(schema, "", false).getFields();
  }

  public String version(Schema schema) {
    if (schema.getType() == Schema.Type.ENUM) {
      return Long.toString(schema.getEnumSymbols().size());
    }
    long largest = 0L;
    for (Schema.Field field : schema.getFields()) {
      largest = Math.max(largest, field.getJsonProp(AnoaParserBase.ORDINAL_PROP_KEY).asLong());
    }
    return Long.toString(largest);
  }

  public boolean isDeprecated(JsonProperties schema) {
    return Optional.ofNullable(schema.getJsonProp("deprecated"))
        .map(JsonNode::asBoolean)
        .orElse(false);
  }

  public Schema.Field aliasField(Schema.Field field, String alias) {
    return new Schema.Field(
        alias,
        field.schema(),
        field.doc(),
        field.defaultValue(),
        field.order());
  }
}
