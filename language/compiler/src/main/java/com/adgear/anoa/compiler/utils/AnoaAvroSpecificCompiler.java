package com.adgear.anoa.compiler.utils;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;

import java.util.Optional;

public class AnoaAvroSpecificCompiler extends SpecificCompiler {

  public AnoaAvroSpecificCompiler(Protocol protocol) {
    super(protocol);
    anoaDefaults();
  }

  public AnoaAvroSpecificCompiler(Schema schema) {
    super(schema);
    anoaDefaults();
  }

  private void anoaDefaults() {
    setTemplateDir("/com/adgear/anoa/avro/");
    setStringType(GenericData.StringType.CharSequence);
    setFieldVisibility(FieldVisibility.PRIVATE);
    setCreateSetters(false);
    setOutputCharacterEncoding("UTF-8");
  }

  public Schema.Field getAliasField(Schema.Field field, String alias) {
    return new Schema.Field(
        alias,
        field.schema(),
        field.doc(),
        field.defaultValue(),
        field.order());
  }

  public boolean isDeprecated(JsonProperties schema) {
    return Optional.ofNullable(schema.getJsonProp("deprecated"))
        .map(JsonNode::asBoolean)
        .orElse(false);
  }
}
