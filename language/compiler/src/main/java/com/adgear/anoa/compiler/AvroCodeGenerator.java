package com.adgear.anoa.compiler;

import com.adgear.anoa.compiler.utils.AnoaAvroSpecificCompiler;

import org.apache.avro.compiler.specific.SpecificCompiler;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

final class AvroCodeGenerator extends AnoaCodeGeneratorBase {

  AvroCodeGenerator(CompilationUnit cu, Consumer<String> logger) {
    super(cu, "Avro", logger);
  }

  @Override
  public String generateSchema() {
    return protocol.toString(true);
  }

  @Override
  protected String schemaFileName(String namespace) {
    return namespace.substring(namespace.lastIndexOf('.') + 1) + ".avpr";
  }

  @Override
  public void generateJava(File schemaRootDir, File javaRootDir)
      throws JavaCodeGenerationException {
    SpecificCompiler compiler = new AnoaAvroSpecificCompiler(protocol);
    File source = new File(schemaRootDir, getSchemaFile().toString());
    log("Generating java code for Avro schema in '" + getSchemaFile() + "'...");
    try {
      compiler.compileToDestination(source, javaRootDir);
    } catch (IOException e) {
      throw new JavaCodeGenerationException("Avro code generation failed for '" + source + "'.", e);
    }
    log("Successfully generated java code for Avro schema in '" + getSchemaFile() + "'.");
  }
}
