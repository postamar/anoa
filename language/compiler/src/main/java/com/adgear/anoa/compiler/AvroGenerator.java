package com.adgear.anoa.compiler;

import com.adgear.anoa.compiler.javagen.AvroJavaGenerator;

import org.apache.avro.compiler.specific.SpecificCompiler;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Generates Avro protocol schema and Avro enum and SpecificRecord implementations.
 */
final class AvroGenerator extends GeneratorBase {

  final public String schema;

  AvroGenerator(CompilationUnit cu, Consumer<String> logger) {
    super(cu, "Avro", logger);
    schema = cu.generate("Avro", false).toString(true);
  }

  @Override
  public String generateSchema() {
    return schema;
  }

  @Override
  protected String schemaFileName(String namespace) {
    return namespace.substring(namespace.lastIndexOf('.') + 1) + ".avpr";
  }

  @Override
  public void generateJava(File schemaRootDir, File javaRootDir)
      throws CodeGenerationException {
    SpecificCompiler compiler = new AvroJavaGenerator(protocol);
    File source = new File(schemaRootDir, getSchemaFile().toString());
    log("Generating java code for Anoa schema in '" + getSchemaFile() + "'...");
    try {
      compiler.compileToDestination(source, javaRootDir);
    } catch (IOException e) {
      throw new CodeGenerationException("Anoa code generation failed for '" + source + "'.", e);
    }
    log("Successfully generated java code for Anoa schema in '" + getSchemaFile() + "'.");
  }
}
