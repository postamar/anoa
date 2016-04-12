package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for generating schema files and java source code.
 */
abstract class GeneratorBase implements Generator {

  final protected Protocol protocol;
  final private List<String> importedNamespaces;
  final private Consumer<String> logger;

  protected GeneratorBase(CompilationUnit cu, String suffix, Consumer<String> logger) {
    this.protocol = cu.generate(suffix, true);
    this.importedNamespaces = cu.getImportedNamespaces().collect(Collectors.toList());
    this.logger = logger;
  }

  protected Stream<Path> getImports() {
    return importedNamespaces.stream().sequential()
        .map(ns -> new File(ns.replace('.', File.separatorChar), schemaFileName(ns)).toPath());
  }


  File getSchemaFile() {
    return new File(protocol.getNamespace().replace('.', File.separatorChar),
                    schemaFileName(protocol.getNamespace()));
  }

  @Override
  public void generateSchema(File schemaRootDir) throws SchemaGenerationException {
    File outputFile = new File(schemaRootDir, getSchemaFile().toString());
    try {
      outputFile.getParentFile().mkdirs();
      try (FileWriter writer = new FileWriter(outputFile)) {
        writer.write(generateSchema());
      }
    } catch (IOException e) {
      throw new SchemaGenerationException("Error writing schema to '" + outputFile + "'." , e);
    }
  }

  abstract protected String schemaFileName(String namespace);

  protected String comments(String docString, String prefix, String suffix) {
    return (docString == null || docString.trim().isEmpty())
           ? ""
           : (prefix + "/** " + docString.trim() + " */" + suffix);
  }

  protected void log(String message) {
    logger.accept(message);
  }

  protected void runCommand(String cmd, Stream<String> opts, File cwd)
      throws JavaCodeGenerationException {
    File source = new File(cwd, getSchemaFile().toString());
    String src = cwd.toPath().relativize(source.toPath()).toString();
    Stream<String> cmdStream = Stream.concat(Stream.concat(Stream.of(cmd), opts), Stream.of(src));
    String[] cmdArray = cmdStream.toArray(String[]::new);
    log(Stream.of(cmdArray).collect(Collectors.joining(" ", "Executing '", "'...")));
    final Process process;
    try {
      process = Runtime.getRuntime().exec(cmdArray, null, cwd);
      if (process.waitFor() != 0) {
        Scanner scanner = new Scanner(process.getErrorStream());
        while (scanner.hasNextLine()) {
          log(">> " + scanner.nextLine());
        }
        throw new JavaCodeGenerationException(
            cmd + " failed, exit code " + process.exitValue() + " for " + source);
      }
    } catch (InterruptedException e) {
      throw new JavaCodeGenerationException(cmd + " interrupted for " + source, e);
    } catch (IOException e) {
      throw new JavaCodeGenerationException(cmd + " failed for " + source, e);
    }
    log(Stream.of(cmdArray).collect(Collectors.joining(" ", "Successfully executed '", "'.")));
  }

  static boolean isUnsigned(Schema schema) {
    return Optional.ofNullable(schema.getJsonProp(AnoaParserBase.LOWER_BOUND_PROP_KEY))
        .filter(node -> node.isFloatingPointNumber()
                        ? (node.asDouble() >= 0.0)
                        : (node.asLong() >= 0L))
        .isPresent();
  }

  static int getPrecision(Schema schema) {
    switch (schema.getType()) {
      case FLOAT:
      case DOUBLE:
        return Optional.ofNullable(schema.getJsonProp(AnoaParserBase.MANTISSA_BITS_PROP_KEY))
                   .filter(node -> node.asInt() < 24)
                   .isPresent() ? 32 : 64;
      case INT:
      case LONG:
        final long lb = Optional.ofNullable(schema.getJsonProp(AnoaParserBase.LOWER_BOUND_PROP_KEY))
            .map(JsonNode::asLong)
            .orElse(Long.MIN_VALUE);
        final long ub = Optional.ofNullable(schema.getJsonProp(AnoaParserBase.UPPER_BOUND_PROP_KEY))
            .map(JsonNode::asLong)
            .orElse(Long.MAX_VALUE);
        final long b = Math.max(Math.max(Math.abs(lb), Math.abs(ub)), Math.abs(ub - lb));
        return (b < 0x100000000L) ? ((b < 0x10000L) ? ((b < 0x100L) ? 8 : 16) : 32) : 64;
    }
    throw new IllegalArgumentException(schema.toString());
  }
}
