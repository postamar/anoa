package com.adgear.anoa.plugin;

import com.adgear.anoa.parser.Parser;
import com.adgear.anoa.parser.SchemaGenerator;

import org.apache.avro.Schema;
import org.apache.maven.model.Resource;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.shared.model.fileset.FileSet;
import org.apache.maven.shared.model.fileset.util.FileSetManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Mojo(name = "run", defaultPhase = LifecyclePhase.GENERATE_SOURCES, threadSafe = true)
public class AnoaMojo extends AbstractMojo {

  @Parameter(readonly = true, required = true, defaultValue = "${project}")
  private MavenProject project;

  @Parameter(property = "sourceDirectory",
      defaultValue = "${basedir}/src/main/anoa")
  private File sourceDirectory;

  @Parameter(property = "outputDirectory",
      defaultValue = "${project.build.directory}/generated-sources")
  private File outputDirectory;

  @Parameter(property = "testSourceDirectory",
      defaultValue = "${basedir}/src/test/anoa")
  private File testSourceDirectory;

  @Parameter(property = "testOutputDirectory",
      defaultValue = "${project.build.directory}/generated-test-sources")
  private File testOutputDirectory;

  @Parameter(property = "generateAvro", defaultValue = "true")
  private boolean generateAvro;

  @Parameter(property = "generateProtobuf", defaultValue = "true")
  private boolean generateProtobuf;

  @Parameter(property = "generateThrift", defaultValue = "false")
  private boolean generateThrift;

  @Parameter(property = "generateCsv", defaultValue = "true")
  private boolean generateCsv;

  @Parameter(property = "protocCommand", defaultValue = "protoc")
  private String protocCommand;

  @Parameter(property = "thriftCommand", defaultValue = "thrift")
  private String thriftCommand;

  static final private String[] RESOURCE_INCLUDES =
      new String[] { "**/*.enum", "**/*.struct", "**/*.thrift", "**/*.proto", "**/*.avsc", "**/*.csv"};

  private void write(String contents, File file)
      throws MojoExecutionException {
    try {
      file.getParentFile().mkdirs();
      try (FileWriter writer = new FileWriter(file)) {
        writer.write(contents);
      }
    } catch (IOException e) {
      throw new MojoExecutionException("Write error: " + file , e);
    }
  }

  private void runCommand(String cmd, Stream<String> opts, File source, File cwd)
      throws MojoExecutionException  {
    String src = cwd.toPath().relativize(source.toPath()).toString();
    Stream<String> cmdStream = Stream.concat(Stream.concat(Stream.of(cmd), opts), Stream.of(src));
    String[] cmdArray = cmdStream.toArray(String[]::new);
    getLog().info(Stream.of(cmdArray).collect(Collectors.joining(" ", "Executing: '", "'.")));
    final Process process;
    try {
      process = Runtime.getRuntime().exec(cmdArray, null, cwd);
      if (process.waitFor() != 0) {
        Scanner scanner = new Scanner(process.getErrorStream());
        while (scanner.hasNextLine()) {
          getLog().error(">> " + scanner.nextLine());
        }
        throw new MojoExecutionException(
            cmd + " failed, exit code " + process.exitValue() + " for " + source);
      }
    } catch (InterruptedException e) {
      throw new MojoExecutionException(cmd + " interrupted for " + source, e);
    } catch (IOException e) {
      throw new MojoExecutionException(cmd + " failed for " + source, e);
    }
  }

  private void protoCompile(SchemaGenerator schemaGenerator, File cwd, File outputDir)
      throws MojoExecutionException {
    File proto = new File(cwd, schemaGenerator.protoFileName());
    write(schemaGenerator.protoSchema(), proto);
    if (generateProtobuf) {
      runCommand(protocCommand,
                 Stream.of("--java_out=" + cwd.toPath().relativize(outputDir.toPath())),
                 proto,
                 cwd);
    }
  }

  private void thriftCompile(SchemaGenerator schemaGenerator, File cwd, File outputDir)
      throws MojoExecutionException {
    File thrift = new File(cwd, schemaGenerator.thriftFileName());
    Path out = cwd.toPath().relativize(outputDir.toPath());
    write(schemaGenerator.thriftSchema(), thrift);
    if (generateThrift) {
      runCommand(thriftCommand,
                 Stream.of("--out", out.toString(), "--gen", "java"),
                 thrift,
                 cwd);
    }
  }

  private void avroCompile(SchemaGenerator schemaGenerator, File cwd, File outputDir)
      throws MojoExecutionException {
    File avro = new File(cwd, schemaGenerator.avroFileName());
    write(schemaGenerator.avroSchema().toString(true), avro);
    if (generateAvro) {
      try {
        Schema schema = new Schema.Parser().parse(avro);
        new AnoaAvroSpecificCompiler(schema).compileToDestination(avro, outputDir);
      } catch (IOException e) {
        throw new MojoExecutionException("avro compilation failed for " + avro, e);
      }
    }
  }

  private Stream<SchemaGenerator> parse(File anoaSourceDir)
      throws MojoExecutionException, MojoFailureException {
    Map<String, Stream<String>> map = new LinkedHashMap<>();
    FileSet includes = new FileSet();
    includes.setDirectory(anoaSourceDir.getAbsolutePath());
    includes.setFollowSymlinks(false);
    Stream.of("**/*.enum", "**/*.struct").forEach(includes::addInclude);
    for (String fileName : new FileSetManager().getIncludedFiles(includes)) {
      File source = new File(anoaSourceDir, fileName);
      try {
        map.put(source.getName(), new BufferedReader(new FileReader(source)).lines());
      } catch (IOException e) {
        throw new MojoExecutionException("Read error: " + source, e);
      }
    }
    return new Parser(getLog()::error, map::get).apply(map.keySet().stream()).orElseThrow(
        () -> new MojoFailureException("Compilation errors in '" + anoaSourceDir + "'."));
  }

  private void execute(File anoaSourceDir, File outDir)
      throws MojoExecutionException, MojoFailureException {
    List<SchemaGenerator> results = parse(anoaSourceDir).collect(Collectors.toList());
    File anoaDir = new File(outDir, "anoa");
    if (anoaDir.exists() && anoaDir.delete()) {
      throw new MojoExecutionException("Could not delete existing directory " + anoaDir);
    }
    if (!anoaDir.mkdirs()) {
      throw new MojoExecutionException("Could not create directory " + anoaDir);
    }
    File javaDir = new File(outDir, "java");
    if ((generateAvro || generateProtobuf || generateThrift) && !javaDir.mkdirs()) {
      throw new MojoExecutionException("Could not create directory " + javaDir);
    }
    for (SchemaGenerator g : results) {
      if (generateCsv && g.csvSchema().isPresent()) {
        write(g.csvSchema().get(), new File(anoaDir, g.csvFileName()));
      }
      avroCompile(g, anoaDir, javaDir);
      protoCompile(g, anoaDir, javaDir);
      thriftCompile(g, anoaDir, javaDir);
    }
  }

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    Optional<File> source = Optional.ofNullable(sourceDirectory).filter(File::isDirectory);
    Optional<File> test = Optional.ofNullable(testSourceDirectory).filter(File::isDirectory);
    if (!source.isPresent() && !test.isPresent()) {
      throw new MojoExecutionException(
          "neither sourceDirectory '" + sourceDirectory + "' or testSourceDirectory '"
          + testSourceDirectory + "' are directories");
    }

    if (source.isPresent()) {
      execute(source.get().getAbsoluteFile(), outputDirectory.getAbsoluteFile());
      if (generateAvro || generateProtobuf || generateThrift) {
        project.addCompileSourceRoot(new File(outputDirectory, "java").getAbsolutePath());
      }
      Resource resource = new Resource();
      resource.setDirectory(new File(outputDirectory, "anoa").getAbsolutePath());
      Stream.of(RESOURCE_INCLUDES).forEach(resource::addInclude);
      project.addResource(resource);
    }
    if (test.isPresent()) {
      execute(test.get().getAbsoluteFile(), testOutputDirectory.getAbsoluteFile());
      if (generateAvro || generateProtobuf || generateThrift) {
        project.addTestCompileSourceRoot(new File(testOutputDirectory, "java").getAbsolutePath());
      }
      Resource resource = new Resource();
      resource.setDirectory(new File(testOutputDirectory, "anoa").getAbsolutePath());
      Stream.of(RESOURCE_INCLUDES).forEach(resource::addInclude);
      project.addTestResource(resource);
    }
  }
}
