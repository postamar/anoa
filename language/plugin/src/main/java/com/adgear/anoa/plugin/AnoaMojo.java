package com.adgear.anoa.plugin;

import com.adgear.anoa.compiler.CompilationUnit;
import com.adgear.anoa.compiler.JavaCodeGenerationException;
import com.adgear.anoa.compiler.ParseException;
import com.adgear.anoa.compiler.SchemaGenerationException;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.shared.model.fileset.FileSet;
import org.apache.maven.shared.model.fileset.util.FileSetManager;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

  @Parameter(property = "generateThrift", defaultValue = "true")
  private boolean generateThrift;

  @Parameter(property = "protocCommand", defaultValue = "protoc")
  private String protocCommand;

  @Parameter(property = "thriftCommand", defaultValue = "thrift")
  private String thriftCommand;

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
      execute(source.get(), outputDirectory);
      if (generateAvro || generateProtobuf || generateThrift) {
        project.addCompileSourceRoot(new File(outputDirectory, "java").getAbsolutePath());
      }
    }
    if (test.isPresent()) {
      execute(test.get(), testOutputDirectory);
      if (generateAvro || generateProtobuf || generateThrift) {
        project.addTestCompileSourceRoot(new File(testOutputDirectory, "java").getAbsolutePath());
      }
    }
  }

  private void execute(File anoaSourceDir, File outDir)
      throws MojoExecutionException, MojoFailureException {
    getLog().info("Parsing all anoa files in '" + anoaSourceDir + "'...");
    List<CompilationUnit> parsed = parse(anoaSourceDir);
    getLog().info("Successfully parsed all anoa files.");
    File anoaDir = new File(outDir, "anoa");
    if (anoaDir.exists() && anoaDir.delete()) {
      throw new MojoExecutionException("Could not delete existing directory " + anoaDir);
    }
    if (!anoaDir.exists() && !anoaDir.mkdirs()) {
      throw new MojoExecutionException("Could not create directory " + anoaDir);
    }
    File javaDir = new File(outDir, "java");
    if ((generateAvro || generateProtobuf || generateThrift)
        && !javaDir.exists() && !javaDir.mkdirs()) {
      throw new MojoExecutionException("Could not create directory " + javaDir);
    }
    try {
      compile(parsed, anoaDir, javaDir);
    } catch (SchemaGenerationException e) {
      throw new MojoExecutionException("Error generating schema.", e);
    } catch (JavaCodeGenerationException e) {
      throw new MojoExecutionException("Error generating java source.", e);
    }
  }

  private void compile(Iterable<CompilationUnit> parsed, File schemaDir, File javaDir)
      throws SchemaGenerationException, JavaCodeGenerationException {
    for (CompilationUnit cu : parsed) {
      cu.avroGenerator().generateSchema(schemaDir);
      cu.protobufGenerator().generateSchema(schemaDir);
      cu.thriftGenerator().generateSchema(schemaDir);
    }
    for (CompilationUnit cu : parsed) {
      if (generateAvro) {
        cu.avroGenerator().generateJava(schemaDir, javaDir);
      }
      if (generateProtobuf) {
        cu.protobufGenerator(protocCommand).generateJava(schemaDir, javaDir);
      }
      if (generateThrift) {
        cu.thriftGenerator(thriftCommand).generateJava(schemaDir, javaDir);
      }
    }
  }

  private List<CompilationUnit> parse(File anoaSourceDir)
      throws MojoExecutionException, MojoFailureException {
    FileSet includes = new FileSet();
    includes.setDirectory(anoaSourceDir.getAbsolutePath());
    includes.setFollowSymlinks(false);
    includes.addInclude("**/*.anoa");
    ClassLoader classLoader = getResourceLoader(anoaSourceDir);
    List<CompilationUnit> cu = new ArrayList<>();
    for (String fileName : new FileSetManager().getIncludedFiles(includes)) {
      String namespace = fileName
          .substring(0, fileName.length() - ".anoa".length())
          .replace(File.separatorChar, '.');
      try {
        cu.add(new CompilationUnit(namespace, anoaSourceDir, classLoader).parse(getLog()::info));
      } catch (ParseException e) {
        throw new MojoFailureException("Error parsing '" + fileName + "'.", e);
      } catch (IOException e) {
        throw new MojoExecutionException("Read error for '" + fileName + "'.", e);
      }
    }
    return cu;
  }

  private ClassLoader getResourceLoader(File anoaSourceDir) throws MojoExecutionException {
    try {
      List<String> runtimeClasspathElements = project.getRuntimeClasspathElements();
      List<URL> runtimeUrls = new ArrayList<>();
      runtimeUrls.add(anoaSourceDir.toURI().toURL());
      if (runtimeClasspathElements != null) {
        for (String runtimeClasspathElement : runtimeClasspathElements) {
          runtimeUrls.add(new File(runtimeClasspathElement).toURI().toURL());
        }
      }
      return new URLClassLoader(runtimeUrls.toArray(new URL[runtimeUrls.size()]),
                                Thread.currentThread().getContextClassLoader());
    } catch (DependencyResolutionRequiredException | MalformedURLException e) {
      throw new MojoExecutionException("ClassLoader error: " + e);
    }
  }
}
