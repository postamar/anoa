package com.adgear.anoa.compiler;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final public class CommandLineUtils {

  private CommandLineUtils() {}

  static public void runCommand(String cmd, Stream<String> args, File cwd, Consumer<String> logger)
      throws CodeGenerationException {
    String[] cmdArray = Stream.concat(Stream.of(cmd), args).toArray(String[]::new);
    logger.accept(report(cmdArray, "Launching") + "..");
    final Process process;
    try {
      process = Runtime.getRuntime().exec(cmdArray, null, cwd);
      if (process.waitFor() != 0) {
        Scanner scanner = new Scanner(process.getErrorStream());
        while (scanner.hasNextLine()) {
          logger.accept(">> " + scanner.nextLine());
        }
        throw new CodeGenerationException(
            report(cmdArray, "Failed (exit code " + process.exitValue() + ")"));
      }
    } catch (InterruptedException e) {
      throw new CodeGenerationException(report(cmdArray, "Interrupted"), e);
    } catch (IOException e) {
      throw new CodeGenerationException(report(cmdArray, "Failed"), e);
    }
    logger.accept(report(cmdArray, "Successful"));
  }

  static private String report(String[] cmdArray, String msg) {
    return Stream.of(cmdArray).collect(Collectors.joining(" ", msg + " execution of '", "'."));
  }
}
