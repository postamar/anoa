package com.adgear.anoa.factory.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;

import java.io.IOException;
import java.io.UncheckedIOException;

public class JacksonWriteConsumer<N extends TreeNode, G extends JsonGenerator>
    implements WriteConsumer<N> {

  final public G jacksonGenerator;

  public JacksonWriteConsumer(G jacksonGenerator) {
    this.jacksonGenerator = jacksonGenerator;
  }

  @Override
  public void accept(N treeNode) {
    try {
      jacksonGenerator.writeTree(treeNode);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    jacksonGenerator.flush();
  }

  @Override
  public void close() throws IOException {
    flush();
    jacksonGenerator.close();
  }
}
