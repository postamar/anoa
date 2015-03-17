package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;

import java.io.IOException;
import java.io.UncheckedIOException;

class TreeNodeWriteConsumer<N extends TreeNode>
    implements WriteConsumer<N, IOException> {

  final JsonGenerator jacksonGenerator;

  TreeNodeWriteConsumer(JsonGenerator jacksonGenerator) {
    this.jacksonGenerator = jacksonGenerator;
  }

  @Override
  public void acceptChecked(N treeNode) throws IOException {
    jacksonGenerator.writeTree(treeNode);
  }

  @Override
  public void accept(N treeNode) {
    try {
      acceptChecked(treeNode);
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
