package com.adgear.anoa.library.write;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;

import java.io.IOException;

class TreeNodeWriteConsumer<N extends TreeNode> implements WriteConsumer<N> {

  final JsonGenerator jacksonGenerator;

  TreeNodeWriteConsumer(JsonGenerator jacksonGenerator) {
    this.jacksonGenerator = jacksonGenerator;
  }

  @Override
  public void acceptChecked(N treeNode) throws IOException {
    jacksonGenerator.writeTree(treeNode);
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
