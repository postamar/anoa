package com.adgear.anoa.factory.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

public class JacksonReadIterator<N extends TreeNode, P extends JsonParser>
    extends AbstractReadIterator<N> {

  final public P jacksonParser;

  public JacksonReadIterator(P jacksonParser) {
    super(jacksonParser::isClosed);
    this.jacksonParser = jacksonParser;
  }

  @Override
  protected N doNext() {
    try {
      final N tree = jacksonParser.readValueAsTree();
      if (tree == null && jacksonParser.getCurrentToken() == null) {
        declareNoNext();
      }
      return tree;
    } catch (EOFException e) {
      declareNoNext();
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
