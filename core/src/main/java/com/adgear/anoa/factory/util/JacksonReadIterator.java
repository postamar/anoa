package com.adgear.anoa.factory.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;

public class JacksonReadIterator<N extends TreeNode, P extends JsonParser>
    implements ReadIterator<N> {

  final public P jacksonParser;

  public JacksonReadIterator(P jacksonParser) {
    this.jacksonParser = jacksonParser;
  }

  private boolean isStale = true;
  private boolean hasNext = true;
  private N nextTree = null;

  @Override
  public boolean hasNext() {
    if (isStale) {
      if (!jacksonParser.isClosed()) {
        try {
          nextTree = jacksonParser.readValueAsTree();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      isStale = false;
      hasNext = !jacksonParser.isClosed();
    }
    return hasNext;
  }

  @Override
  public N next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    isStale = true;
    return nextTree;
  }
}
