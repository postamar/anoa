package com.adgear.anoa.write;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A {@code WriteConsumer} implementation backed by a {@link com.fasterxml.jackson.databind.util.TokenBuffer}
 */
public class TokenBufferWriteConsumer implements WriteConsumer<TreeNode>, Supplier<TokenBuffer> {

  final protected TokenBuffer tokenBuffer;

  public TokenBufferWriteConsumer(TokenBuffer tokenBuffer) {
    this.tokenBuffer = tokenBuffer;
  }

  /**
   * @return this instance's TokenBuffer object
   */
  @Override
  public TokenBuffer get() {
    return tokenBuffer;
  }

  @Override
  public void acceptChecked(TreeNode record) throws IOException {
    tokenBuffer.writeTree(record);
  }

  @Override
  public void flush() throws IOException {
    tokenBuffer.flush();
  }
}
