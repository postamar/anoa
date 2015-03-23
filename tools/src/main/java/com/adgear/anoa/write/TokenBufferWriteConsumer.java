package com.adgear.anoa.write;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import java.io.IOException;
import java.util.function.Supplier;

public class TokenBufferWriteConsumer implements WriteConsumer<TreeNode>, Supplier<TokenBuffer> {

  final TokenBuffer tokenBuffer;

  public TokenBufferWriteConsumer(TokenBuffer tokenBuffer) {
    this.tokenBuffer = tokenBuffer;
  }

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
