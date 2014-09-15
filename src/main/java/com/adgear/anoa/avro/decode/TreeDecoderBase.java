package com.adgear.anoa.avro.decode;

import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

/**
 * {@link org.apache.avro.io.Decoder} implementation base class for deserializing Avro records from
 * tree data structures.
 */
abstract public class TreeDecoderBase<N> extends DecoderBase {

  protected Stack<Iterator<N>> iteratorStack;
  protected N nextValue = null;

  protected TreeDecoderBase(N input) {
    iteratorStack = new Stack<>();
    ArrayList<N> list = new ArrayList<>();
    list.add(input);
    iteratorStack.push(list.iterator());
  }

  protected N peek() throws IOException {
    if (nextValue != null) {
      return nextValue;
    }
    while (!iteratorStack.empty() && !iteratorStack.peek().hasNext()) {
      iteratorStack.pop();
    }
    if (iteratorStack.empty()) {
      throw new IOException("Empty msgpack stream.");
    }
    nextValue = iteratorStack.peek().next();
    return nextValue;
  }

  protected N pop() throws IOException {
    final N node = peek();
    nextValue = null;
    if (isArray(node)) {
      iteratorStack.push(getArrayIterator(node));
    } else if (isMap(node)) {
      iteratorStack.push(getMapIterator(node));
    }
    return node;
  }

  abstract protected boolean isArray(N node);

  abstract protected boolean isMap(N node);

  abstract protected Iterator<N> getArrayIterator(N node);

  abstract protected Iterator<N> getMapIterator(N node);

  private void skip() throws IOException {
    peek();
    nextValue = null;
  }

  @Override
  public void readNull() throws IOException {
    skip();
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return (old == null) ? (new Utf8(readString())) : old.set(readString());
  }

  @Override
  public void skipString() throws IOException {
    skip();
  }

  @Override
  public void skipBytes() throws IOException {
    skip();
  }

  @Override
  public void skipFixed(int length) throws IOException {
    skip();
  }

  @Override
  public int readEnum() throws IOException {
    return readInt();
  }

  @Override
  public long arrayNext() throws IOException {
    return 0;
  }

  @Override
  public long skipArray() throws IOException {
    skip();
    return 0;
  }

  @Override
  public long mapNext() throws IOException {
    return 0;
  }

  @Override
  public long skipMap() throws IOException {
    skip();
    return 0;
  }
}
