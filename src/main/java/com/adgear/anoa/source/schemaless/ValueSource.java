package com.adgear.anoa.source.schemaless;

import com.adgear.anoa.provider.base.CounterlessProviderBase;
import com.adgear.anoa.source.Source;

import org.msgpack.MessagePack;
import org.msgpack.type.Value;
import org.msgpack.unpacker.MessagePackUnpacker;
import org.msgpack.unpacker.Unpacker;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ValueSource extends CounterlessProviderBase<Value> implements Source<Value> {

  final protected Unpacker unpacker;
  final private Iterator<Value> iterator;

  public ValueSource(BufferedInputStream in) {
    this(new MessagePackUnpacker(new MessagePack(), in));
  }

  public ValueSource(InputStream in) {
    this(new MessagePackUnpacker(new MessagePack(), new BufferedInputStream(in)));
  }

  protected ValueSource(Unpacker unpacker) {
    this.unpacker = unpacker;
    this.iterator = unpacker.iterator();
  }

  @Override
  protected Value getNext() throws IOException {
    return iterator.next();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public void close() throws IOException {
    unpacker.close();
  }
}
