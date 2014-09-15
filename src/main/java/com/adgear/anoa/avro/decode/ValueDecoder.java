package com.adgear.anoa.avro.decode;

import org.msgpack.type.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * {@link org.apache.avro.io.Decoder} implementation for deserializing Avro records from MessagePack
 * <code>Value</code> instances.
 */
public class ValueDecoder extends TreeDecoderBase<Value> {

  public ValueDecoder(Value value) {
    super(value);
  }

  @Override
  protected boolean isArray(Value node) {
    return node.isArrayValue();
  }

  @Override
  protected boolean isMap(Value node) {
    return node.isMapValue();
  }

  @Override
  protected Iterator<Value> getArrayIterator(Value node) {
    return node.asArrayValue().iterator();
  }

  @Override
  protected Iterator<Value> getMapIterator(Value node) {
    return Arrays.asList(node.asMapValue().getKeyValueArray()).iterator();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return peek().isBooleanValue()
           ? pop().asBooleanValue().getBoolean()
           : parseBoolean(pop().asRawValue().getString());
  }

  @Override
  public int readInt() throws IOException {
    return peek().isIntegerValue()
           ? pop().asIntegerValue().getInt()
           : parseInteger(pop().asRawValue().getString());
  }

  @Override
  public long readLong() throws IOException {
    return peek().isIntegerValue()
           ? pop().asIntegerValue().getLong()
           : parseLong(pop().asRawValue().getString());
  }

  @Override
  public float readFloat() throws IOException {
    return peek().isFloatValue()
           ? pop().asFloatValue().getFloat()
           : parseFloat(pop().asRawValue().getString());
  }

  @Override
  public double readDouble() throws IOException {
    return peek().isFloatValue()
           ? pop().asFloatValue().getDouble()
           : parseDouble(pop().asRawValue().getString());
  }

  @Override
  public String readString() throws IOException {
    return peek().isRawValue() ? pop().asRawValue().getString() : pop().toString();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return ByteBuffer.wrap(pop().asRawValue().getByteArray());
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    ByteBuffer bb = ByteBuffer.wrap(bytes, start, length);
    bb.put(pop().asRawValue().getByteArray());
  }

  @Override
  public long readArrayStart() throws IOException {
    return pop().asArrayValue().size();
  }

  @Override
  public long readMapStart() throws IOException {
    return pop().asMapValue().entrySet().size();
  }

  @Override
  public int readIndex() throws IOException {
    return (peek().isNilValue()) ? 0 : 1;
  }
}
