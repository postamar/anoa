package com.adgear.anoa.avro.encode;

import org.apache.avro.util.Utf8;
import org.msgpack.MessagePack;
import org.msgpack.packer.Unconverter;
import org.msgpack.type.MapValue;
import org.msgpack.type.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Encoder implementation for serializing Avro records as MessagePack <code>Value</code> objects.
 */
public class ValueEncoder extends EncoderBase<Value> {

  protected Type type;
  protected Unconverter packer;
  protected Unconverter sparsePacker;

  public ValueEncoder() {
    this.type = Type.NONE;
    MessagePack messagePack = new MessagePack();
    this.packer = new Unconverter(messagePack);
    this.sparsePacker = new Unconverter(messagePack);
  }

  @Override
  public Value build() {
    Value fullValue = packer.getResult();
    packer.resetResult();
    sparsePacker.resetResult();
    try {
      recursiveWrite(fullValue);
    } catch (IOException e) {
      return null;
    }
    return sparsePacker.getResult();
  }

  protected void recursiveWrite(Value value) throws IOException {
    if (value.isMapValue()) {
      MapValue map = value.asMapValue();
      int newLength = 0;
      for (Map.Entry<Value, Value> entry : map.entrySet()) {
        if (!entry.getValue().isNilValue()) {
          ++newLength;
        }
      }
      sparsePacker.writeMapBegin(newLength);
      for (Map.Entry<Value, Value> entry : map.entrySet()) {
        if (!entry.getValue().isNilValue()) {
          sparsePacker.write(entry.getKey());
          recursiveWrite(entry.getValue());
        }
      }
      sparsePacker.writeMapEnd();
    } else if (value.isArrayValue()) {
      Value[] array = value.asArrayValue().getElementArray();
      sparsePacker.writeArrayBegin(array.length);
      for (Value element : array) {
        recursiveWrite(element);
      }
      sparsePacker.writeArrayEnd();
    } else {
      sparsePacker.write(value);
    }
  }

  @Override
  public void writeNull() throws IOException {
    packer.writeNil();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    packer.write(b);
  }

  @Override
  public void writeInt(int n) throws IOException {
    packer.write(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    packer.write(n);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    packer.write(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    packer.write(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    packer.write(utf8.toString());
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    packer.write(bytes);
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    packer.write(bytes, start, len);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    packer.write(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    throw new IOException("Appropriate DatumWriter implementation does not call this method");
  }

  @Override
  public void writeArrayStart() throws IOException {
    type = Type.ARRAY;
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    final int n = new Long(itemCount).intValue();
    switch (type) {
      case ARRAY:
        packer.writeArrayBegin(n);
        break;
      case MAP:
        packer.writeMapBegin(n);
        break;
      default:
        throw new IOException("Unsupported type " + type);
    }
    type = Type.NONE;
  }

  @Override
  public void startItem() throws IOException {
    // do nothing?
  }

  @Override
  public void writeArrayEnd() throws IOException {
    packer.writeArrayEnd(false);
  }

  @Override
  public void writeMapStart() throws IOException {
    type = Type.MAP;
  }

  @Override
  public void writeMapEnd() throws IOException {
    packer.writeMapEnd(false);
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
  }

  @Override
  public void flush() throws IOException {
    packer.flush();
  }

  static protected enum Type {NONE, MAP, ARRAY}
}
