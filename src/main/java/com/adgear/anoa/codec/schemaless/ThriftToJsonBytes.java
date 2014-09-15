package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.codec.base.CodecBase;
import com.adgear.anoa.provider.Provider;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;

/**
 * Transforms Thrift records into serialized JSON objects.
 *
 * @param <IN> Type of the Thrift records consumed from the upstream Provider.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.codec.thrift.AvroBytesToThrift
 * @see com.adgear.anoa.codec.thrift.CompactBytesToThrift
 */
public class ThriftToJsonBytes<IN extends TBase<?, ?>>
    extends CodecBase<IN, byte[], ThriftToJsonBytes.Counter> {

  final protected TSerializer serializer;

  public ThriftToJsonBytes(Provider<IN> provider) {
    super(provider, Counter.class);
    serializer = new TSerializer(new TSimpleJSONProtocolModified.Factory());
  }

  @Override
  public byte[] transform(IN input) {
    try {
      return serializer.serialize(input);
    } catch (TException e) {
      logger.warn(e.getMessage());
      increment(Counter.THRIFT_JSON_SERIALIZATION_FAIL);
      return null;
    }
  }

  static public enum Counter {
    /**
     * Counts the records which could not be serialized to JSON.
     */
    THRIFT_JSON_SERIALIZATION_FAIL,
  }

  static private class TSimpleJSONProtocolModified extends TSimpleJSONProtocol {

    public TSimpleJSONProtocolModified(TTransport trans) {
      super(trans);
    }

    @Override
    public void writeBinary(ByteBuffer bin) throws TException {
      super.writeBinary(ByteBuffer.wrap(Base64.encodeBase64(bin.array())));
    }

    static private class Factory implements TProtocolFactory {

      public TProtocol getProtocol(TTransport trans) {
        return new TSimpleJSONProtocolModified(trans);
      }
    }
  }
}
