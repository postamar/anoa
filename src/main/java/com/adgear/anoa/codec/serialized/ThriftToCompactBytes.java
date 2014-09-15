package com.adgear.anoa.codec.serialized;

import com.adgear.anoa.codec.base.CodecBase;
import com.adgear.anoa.provider.Provider;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Transforms Thrift objects into Thrift compact binary serializations.
 *
 * @param <T> Type of the Thrift records consumed from the upstream Provider.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.thrift.ThriftCompactSource
 * @see com.adgear.anoa.codec.thrift.AvroBytesToThrift
 * @see com.adgear.anoa.codec.thrift.CompactBytesToThrift
 */
public class ThriftToCompactBytes<T extends TBase<?, ?>>
    extends CodecBase<T, byte[], ThriftToCompactBytes.Counter> {

  final protected TSerializer serializer;

  public ThriftToCompactBytes(Provider<T> provider) {
    super(provider, Counter.class);
    this.serializer = new TSerializer(new TCompactProtocol.Factory());
  }

  @Override
  public byte[] transform(T element) {
    try {
      return serializer.serialize(element);
    } catch (TException e) {
      increment(Counter.THRIFT_COMPACT_SERIALIZATION_FAIL);
      logger.warn(e.getMessage());
      return null;
    }
  }

  static public enum Counter {
    /**
     * Counts the records which could not be serialized to Thrift compact binary.
     */
    THRIFT_COMPACT_SERIALIZATION_FAIL
  }
}