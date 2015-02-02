package com.adgear.anoa.codec.thrift;

import com.adgear.anoa.codec.base.CodecBase;
import com.adgear.anoa.provider.Provider;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.lang.reflect.Constructor;

/**
 * Transforms Thrift compact binary serializations into Thrift objects.
 *
 * @param <T> Type of the Thrift record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.codec.serialized.ThriftToCompactBytes
 */
public class CompactBytesToThrift<T extends TBase<T,?>>
    extends CodecBase<byte[], T, CompactBytesToThrift.Counter> {

  final protected Class<T> thriftClass;
  final protected Constructor<T> constructor;
  final protected TDeserializer deserializer;

  /**
   * @param provider    An upstream Provider of Thrift compact binary serializations.
   * @param thriftClass The class object corresponding to the serialized Thrift records.
   */
  public CompactBytesToThrift(Provider<byte[]> provider, Class<T> thriftClass) {
    super(provider, Counter.class);
    this.thriftClass = thriftClass;
    this.deserializer = new TDeserializer(new TCompactProtocol.Factory());
    if (!TBase.class.isAssignableFrom(thriftClass)) {
      throw new IllegalArgumentException("Supplied class does not derive from TBase.");
    }
    try {
      this.constructor = thriftClass.getConstructor();
      constructor.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public T transform(byte[] input) {
    T result;
    try {
      result = constructor.newInstance();
    } catch (Exception e) {
      increment(Counter.THRIFT_REFLECT_FAIL);
      logger.error(e.getMessage());
      return null;
    }
    try {
      deserializer.deserialize(result, input);
    } catch (TException e) {
      increment(Counter.THRIFT_DESERIALIZE_FAIL);
      logger.warn(e.getMessage());
      return null;
    }
    return result;
  }

  static public enum Counter {
    /**
     * Java reflection error counter.
     */
    THRIFT_REFLECT_FAIL,

    /**
     * Counts the number of records which could not be deserialized.
     */
    THRIFT_DESERIALIZE_FAIL
  }
}
