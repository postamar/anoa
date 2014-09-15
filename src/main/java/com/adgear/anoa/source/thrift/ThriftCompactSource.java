package com.adgear.anoa.source.thrift;

import com.adgear.anoa.provider.base.ProviderBase;
import com.adgear.anoa.source.Source;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;

/**
 * Iterates over a stream of Thrift compact binary serializations.
 *
 * @param <T> Type of the Thrift record to be provided by the Codec.
 */
public class ThriftCompactSource<T extends TBase<?, ?>>
    extends ProviderBase<T, ThriftCompactSource.Counter>
    implements Source<T> {

  final protected Class<T> thriftClass;
  final protected Constructor<T> constructor;
  final private BufferedInputStream in;
  final private TIOStreamTransport tioStreamTransport;
  final private TCompactProtocol tCompactProtocol;
  private T nextRecord = null;

  public ThriftCompactSource(BufferedInputStream in, Class<T> thriftClass) {
    super(Counter.class);
    this.in = in;
    if (!TBase.class.isAssignableFrom(thriftClass)) {
      throw new IllegalArgumentException("Supplied class does not derive from TBase.");
    }
    try {
      this.constructor = thriftClass.getConstructor();
      constructor.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    this.thriftClass = thriftClass;
    tioStreamTransport = new TIOStreamTransport(in);
    tCompactProtocol = new TCompactProtocol(tioStreamTransport);
  }

  public ThriftCompactSource(InputStream in, Class<T> thriftClass) {
    this(new BufferedInputStream(in), thriftClass);
  }

  @Override
  public void close() throws IOException {
    tioStreamTransport.close();
  }

  private void update() throws IOException {
    if (nextRecord == null) {
      final int val;
      try {
        in.mark(1);
        val = in.read();
        in.reset();
      } catch (IOException e) {
        increment(Counter.STREAM_READ_FAIL);
        logger.error(e.getMessage());
        throw e;
      }
      if (val == -1) {
        return;
      }
      final T record;
      try {
        record = constructor.newInstance();
      } catch (Exception e) {
        increment(Counter.THRIFT_REFLECT_FAIL);
        logger.error(e.getMessage());
        throw new IOException(e);
      }
      try {
        record.read(tCompactProtocol);
      } catch (TException e) {
        increment(Counter.THRIFT_DESERIALIZE_FAIL);
        logger.warn(e.getMessage());
        throw new IOException(e);
      }
      nextRecord = record;
    }
  }

  @Override
  protected T getNext() throws IOException {
    update();
    T next = nextRecord;
    nextRecord = null;
    return next;
  }

  @Override
  public boolean hasNext() {
    try {
      update();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return (nextRecord != null);
  }

  static public enum Counter {
    /**
     * Counts the number of times an attempt to read from the stream failed.
     */
    STREAM_READ_FAIL,

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
