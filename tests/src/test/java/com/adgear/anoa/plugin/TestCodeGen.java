package com.adgear.anoa.plugin;

import com.adgear.anoa.test.nested.Bytes;

import org.junit.Test;

import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitx.framework.Assert;

public class TestCodeGen {

  private void test(Supplier<Bytes.Builder<?>> builderSupplier) {
    byte[] array = "foo".getBytes();
    Supplier<byte[]> supplier = () -> array;

    Bytes<?> b1 = builderSupplier.get()
        .setBytesField(supplier)
        .setBytesList(Stream.of(supplier).collect(Collectors.toList()))
        .setBytesMap(Stream.of(supplier).collect(Collectors.toMap(k -> "", v -> v)))
        .build();

    Assert.assertEquals(3, b1.getBytesField().get().length);
    Assert.assertEquals((byte) 'f', b1.getBytesField().get()[0]);
    Assert.assertFalse(array == b1.getBytesField().get());
    Assert.assertFalse(array == b1.getBytesList().get(0).get());
    Assert.assertFalse(array == b1.getBytesMap().get("").get());

    Bytes<?> b2 = builderSupplier.get()
        .setBytesField(supplier)
        .setBytesList(Stream.of(supplier).collect(Collectors.toList()))
        .setBytesMap(Stream.of(supplier).collect(Collectors.toMap(k -> "", v -> v)))
        .build();

    Assert.assertFalse(b1.getBytesField().get() == b2.getBytesField().get());
    Assert.assertFalse(b1.getBytesList().get(0).get() == b2.getBytesList().get(0).get());
    Assert.assertFalse(b1.getBytesMap().get("").get() == b2.getBytesMap().get("").get());

    array[0] = 0;

    Assert.assertEquals((byte) 'f', b1.getBytesField().get()[0]);
    Assert.assertEquals((byte) 'f', b1.getBytesList().get(0).get()[0]);
    Assert.assertEquals((byte) 'f', b1.getBytesMap().get("").get()[0]);
  }

  @Test
  public void testBytesFieldNative() {
    test(Bytes::newNativeImplBuilder);
  }

  @Test
  public void testBytesFieldAvro() {
    test(Bytes::newAvroBuilder);
  }

  @Test
  public void testBytesFieldProtobuf() {
    test(Bytes::newProtobufBuilder);
  }

  @Test
  public void testBytesFieldThrift() {
    test(Bytes::newThriftBuilder);
  }
}
