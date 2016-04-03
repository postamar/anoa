package com.adgear.anoa.test;


import com.google.protobuf.ByteString;

import com.adgear.anoa.test.nested.Nested;
import com.adgear.anoa.test.nested.NestedAvro;
import com.adgear.anoa.test.nested.NestedProtobuf;

import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestDefaults {

  void testIsDefault(Nested<?> nested) {
    Assert.assertTrue(nested.isDefaultEnumField());
    Assert.assertTrue(nested.isDefaultEnumListField());
    Assert.assertTrue(nested.isDefaultEnumMapField());
    Assert.assertTrue(nested.isDefaultVariant());
    Assert.assertTrue(nested.getVariant().isDefaultBooleanVariant());
    Assert.assertTrue(nested.getVariant().isDefaultBytesVariant());
    Assert.assertTrue(nested.getVariant().isDefaultDoubleVariant());
    Assert.assertTrue(nested.getVariant().isDefaultFloatVariant());
    Assert.assertTrue(nested.getVariant().isDefaultIntVariant());
    Assert.assertTrue(nested.getVariant().isDefaultLongVariant());
    Assert.assertTrue(nested.getVariant().isDefaultStringVariant());
  }


  void testNonDefault(Nested<?> nested) {
    Assert.assertFalse(nested.isDefaultEnumField());
    Assert.assertFalse(nested.isDefaultEnumListField());
    Assert.assertFalse(nested.isDefaultEnumMapField());
    Assert.assertFalse(nested.isDefaultVariant());
    Assert.assertFalse(nested.getVariant().isDefaultBooleanVariant());
    Assert.assertFalse(nested.getVariant().isDefaultBytesVariant());
    Assert.assertFalse(nested.getVariant().isDefaultDoubleVariant());
    Assert.assertFalse(nested.getVariant().isDefaultFloatVariant());
    Assert.assertFalse(nested.getVariant().isDefaultIntVariant());
    Assert.assertFalse(nested.getVariant().isDefaultLongVariant());
    Assert.assertFalse(nested.getVariant().isDefaultStringVariant());
  }

  Nested<NestedProtobuf.Nested> nonDefault = Nested.Protobuf.from(
      NestedProtobuf.Nested.newBuilder()
          .setEnumField(NestedProtobuf.Enum.ON)
          .addEnumListField(
              NestedProtobuf.Enum.OFF)
          .putAllEnumMapField(Stream.of(NestedProtobuf.Enum.ON, NestedProtobuf.Enum.OFF)
                                  .collect(Collectors.toMap(e -> e.toString(), e -> e)))
          .setVariant(NestedProtobuf.Variant
                          .newBuilder()
                          .setBooleanVariant(false)
                          .setBytesVariant(ByteString.copyFromUtf8("bar"))
                          .setDoubleVariant(0)
                          .setFloatVariant(0)
                          .setIntVariant(0)
                          .setLongVariant(0)
                          .setStringVariant("baz")
                          .build())
          .build());

  @Test
  public void testAvro() {
    testIsDefault(NestedAvro.newBuilder().build());
    testNonDefault(NestedAvro.from(nonDefault));
  }

  @Test
  public void testProtobuf() {
    testIsDefault(Nested.Protobuf.from(NestedProtobuf.Nested.getDefaultInstance()));
    testNonDefault(nonDefault);
  }

  @Test
  public void testThrift() {
    testIsDefault(new Nested.Thrift());
    testNonDefault(Nested.Thrift.from(nonDefault));
  }
}
