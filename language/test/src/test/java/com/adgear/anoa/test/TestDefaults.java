package com.adgear.anoa.test;


import com.google.protobuf.ByteString;

import com.adgear.anoa.test.nested.Nested;
import com.adgear.anoa.test.nested.NestedProtobuf;
import com.adgear.anoa.test.nested.Variant;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestDefaults {

  void testIsDefault(Nested<?> nested) {
    Assert.assertTrue(nested.isDefault());
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
    Assert.assertFalse(nested.isDefault());
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

  Nested<?> nonDefault = Nested.protobuf(
      Nested.newProtobufBuilder()
          .setEnumField(com.adgear.anoa.test.nested.Enum.Values.ON)
          .setEnumListField(Collections.singletonList(com.adgear.anoa.test.nested.Enum.Values.OFF))
          .setEnumMapField(
              Stream.of(com.adgear.anoa.test.nested.Enum.Values.ON,
                        com.adgear.anoa.test.nested.Enum.Values.OFF)
                  .collect(Collectors.toMap(e -> e.toString(), e -> e)))
          .setVariant(Variant.newProtobufBuilder()
                          .setBooleanVariant(false)
                          .setBytesVariant("foo"::getBytes)
                          .setDoubleVariant(0)
                          .setFloatVariant(0)
                          .setIntVariant(0)
                          .setLongVariant(0)
                          .setStringVariant("baz")
                          .build())
          .build());

  @Test
  public void testAvro() {
    testIsDefault(Nested.avro(Nested.newAvroBuilder().build()));
    testNonDefault(Nested.avro(nonDefault));
  }

  @Test
  public void testProtobuf() {
    testIsDefault(Nested.protobuf(NestedProtobuf.Nested.getDefaultInstance()));
    testNonDefault(nonDefault);
  }

  @Test
  public void testThrift() {
    testIsDefault(new Nested.Thrift());
    testNonDefault(Nested.thrift(nonDefault));
  }
}
