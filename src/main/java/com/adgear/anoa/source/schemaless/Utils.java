package com.adgear.anoa.source.schemaless;

import org.apache.avro.Schema;
import org.apache.commons.codec.digest.DigestUtils;

class Utils {

  static Schema createSchema(String type, String toBeHashed) {
    return Schema.createRecord(type + "_" + DigestUtils.sha1Hex(toBeHashed),
                               "induced, type " + type,
                               "com.adgear.generated.avro.induced",
                               false);
  }
}
