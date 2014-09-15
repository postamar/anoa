package com.adgear.anoa.tools.runnable;


import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class AvroConcatenateTest extends TestBase {

  @Test
  public void test() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(toJson(open()));
    baos.write(toJson(open("/multirecord_cleared.json")));
    baos.write(toJson(open("/multirecord_filtered.json")));
    baos.flush();
    byte[] expected = baos.toByteArray();

    baos.reset();
    new AvroConcatenate(Arrays.asList((InputStream) toAvroStream(open()),
                                      toAvroStream(open("/multirecord_cleared.json")),
                                      toAvroStream(open("/multirecord_filtered.json"))),
                        baos)
        .run();
    byte[] actual = toJson(fromAvro(baos.toByteArray()));

    Assert.assertArrayEquals(expected, actual);
  }

}
