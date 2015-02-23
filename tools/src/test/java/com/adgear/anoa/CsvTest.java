package com.adgear.anoa;

import com.adgear.anoa.read.AnoaRead;
import com.adgear.anoa.write.AnoaWrite;
import com.adgear.avro.Simple;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Scanner;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class CsvTest {


  @Test
  public void test() throws Exception {
    BiFunction<JsonParser,Boolean,Simple> readBiFn = AnoaRead.biFn(Simple.class);
    BiConsumer<Simple, JsonGenerator> writeBiCo = AnoaWrite.biCo(Simple.class);

    InputStream inputStream = getClass().getResourceAsStream("/in/simple.csv");
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    JsonParser csvParser = AnoaCsv.from(inputStream,
                                        CsvSchema.builder().setUseHeader(true).build());
    JsonGenerator tsvGenerator = AnoaCsv.to(outputStream,
                                            CsvSchema.builder()
                                                .addColumn("foo").addColumn("bar").addColumn("baz")
                                                .setColumnSeparator('\t')
                                                .setUseHeader(true)
                                                .build());

    Stream.of(false, false)
        .sequential()
        .map(b -> readBiFn.apply(csvParser, b))
        .forEach(s -> writeBiCo.accept(s, tsvGenerator));
    tsvGenerator.flush();

    Assert.assertEquals(new Scanner(getClass().getResourceAsStream("/out/simple.csv"), "UTF-8")
                            .useDelimiter("\\A").next(),
                        outputStream.toString("UTF-8"));




  }

}
