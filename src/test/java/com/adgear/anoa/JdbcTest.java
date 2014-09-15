package com.adgear.anoa;

import com.google.common.collect.Lists;

import com.adgear.anoa.codec.Codec;
import com.adgear.anoa.codec.avro.StringListToAvro;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.source.schemaless.JdbcSource;
import com.adgear.generated.avro.flat.Simple;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test parsing records from JDBC result sets
 */
public class JdbcTest {

  @BeforeClass
  static public void init() throws Exception {
    Class.forName("org.h2.Driver");
  }

  public ResultSet createResultSet(String query) throws Exception {
    return DriverManager
        .getConnection("jdbc:h2:file:target/db/testdb", "usertest", "pwtest")
        .createStatement()
        .executeQuery(query);
  }

  @Test
  public void testTestDatabase() throws Exception {
    createResultSet("SELECT * FROM simple");
  }

  @Test
  public void testSimpleSpecific() throws Exception {
    Provider<List<String>>
        flat = new JdbcSource(createResultSet("SELECT foo, bar, baz FROM simple"));
    Codec<List<String>, Simple> toAvro = new StringListToAvro<>(flat, Simple.class);
    List<Simple> list = Lists.newArrayList(toAvro.iterator());
    assertEquals(2, list.size());
    Simple r1 = list.get(0);
    Simple r2 = list.get(1);

    assertEquals(101L, r1.getFoo().longValue());
    assertEquals("brick", r1.getBar().toString());
    assertEquals(789.1, r1.getBaz(), 0.0);

    assertEquals(-102L, r2.getFoo().longValue());
    assertEquals("mortar", r2.getBar().toString());
    assertEquals(543.2, r2.getBaz(), 0.0);
  }
}
