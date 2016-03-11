package com.adgear.anoa;

import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.read.CsvStreams;
import com.adgear.anoa.read.JdbcStreams;
import com.adgear.anoa.read.ThriftDecoders;
import com.adgear.anoa.tools.runnable.DataTool;
import com.adgear.anoa.tools.runnable.Format;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.WriteConsumer;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcTest {

  static protected Connection openDBConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
  }

  @BeforeClass
  static public void initDB() throws Exception {
    Class.forName("org.h2.Driver");
    try (Connection conn = openDBConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE simple (foo INTEGER, bar VARBINARY(255), baz DOUBLE)");
      }
    }
  }

  @Before
  public void resetTable() throws Exception {
    try (Connection conn = openDBConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("TRUNCATE TABLE simple");
        stmt.executeUpdate("INSERT INTO simple VALUES ('101', 'FEEB', '789.1')");
        stmt.executeUpdate("INSERT INTO simple VALUES ('-102', 'F00B', '543.2')");
      }
    }
  }

  /*
  @Test
  public void testToThrift() throws Exception {
    AnoaHandler<Throwable> f = AnoaHandler.NO_OP_HANDLER;
    try (Connection connection = openDBConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM simple")) {
          List<Simple> simples = new JdbcStreams().resultSet(f, resultSet)
              .map(f.function(TreeNode::traverse))
              .map(f.function(ThriftDecoders.jackson(Simple.class, false)))
              .peek(System.out::println)
              .filter(Anoa::isPresent)
              .map(Anoa::get)
              .collect(Collectors.toList());

          Assert.assertEquals(2, simples.size());
          Assert.assertEquals(101, simples.get(0).getFoo());
          Assert.assertArrayEquals(Hex.decodeHex("FEEB".toCharArray()), simples.get(0).getBar());
          Assert.assertEquals(789.1, simples.get(0).getBaz(), 0.0000001);
          Assert.assertEquals(-102, simples.get(1).getFoo());
          Assert.assertArrayEquals(Hex.decodeHex("F00B".toCharArray()), simples.get(1).getBar());
          Assert.assertEquals(543.2, simples.get(1).getBaz(), 0.0000001);
        }
      }
    }
  }
*/
  @Test
  public void testDB() throws Exception {
    try (Connection connection = openDBConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM simple")) {
          new JdbcStreams().resultSet(resultSet).forEach(System.out::println);
        }
      }
    }
  }
  /*

  @Test
  public void testSchema() throws Exception {
    try (Connection connection = openDBConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM simple")) {
          Schema induced = JdbcStreams.induceSchema(resultSet.getMetaData());

          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try (WriteConsumer<GenericRecord> consumer = AvroConsumers.batch(induced, baos)) {
            new JdbcStreams().resultSet(resultSet)
                .map(ObjectNode::traverse)
                .map(AvroDecoders.jackson(induced, false))
                .forEach(consumer);
          }
          Assert.assertEquals(2, AvroStreams.batch(
              com.adgear.avro.Simple.class,
              new ByteArrayInputStream(baos.toByteArray())).count());
        }
      }
    }
  }


  @Test
  public void testDataToolCsv() throws Exception {
    final byte[] bytes;
    try (Connection connection = JdbcTest.openDBConnection()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new DataTool<>(null,
                     null,
                     null,
                     Format.CSV,
                     baos,
                     connection,
                     Collections.emptyList(),
                     "SELECT * FROM simple",
                     4096).run();
      bytes = baos.toByteArray();
    }
    System.out.println(new String(bytes));
    Assert.assertEquals(2, CsvStreams.csvWithHeader().from(bytes)
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(com.adgear.avro.Simple.class, false))
        .filter(x -> x != null)
        .count());
  }

  @Test
  public void testDataToolAvro() throws Exception {
    final byte[] bytes;
    try (Connection connection = JdbcTest.openDBConnection()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new DataTool<>(com.adgear.avro.Simple.getClassSchema(),
                     null,
                     null,
                     Format.AVRO_JSON,
                     baos,
                     connection,
                     Collections.emptyList(),
                     "SELECT * FROM simple",
                     4096).run();
      bytes = baos.toByteArray();
    }
    System.out.println(new String(bytes));
    Assert.assertEquals(2, AvroStreams.json(com.adgear.avro.Simple.class,
                                            new ByteArrayInputStream(bytes))
        .filter(x -> x != null)
        .count());
  }
 */
}
