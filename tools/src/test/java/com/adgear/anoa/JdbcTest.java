package com.adgear.anoa;

import com.adgear.anoa.read.AnoaRead;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.commons.codec.binary.Hex;
import org.jooq.lambda.Unchecked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import thrift.com.adgear.avro.Simple;

public class JdbcTest {

  static protected Connection openDBConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
  }

  @BeforeClass
  static public void initDB() throws Exception {
    Class.forName("org.h2.Driver");
    try (Connection conn = openDBConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE simple (foo INTEGER, bar VARBINARY(255), baz NUMERIC)");
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

  @Test
  public void testToThrift() throws Exception {

    ObjectMapper m = new ObjectMapper();
    try (Connection connection = openDBConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM simple")) {

          List<Simple> simples = AnoaJdbc.from(resultSet)
              .map(AnoaRecord::of)
              .map(AnoaFunction.of(TokenBuffer::asParser))
              .map(AnoaRead.anoaFn(Simple.class, false))
              .peek(System.out::println)
              .collect(AnoaCollector.toList())
              .streamPresent()
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

  @Test
  public void test() throws Exception {

    ObjectMapper m = new ObjectMapper();
    try (Connection connection = openDBConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SELECT * FROM simple")) {

          AnoaJdbc.from(resultSet)
              .map(TokenBuffer::asParser)
              .map(Unchecked.function(JsonParser::readValueAsTree))
              .forEach(System.out::println);
        }
      }
    }
  }
}