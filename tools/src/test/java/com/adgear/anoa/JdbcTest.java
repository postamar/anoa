package com.adgear.anoa;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcTest {

  static protected Connection openDBConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
  }

  @BeforeClass
  static public void initDB() throws Exception {
    Class.forName("org.h2.Driver");
    try (Connection conn = openDBConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE simple (foo INTEGER, bar VARCHAR(255), baz NUMERIC)");
        stmt.executeUpdate("INSERT INTO simple VALUES ('101', 'brick', '789.1')");
        stmt.executeUpdate("INSERT INTO simple VALUES ('-102', 'mortar', '543.2')");
      }
    }
  }

  @Test
  public void testDB() throws SQLException {
    try (Connection conn = openDBConnection()) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM simple")) {
          Assert.assertEquals(3, rs.getMetaData().getColumnCount());
          Assert.assertTrue(rs.next());
          Assert.assertEquals(101, rs.getInt(1));
          Assert.assertEquals("brick", rs.getString(2));
          Assert.assertEquals(789.1, rs.getDouble(3), 0.0000001);
          Assert.assertTrue(rs.next());
          Assert.assertEquals(-102, rs.getInt(1));
          Assert.assertEquals("mortar", rs.getString(2));
          Assert.assertEquals(543.2, rs.getDouble(3), 0.0000001);
          Assert.assertFalse(rs.next());
        }
      }
    }
  }
}
