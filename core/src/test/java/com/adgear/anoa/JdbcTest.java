package com.adgear.anoa;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
          assertEquals(3, rs.getMetaData().getColumnCount());
          assertTrue(rs.next());
          assertEquals(101, rs.getInt(1));
          assertEquals("brick", rs.getString(2));
          assertEquals(789.1, rs.getDouble(3), 0.0000001);
          assertTrue(rs.next());
          assertEquals(-102, rs.getInt(1));
          assertEquals("mortar", rs.getString(2));
          assertEquals(543.2, rs.getDouble(3), 0.0000001);
          assertFalse(rs.next());
        }
      }
    }
  }
}
