package org.adrianwalker.rdbmsmap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public final class RdbmsMapTest {

  private static final String DRIVER = "org.postgresql.Driver";
  private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
  private static final String USERNAME = "postgres";
  private static final String PASSWORD = "postgres";

  private static Connection connection;

  public RdbmsMapTest() {
  }

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException {

    Class.forName(DRIVER);

    connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
  }

  @AfterClass
  public static void tearDownClass() throws SQLException {

    if (null != connection) {
      connection.close();
    }
  }

  @Test
  public void testGetMapId() {

    RdbmsMap map = new RdbmsMap(connection);
    int mapId = map.getMapId();
    assertTrue(mapId > 0);
  }

  @Test
  public void testClear() {

    RdbmsMap map = new RdbmsMap(connection);
    map.put(1, 2);
    map.put("1", "2");
    map.put(1.1, 2.2);
    map.put(true, false);
    map.put(null, null);
    assertEquals(5, map.size());
    map.clear();
    assertTrue(map.isEmpty());
  }

  @Test
  public void testContainsKey() {

    RdbmsMap map = new RdbmsMap(connection);
    map.put(1, 2);
    map.put("1", "2");
    map.put(1.1, 2.2);
    map.put(true, false);
    map.put(null, null);

    assertTrue(map.containsKey(1));
    assertTrue(map.containsKey("1"));
    assertTrue(map.containsKey(1.1));
    assertTrue(map.containsKey(true));
    assertTrue(map.containsKey(null));

    assertFalse(map.containsKey(2));
    assertFalse(map.containsKey("2"));
    assertFalse(map.containsKey(2.2));
    assertFalse(map.containsKey(false));
  }

  @Test
  public void testContainsValue() {

    RdbmsMap map = new RdbmsMap(connection);
    map.put(1, 2);
    map.put("1", "2");
    map.put(1.1, 2.2);
    map.put(true, false);
    map.put(null, null);

    assertTrue(map.containsValue(2));
    assertTrue(map.containsValue("2"));
    assertTrue(map.containsValue(2.2));
    assertTrue(map.containsValue(false));
    assertTrue(map.containsValue(null));

    assertFalse(map.containsValue(1));
    assertFalse(map.containsValue("1"));
    assertFalse(map.containsValue(1.1));
    assertFalse(map.containsValue(true));
  }

  @Test
  public void testEntrySet() {

    RdbmsMap map = new RdbmsMap(connection);
    map.put(1, 2);
    map.put("1", "2");
    map.put(1.1, 2.2);
    map.put(true, false);
    map.put(null, null);

    Set<Entry> set = map.entrySet();
    assertTrue(set.contains(new AbstractMap.SimpleEntry(1, 2)));
    assertTrue(set.contains(new AbstractMap.SimpleEntry("1", "2")));
    assertTrue(set.contains(new AbstractMap.SimpleEntry(1.1, 2.2)));
    assertTrue(set.contains(new AbstractMap.SimpleEntry(true, false)));
    assertTrue(set.contains(new AbstractMap.SimpleEntry(null, null)));
  }

  @Test
  public void testGet() {

    RdbmsMap map = new RdbmsMap(connection);
    map.put(1, 2);
    map.put("1", "2");
    map.put(1.1, 2.2);
    map.put(true, false);
    map.put(null, null);

    assertEquals(2, map.get(1));
    assertEquals("2", map.get("1"));
    assertEquals(2.2, map.get(1.1));
    assertEquals(false, map.get(true));
    assertEquals(null, map.get(null));

    assertEquals(null, map.get(2));
    assertEquals(null, map.get("2"));
    assertEquals(null, map.get(2.2));
    assertEquals(null, map.get(false));
  }

  @Test
  public void testKeySet() {

    RdbmsMap map = new RdbmsMap(connection);
    map.put(1, 2);
    map.put("1", "2");
    map.put(1.1, 2.2);
    map.put(true, false);
    map.put(null, null);

    Set keys = map.keySet();
    assertTrue(keys.contains(1));
    assertTrue(keys.contains("1"));
    assertTrue(keys.contains(1.1));
    assertTrue(keys.contains(true));
    assertTrue(keys.contains(null));

    assertFalse(keys.contains(2));
    assertFalse(keys.contains("2"));
    assertFalse(keys.contains(2.2));
    assertFalse(keys.contains(false));
  }

  @Test
  public void testPut() {

    RdbmsMap map = new RdbmsMap(connection);
    assertNull(map.put(1, 2));
    assertNull(map.put("1", "2"));
    assertNull(map.put(1.1, 2.2));
    assertNull(map.put(true, false));
    assertNull(map.put(null, null));

    assertEquals(2, map.put(1, 3));
    assertEquals("2", map.put("1", "3"));
    assertEquals(2.2, map.put(1.1, 3.3));
    assertEquals(false, map.put(true, true));
    assertEquals(null, map.put(null, null));
  }

  @Test
  public void testPutAll() {

    Map map1 = new HashMap();
    map1.put(1, 2);
    map1.put("1", "2");
    map1.put(1.1, 2.2);
    map1.put(true, false);
    map1.put(null, null);

    RdbmsMap map2 = new RdbmsMap(connection);
    map2.putAll(map1);

    assertEquals(5, map2.size());
  }

  @Test
  public void testRemove() {

    RdbmsMap map = new RdbmsMap(connection);
    assertNull(map.put(1, 2));
    assertNull(map.put("1", "2"));
    assertNull(map.put(1.1, 2.2));
    assertNull(map.put(true, false));
    assertNull(map.put(null, null));

    assertEquals(2, map.remove(1));
    assertEquals("2", map.remove("1"));
    assertEquals(2.2, map.remove(1.1));
    assertEquals(false, map.remove(true));
    assertEquals(null, map.remove(null));

    assertEquals(0, map.size());
  }

  @Test
  public void testSize() {

    RdbmsMap map = new RdbmsMap(connection);
    assertNull(map.put(1, 2));
    assertNull(map.put("1", "2"));
    assertNull(map.put(1.1, 2.2));
    assertNull(map.put(true, false));
    assertNull(map.put(null, null));

    assertEquals(5, map.size());
  }

  @Test
  public void testIsEmpty() {

    RdbmsMap map = new RdbmsMap(connection);

    assertTrue(map.isEmpty());

    assertNull(map.put(1, 2));
    assertNull(map.put("1", "2"));
    assertNull(map.put(1.1, 2.2));
    assertNull(map.put(true, false));
    assertNull(map.put(null, null));

    assertFalse(map.isEmpty());
  }

  @Test
  public void testValues() {

    RdbmsMap map = new RdbmsMap(connection);
    map.put(1, 2);
    map.put("1", "2");
    map.put(1.1, 2.2);
    map.put(true, false);
    map.put(null, null);

    Collection values = map.values();
    assertTrue(values.contains(2));
    assertTrue(values.contains("2"));
    assertTrue(values.contains(2.2));
    assertTrue(values.contains(false));
    assertTrue(values.contains(null));

    assertFalse(values.contains(1));
    assertFalse(values.contains("1"));
    assertFalse(values.contains(1.1));
    assertFalse(values.contains(true));
  }

  @Test
  public void testMapOfMaps() {

    RdbmsMap map1 = new RdbmsMap(connection);
    RdbmsMap map2 = new RdbmsMap(connection);

    map1.put(1, 2);
    map1.put("1", "2");
    map1.put(1.1, 2.2);
    map1.put(true, false);
    map1.put(null, null);
    map1.put("map2", map2);

    assertEquals(6, map1.size());

    map2.put(3, 4);
    map2.put("3", "4");
    map2.put(3.3, 4.4);
    map2.put(false, true);
    map2.put(null, null);

    assertEquals(5, map2.size());

    assertEquals(((RdbmsMap) map1.get("map2")).get(3), 4);
    assertEquals(((RdbmsMap) map1.get("map2")).get("3"), "4");
    assertEquals(((RdbmsMap) map1.get("map2")).get(3.3), 4.4);
    assertEquals(((RdbmsMap) map1.get("map2")).get(false), true);
    assertEquals(((RdbmsMap) map1.get("map2")).get(null), null);
  }
}
