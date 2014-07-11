package org.adrianwalker.rdbmsmap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class RdbmsMap<K, V> implements Map<K, V> {

  // object types
  private static final String OBJECT_NULL_TYPE = "0";
  private static final String OBJECT_INTEGER_TYPE = "I";
  private static final String OBJECT_BOOLEAN_TYPE = "B";
  private static final String OBJECT_NUMERIC_TYPE = "N";
  private static final String OBJECT_TEXT_TYPE = "T";
  private static final String OBJECT_MAP_TYPE = "M";
  // entry types
  private static final String ENTRY_KEY_TYPE = "K";
  private static final String ENTRY_VALUE_TYPE = "V";
  // object types
  private static final String OBJECT_TABLE = "object_table";
  private static final String OBJECT_NULL_TABLE = "object_null";
  private static final String OBJECT_INTEGER_TABLE = "object_integer";
  private static final String OBJECT_BOOLEAN_TABLE = "object_boolean";
  private static final String OBJECT_NUMERIC_TABLE = "object_numeric";
  private static final String OBJECT_TEXT_TABLE = "object_text";
  private static final String OBJECT_MAP_TABLE = "object_map";
  // inserts
  private static final String INSERT_MAP = "insert into map(id) values(nextval('map_id_seq')) returning id";
  private static final String INSERT_ENTRY = "insert into entry(id, map_id, key_type, value_type) values(nextval('entry_id_seq'), ?, ?, ?) returning id";
  private static final String INSERT_OBJECT = "insert into " + OBJECT_TABLE + "(id, entry_id, map_id, type, value) values(nextval('" + OBJECT_TABLE + "_id_seq'), ?, ?, ?, ?) returning id";
  // counts
  private static final String COUNT_ENTRIES = "select count(*) from entry where map_id = ?";
  private static final String COUNT_OBJECT = "select count(*) from " + OBJECT_TABLE + " where map_id = ? and type = ? and value = ?";
  // selects
  private static final String SELECT_ENTRIES = "select id, key_type, value_type from entry where map_id = ?";
  private static final String SELECT_ENTRY_BY_OBJECT = "select value_type, id from entry where id = (select entry_id from " + OBJECT_TABLE + " where map_id = ? and type = ? and value = ?)";
  private static final String SELECT_OBJECT_BY_ENTRY = "select value from " + OBJECT_TABLE + " where map_id = ? and type = ? and entry_id = ?";
  private static final String SELECT_OBJECT = "select value from " + OBJECT_TABLE + " where map_id = ? and type = ?";
  // deletes
  private static final String DELETE_ENTRIES = "delete from entry where map_id = ?";
  private static final String DELETE_ENTRY_BY_OBJECT = "delete from entry where id = (select entry_id from " + OBJECT_TABLE + " where map_id = ? and type = ? and value = ?)";
  // specific cases for nulls
  private static final String INSERT_OBJECT_NULL = "insert into " + OBJECT_NULL_TABLE + "(id, entry_id, map_id, type) values(nextval('" + OBJECT_NULL_TABLE + "_id_seq'), ?, ?, ?) returning id";
  private static final String COUNT_OBJECT_NULL = "select count(*) from " + OBJECT_NULL_TABLE + " where map_id = ? and type = ?";
  private static final String SELECT_ENTRY_BY_OBJECT_NULL = "select value_type, id from entry where id = (select entry_id from " + OBJECT_NULL_TABLE + " where map_id = ? and type = ?)";
  private static final String SELECT_OBJECT_NULL_BY_ENTRY = "select null from " + OBJECT_NULL_TABLE + " where map_id = ? and type = ? and entry_id = ?";
  private static final String SELECT_OBJECT_NULL = "select null from " + OBJECT_NULL_TABLE + " where map_id = ? and type = ?";
  private static final String DELETE_ENTRY_BY_OBJECT_NULL = "delete from entry where id = (select entry_id from " + OBJECT_NULL_TABLE + " where map_id = ? and type = ?)";

  private final Connection connection;
  private final int mapId;

  public RdbmsMap(final Connection connection) {

    this.connection = connection;

    try {
      this.mapId = inserMap();

    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  private RdbmsMap(final Connection connection, final int mapId) {

    this.connection = connection;
    this.mapId = mapId;
  }

  public int getMapId() {
    return mapId;
  }

  @Override
  public void clear() {

    try {
      delete();
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  @Override
  public boolean containsKey(final Object key) {

    try {
      return countObjects(key, ENTRY_KEY_TYPE) > 0;
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  @Override
  public boolean containsValue(final Object value) {

    try {
      return countObjects(value, ENTRY_VALUE_TYPE) > 0;
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {

    try {
      return selectEntries();
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  @Override
  public V get(final Object key) {

    try {
      return select(key);
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  @Override
  public Set<K> keySet() {

    try {
      return (Set<K>) selectObjects(ENTRY_KEY_TYPE);
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  @Override
  public V put(final K key, final V value) {

    Object previousValue = remove(key);

    try {
      insert(key, value);
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }

    return (V) previousValue;
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {

    for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public V remove(final Object key) {

    try {
      if (countObjects(key, ENTRY_KEY_TYPE) > 0) {
        V value = select(key);
        delete(key, ENTRY_KEY_TYPE);

        return value;
      }
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }

    return null;
  }

  @Override
  public int size() {

    try {
      return countEntries();
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  @Override
  public boolean isEmpty() {

    return size() == 0;
  }

  @Override
  public Collection<V> values() {

    try {
      return selectObjects(ENTRY_VALUE_TYPE);
    } catch (final SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  private PreparedStatement prepareStatement(final String sql) throws SQLException {

    return connection.prepareStatement(sql);
  }

  private int inserMap() throws SQLException {

    PreparedStatement insertMap = prepareStatement(INSERT_MAP);

    ResultSet result = insertMap.executeQuery();
    if (!result.next()) {
      throw new RuntimeException();
    }

    return result.getInt(1);
  }

  private int countEntries() throws SQLException {

    PreparedStatement countEntries = prepareStatement(COUNT_ENTRIES);
    countEntries.setInt(1, mapId);

    ResultSet result = countEntries.executeQuery();
    if (!result.next()) {
      return 0;
    }

    return result.getInt(1);
  }

  private int countObjects(final Object obj, final String entryType) throws SQLException {

    String objectType = getType(obj);

    PreparedStatement countObject;

    if (objectType.equals(OBJECT_NULL_TYPE)) {
      countObject = prepareStatement(COUNT_OBJECT_NULL);
    } else if (objectType.equals(OBJECT_INTEGER_TYPE)) {
      countObject = prepareStatement(COUNT_OBJECT.replace(OBJECT_TABLE, OBJECT_INTEGER_TABLE));
      countObject.setInt(3, (Integer) obj);
    } else if (objectType.equals(OBJECT_BOOLEAN_TYPE)) {
      countObject = prepareStatement(COUNT_OBJECT.replace(OBJECT_TABLE, OBJECT_BOOLEAN_TABLE));
      countObject.setBoolean(3, (Boolean) obj);
    } else if (objectType.equals(OBJECT_NUMERIC_TYPE)) {
      countObject = prepareStatement(COUNT_OBJECT.replace(OBJECT_TABLE, OBJECT_NUMERIC_TABLE));
      countObject.setDouble(3, (Double) obj);
    } else if (objectType.equals(OBJECT_TEXT_TYPE)) {
      countObject = prepareStatement(COUNT_OBJECT.replace(OBJECT_TABLE, OBJECT_TEXT_TABLE));
      countObject.setString(3, (String) obj);
    } else if (objectType.equals(OBJECT_MAP_TYPE)) {
      countObject = prepareStatement(COUNT_OBJECT.replace(OBJECT_TABLE, OBJECT_MAP_TABLE));
      countObject.setString(3, (String) obj);
    } else {
      return 0;
    }

    countObject.setInt(1, mapId);
    countObject.setString(2, entryType);
    countObject.executeQuery();

    ResultSet result = countObject.executeQuery();
    if (!result.next()) {
      return 0;
    }

    return result.getInt(1);
  }

  private V select(final Object key) throws SQLException {

    String keyType = getType(key);

    PreparedStatement selectEntry;

    if (keyType.equals(OBJECT_NULL_TYPE)) {
      selectEntry = prepareStatement(SELECT_ENTRY_BY_OBJECT_NULL);
    } else if (keyType.equals(OBJECT_INTEGER_TYPE)) {
      selectEntry = prepareStatement(SELECT_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_INTEGER_TABLE));
      selectEntry.setInt(3, (Integer) key);
    } else if (keyType.equals(OBJECT_BOOLEAN_TYPE)) {
      selectEntry = prepareStatement(SELECT_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_BOOLEAN_TABLE));
      selectEntry.setBoolean(3, (Boolean) key);
    } else if (keyType.equals(OBJECT_NUMERIC_TYPE)) {
      selectEntry = prepareStatement(SELECT_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_NUMERIC_TABLE));
      selectEntry.setDouble(3, (Double) key);
    } else if (keyType.equals(OBJECT_TEXT_TYPE)) {
      selectEntry = prepareStatement(SELECT_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_TEXT_TABLE));
      selectEntry.setString(3, (String) key);
    } else if (keyType.equals(OBJECT_MAP_TYPE)) {
      selectEntry = prepareStatement(SELECT_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_MAP_TABLE));
      selectEntry.setString(3, (String) key);
    } else {
      return null;
    }

    selectEntry.setInt(1, mapId);
    selectEntry.setString(2, ENTRY_KEY_TYPE);

    ResultSet result = selectEntry.executeQuery();
    if (!result.next()) {
      return null;
    }

    String objectType = result.getString(1);
    int entryId = result.getInt(2);

    Object value = selectObject(objectType, ENTRY_VALUE_TYPE, entryId);

    return (V) value;
  }

  private Collection selectObjects(final String entryType, final String objectType) throws SQLException {

    Collection objs;

    if (entryType.equals(ENTRY_KEY_TYPE)) {
      objs = new HashSet();
    } else if (entryType.equals(ENTRY_VALUE_TYPE)) {
      objs = new ArrayList();
    } else {
      return null;
    }

    PreparedStatement selectObject;

    if (objectType.equals(OBJECT_NULL_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT_NULL);
    } else if (objectType.equals(OBJECT_INTEGER_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT.replace(OBJECT_TABLE, OBJECT_INTEGER_TABLE));
    } else if (objectType.equals(OBJECT_BOOLEAN_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT.replace(OBJECT_TABLE, OBJECT_BOOLEAN_TABLE));
    } else if (objectType.equals(OBJECT_NUMERIC_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT.replace(OBJECT_TABLE, OBJECT_NUMERIC_TABLE));
    } else if (objectType.equals(OBJECT_TEXT_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT.replace(OBJECT_TABLE, OBJECT_TEXT_TABLE));
    } else if (objectType.equals(OBJECT_MAP_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT.replace(OBJECT_TABLE, OBJECT_MAP_TABLE));
    } else {
      return objs;
    }

    selectObject.setInt(1, mapId);
    selectObject.setString(2, entryType);

    ResultSet result = selectObject.executeQuery();

    while (result.next()) {
      if (objectType.equals(OBJECT_NULL_TYPE)) {
        objs.add(null);
      } else if (objectType.equals(OBJECT_INTEGER_TYPE)) {
        objs.add(result.getInt(1));
      } else if (objectType.equals(OBJECT_BOOLEAN_TYPE)) {
        objs.add(result.getBoolean(1));
      } else if (objectType.equals(OBJECT_NUMERIC_TYPE)) {
        objs.add(result.getDouble(1));
      } else if (objectType.equals(OBJECT_TEXT_TYPE)) {
        objs.add(result.getString(1));
      } else if (objectType.equals(OBJECT_MAP_TYPE)) {
        objs.add(new RdbmsMap<K, V>(connection, result.getInt(1)));
      }
    }

    return objs;
  }

  private Collection selectObjects(final String entryType) throws SQLException {

    Collection objs = selectObjects(entryType, OBJECT_NULL_TYPE);
    objs.addAll(selectObjects(entryType, OBJECT_INTEGER_TYPE));
    objs.addAll(selectObjects(entryType, OBJECT_BOOLEAN_TYPE));
    objs.addAll(selectObjects(entryType, OBJECT_NUMERIC_TYPE));
    objs.addAll(selectObjects(entryType, OBJECT_TEXT_TYPE));
    objs.addAll(selectObjects(entryType, OBJECT_MAP_TYPE));

    return objs;
  }

  private String getType(final Object obj) {

    String keyType;

    if (null == obj) {
      keyType = OBJECT_NULL_TYPE;
    } else if (obj instanceof Integer) {
      keyType = OBJECT_INTEGER_TYPE;
    } else if (obj instanceof Boolean) {
      keyType = OBJECT_BOOLEAN_TYPE;
    } else if (obj instanceof Number) {
      keyType = OBJECT_NUMERIC_TYPE;
    } else if (obj instanceof String) {
      keyType = OBJECT_TEXT_TYPE;
    } else if (obj instanceof RdbmsMap) {
      keyType = OBJECT_MAP_TYPE;
    } else {
      return null;
    }

    return keyType;
  }

  private int insertEntry(final K key, final V value) throws SQLException {

    String keyType = getType(key);
    String valueType = getType(value);

    PreparedStatement insertEntry = prepareStatement(INSERT_ENTRY);
    insertEntry.setInt(1, mapId);
    insertEntry.setString(2, keyType);
    insertEntry.setString(3, valueType);

    ResultSet result = insertEntry.executeQuery();
    if (!result.next()) {
      return 0;
    }

    return result.getInt(1);
  }

  private void insertObject(final int entryId, final Object obj, final String entryType) throws SQLException {

    String objectType = getType(obj);

    PreparedStatement insertObject;

    if (objectType.equals(OBJECT_NULL_TYPE)) {
      insertObject = prepareStatement(INSERT_OBJECT_NULL);
    } else if (objectType.equals(OBJECT_INTEGER_TYPE)) {
      insertObject = prepareStatement(INSERT_OBJECT.replace(OBJECT_TABLE, OBJECT_INTEGER_TABLE));
      insertObject.setInt(4, (Integer) obj);
    } else if (objectType.equals(OBJECT_BOOLEAN_TYPE)) {
      insertObject = prepareStatement(INSERT_OBJECT.replace(OBJECT_TABLE, OBJECT_BOOLEAN_TABLE));
      insertObject.setBoolean(4, (Boolean) obj);
    } else if (objectType.equals(OBJECT_NUMERIC_TYPE)) {
      insertObject = prepareStatement(INSERT_OBJECT.replace(OBJECT_TABLE, OBJECT_NUMERIC_TABLE));
      insertObject.setDouble(4, (Double) obj);
    } else if (objectType.equals(OBJECT_TEXT_TYPE)) {
      insertObject = prepareStatement(INSERT_OBJECT.replace(OBJECT_TABLE, OBJECT_TEXT_TABLE));
      insertObject.setString(4, (String) obj);
    } else if (objectType.equals(OBJECT_MAP_TYPE)) {
      insertObject = prepareStatement(INSERT_OBJECT.replace(OBJECT_TABLE, OBJECT_MAP_TABLE));
      insertObject.setInt(4, ((RdbmsMap) obj).mapId);
    } else {
      return;
    }

    insertObject.setInt(1, entryId);
    insertObject.setInt(2, mapId);
    insertObject.setString(3, entryType);
    insertObject.executeQuery();
  }

  private void insert(final K key, final V value) throws SQLException {

    int entryId = insertEntry(key, value);
    insertObject(entryId, key, ENTRY_KEY_TYPE);
    insertObject(entryId, value, ENTRY_VALUE_TYPE);
  }

  private void delete(final Object obj, final String entryType) throws SQLException {

    String objectType = getType(obj);

    PreparedStatement deleteEntry;
    if (objectType.equals(OBJECT_NULL_TYPE)) {
      deleteEntry = prepareStatement(DELETE_ENTRY_BY_OBJECT_NULL);
    } else if (objectType.equals(OBJECT_INTEGER_TYPE)) {
      deleteEntry = prepareStatement(DELETE_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_INTEGER_TABLE));
      deleteEntry.setInt(3, (Integer) obj);
    } else if (objectType.equals(OBJECT_BOOLEAN_TYPE)) {
      deleteEntry = prepareStatement(DELETE_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_BOOLEAN_TABLE));
      deleteEntry.setBoolean(3, (Boolean) obj);
    } else if (objectType.equals(OBJECT_NUMERIC_TYPE)) {
      deleteEntry = prepareStatement(DELETE_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_NUMERIC_TABLE));
      deleteEntry.setDouble(3, (Double) obj);
    } else if (objectType.equals(OBJECT_TEXT_TYPE)) {
      deleteEntry = prepareStatement(DELETE_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_TEXT_TABLE));
      deleteEntry.setString(3, (String) obj);
    } else if (objectType.equals(OBJECT_MAP_TYPE)) {
      deleteEntry = prepareStatement(DELETE_ENTRY_BY_OBJECT.replace(OBJECT_TABLE, OBJECT_MAP_TABLE));
      deleteEntry.setInt(3, ((RdbmsMap) obj).mapId);
    } else {
      return;
    }

    deleteEntry.setInt(1, mapId);
    deleteEntry.setString(2, entryType);
    deleteEntry.executeUpdate();
  }

  private void delete() throws SQLException {

    PreparedStatement deleteEntries = prepareStatement(DELETE_ENTRIES);
    deleteEntries.setInt(1, mapId);
    deleteEntries.executeUpdate();
  }

  private Object selectObject(final String objectType, final String entryType, final int entryId) throws SQLException {

    PreparedStatement selectObject;
    if (objectType.equals(OBJECT_NULL_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT_NULL_BY_ENTRY);
    } else if (objectType.equals(OBJECT_INTEGER_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT_BY_ENTRY.replace(OBJECT_TABLE, OBJECT_INTEGER_TABLE));
    } else if (objectType.equals(OBJECT_BOOLEAN_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT_BY_ENTRY.replace(OBJECT_TABLE, OBJECT_BOOLEAN_TABLE));
    } else if (objectType.equals(OBJECT_NUMERIC_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT_BY_ENTRY.replace(OBJECT_TABLE, OBJECT_NUMERIC_TABLE));
    } else if (objectType.equals(OBJECT_TEXT_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT_BY_ENTRY.replace(OBJECT_TABLE, OBJECT_TEXT_TABLE));
    } else if (objectType.equals(OBJECT_MAP_TYPE)) {
      selectObject = prepareStatement(SELECT_OBJECT_BY_ENTRY.replace(OBJECT_TABLE, OBJECT_MAP_TABLE));
    } else {
      return null;
    }

    selectObject.setInt(1, mapId);
    selectObject.setString(2, entryType);
    selectObject.setInt(3, entryId);

    ResultSet result = selectObject.executeQuery();

    if (!result.next()) {
      return null;
    }

    Object value;
    if (objectType.equals(OBJECT_NULL_TYPE)) {
      value = null;
    } else if (objectType.equals(OBJECT_INTEGER_TYPE)) {
      value = result.getInt(1);
    } else if (objectType.equals(OBJECT_BOOLEAN_TYPE)) {
      value = result.getBoolean(1);
    } else if (objectType.equals(OBJECT_NUMERIC_TYPE)) {
      value = result.getDouble(1);
    } else if (objectType.equals(OBJECT_TEXT_TYPE)) {
      value = result.getString(1);
    } else if (objectType.equals(OBJECT_MAP_TYPE)) {
      value = new RdbmsMap<K, V>(connection, result.getInt(1));
    } else {
      return null;
    }

    return value;
  }

  private Set<Entry<K, V>> selectEntries() throws SQLException {

    Set<Entry<K, V>> entries = new HashSet<Entry<K, V>>();

    PreparedStatement selectEntries = prepareStatement(SELECT_ENTRIES);
    selectEntries.setInt(1, mapId);

    ResultSet result = selectEntries.executeQuery();

    while (result.next()) {
      int entryId = result.getInt(1);
      String keyType = result.getString(2);
      String valueType = result.getString(3);

      Entry<K, V> entry = new AbstractMap.SimpleEntry<K, V>(
              (K) selectObject(keyType, ENTRY_KEY_TYPE, entryId),
              (V) selectObject(valueType, ENTRY_VALUE_TYPE, entryId));

      entries.add(entry);
    }

    return entries;
  }
}
