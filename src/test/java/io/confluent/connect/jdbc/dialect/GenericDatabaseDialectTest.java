/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import vn.bnh.connect.jdbc.sink.SqliteHelper;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class GenericDatabaseDialectTest extends BaseDialectTest<GenericDatabaseDialect> {

    public static final Set<String> TABLE_TYPES = Collections.singleton("TABLE");
    public static final Set<String> VIEW_TYPES = Collections.singleton("VIEW");
    public static final Set<String> ALL_TABLE_TYPES =
            Set.of("TABLE", "VIEW");

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());
    private Map<String, String> connProps;
    private JdbcSourceConnectorConfig config;
    private JdbcSinkConfig sinkConfig;
    private EmbeddedDerby db;
    private ConnectionProvider connectionProvider;
    private Connection conn;

    @Before
    public void setup() throws Exception {
        db = new EmbeddedDerby();
        connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        newDialectFor(null, null);
        super.setup();
        connectionProvider = dialect;
        conn = connectionProvider.getConnection();
    }

    @After
    public void cleanup() throws Exception {
        connectionProvider.close();
        conn.close();
        db.close();
        db.dropDatabase();
    }

    protected GenericDatabaseDialect createDialect() {
        return new GenericDatabaseDialect(sourceConfigWithUrl(db.getUrl()));
    }

    protected GenericDatabaseDialect createDialect(AbstractConfig config) {
        return new GenericDatabaseDialect(config);
    }

    protected GenericDatabaseDialect newDialectFor(
            Set<String> tableTypes,
            String schemaPattern
    ) {
        if (schemaPattern != null) {
            connProps.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, schemaPattern);
        } else {
            connProps.remove(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG);
        }
        if (tableTypes != null) {
            connProps.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, StringUtils.join(tableTypes, ","));
        } else {
            connProps.remove(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG);
        }
        config = new JdbcSourceConnectorConfig(connProps);
        dialect = createDialect(config);
        return dialect;
    }

    protected GenericDatabaseDialect newSinkDialectFor(Set<String> tableTypes) {
        assertNotNull(tableTypes);
        assertFalse(tableTypes.isEmpty());
        connProps.put(JdbcSinkConfig.TABLE_TYPES_CONFIG, StringUtils.join(tableTypes, ","));
        sinkConfig = new JdbcSinkConfig(connProps);
        dialect = createDialect(sinkConfig);
        assertTrue(dialect.tableTypes.containsAll(tableTypes));
        return dialect;
    }

    @Test
    public void testDialectForSinkConnectorWithTablesOnly() throws Exception {
        newSinkDialectFor(TABLE_TYPES);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testDialectForSinkConnectorWithViewsOnly() throws Exception {
        newSinkDialectFor(VIEW_TYPES);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testDialectForSinkConnectorWithTablesAndViews() throws Exception {
        newSinkDialectFor(ALL_TABLE_TYPES);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testGetTablesEmpty() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        assertEquals(Collections.emptyList(), dialect.tableIds(conn));
    }

    @Test
    public void testGetTablesSingle() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        db.createTable("test", "id", "INT");
        TableId test = new TableId(null, "APP", "test");
        assertEquals(Arrays.asList(test), dialect.tableIds(conn));
    }

    @Test
    public void testFindTablesWithKnownTableType() throws Exception {
        Set<String> types = Collections.singleton("TABLE");
        newDialectFor(types, null);
        db.createTable("test", "id", "INT");
        TableId test = new TableId(null, "APP", "test");
        assertEquals(Arrays.asList(test), dialect.tableIds(conn));
    }

    @Test
    public void testNotFindTablesWithUnknownTableType() throws Exception {
        newDialectFor(Collections.singleton("view"), null);
        db.createTable("test", "id", "INT");
        assertEquals(Arrays.asList(), dialect.tableIds(conn));
    }

    @Test
    public void testGetTablesMany() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        db.createTable("test", "id", "INT");
        db.createTable("foo", "id", "INT", "bar", "VARCHAR(20)");
        db.createTable("zab", "id", "INT");
        db.createView("fooview", "foo", "id", "bar");
        TableId test = new TableId(null, "APP", "test");
        TableId foo = new TableId(null, "APP", "foo");
        TableId zab = new TableId(null, "APP", "zab");
        TableId vfoo = new TableId(null, "APP", "fooview");

        // Does not contain views
        assertEquals(
                new HashSet<>(Arrays.asList(test, foo, zab)), new HashSet<>(dialect.tableIds(conn)));

        newDialectFor(ALL_TABLE_TYPES, null);
        assertEquals(
                new HashSet<>(Arrays.asList(test, foo, zab, vfoo)), new HashSet<>(dialect.tableIds(conn)));
    }

    @Test
    public void testGetTablesNarrowedToSchemas() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        db.createTable("some_table", "id", "INT");

        db.execute("CREATE SCHEMA PUBLIC_SCHEMA");
        db.execute("SET SCHEMA PUBLIC_SCHEMA");
        db.createTable("public_table", "id", "INT");
        db.createView("public_view", "public_table", "id");

        db.execute("CREATE SCHEMA PRIVATE_SCHEMA");
        db.execute("SET SCHEMA PRIVATE_SCHEMA");
        db.createTable("private_table", "id", "INT");
        db.createTable("another_private_table", "id", "INT");

        TableId someTable = new TableId(null, "APP", "some_table");
        TableId publicTable = new TableId(null, "PUBLIC_SCHEMA", "public_table");
        TableId privateTable = new TableId(null, "PRIVATE_SCHEMA", "private_table");
        TableId anotherPrivateTable = new TableId(null, "PRIVATE_SCHEMA", "another_private_table");
        TableId publicView = new TableId(null, "PUBLIC_SCHEMA", "public_view");

        assertTableNames(TABLE_TYPES, "PUBLIC_SCHEMA", publicTable);
        assertTableNames(TABLE_TYPES, "PRIVATE_SCHEMA", privateTable, anotherPrivateTable);
        assertTableNames(TABLE_TYPES, null, someTable, publicTable, privateTable, anotherPrivateTable);
        Set<String> types = Collections.singleton("TABLE");
        assertTableNames(types, "PUBLIC_SCHEMA", publicTable);
        assertTableNames(types, "PRIVATE_SCHEMA", privateTable, anotherPrivateTable);
        assertTableNames(types, null, someTable, publicTable, privateTable, anotherPrivateTable);

        TableDefinition defn = dialect.describeTable(db.getConnection(), someTable);
        assertEquals(someTable, defn.id());
        assertEquals(TableType.TABLE, defn.type());
        assertEquals("INTEGER", defn.definitionForColumn("id").typeName());

        defn = dialect.describeTable(db.getConnection(), publicTable);
        assertEquals(publicTable, defn.id());
        assertEquals(TableType.TABLE, defn.type());
        assertEquals("INTEGER", defn.definitionForColumn("id").typeName());

        defn = dialect.describeTable(db.getConnection(), privateTable);
        assertEquals(privateTable, defn.id());
        assertEquals(TableType.TABLE, defn.type());
        assertEquals("INTEGER", defn.definitionForColumn("id").typeName());

        defn = dialect.describeTable(db.getConnection(), anotherPrivateTable);
        assertEquals(anotherPrivateTable, defn.id());
        assertEquals(TableType.TABLE, defn.type());
        assertEquals("INTEGER", defn.definitionForColumn("id").typeName());

        // Create a new dialect that uses views, and describe the view
        newDialectFor(ALL_TABLE_TYPES, null);
        defn = dialect.describeTable(db.getConnection(), publicView);
        assertEquals(publicView, defn.id());
        assertEquals(TableType.VIEW, defn.type());
        assertEquals("INTEGER", defn.definitionForColumn("id").typeName());
    }

    @Test
    public void testBuildCreateTableStatement() {
        newDialectFor(TABLE_TYPES, null);
        assertEquals(
                "INSERT INTO \"myTable\"(\"id1\",\"id2\",\"columnA\",\"columnB\",\"columnC\",\"columnD\") VALUES(?,?,?,?,?,?)",
                dialect.buildInsertStatement(tableId, pkColumns, columnsAtoD));
    }

    @Test
    public void testBuildDeleteStatement() {
        newDialectFor(TABLE_TYPES, null);
        assertEquals(
                "DELETE FROM \"myTable\" WHERE \"id1\" = ? AND \"id2\" = ?",
                dialect.buildDeleteStatement(tableId, pkColumns));
    }

    @Test
    public void testBuildDeleteAsUpdateStatement() {
        newDialectFor(TABLE_TYPES, null);
        List<ColumnId> columns = List.of("TABLE_NAME", "UPDATE_TIME").stream().map(f -> new ColumnId(this.tableId, f)).collect(Collectors.toList());
        ExpressionBuilder expressionBuilder = dialect.expressionBuilder();
        expressionBuilder.append("UPDATE ").append(this.tableId).append(" SET ");
        expressionBuilder.appendList().delimitedBy(", ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(columns);
        expressionBuilder.append(" WHERE ").append(new ColumnId(this.tableId, "RECID")).append(" = ?")
                .append(" AND ")
                .append(new ColumnId(this.tableId, "OP_TYPE"))
                .append(" != ")
                .appendStringQuoted("D");

        assertEquals(
                "UPDATE \"myTable\" SET \"TABLE_NAME\" = ?, \"UPDATE_TIME\" = ? WHERE \"myTable\".\"RECID\" = ? AND \"myTable\".\"OP_TYPE\" != 'D'",
                expressionBuilder.toString());
    }

    protected void assertTableNames(
            Set<String> tableTypes,
            String schemaPattern,
            TableId... expectedTableIds
    ) throws Exception {
        newDialectFor(tableTypes, schemaPattern);
        Collection<TableId> ids = dialect.tableIds(db.getConnection());
        for (TableId expectedTableId : expectedTableIds) {
            assertTrue(ids.contains(expectedTableId));
        }
        assertEquals(expectedTableIds.length, ids.size());
    }


    @Test(expected = ConnectException.class)
    public void shouldBuildCreateQueryStatement() {
        dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    }

    @Test(expected = ConnectException.class)
    public void shouldBuildAlterTableStatement() {
        dialect.buildAlterTable(tableId, sinkRecordFields);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldBuildUpsertStatement() {
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    }


    @Test
    public void shouldAddExtraProperties() {
        // When adding extra properties with the 'connection.' prefix
        connProps.put("connection.oracle.something", "somethingValue");
        connProps.put("connection.oracle.else", "elseValue");
        connProps.put("connection.foo", "bar");
        // and some other extra properties not prefixed with 'connection.'
        connProps.put("foo2", "bar2");
        config = new JdbcSourceConnectorConfig(connProps);
        dialect = createDialect(config);
        // and the dialect computes the connection properties
        Properties props = new Properties();
        Properties modified = dialect.addConnectionProperties(props);
        // then the resulting properties
        // should be the same properties object as what was passed in
        assertSame(props, modified);
        // should include props that began with 'connection.' but without prefix
        assertEquals("somethingValue", modified.get("oracle.something"));
        assertEquals("elseValue", modified.get("oracle.else"));
        assertEquals("bar", modified.get("foo"));
        // should not include any 'connection.*' properties defined by the connector
        assertFalse(modified.containsKey("url"));
        assertFalse(modified.containsKey("password"));
        assertFalse(modified.containsKey("connection.url"));
        assertFalse(modified.containsKey("connection.password"));
        // should not include the prefixed props
        assertFalse(modified.containsKey("connection.oracle.something"));
        assertFalse(modified.containsKey("connection.oracle.else"));
        assertFalse(modified.containsKey("connection.foo"));
        // should not include props not prefixed with 'connection.'
        assertFalse(modified.containsKey("foo2"));
        assertFalse(modified.containsKey("connection.foo2"));
    }
}