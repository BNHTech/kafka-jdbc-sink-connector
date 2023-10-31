package vn.bnh.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;
import vn.bnh.connect.jdbc.sink.SqliteHelper;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig.*;

class BufferedRecordsTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private Map<Object, Object> props;

    @BeforeEach
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
        props = new HashMap<>();
        props.put("name", "my-connector");
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put(
                "batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
        // We don't manually create the table, so let the connector do it
        props.put("auto.create", true);
        // We use various schemas, so let the connector add missing columns
        props.put("auto.evolve", true);
        props.put(DELETE_MODE, "UPDATE");
        props.put(DELETE_AS_UPDATE_KEY, "RECID");
        props.put(DELETE_AS_UPDATE_SET_VALUE, "OP_TYPE=D");
        props.put(DELETE_AS_UPDATE_VALUE_SCHEMA, List.of("UPDATE_TIME", "TABLE_NAME"));
    }

    @AfterEach
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }

    @Test
    void correctBatching() throws SQLException {
        final JdbcAuditSinkConfig config = new JdbcAuditSinkConfig(props);
        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer =
                new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema schemaA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        final Struct valueA = new Struct(schemaA).put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);

        final Schema schemaB =
                SchemaBuilder.struct()
                        .field("name", Schema.STRING_SCHEMA)
                        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                        .build();
        final Struct valueB = new Struct(schemaB).put("name", "cuba").put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, null, null, schemaB, valueB, 1);

        // test records are batched correctly based on schema equality as records are added
        //   (schemaA,schemaA,schemaA,schemaB,schemaA) ->
        // ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        assertEquals(Arrays.asList(recordA, recordA, recordA), buffer.add(recordB));

        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    void testDeleteAsUpdate() throws SQLException {
        String expected = "UPDATE \"myTable\" SET \"myTable\".\"OP_TYPE\" = 'D', \"TABLE_NAME\" = ?, \"UPDATE_TIME\" = ? WHERE \"myTable\".\"RECID\" = ? AND \"myTable\".\"OP_TYPE\" != 'D'";
        final JdbcAuditSinkConfig config = new JdbcAuditSinkConfig(props);
        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "myTable");
        final BufferedRecords buffer =
                new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        String sql = buffer.getDeleteAsUpdateSql();
        Assertions.assertEquals(expected, sql);
        Schema keySchema = SchemaBuilder.struct().field("RECID", Schema.STRING_SCHEMA).build();
        Schema valueSchema = SchemaBuilder.struct().field("TABLE_NAME", Schema.STRING_SCHEMA).field("UPDATE_TIME", Schema.STRING_SCHEMA).build();
        Struct key = new Struct(keySchema);
        key.put("RECID", "unittest");
        Struct value = new Struct(valueSchema);
        value.put("TABLE_NAME", "table-name");
        value.put("UPDATE_TIME", "2023-11-01");
        SinkRecord r = new SinkRecord("myTable", 0, keySchema, key, valueSchema, value, 0L);

        FieldsMetadata metadata = FieldsMetadata.extract(tableId.tableName(), PrimaryKeyMode.RECORD_KEY, config.pkFields, config.fieldsWhitelist, new SchemaPair(keySchema, valueSchema));
        System.out.println(sql);
        PreparedStatement deleteAsUpdatePreparedStatement = dbDialect.createPreparedStatement(sqliteHelper.connection, sql);
        DatabaseDialect.StatementBinder deleteAsUpdateStatementBinder = dbDialect.statementBinder(deleteAsUpdatePreparedStatement, PrimaryKeyMode.RECORD_KEY, new SchemaPair(keySchema, valueSchema), metadata, dbStructure.tableDefinition(sqliteHelper.connection, tableId), JdbcSinkConfig.InsertMode.UPDATE);
        deleteAsUpdateStatementBinder.bindRecord(r);
        System.out.println(deleteAsUpdateStatementBinder);

    }
}