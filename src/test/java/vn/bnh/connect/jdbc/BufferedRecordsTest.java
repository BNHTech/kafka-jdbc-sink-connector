package vn.bnh.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;
import vn.bnh.connect.jdbc.sink.SqliteHelper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig.*;

class BufferedRecordsTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private Map<Object, Object> props;

    @BeforeEach
    public void setUp() throws IOException, SQLException {
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
        props.put(DELETE_AS_UPDATE_IDENTIFIER, "OP_TYPE=D");
        props.put("pk.mode", "record_value");
        props.put("pk.fields", List.of("RECID", "V_M", "V_S"));
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
        Map<String, Object> properties = new HashMap<>();
        properties.put("value.converter.schema.registry.url", "http://registry-1.bnh.vn:8081");
        properties.put("key.converter.schema.registry.url", "http://registry-1.bnh.vn:8081");
        properties.put("name", "AuditDeleteSink");
        properties.put("connector.class", "vn.bnh.connect.jdbc.JdbcAuditSinkConnector");
        properties.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        properties.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        properties.put("topics", "test_audit_delete");
        properties.put("connection.url", "jdbc:oracle:thin:@10.10.11.58:1521/tafjr22");
        properties.put("connection.user", "test_go");
        properties.put("connection.password", "oracle_4U");
        properties.put("dialect.name", "OracleDatabaseDialect");
        properties.put("insert.mode", "UPSERT");
        properties.put("table.name.format", "SINK_AUDIT_DELETE_OP");
        properties.put("pk.mode", "record_value");
        properties.put("pk.fields", List.of("RECID", "V_M", "V_S"));
        properties.put("auto.create", "false");
        properties.put("auto.evolve", "false");
        properties.put("delete.mode", "UPDATE");
        properties.put("delete.as.update.identifier", "OP_TYPE=D");
        properties.put("delete.as.update.key", "RECID");
        properties.put("delete.as.update.value.schema", List.of("TIME_UPDATE", "TABLE_NAME"));
        final JdbcAuditSinkConfig config = new JdbcAuditSinkConfig(properties);

    }
}