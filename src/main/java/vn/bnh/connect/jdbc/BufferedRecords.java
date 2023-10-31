package vn.bnh.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.RecordValidator;
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BufferedRecords extends io.confluent.connect.jdbc.sink.BufferedRecords {
    private final JdbcAuditSinkConfig config;
    private static final String TOMBSTONE_IDENTIFIER = "_TOMBSTONE";
    private Schema deleteOpKeySchema = null;
    private Schema deleteOpValueSchema = null;
    private final boolean isDeleteAsUpdate;
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);
    private final TableId tableId;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;
    private List<SinkRecord> records = new ArrayList();
    private Schema keySchema;
    private Schema valueSchema;
    private RecordValidator recordValidator;
    private FieldsMetadata fieldsMetadata;
    private PreparedStatement updatePreparedStatement;
    private DatabaseDialect.StatementBinder updateStatementBinder;
    private PreparedStatement deleteAsUpdatePreparedStatement;
    private DatabaseDialect.StatementBinder deleteAsUpdateStatementBinder;

    public BufferedRecords(
            JdbcAuditSinkConfig config,
            TableId tableId,
            DatabaseDialect dbDialect,
            DbStructure dbStructure,
            Connection connection
    ) {
        super(config, tableId, dbDialect, dbStructure, connection);
        this.config = config;
        this.tableId = tableId;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
        this.isDeleteAsUpdate = config.deleteMode.equals(JdbcAuditSinkConfig.DeleteMode.UPDATE);
    }

    private void initDeleteAsUpdateSchema(SinkRecord sinkRecord) throws SQLException {
        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field(config.deleteAsUpdateKey, sinkRecord.keySchema().field(config.deleteAsUpdateKey).schema());
        this.deleteOpKeySchema = builder.build();
        SchemaBuilder valueBuilder = SchemaBuilder.struct();
        sinkRecord.valueSchema().fields().stream()
                .filter(f -> config.deleteAsUpdateValueFields.contains(f.name()))
                .forEach(f -> valueBuilder.field(f.name(), f.schema()));
        this.deleteOpValueSchema = valueBuilder.build();
        String deleteAsUpdateSql = this.getDeleteAsUpdateSql();
        this.deleteAsUpdatePreparedStatement = this.dbDialect.createPreparedStatement(this.connection, deleteAsUpdateSql);
        this.deleteAsUpdateStatementBinder = this.dbDialect.statementBinder(this.deleteAsUpdatePreparedStatement, this.config.pkMode, new SchemaPair(deleteOpKeySchema, deleteOpValueSchema), this.fieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), JdbcSinkConfig.InsertMode.UPDATE);

    }

    private SinkRecord convertDeleteAsUpdateRecord(SinkRecord sinkRecord) {
        String id = ((Struct) sinkRecord.key()).get(config.deleteAsUpdateKey).toString().replace(TOMBSTONE_IDENTIFIER, "");
        Struct newKey = new Struct(deleteOpKeySchema);
        newKey.put(config.deleteAsUpdateKey, id);
        Struct oldValue = (Struct) sinkRecord.value();
        Struct newValue = new Struct(deleteOpValueSchema);
        deleteOpValueSchema.fields().forEach(f -> {
            newValue.put(f, oldValue.get(f));
        });
        return new SinkRecord(sinkRecord.topic(), sinkRecord.kafkaPartition(), deleteOpKeySchema, newKey, deleteOpValueSchema, newValue, sinkRecord.kafkaOffset());

    }

    @Override
    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (SinkRecord sinkRecord : records) {
            if (((Struct) sinkRecord.key()).getString(config.deleteAsUpdateKey).endsWith(TOMBSTONE_IDENTIFIER) && isDeleteAsUpdate) {
                sinkRecord = convertDeleteAsUpdateRecord(sinkRecord);
                deleteAsUpdateStatementBinder.bindRecord(sinkRecord);
            } else {
                updateStatementBinder.bindRecord(sinkRecord);
            }
        }
        executeUpdates();

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }


    @Override
    public List<SinkRecord> add(SinkRecord sinkRecord) throws SQLException, TableAlterOrCreateException {
        if (isDeleteAsUpdate && deleteOpKeySchema == null) {
            initDeleteAsUpdateSchema(sinkRecord);
        }
        this.recordValidator.validate(sinkRecord);
        List<SinkRecord> flushed = new ArrayList();
        boolean schemaChanged = false;
        if (!Objects.equals(this.keySchema, sinkRecord.keySchema())) {
            this.keySchema = sinkRecord.keySchema();
            schemaChanged = true;
        }

        if (!Objects.equals(this.valueSchema, sinkRecord.valueSchema())) {
            this.valueSchema = sinkRecord.valueSchema();
            schemaChanged = true;
        }

        if (schemaChanged || this.updateStatementBinder == null) {
            flushed.addAll(this.flush());
            SchemaPair schemaPair = new SchemaPair(sinkRecord.keySchema(), sinkRecord.valueSchema());
            this.fieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), this.config.pkMode, this.config.pkFields, this.config.fieldsWhitelist, schemaPair);
            this.dbStructure.createOrAmendIfNecessary(this.config, this.connection, this.tableId, this.fieldsMetadata);
            String insertSql = this.getInsertSql();
            log.debug("{} sql: {} meta: {}", this.config.insertMode, insertSql, this.fieldsMetadata);
            this.close();
            this.updatePreparedStatement = this.dbDialect.createPreparedStatement(this.connection, insertSql);
            this.updateStatementBinder = this.dbDialect.statementBinder(this.updatePreparedStatement, this.config.pkMode, schemaPair, this.fieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), this.config.insertMode);
        }

        this.records.add(sinkRecord);
        if (this.records.size() >= this.config.batchSize) {
            flushed.addAll(this.flush());
        }

        return flushed;
    }

    String getDeleteAsUpdateSql() {
        List<ColumnId> columns = config.deleteAsUpdateValueFields.stream().map(f -> new ColumnId(this.tableId, f)).collect(Collectors.toList());
        ExpressionBuilder expressionBuilder = this.dbDialect.expressionBuilder();
        expressionBuilder.append("UPDATE ").append(this.tableId).append(" SET ")
                .append(new ColumnId(this.tableId, config.deleteAsUpdateColName)).append(" = ").appendStringQuoted(config.deleteAsUpdateColValue)
                .append(", ");
        expressionBuilder.appendList().delimitedBy(", ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(columns);
        expressionBuilder.append(" WHERE ").append(new ColumnId(this.tableId, config.deleteAsUpdateKey)).append(" = ?")
                .append(" AND ")
                .append(new ColumnId(this.tableId, config.deleteAsUpdateColName))
                .append(" != ")
                .appendStringQuoted(config.deleteAsUpdateColValue);
        return expressionBuilder.toString();
    }

    private String getInsertSql() throws SQLException {
        switch (this.config.insertMode) {
            case INSERT:
                return this.dbDialect.buildInsertStatement(this.tableId, this.asColumns(this.fieldsMetadata.keyFieldNames), this.asColumns(this.fieldsMetadata.nonKeyFieldNames), this.dbStructure.tableDefinition(this.connection, this.tableId));
            case UPSERT:
                if (this.fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format("Write to table '%s' in UPSERT mode requires key field names to be known, check the primary key configuration", this.tableId));
                } else {
                    try {
                        return this.dbDialect.buildUpsertQueryStatement(this.tableId, this.asColumns(this.fieldsMetadata.keyFieldNames), this.asColumns(this.fieldsMetadata.nonKeyFieldNames), this.dbStructure.tableDefinition(this.connection, this.tableId));
                    } catch (UnsupportedOperationException var2) {
                        throw new ConnectException(String.format("Write to table '%s' in UPSERT mode is not supported with the %s dialect.", this.tableId, this.dbDialect.name()));
                    }
                }
            case UPDATE:
                return this.dbDialect.buildUpdateStatement(this.tableId, this.asColumns(this.fieldsMetadata.keyFieldNames), this.asColumns(this.fieldsMetadata.nonKeyFieldNames), this.dbStructure.tableDefinition(this.connection, this.tableId));
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private Collection<ColumnId> asColumns(Collection<String> names) {
        return names.stream().map(name -> new ColumnId(this.tableId, name)).collect(Collectors.toList());
    }

    private void executeUpdates() throws SQLException {
        int[] batchStatus = updatePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException(
                        "Execution failed for part of the batch update", batchStatus);
            }
        }
    }

}
