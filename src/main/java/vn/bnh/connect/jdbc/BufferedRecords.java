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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class BufferedRecords extends io.confluent.connect.jdbc.sink.BufferedRecords {
    private final JdbcAuditSinkConfig config;
    private Schema deleteOpValueSchema = null;
    private final boolean isDeleteAsUpdate;
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);
    private final TableId tableId;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    final Connection connection;
    private List<SinkRecord> records = new ArrayList();
    private Schema keySchema;
    private Schema valueSchema;
    FieldsMetadata fieldsMetadata;
    private PreparedStatement updatePreparedStatement;
    private DatabaseDialect.StatementBinder updateStatementBinder;
    private PreparedStatement deleteAsUpdatePreparedStatement;
    private DatabaseDialect.StatementBinder deleteAsUpdateStatementBinder;
    private RecordValidator recordValidator;

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
        log.debug("DELETE as UPDATE mode: {}", this.isDeleteAsUpdate);
        this.recordValidator = RecordValidator.create(config);

    }

    private void initDeleteAsUpdateSchema(SinkRecord sinkRecord) throws SQLException {
        log.debug("Begin constructing DELETE as UPDATE value schema");
        SchemaBuilder valueBuilder = SchemaBuilder.struct();
        config.deleteAsUpdateValueFields.forEach(field -> {
            Field f = sinkRecord.valueSchema().field(field);
            log.debug("{} - {} - {}", f.name(), f.schema(), config.deleteAsUpdateValueFields);
            valueBuilder.field(field, f.schema());
        });
        this.deleteOpValueSchema = valueBuilder.build();
        log.debug("DELETE as UPDATE value schema: {}", this.deleteOpValueSchema);
        log.debug("End constructing DELETE as UPDATE value schema");

        log.debug("Begin constructing DELETE as UPDATE SQL statement");
        String deleteAsUpdateSql = this.getDeleteAsUpdateSql();
        log.info("DELETE as UPDATE SQL statement: {}", deleteAsUpdateSql);
        log.debug("End constructing DELETE as UPDATE SQL statement");

        log.debug("Begin constructing DELETE as UPDATE prepared statement");
        this.deleteAsUpdatePreparedStatement = this.dbDialect.createPreparedStatement(this.connection, deleteAsUpdateSql);
        log.debug("End constructing DELETE as UPDATE prepared statement");

        log.debug("Begin constructing DELETE as UPDATE statement binder");
        SchemaPair schemaPair = new SchemaPair(sinkRecord.keySchema(), this.deleteOpValueSchema);
        FieldsMetadata deleteAsUpdateFieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), this.config.pkMode, Collections.singletonList(this.config.deleteAsUpdateKey), this.config.fieldsWhitelist, schemaPair);

        this.deleteAsUpdateStatementBinder = this.dbDialect.statementBinder(this.deleteAsUpdatePreparedStatement, this.config.pkMode, schemaPair, deleteAsUpdateFieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), JdbcSinkConfig.InsertMode.UPDATE);
        log.debug("End constructing DELETE as UPDATE statement binder");

    }

    private SinkRecord convertDeleteAsUpdateRecord(SinkRecord sinkRecord) {
        log.debug("Begin convert DELETE as UPDATE");
        Struct oldValue = (Struct) sinkRecord.value();
        Struct newValue = new Struct(deleteOpValueSchema);
        deleteOpValueSchema.fields().forEach(f -> {
            log.debug("{}", f);
            newValue.put(f, oldValue.get(f.name()));
        });
        log.debug("UPDATE as DELETE record value: {}", newValue);
        return new SinkRecord(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.keySchema(), sinkRecord.key(), deleteOpValueSchema, newValue, sinkRecord.kafkaOffset());

    }

    @Override
    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (SinkRecord sinkRecord : records) {
            if (isDeleteAsUpdate && ((Struct) sinkRecord.value()).getString(config.deleteAsUpdateColName).equals(config.deleteAsUpdateColValue)) {
                sinkRecord = convertDeleteAsUpdateRecord(sinkRecord);
                deleteAsUpdateStatementBinder.bindRecord(sinkRecord);
            } else {
                updateStatementBinder.bindRecord(sinkRecord);
            }
        }
        executeDeletes();
        executeUpdates();

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }


    @Override
    public List<SinkRecord> add(SinkRecord sinkRecord) throws SQLException, TableAlterOrCreateException {
        this.recordValidator.validate(sinkRecord);
        if (isDeleteAsUpdate && deleteOpValueSchema == null) {
            log.debug("Begin Initialize DELETE as UPDATE configurations");
            initDeleteAsUpdateSchema(sinkRecord);
            log.debug("End Initialize DELETE as UPDATE configurations");
        }
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
        List<ColumnId> columns = config.deleteAsUpdateValueFields.stream()
                .filter(f -> !f.equals(config.deleteAsUpdateKey))
                .map(f -> new ColumnId(this.tableId, f))
                .collect(Collectors.toList());
        ExpressionBuilder expressionBuilder = this.dbDialect.expressionBuilder();
        expressionBuilder.append("UPDATE ").append(this.tableId).append(" SET ")
                .append(new ColumnId(this.tableId, config.deleteAsUpdateColName)).append(" = ")
                .appendStringQuoted(config.deleteAsUpdateColValue)
                .append(", ");
        expressionBuilder.appendList().delimitedBy(", ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(columns);
        expressionBuilder.append(" WHERE ");
        expressionBuilder.append(new ColumnId(this.tableId, config.deleteAsUpdateKey))
                .append(" = ?");
        expressionBuilder.append(" AND ")
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

    private void executeDeletes() throws SQLException {
        int[] batchStatus = deleteAsUpdatePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException(
                        "Execution failed for part of the batch update", batchStatus);
            }
        }
    }

}
