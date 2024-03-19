package vn.bnh.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.RecordValidator;
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.bnh.connect.jdbc.query.DeleteBuilder;
import vn.bnh.connect.jdbc.query.HistBuilder;
import vn.bnh.connect.jdbc.query.UpsertBuilder;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class BufferedRecords extends io.confluent.connect.jdbc.sink.BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);
    final Connection connection;
    private final JdbcAuditSinkConfig config;
    private final boolean isDeleteAsUpdate;
    private final TableId tableId;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    FieldsMetadata fieldsMetadata;
    private List<SinkRecord> records = new ArrayList<>();
    private Schema keySchema;
    private Schema valueSchema;
    private PreparedStatement updatePreparedStatement;
    private DatabaseDialect.StatementBinder updateStatementBinder;
    private Schema deleteOpValueSchema = null;
    private PreparedStatement[] deleteAsUpdatePreparedStatements;
    private DatabaseDialect.StatementBinder[] deleteAsUpdateStatementBinders;
    private final RecordValidator recordValidator;
    private static final JdbcSinkConfig.PrimaryKeyMode PK_MODE = JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE;
    private final boolean shouldProcessHistRecord;
    private Schema histTableValueSchema = null;
    private PreparedStatement histTablePreparedStatement;
    private DatabaseDialect.StatementBinder histTableStatementBinder;

    private final DeleteBuilder dBuilder;
    private final UpsertBuilder uBuilder;
    private final HistBuilder hBuilder;

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
        log.info("DELETE as UPDATE mode: {}", this.isDeleteAsUpdate);
        this.shouldProcessHistRecord = config.histRecordKey != null;
        log.info("Should process HIST table's records: {}", this.shouldProcessHistRecord);
        this.recordValidator = RecordValidator.create(config);
        this.dBuilder = new DeleteBuilder(this.config, this.tableId, this.dbDialect);
        this.uBuilder = new UpsertBuilder(this.config, this.tableId, this.dbDialect);
        this.hBuilder = new HistBuilder(this.config, this.tableId, this.dbDialect);
    }

    /**
     * initialize multiple DELETE AS UPDATE statements based on <b>delete.as.update.identifier</b> and <b>delete.as.update.value.schema</b>
     * <p>
     * example : delete.as.update.identifier: "F1=D, F2=D"
     * delete.as.update.value.schema: "X, Y, Z"
     * <p>
     * SQLs: UPDATE TABLE ... SET X,Y,Z, F1 WHERE F1 != D; UPDATE TABLE ... SET X,Y,Z,F2 WHERE F2 != D
     *
     * @param sinkRecord
     * @throws SQLException
     */
    private void initDeleteAsUpdateSchema(SinkRecord sinkRecord) throws SQLException {
        SchemaBuilder valueBuilder = SchemaBuilder.struct();
        config.getDeleteAsUpdateFields().forEach(field -> {
            Field f = sinkRecord.valueSchema().field(field);
            if (f == null) {
                log.error("Field name '{}' does not exists in source schema {} ", field, sinkRecord.valueSchema());
                throw new RuntimeException(String.format("Field '%s' does not exists in message schema", field));
            }
            valueBuilder.field(field, f.schema());
        });
        this.deleteOpValueSchema = valueBuilder.build();
        String[] sqlStatements = dBuilder.buildStatements();
//        log.trace("DELETE AS UPDATE SQL: {}", deleteAsUpdateSql);
        this.deleteAsUpdatePreparedStatements = new PreparedStatement[sqlStatements.length];
        this.deleteAsUpdateStatementBinders = new DatabaseDialect.StatementBinder[sqlStatements.length];
        SchemaPair schemaPair = new SchemaPair(sinkRecord.keySchema(), this.deleteOpValueSchema);
        FieldsMetadata deleteAsUpdateFieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), PK_MODE, new ArrayList<>(this.config.getDeleteAsUpdateKey()), this.config.fieldsWhitelist, schemaPair);
        for (int i = 0; i < sqlStatements.length; i++) {
            this.deleteAsUpdatePreparedStatements[i] = this.dbDialect.createPreparedStatement(this.connection, sqlStatements[i]);
            this.deleteAsUpdateStatementBinders[i] = this.dbDialect.statementBinder(this.deleteAsUpdatePreparedStatements[i], PK_MODE, schemaPair, deleteAsUpdateFieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), JdbcSinkConfig.InsertMode.UPDATE);
        }

    }

    private void initHistRecordSchema(SinkRecord sinkRecord) throws SQLException {
        SchemaBuilder valueBuilder = SchemaBuilder.struct();
        config.getHistRecordValueFields().forEach(field -> {
            Field f = sinkRecord.valueSchema().field(field);
            if (f == null) {
                log.error("Field name '{}' does not exists in source schema {} ", field, sinkRecord.valueSchema());
                throw new RuntimeException(String.format("Field '%s' does not exists in message schema", field));
            }
            valueBuilder.field(field, f.schema());
        });
        this.histTableValueSchema = valueBuilder.build();
        log.trace("HIST table value schema: {}", this.histTableValueSchema.fields());
        String histTableSql = hBuilder.buildStatement();
        log.trace("HIST table SQL: {}", histTableSql);
        this.histTablePreparedStatement = this.dbDialect.createPreparedStatement(this.connection, histTableSql);
        SchemaPair schemaPair = new SchemaPair(sinkRecord.keySchema(), this.histTableValueSchema);
        FieldsMetadata histTableFieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), PK_MODE, Collections.singletonList(this.config.histRecordKey), this.config.fieldsWhitelist, schemaPair);
        this.histTableStatementBinder = this.dbDialect.statementBinder(this.histTablePreparedStatement, PK_MODE, schemaPair, histTableFieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), JdbcSinkConfig.InsertMode.UPDATE);
    }

    private SinkRecord convertDeleteAsUpdateRecord(SinkRecord sinkRecord) {
        log.debug("Begin convert DELETE as UPDATE");
        Struct oldValue = (Struct) sinkRecord.value();
        Struct newValue = new Struct(deleteOpValueSchema);
        deleteOpValueSchema.fields().forEach(f -> {
            newValue.put(f, oldValue.get(f.name()));
        });
        log.trace("UPDATE as DELETE record value: {}", newValue);
        return new SinkRecord(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.keySchema(), sinkRecord.key(), deleteOpValueSchema, newValue, sinkRecord.kafkaOffset());

    }

    private SinkRecord convertToHistRecord(SinkRecord sinkRecord) {
        log.trace("Begin convert to HIST table record");
        Struct oldValue = (Struct) sinkRecord.value();
        Struct newValue = new Struct(histTableValueSchema);
        histTableValueSchema.fields().forEach(f -> newValue.put(f, oldValue.get(f.name())));
        return new SinkRecord(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.keySchema(), sinkRecord.key(), histTableValueSchema, newValue, sinkRecord.kafkaOffset());
    }

    private void flushWithDelete() throws SQLException {
        boolean recordIsDelOp = false;
        boolean prevRecordIsDelOp;
        log.debug("Flushing {} buffered records", records.size());
        for (SinkRecord sinkRecord : records) {
            Struct recordValue = (Struct) sinkRecord.value();
            if (shouldProcessHistRecord) {
                String recordHistValue = recordValue.getString(config.histRecStatusCol);
                if (recordHistValue != null && config.histRecStatusValue.matcher(recordHistValue).matches()) {
                    log.debug("Adding record to HIST table batch on (config.histValue) '{}' matches (record.histValue) '{}'", config.histRecStatusValue, recordHistValue);
                    sinkRecord = convertToHistRecord(sinkRecord);
                    histTableStatementBinder.bindRecord(sinkRecord);
                    continue;
                }
            }
//            init
            int deleteOpIdx = -1;
            for (int i = 0; i < config.getDeleteAsUpdateConditions().size(); i++) {
                String field = config.getDeleteAsUpdateConditions().get(i)[0];
                String value = config.getDeleteAsUpdateConditions().get(i)[1];
                if (recordValue.getString(field).equalsIgnoreCase(value)) {
                    deleteOpIdx = i;
                    break;
                }
            }
            prevRecordIsDelOp = recordIsDelOp;
            recordIsDelOp = deleteOpIdx > -1;
            if (isDeleteAsUpdate && recordIsDelOp) {
                sinkRecord = convertDeleteAsUpdateRecord(sinkRecord);
                log.debug("creating DELETE statement for message's key: {}, DELETE AS UPDATE key: {}", sinkRecord.key(), config.getDeleteAsUpdateKey());
                deleteAsUpdateStatementBinders[deleteOpIdx].bindRecord(sinkRecord);
            } else {
                log.debug("creating UPSERT statement for message's key: {}, DELETE AS UPDATE key: {}", sinkRecord.key(), config.getDeleteAsUpdateKey());
                updateStatementBinder.bindRecord(sinkRecord);
            }
            if (prevRecordIsDelOp != recordIsDelOp) {
                log.debug("Trigger batch execution on OP_TYPE changes");
                if (recordIsDelOp) {
                    log.debug("Execute batched DELETE statements");
                    executeDeletesStmt();
                } else {
                    log.debug("Execute batched UPSERT statements");
                    executeUpdatesStmt();
                }
//                TODO: switch isProcessingDelOp
            }

        }
        log.debug("Execute left-over batched statements");
        executeDeletesStmt();
        executeUpdatesStmt();
        executeHistUpdateStmt();
    }


    private void flushWithUpsertOnly() throws SQLException {
        log.debug("Flushing {} buffered records", records.size());
        for (SinkRecord sinkRecord : records) {
            if (shouldProcessHistRecord) {
                String recordHistValue = ((Struct) sinkRecord.value()).getString(config.histRecStatusCol);
                if (recordHistValue != null && config.histRecStatusValue.matcher(recordHistValue).matches()) {
                    log.debug("Adding record to HIST table batch on (config.histValue) '{}' matches (record.histValue) '{}'", config.histRecStatusValue, recordHistValue);
                    sinkRecord = convertToHistRecord(sinkRecord);
                    histTableStatementBinder.bindRecord(sinkRecord);
                    continue;
                }
            }

            log.debug("creating UPSERT statement for message's key: {}", sinkRecord.key());
            updateStatementBinder.bindRecord(sinkRecord);
        }
        executeUpdatesStmt();
        executeHistUpdateStmt();

    }

    @Override
    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        if (config.deleteMode != JdbcAuditSinkConfig.DeleteMode.NONE) {
            flushWithDelete();
        } else {
            flushWithUpsertOnly();
        }
        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    @Override
    public List<SinkRecord> add(SinkRecord sinkRecord) throws SQLException, TableAlterOrCreateException {
        log.trace("adding new record");
        this.recordValidator.validate(sinkRecord);
        if (isDeleteAsUpdate && deleteOpValueSchema == null) {
            log.debug("Begin Initialize DELETE as UPDATE configurations");
            initDeleteAsUpdateSchema(sinkRecord);
            log.debug("End Initialize DELETE as UPDATE configurations");
        }
        if (shouldProcessHistRecord && histTableValueSchema == null) {
            log.debug("Begin initialize HIST table record configuration");
            initHistRecordSchema(sinkRecord);
            log.debug("End initialize HIST table record configuration");
        }
        List<SinkRecord> flushed = new ArrayList<>();
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
            this.fieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), PK_MODE, this.config.pkFields, this.config.fieldsWhitelist, schemaPair);
            this.dbStructure.createOrAmendIfNecessary(this.config, this.connection, this.tableId, this.fieldsMetadata);
            String insertSql = uBuilder.buildStatement(this.toColumns(this.fieldsMetadata.keyFieldNames), this.toColumns(this.fieldsMetadata.nonKeyFieldNames));
            this.close();
            this.updatePreparedStatement = this.dbDialect.createPreparedStatement(this.connection, insertSql);
            this.updateStatementBinder = this.dbDialect.statementBinder(this.updatePreparedStatement, PK_MODE, schemaPair, this.fieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), this.config.insertMode);
        }

        this.records.add(sinkRecord);
        if (this.records.size() >= this.config.batchSize) {
            flushed.addAll(this.flush());
        }

        return flushed;
    }

    private Collection<ColumnId> toColumns(Collection<String> names) {
        return names.stream().filter(name -> !name.equalsIgnoreCase(config.auditTsCol)).map(name -> new ColumnId(this.tableId, name)).collect(Collectors.toList());
    }

    private void executeUpdatesStmt() throws SQLException {
        int[] batchStatus = updatePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException("Execution failed for part of the batch update", batchStatus);
            }
        }
        log.trace("UPSERT batch affected rows: {}", batchStatus.length);

    }


    private void executeHistUpdateStmt() throws SQLException {
        if (!this.shouldProcessHistRecord) {
            return;
        }
        log.debug("Execute HIST table batch");
        int[] batchStatus = histTablePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException("Execution failed for part of the batch update", batchStatus);
            }
        }
        log.trace("HIST batch affected rows: {}", batchStatus.length);
    }

    //
//    private void executeDeletesStmt() throws SQLException {
//        if (config.deleteMode == JdbcAuditSinkConfig.DeleteMode.NONE) {
//            return;
//        }
//        int[] batchStatus = deleteAsUpdatePreparedStatement.executeBatch();
//        for (int updateCount : batchStatus) {
//            if (updateCount == Statement.EXECUTE_FAILED) {
//                throw new BatchUpdateException("Execution failed for part of the batch update", batchStatus);
//            }
//        }
//        log.trace("DELETE batch affected rows: {}", batchStatus.length);
//    }
    private void executeDeletesStmt() throws SQLException {
        if (config.deleteMode == JdbcAuditSinkConfig.DeleteMode.NONE) {
            return;
        }
        for (int i = 0; i < deleteAsUpdatePreparedStatements.length; i++) {
            executeDeletesStmt(i);
        }
    }

    private void executeDeletesStmt(int i) throws SQLException {
        if (config.deleteMode == JdbcAuditSinkConfig.DeleteMode.NONE) {
            return;
        }
        int[] batchStatus = deleteAsUpdatePreparedStatements[i].executeBatch();
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException("Execution failed for part of the batch update", batchStatus);
            }
        }
        log.trace("DELETE batch affected rows: {}", batchStatus.length);
    }

}
