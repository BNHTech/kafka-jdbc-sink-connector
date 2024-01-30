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
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class BufferedRecords extends io.confluent.connect.jdbc.sink.BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);
    private static final String AUDIT_TS_VALUE = "SYSTIMESTAMP";
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

    }

    private void initDeleteAsUpdateSchema(SinkRecord sinkRecord) throws SQLException {
        SchemaBuilder valueBuilder = SchemaBuilder.struct();
        config.getDeleteAsUpdateValueFields().forEach(field -> {
            Field f = sinkRecord.valueSchema().field(field);
            if (f == null) {
                log.error("Field name '{}' does not exists in source schema {} ", field, sinkRecord.valueSchema());
                throw new RuntimeException(String.format("Field '%s' does not exists in message schema", field));
            }
            valueBuilder.field(field, f.schema());
        });
        SchemaPair schemaPair = new SchemaPair(sinkRecord.keySchema(), this.deleteOpValueSchema);
        FieldsMetadata deleteAsUpdateFieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), PK_MODE, Collections.singletonList(this.config.getDeleteAsUpdateKey()), this.config.fieldsWhitelist, schemaPair);
        this.deleteOpValueSchema = valueBuilder.build();
        this.deleteAsUpdatePreparedStatements = new PreparedStatement[config.getDeleteAsUpdateConditions().size()];

        for (int i = 0; i < config.getDeleteAsUpdateConditions().size(); i++) {
            String deleteAsUpdateSql = this.buildDeleteQueryStatement(i);
            log.trace("DELETE AS UPDATE SQL: {}", deleteAsUpdateSql);
            this.deleteAsUpdatePreparedStatements[i] = this.dbDialect.createPreparedStatement(this.connection, deleteAsUpdateSql);
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
        String histTableSql = this.buildUpdateHistQueryStatement();
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
            for (int i = 0; i < config.getDeleteAsUpdateCols().length; i++) {
                String field = config.getDeleteAsUpdateCols()[i];
                String value = config.getDeleteAsUpdateValues()[i];
                if (recordValue.getString(field).equalsIgnoreCase(value)) {
                    deleteOpIdx = i;
                    break;
                }
            }
            prevRecordIsDelOp = recordIsDelOp;
            recordIsDelOp = deleteOpIdx > -1;
            if (isDeleteAsUpdate && recordIsDelOp) {
                sinkRecord = convertDeleteAsUpdateRecord(sinkRecord);
                log.debug("creating DELETE statement for message's key: {}, DELETE AS UPDATE key: {}", sinkRecord.key(), ((Struct) sinkRecord.value()).get(config.getDeleteAsUpdateKey()));
                deleteAsUpdateStatementBinders[deleteOpIdx].bindRecord(sinkRecord);
            } else {
                log.debug("creating UPSERT statement for message's key: {}, DELETE AS UPDATE key: {}", sinkRecord.key(), ((Struct) sinkRecord.value()).get(config.getDeleteAsUpdateKey()));
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
//                currentOpType = recordOpType;
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
            String insertSql = this.generateInsertStatement();
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

    private String generateInsertStatement() {
        return buildUpsertQueryStatement(this.tableId, this.toColumns(this.fieldsMetadata.keyFieldNames), this.toColumns(this.fieldsMetadata.nonKeyFieldNames));
    }

    private String buildUpsertQueryStatement(
            TableId table,
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> builder.append(table).append(".").appendColumnName(col.name()).append("=incoming.").appendColumnName(col.name());
        ExpressionBuilder builder = this.dbDialect.expressionBuilder();
        builder.append("merge into ");
        builder.append(table);
        builder.append(" using (select ");
        builder.appendList().delimitedBy(", ").transformedBy(ExpressionBuilder.columnNamesWithPrefix("? ")).of(keyColumns, nonKeyColumns);
        builder.append(" FROM dual) incoming on (");
        builder.appendList().delimitedBy(" and ").transformedBy(transform).of(keyColumns);
        builder.append(")");
        if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
            builder.append(" when matched then update set ");
            builder.appendList().delimitedBy(",").transformedBy(transform).of(nonKeyColumns);
            // UPDATE - audit timestamp
            if (!nonKeyColumns.isEmpty()) {
                builder.append(",");
            }
            builder.append(new ColumnId(this.tableId, config.auditTsCol)).append(" = ").append(AUDIT_TS_VALUE);
        }
        builder.append(" when not matched then insert(");
        builder.appendList().delimitedBy(",").of(nonKeyColumns, keyColumns);
        // INSERT - audit timestamp
        builder.append(",");
        builder.append(new ColumnId(this.tableId, config.auditTsCol));
        builder.append(") values (");
        builder.appendList().delimitedBy(",").transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming.")).of(nonKeyColumns, keyColumns);
        // audit ts
        builder.append(",").append(AUDIT_TS_VALUE);
        builder.append(")");
        return builder.toString();
    }

    String buildDeleteQueryStatement(int filterConditionIdx) {
        List<ColumnId> columns = config.getDeleteAsUpdateValueFields().stream().map(f -> new ColumnId(this.tableId, f)).collect(Collectors.toList());
        ExpressionBuilder expressionBuilder = this.dbDialect.expressionBuilder();
        String filterCol = config.getDeleteAsUpdateCols()[filterConditionIdx];
        String filterVal = config.getDeleteAsUpdateValues()[filterConditionIdx];
        expressionBuilder.append("UPDATE ").append(this.tableId).append(" SET ");
        // set audit timestamp
        expressionBuilder.append(config.auditTsCol).append(" = ").append(AUDIT_TS_VALUE);
        if (!columns.isEmpty()) {
            expressionBuilder.append(", ");
        }
        expressionBuilder.appendList().delimitedBy(", ").transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(columns);
        expressionBuilder.append(" WHERE ");
        expressionBuilder.append(new ColumnId(this.tableId, config.getDeleteAsUpdateKey())).append(" = ?");
        expressionBuilder.append(" AND (");

        expressionBuilder.append(" ").append(new ColumnId(this.tableId, filterCol));

        if (filterVal.equalsIgnoreCase("null")) {
            expressionBuilder.append("IS NOT NULL");
        } else {
            expressionBuilder.append(" != ").appendStringQuoted(filterVal);
        }

        expressionBuilder.append(" )");
        return expressionBuilder.toString();
    }

    String buildUpdateHistQueryStatement() {
        List<ColumnId> columns = config.getHistRecordValueFields().stream().filter(f -> !f.equalsIgnoreCase(config.histRecordKey)).map(f -> new ColumnId(this.tableId, f)).collect(Collectors.toList());
        ExpressionBuilder expressionBuilder = this.dbDialect.expressionBuilder();
        expressionBuilder.append("UPDATE ").append(this.tableId).append(" SET ")
                // set audit timestamp
                .append(config.auditTsCol).append(" = ").append(AUDIT_TS_VALUE);
        if (!columns.isEmpty()) {
            expressionBuilder.append(", ");
        }
        expressionBuilder.appendList().delimitedBy(", ").transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(columns);
        expressionBuilder.append(" WHERE ");
        expressionBuilder.append(new ColumnId(this.tableId, config.histRecordKey)).append(" = ?");
        expressionBuilder.append(" AND (").append(new ColumnId(this.tableId, config.histRecStatusCol));
        if (config.histRecStatusValue == null) {
            expressionBuilder.append(" IS NOT NULL )");
        } else {
            expressionBuilder.append(" != ").appendStringQuoted(config.histRecStatusValue).append(" OR ").append(config.histRecStatusCol).append(" IS NULL)");
        }

        return expressionBuilder.toString();
    }
}
