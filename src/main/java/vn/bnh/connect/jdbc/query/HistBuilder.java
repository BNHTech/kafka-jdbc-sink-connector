package vn.bnh.connect.jdbc.query;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.util.List;
import java.util.stream.Collectors;

public class HistBuilder extends QueryBuilder {
    public HistBuilder(
            JdbcAuditSinkConfig config,
            TableId tableId,
            DatabaseDialect dbDialect
    ) {
        super(config, tableId, dbDialect);
    }

    public String buildStatement() {
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
