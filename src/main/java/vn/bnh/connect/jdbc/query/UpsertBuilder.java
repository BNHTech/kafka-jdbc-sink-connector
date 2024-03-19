package vn.bnh.connect.jdbc.query;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.util.Collection;

public class UpsertBuilder extends QueryBuilder {

    public UpsertBuilder(
            JdbcAuditSinkConfig config,
            TableId tableId,
            DatabaseDialect dbDialect
    ) {
        super(config, tableId, dbDialect);
    }

    public String buildStatement(
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> builder.append(this.tableId).append(".").appendColumnName(col.name()).append("=incoming.").appendColumnName(col.name());
        ExpressionBuilder builder = this.dbDialect.expressionBuilder();
        builder.append("merge into ");
        builder.append(this.tableId);
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
}
