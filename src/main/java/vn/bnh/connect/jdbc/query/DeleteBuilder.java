package vn.bnh.connect.jdbc.query;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DeleteBuilder extends QueryBuilder {


    public DeleteBuilder(
            JdbcAuditSinkConfig config,
            TableId tableId,
            DatabaseDialect dbDialect
    ) {
        super(config, tableId, dbDialect);
    }

    /**
     * build multiple SQL statements from <b>delete.as.update.identifier</b>
     * example: delete.as.update.identifier: F1=D, F2=D
     * delete.as.update.value.schema: X,Y,Z
     *
     * @return DELETE AS UPDATE SQL statements: [
     * UPDATE TABLE ... SET TIMESTAMP_COL, X,Y,Z, F1 WHERE KEY = ... AND F1 !=D;
     * UPDATE TABLE ... SET TIMESTAMP_COL, X,Y,Z, F2 WHERE KEY = ... AND F2 !=D;
     * <p>
     * ]
     */
    public String[] buildStatements() {
        List<ColumnId> keyCols = this.config.getDeleteAsUpdateKey().stream()
                .map(f -> new ColumnId(this.tableId, f))
                .collect(Collectors.toList());
        String[] sqlStatements = new String[config.getDeleteAsUpdateConditions().size()];

        List<String[]> deleteAsUpdateConditions = config.getDeleteAsUpdateConditions();
        for (int i = 0; i < deleteAsUpdateConditions.size(); i++) {
            String[] condition = deleteAsUpdateConditions.get(i);
            Set<ColumnId> columns = this.config.getDeleteAsUpdateValueFields().stream()
                    .filter(f -> !f.equalsIgnoreCase(condition[0]))
                    .map(f -> new ColumnId(this.tableId, f))
                    .collect(Collectors.toSet());
            ExpressionBuilder builder = this.dbDialect.expressionBuilder();
            builder.append("UPDATE ").append(this.tableId).append(" SET ")
                    .append(new ColumnId(this.tableId, condition[0]))
                    .append(" = ").appendStringQuoted(condition[1])
                    .append(",").append(config.auditTsCol).append(" = ").append(AUDIT_TS_VALUE);
            if (!columns.isEmpty()) {
                builder.append(", ");
            }
            builder.appendList()
                    .delimitedBy(", ")
                    .transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(columns);
            builder.append(" WHERE ");
            builder.appendList().delimitedBy(" AND ").transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(keyCols);
            builder.append(" AND ").append(new ColumnId(this.tableId, condition[0])).append(" != ").appendStringQuoted(condition[1]);
            sqlStatements[i] = builder.toString();

        }

        return sqlStatements;
    }

    /**
     * build single SQL statement from <b>delete.as.update.identifier</b>
     * example: delete.as.update.identifier: F1=D, F2=D
     * delete.as.update.value.schema: X,Y,Z
     *
     * @return DELETE AS UPDATE SQL statement: "UPDATE TABLE ... SET TIMESTAMP_COL, X,Y,Z WHERE KEY=... AND (F1 != D OR F2 != D)"
     */
    public String buildStatement() {
        List<ColumnId> columns = this.config.getDeleteAsUpdateValueFields().stream()
                .map(f -> new ColumnId(this.tableId, f))
                .collect(Collectors.toList());
        List<ColumnId> keyCols = this.config.getDeleteAsUpdateKey().stream()
                .map(f -> new ColumnId(this.tableId, f))
                .collect(Collectors.toList());

        ExpressionBuilder builder = this.dbDialect.expressionBuilder();
        builder.append("UPDATE ").append(this.tableId).append(" SET ")
                .append(new ColumnId(this.tableId, this.config.getDeleteAsUpdateColName()))
                .append(" = ").appendStringQuoted(config.getDeleteAsUpdateColValue())
                .append(", ");
        // set audit timestamp
        builder.append(this.config.auditTsCol).append(" = ").append(AUDIT_TS_VALUE);
        if (!columns.isEmpty()) {
            builder.append(", ");
        }
        builder.appendList().delimitedBy(", ").transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(columns);
        builder.append(" WHERE ");
        builder.appendList().delimitedBy(" AND ").transformedBy(ExpressionBuilder.columnNamesWith(" = ?")).of(keyCols);
        builder.append(" AND (");
        for (int i = 0; i < config.getDeleteAsUpdateConditions().size(); i++) {
            String[] cond = config.getDeleteAsUpdateConditions().get(i);
            if (i > 0) {
                builder.append(" OR");
            }
            builder.append(" ")
                    .append(new ColumnId(this.tableId, cond[0]));
            if (cond[1].equalsIgnoreCase("null")) {
                builder.append("IS NOT NULL");
            } else {
                builder.append(" != ").appendStringQuoted(cond[1]);
            }
        }
        builder.append(" )");
        return builder.toString();
    }
}
