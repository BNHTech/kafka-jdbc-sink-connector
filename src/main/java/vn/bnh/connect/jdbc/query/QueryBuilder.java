package vn.bnh.connect.jdbc.query;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

public class QueryBuilder {
    final JdbcAuditSinkConfig config;
    final TableId tableId;
    final DatabaseDialect dbDialect;
    static final String AUDIT_TS_VALUE = "SYSTIMESTAMP";

    public QueryBuilder(
            JdbcAuditSinkConfig config,
            TableId tableId,
            DatabaseDialect dbDialect
    ) {
        this.config = config;
        this.tableId = tableId;
        this.dbDialect = dbDialect;
    }
}
