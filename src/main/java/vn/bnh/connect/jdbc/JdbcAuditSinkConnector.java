package vn.bnh.connect.jdbc;

import io.confluent.connect.jdbc.JdbcSinkConnector;
import io.confluent.connect.jdbc.sink.JdbcAuditSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

public class JdbcAuditSinkConnector extends JdbcSinkConnector {

    @Override
    public ConfigDef config() {
        return JdbcAuditSinkConfig.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcAuditSinkTask.class;
    }

}
