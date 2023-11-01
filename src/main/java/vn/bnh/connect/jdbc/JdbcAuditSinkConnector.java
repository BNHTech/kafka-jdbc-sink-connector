package vn.bnh.connect.jdbc;

import io.confluent.connect.jdbc.JdbcSinkConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkTask;

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
