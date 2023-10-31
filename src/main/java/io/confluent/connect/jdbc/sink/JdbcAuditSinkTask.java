package io.confluent.connect.jdbc.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.bnh.connect.jdbc.sink.JdbcAuditSinkConfig;

import java.util.Map;

public class JdbcAuditSinkTask extends JdbcSinkTask {
    private static final Logger log = LoggerFactory.getLogger(JdbcAuditSinkTask.class);

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting JDBC Sink task");
        this.config = new JdbcAuditSinkConfig(props);
        initWriter();
        remainingRetries = config.maxRetries;
        shouldTrimSensitiveLogs = config.trimSensitiveLogsEnabled;
        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            // Will occur in Connect runtimes earlier than 2.6
            reporter = null;
        }

    }
}
