package vn.bnh.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.util.LogUtil;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class JdbcAuditSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(JdbcAuditSinkTask.class);
    private JdbcAuditDbWriter writer;

    ErrantRecordReporter reporter;
    DatabaseDialect dialect;
    JdbcAuditSinkConfig config;
    int remainingRetries;

    boolean shouldTrimSensitiveLogs;

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting JDBC Sink task");
        config = new JdbcAuditSinkConfig(props);
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

    void initWriter() {
        log.info("Initializing JDBC writer");
        if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(config.dialectName, config);
        } else {
            dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new JdbcAuditDbWriter(config, dialect, dbStructure);
        log.info("JDBC writer initialized");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                        + "database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            writer.write(records);
        } catch (TableAlterOrCreateException tacE) {
            if (reporter != null) {
                unrollAndRetry(records);
            } else {
                log.error(tacE.toString());
                throw tacE;
            }
        } catch (SQLException sqlE) {
            SQLException trimmedException = shouldTrimSensitiveLogs
                    ? LogUtil.trimSensitiveData(sqlE) : sqlE;
            log.warn(
                    "Write of {} records failed, remainingRetries={}",
                    records.size(),
                    remainingRetries,
                    trimmedException
            );
            int totalExceptions = 0;
            for (Throwable e : sqlE) {
                totalExceptions++;
            }
            SQLException sqlAllMessagesException = getAllMessagesException(sqlE);
            if (remainingRetries > 0) {
                writer.closeQuietly();
                initWriter();
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                log.debug(sqlAllMessagesException.toString());
                throw new RetriableException(sqlAllMessagesException);
            } else {
                if (reporter != null) {
                    unrollAndRetry(records);
                } else {
                    log.error(
                            "Failing task after exhausting retries; "
                                    + "encountered {} exceptions on last write attempt. "
                                    + "For complete details on each exception, please enable DEBUG logging.",
                            totalExceptions);
                    int exceptionCount = 1;
                    for (Throwable e : trimmedException) {
                        log.debug("Exception {}:", exceptionCount++, e);
                    }
                    throw new ConnectException(sqlAllMessagesException);
                }
            }
        }
        remainingRetries = config.maxRetries;
    }

    private void unrollAndRetry(Collection<SinkRecord> records) {
        writer.closeQuietly();
        initWriter();
        for (SinkRecord sinkRecord : records) {
            try {
                writer.write(Collections.singletonList(sinkRecord));
            } catch (TableAlterOrCreateException tace) {
                log.debug(tace.toString());
                reporter.report(sinkRecord, tace);
                writer.closeQuietly();
            } catch (SQLException sqle) {
                SQLException sqlAllMessagesException = getAllMessagesException(sqle);
                log.debug(sqlAllMessagesException.toString());
                reporter.report(sinkRecord, sqlAllMessagesException);
                writer.closeQuietly();
            }
        }
    }

    private SQLException getAllMessagesException(SQLException sqle) {
        StringBuilder sqleAllMessages = new StringBuilder("Exception chain:" + System.lineSeparator());
        SQLException trimmedException = shouldTrimSensitiveLogs
                ? LogUtil.trimSensitiveData(sqle) : sqle;
        for (Throwable e : trimmedException) {
            sqleAllMessages.append(e).append(System.lineSeparator());
        }
        SQLException sqlAllMessagesException = new SQLException(sqleAllMessages.toString());
        sqlAllMessagesException.setNextException(trimmedException);
        return sqlAllMessagesException;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    public void stop() {
        log.info("Stopping task");
        try {
            if (writer != null) {
                writer.closeQuietly();
            }
        } finally {
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

}
