package vn.bnh.connect.jdbc.sink;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class JdbcAuditSinkConfigTest {

    @Test
    void testInit() {
        Map<String, String> conf = new HashMap<>();
        conf.put("connector.class", "vn.bnh.connect.jdbc.JdbcAuditSinkConnector");
        conf.put("tasks.max", "1");
        conf.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        conf.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        conf.put("transforms", "filterField");
        conf.put("errors.retry.timeout", "1000");
        conf.put("errors.retry.delay.max.ms", "1000");
        conf.put("errors.tolerance", "none");
        conf.put("errors.log.enable", "false");
        conf.put("errors.log.include.messages", "false");
        conf.put("topics", "DUCNM_LIMIT");
        conf.put("errors.deadletterqueue.topic.name", "");
        conf.put("errors.deadletterqueue.topic.replication.factor", "2");
        conf.put("errors.deadletterqueue.context.headers.enable", "false");
        conf.put("transforms.filterField.type", "org.apache.kafka.connect.transforms.ReplaceField$Value");
        conf.put("transforms.filterField.exclude", "CURRENT_TS, REP_TS, OP_TS, LOOKUP_KEY, TABLE_NAME, ROWKEY, RECVER");
        conf.put("connection.url", "jdbc:oracle:thin:@172.29.70.43:1521/T24DS");
        conf.put("connection.user", "DUCNM");
        conf.put("connection.password", "oracle_4U");
        conf.put("dialect.name", "OracleDatabaseDialect");
        conf.put("connection.attempts", "1");
        conf.put("connection.backoff.ms", "5000");
        conf.put("insert.mode", "UPSERT");
        conf.put("table.types", "TABLE");
        conf.put("table.name.format", "DUCNM.ODS_LIMIT");
        conf.put("pk.mode", "record_value");
        conf.put("pk.fields", "RECID");
        conf.put("auto.create", "false");
        conf.put("auto.evolve", "false");
        conf.put("delete.mode", "UPDATE");
        conf.put("delete.as.update.identifier", "COMMIT_ACTION=D");
        conf.put("delete.as.update.value.schema", "COMMIT_TS, REPLICAT_TS, STREAM_TS");
        conf.put("delete.as.update.key", "RECID");
        conf.put("audit.timestamp.column", "TIME_UPDATE");
        conf.put("hist.table.record.status.key", "RECID");
        conf.put("hist.table.record.status.identifier", "RECORD_STATUS=NULL");
        conf.put("hist.table.record.status.value.schema", "COMMIT_TS, REPLICAT_TS, COMMIT_ACTION_HIS, STREAM_TS, AUTHORISER_HIS, INPUTTER_HIS, DATE_TIME_AUTH_HIS, DATE_TIME_INPUT_HIS, RECORD_STATUS");
        conf.put("principal.service.name", "kafkaldap");
        conf.put("principal.service.password", "Vhht@dmin#2023");
        conf.put("value.converter.schema.registry.url", "http://dc1-dev-kkschm01:8081");
        conf.put("key.converter.schema.registry.url", "http://dc1-dev-kkschm01:8081");
        JdbcAuditSinkConfig sinkConf = new JdbcAuditSinkConfig(conf);
        System.out.println(sinkConf);
    }
}