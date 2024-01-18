package vn.bnh.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class JdbcAuditSinkConfig extends JdbcSinkConfig {
    public static final String GROUP = "Audits";
    public static final String DELETE_AS_UPDATE_KEY = "delete.as.update.key";
    public static final String DELETE_AS_UPDATE_KEY_DISPLAY = "Delete as UPDATE key field";
    public static final String DELETE_AS_UPDATE_KEY_DOC = "Key field to build SQL statement when delete.mode = 'UPDATE'";
    public static final String DELETE_AS_UPDATE_IDENTIFIER = "delete.as.update.identifier";
    public static final String DELETE_AS_UPDATE_IDENTIFIER_DISPLAY = "Delete as UPDATE identifier";
    public static final String DELETE_AS_UPDATE_IDENTIFIER_DOC = "Message value to identify the SQL statement is DELETE as UPDATE (e.g: when OP_TYPE = 'D')";
    public static final String DELETE_MODE = "delete.mode";
    public static final String DELETE_MODE_DISPLAY = "Delete Mode";
    public static final String DELETE_MODE_DOC = "the delete mode to use:\n" +
            "- DELETE: actually delete row(s) in database\n" +
            "- UPDATE: update a column to specific value";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA = "delete.as.update.value.schema";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA_DISPLAY = "Delete as UPDATE fields to retain";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA_DOC = "Message's fields to retain (other than field specified in delete.as.update.identifier) when building UPDATE statement for DELETE as UPDATE mode";
    public static final String AUDIT_TS_FIELD = "audit.timestamp.column";
    public static final String AUDIT_TS_FIELD_DISPLAY = "Audit timestamp column";
    public static final String AUDIT_TS_FIELD_DOC = "Database column name to INSERT/UPDATE current time when executing SQL statement";
    public static final String HIST_RECORD_STATUS_KEY = "hist.table.record.status.key";
    public static final String HIST_RECORD_STATUS_KEY_DISPLAY = "HIST table record status key field";
    public static final String HIST_RECORD_STATUS_KEY_DOC = "Key field to build SQL statement for HIST table's record processing";
    public static final String HIST_RECORD_STATUS_IDENTIFIER = "hist.table.record.status.identifier";
    public static final String HIST_RECORD_STATUS_IDENTIFIER_DISPLAY = "HIST table's 'record status' column identifier";
    public static final String HIST_RECORD_STATUS_IDENTIFIER_DOC = "Message's value to identify record is to be used for HIST table workflow (e.g: when REC_STATUS != null ).\n" +
            "Note: negative match (When input value is 'REC_STATUS=NULL' then SQL statement will be '... WHERE REC_STATUS IS NOT null')";
    public static final String HIST_RECORD_STATUS_VALUE_SCHEMA = "hist.table.record.status.value.schema";
    public static final String HIST_RECORD_STATUS_VALUE_SCHEMA_DISPLAY = "HIST table record's value schema";
    public static final String HIST_RECORD_STATUS_VALUE_SCHEMA_DOC = "Value schema (other than field specified in hist.table.record.status.identifier) when building UPDATE statement for HIST table's records";

    public static final ConfigDef CONFIG_DEF = JdbcSinkConfig.CONFIG_DEF
            .define(DELETE_MODE,
                    ConfigDef.Type.STRING,
                    "NONE",
                    EnumValidator.in(DeleteMode.values()),
                    ConfigDef.Importance.MEDIUM,
                    DELETE_MODE_DOC,
                    GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    DELETE_MODE_DISPLAY)
            .define(DELETE_AS_UPDATE_IDENTIFIER,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    DELETE_AS_UPDATE_IDENTIFIER_DOC,
                    GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    DELETE_AS_UPDATE_IDENTIFIER_DISPLAY)
            .define(DELETE_AS_UPDATE_VALUE_SCHEMA,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    DELETE_AS_UPDATE_VALUE_SCHEMA_DOC,
                    GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    DELETE_AS_UPDATE_VALUE_SCHEMA_DISPLAY)
            .define(DELETE_AS_UPDATE_KEY,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.LOW,
                    DELETE_AS_UPDATE_KEY_DOC,
                    GROUP,
                    4,
                    ConfigDef.Width.MEDIUM,
                    DELETE_AS_UPDATE_KEY_DISPLAY)
            .define(AUDIT_TS_FIELD,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    AUDIT_TS_FIELD_DOC,
                    GROUP,
                    5,
                    ConfigDef.Width.MEDIUM,
                    AUDIT_TS_FIELD_DISPLAY)
            .define(HIST_RECORD_STATUS_KEY,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    HIST_RECORD_STATUS_KEY_DOC,
                    GROUP,
                    7,
                    ConfigDef.Width.MEDIUM,
                    HIST_RECORD_STATUS_KEY_DISPLAY)
            .define(HIST_RECORD_STATUS_IDENTIFIER,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    HIST_RECORD_STATUS_IDENTIFIER_DOC,
                    GROUP,
                    8,
                    ConfigDef.Width.MEDIUM,
                    HIST_RECORD_STATUS_IDENTIFIER_DISPLAY)
            .define(HIST_RECORD_STATUS_VALUE_SCHEMA,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    HIST_RECORD_STATUS_VALUE_SCHEMA_DOC,
                    GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    HIST_RECORD_STATUS_VALUE_SCHEMA_DISPLAY);
    private static final Logger log = LoggerFactory.getLogger(JdbcAuditSinkConfig.class);
    public final DeleteMode deleteMode;
    private String deleteAsUpdateColName;
    private String deleteAsUpdateColValue;
    private Set<String> deleteAsUpdateValueFields = new HashSet<>();
    private String deleteAsUpdateKey;
    public final String auditTsCol;
    public final String histRecStatusCol;
    public final String histRecStatusValue;
    private List<String[]> deleteAsUpdateConditions;
    public final String histRecordKey;
    private Set<String> histRecordValueFields = new HashSet<>();

    public JdbcAuditSinkConfig(Map<?, ?> props) {
        super(props);
        auditTsCol = getString(AUDIT_TS_FIELD);
        this.histRecordKey = getString(HIST_RECORD_STATUS_KEY);
        if (this.histRecordKey != null && !this.histRecordKey.isBlank()) {
            String[] histRecStatusUpdateCondition = getString(HIST_RECORD_STATUS_IDENTIFIER).split("=");
            this.histRecStatusCol = histRecStatusUpdateCondition[0];
            this.histRecStatusValue = histRecStatusUpdateCondition[1].equalsIgnoreCase("null") ? null : histRecStatusUpdateCondition[1];
            this.histRecordValueFields = new HashSet<>(this.getList(DELETE_AS_UPDATE_VALUE_SCHEMA));
            this.histRecordValueFields.add(histRecordKey);
            log.info("HIST Record Key: {}", histRecordKey);
            log.info("HIST Record value schema: {}", histRecordValueFields);
            log.debug("HIST record condition: record.{} != {}", histRecStatusCol, histRecStatusValue);
        } else {
            histRecStatusCol = null;
            histRecStatusValue = null;
        }

        deleteMode = DeleteMode.valueOf(getString(DELETE_MODE).toUpperCase());
        log.info("DELETE OP Mode: {}", deleteMode);
        if (deleteMode != DeleteMode.NONE) {
            deleteAsUpdateConditions = this.getList(DELETE_AS_UPDATE_IDENTIFIER).stream()
                    .map(x -> x.split("=")).collect(Collectors.toList());
            deleteAsUpdateColName = deleteAsUpdateConditions.get(0)[0];
            deleteAsUpdateColValue = deleteAsUpdateConditions.get(0)[1];
            deleteAsUpdateKey = getString(DELETE_AS_UPDATE_KEY);
            this.deleteAsUpdateValueFields = new HashSet<>(this.getList(DELETE_AS_UPDATE_VALUE_SCHEMA));
            this.deleteAsUpdateValueFields.add(deleteAsUpdateKey);
            log.info("DELETE OP Key: {}", deleteAsUpdateKey);
            log.info("DELETE OP fields to retains: {}", deleteAsUpdateValueFields);

        }
    }

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }

    public enum DeleteMode {
        DELETE, UPDATE, NONE
    }

    private static class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(
                List<String> canonicalValues,
                Set<String> validValues
        ) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(E[] enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length * 2);
            for (E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toUpperCase());
                validValues.add(e.toString().toLowerCase());
            }
            return new EnumValidator(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(
                String key,
                Object value
        ) {
            if (!validValues.contains(value)) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }

    public String getDeleteAsUpdateColName() {
        return deleteAsUpdateColName;
    }

    public String getDeleteAsUpdateColValue() {
        return deleteAsUpdateColValue;
    }

    public Set<String> getDeleteAsUpdateValueFields() {
        return deleteAsUpdateValueFields;
    }

    public String getDeleteAsUpdateKey() {
        return deleteAsUpdateKey;
    }

    public List<String[]> getDeleteAsUpdateConditions() {
        return deleteAsUpdateConditions;
    }

    public Set<String> getHistRecordValueFields() {
        return histRecordValueFields;
    }

    @Override
    public String toString() {

        return "JdbcAuditSinkConfig{" +
                "deleteMode=" + deleteMode +
                ", \ndeleteAsUpdateColName='" + deleteAsUpdateColName + '\'' +
                ", \ndeleteAsUpdateColValue='" + deleteAsUpdateColValue + '\'' +
                ", \ndeleteAsUpdateValueFields=" + deleteAsUpdateValueFields +
                ", \ndeleteAsUpdateKey='" + deleteAsUpdateKey + '\'' +
                ", \nauditTsCol='" + auditTsCol + '\'' +
                ", \nhistRecStatusCol='" + histRecStatusCol + '\'' +
                ", \nhistRecStatusValue='" + histRecStatusValue + '\'' +
                ", \ndeleteAsUpdateConditions=" + deleteAsUpdateConditions +
                ", \nhistRecordKey='" + histRecordKey + '\'' +
                ", \nhistRecordValueFields=" + histRecordValueFields +
                '}';
    }
}
