package vn.bnh.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.*;

public class JdbcAuditSinkConfig extends JdbcSinkConfig {

    public enum DeleteMode {
        DELETE,
        UPDATE
    }

    public static final String GROUP = "Audits";
    public static final String DELETE_AS_UPDATE_KEY = "delete.as.update.key";
    public static final String DELETE_AS_UPDATE_KEY_DISPLAY = "Delete as UPDATE key field";
    public static final String DELETE_AS_UPDATE_KEY_DOC =
            "Key field to build SQL statement when delete.mode = 'UPDATE'";
    public static final String DELETE_AS_UPDATE_IDENTIFIER = "delete.as.update.identifier";
    public static final String DELETE_AS_UPDATE_IDENTIFIER_DISPLAY = "Delete as UPDATE identifier";
    public static final String DELETE_AS_UPDATE_IDENTIFIER_DOC =
            "Message value to identify the SQL statement is DELETE as UPDATE (e.g: when OP_TYPE = 'D')";
    public static final String DELETE_MODE = "delete.mode";
    public static final String DELETE_MODE_DISPLAY = "Delete Mode";
    public static final String DELETE_MODE_DOC =
            "the delete mode to use:\n"
                    + "- DELETE: actually delete row(s) in database\n"
                    + "- UPDATE: update a column to specific value";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA = "delete.as.update.value.schema";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA_DISPLAY = "Delete as UPDATE value schema fields";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA_DOC = "Message's fields to retain (other than field specified in delete.as.update.identifier) when building UPDATE statement for DELETE as UPDATE mode";

    public static final String AUDIT_TS_FIELD = "audit.timestamp.column";
    public static final String AUDIT_TS_FIELD_DISPLAY = "Audit timestamp column";
    public static final String AUDIT_TS_FIELD_DOC = "Database column name to INSERT/UPDATE current time when executing SQL statement";
    public static final ConfigDef CONFIG_DEF = JdbcSinkConfig.CONFIG_DEF
            .define(
                    DELETE_MODE,
                    ConfigDef.Type.STRING,
                    "DELETE",
                    EnumValidator.in(DeleteMode.values()),
                    ConfigDef.Importance.MEDIUM,
                    DELETE_MODE_DOC,
                    GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    DELETE_MODE_DISPLAY)
            .define(
                    DELETE_AS_UPDATE_IDENTIFIER,
                    ConfigDef.Type.STRING,
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
                    AUDIT_TS_FIELD_DISPLAY);
    public final DeleteMode deleteMode;
    public final String deleteAsUpdateColName;
    public final String deleteAsUpdateColValue;
    public final Set<String> deleteAsUpdateValueFields;
    public final String deleteAsUpdateKey;
    public final String auditTsCol;

    public JdbcAuditSinkConfig(Map<?, ?> props) {
        super(props);
        deleteMode = DeleteMode.valueOf(getString(DELETE_MODE).toUpperCase());
        String[] deleteAsUpdateValue = getString(DELETE_AS_UPDATE_IDENTIFIER).split("=");
        deleteAsUpdateColName = deleteAsUpdateValue[0];
        deleteAsUpdateColValue = deleteAsUpdateValue[1];
        deleteAsUpdateKey = getString(DELETE_AS_UPDATE_KEY);
        this.deleteAsUpdateValueFields = new HashSet<>(this.getList(DELETE_AS_UPDATE_VALUE_SCHEMA));
        this.deleteAsUpdateValueFields.add(deleteAsUpdateKey);
        auditTsCol = getString(AUDIT_TS_FIELD);
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

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }

}
