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
    public static final String DELETE_AS_UPDATE_SET_VALUE = "delete.as.update.set.value";
    public static final String DELETE_AS_UPDATE_SET_VALUE_DISPLAY = "Delete as UPDATE value";
    public static final String DELETE_AS_UPDATE_SET_VALUE_DOC =
            "Set column to specified value when delete.mode = 'UPDATE'";
    public static final String DELETE_MODE = "delete.mode";
    public static final String DELETE_MODE_DISPLAY = "Delete Mode";
    public static final String DELETE_MODE_DOC =
            "the delete mode to use:\n"
                    + "- DELETE: actually delete row(s) in database\n"
                    + "- UPDATE: update a column to specific value";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA = "delete.as.update.value.schema";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA_DISPLAY = "Delete as UPDATE value schema fields";
    public static final String DELETE_AS_UPDATE_VALUE_SCHEMA_DOC = "Message's fields to retain when building UPDATE statement for DELETE as UPDATE mode";
    public static final ConfigDef CONFIG_DEF = JdbcSinkConfig.CONFIG_DEF.define(
                    DELETE_MODE,
                    ConfigDef.Type.STRING,
                    "DELETE",
                    EnumValidator.in(DeleteMode.values()),
                    ConfigDef.Importance.MEDIUM,
                    DELETE_MODE_DOC,
                    GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    DELETE_MODE_DISPLAY)
            .define(
                    DELETE_AS_UPDATE_KEY,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.LOW,
                    DELETE_AS_UPDATE_KEY_DOC,
                    GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    DELETE_AS_UPDATE_KEY_DISPLAY)
            .define(
                    DELETE_AS_UPDATE_SET_VALUE,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.LOW,
                    DELETE_AS_UPDATE_SET_VALUE_DOC,
                    GROUP,
                    7,
                    ConfigDef.Width.MEDIUM,
                    DELETE_AS_UPDATE_SET_VALUE_DISPLAY)
            .define(DELETE_AS_UPDATE_VALUE_SCHEMA,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    DELETE_AS_UPDATE_VALUE_SCHEMA_DOC,
                    GROUP,
                    8,
                    ConfigDef.Width.MEDIUM,
                    DELETE_AS_UPDATE_VALUE_SCHEMA_DISPLAY);
    public final DeleteMode deleteMode;
    public final String deleteAsUpdateKey;
    public final String deleteAsUpdateColName;
    public final String deleteAsUpdateColValue;
    public final Set<String> deleteAsUpdateValueFields;

    public JdbcAuditSinkConfig(Map<?, ?> props) {
        super(props);
        deleteMode = DeleteMode.valueOf(getString(DELETE_MODE).toUpperCase());
        deleteAsUpdateKey = getString(DELETE_AS_UPDATE_KEY);
        String[] deleteAsUpdateValue = getString(DELETE_AS_UPDATE_SET_VALUE).split("=");
        deleteAsUpdateColName = deleteAsUpdateValue[0];
        deleteAsUpdateColValue = deleteAsUpdateValue[1];
        this.deleteAsUpdateValueFields = new HashSet(this.getList(DELETE_AS_UPDATE_VALUE_SCHEMA));
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
