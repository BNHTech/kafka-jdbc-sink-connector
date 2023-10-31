/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Mutability;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.junit.After;
import org.junit.Before;

import java.sql.DriverManager;
import java.util.*;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public abstract class BaseDialectTest<T extends GenericDatabaseDialect> {

    protected static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_DAYS;
    protected static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_MILLIS;
    protected static final GregorianCalendar MARCH_15_2001_MIDNIGHT;

    static {
        EPOCH_PLUS_TEN_THOUSAND_DAYS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_DAYS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000);

        EPOCH_PLUS_TEN_THOUSAND_MILLIS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000);

        MARCH_15_2001_MIDNIGHT = new GregorianCalendar(2001, Calendar.MARCH, 15, 0, 0, 0);
        MARCH_15_2001_MIDNIGHT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    protected QuoteMethod quoteIdentfiiers;
    protected Boolean useHoldlockInMerge;
    protected TableId tableId;
    protected ColumnId columnPK1;
    protected ColumnId columnPK2;
    protected ColumnId columnA;
    protected ColumnId columnB;
    protected ColumnId columnC;
    protected ColumnId columnD;
    protected List<ColumnId> pkColumns;
    protected List<ColumnId> columnsAtoD;
    protected List<SinkRecordField> sinkRecordFields;
    protected T dialect;
    protected int defaultLoginTimeout;

    @Before
    public void setup() throws Exception {
        defaultLoginTimeout = DriverManager.getLoginTimeout();
        DriverManager.setLoginTimeout(1);

        // Set up some data ...
        Schema optionalDateWithDefault = Date.builder().defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
                .optional().build();
        Schema optionalTimeWithDefault = Time.builder().defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
                .optional().build();
        Schema optionalTsWithDefault = Timestamp.builder()
                .defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
                .optional().build();
        Schema optionalDecimal = Decimal.builder(4).optional().parameter("p1", "v1")
                .parameter("p2", "v2").build();
        Schema booleanWithDefault = SchemaBuilder.bool().defaultValue(true);
        tableId = new TableId(null, null, "myTable");
        columnPK1 = new ColumnId(tableId, "id1");
        columnPK2 = new ColumnId(tableId, "id2");
        columnA = new ColumnId(tableId, "columnA");
        columnB = new ColumnId(tableId, "columnB");
        columnC = new ColumnId(tableId, "columnC");
        columnD = new ColumnId(tableId, "columnD");
        pkColumns = Arrays.asList(columnPK1, columnPK2);
        columnsAtoD = Arrays.asList(columnA, columnB, columnC, columnD);

        SinkRecordField f1 = new SinkRecordField(Schema.INT32_SCHEMA, "c1", true);
        SinkRecordField f2 = new SinkRecordField(Schema.INT64_SCHEMA, "c2", false);
        SinkRecordField f3 = new SinkRecordField(Schema.STRING_SCHEMA, "c3", false);
        SinkRecordField f4 = new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "c4", false);
        SinkRecordField f5 = new SinkRecordField(optionalDateWithDefault, "c5", false);
        SinkRecordField f6 = new SinkRecordField(optionalTimeWithDefault, "c6", false);
        SinkRecordField f7 = new SinkRecordField(optionalTsWithDefault, "c7", false);
        SinkRecordField f8 = new SinkRecordField(optionalDecimal, "c8", false);
        SinkRecordField f9 = new SinkRecordField(booleanWithDefault, "c9", false);
        sinkRecordFields = Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8, f9);

        dialect = createDialect();
    }

    @After
    public void teardown() throws Exception {
        DriverManager.setLoginTimeout(defaultLoginTimeout);
    }

    /**
     * Create an instance of the dialect to be tested.
     *
     * @return the dialect; may not be null
     */
    protected abstract T createDialect();

    /**
     * Create a {@link JdbcSourceConnectorConfig} with the specified URL and optional config props.
     *
     * @param url           the database URL; may not be null
     * @param propertyPairs optional set of config name-value pairs; must be an even number
     * @return the config; never null
     */
    protected JdbcSourceConnectorConfig sourceConfigWithUrl(
            String url,
            String... propertyPairs
    ) {
        Map<String, String> connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        connProps.putAll(propertiesFromPairs(propertyPairs));
        connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
        if (quoteIdentfiiers != null) {
            connProps.put("quote.sql.identifiers", quoteIdentfiiers.toString());
        }
        return new JdbcSourceConnectorConfig(connProps);
    }

    /**
     * Create a {@link JdbcSinkConfig} with the specified URL and optional config props.
     *
     * @param url           the database URL; may not be null
     * @param propertyPairs optional set of config name-value pairs; must be an even number
     * @return the config; never null
     */
    protected JdbcSinkConfig sinkConfigWithUrl(
            String url,
            String... propertyPairs
    ) {
        Map<String, String> connProps = new HashMap<>();
        connProps.putAll(propertiesFromPairs(propertyPairs));
        connProps.put(JdbcSinkConfig.CONNECTION_URL, url);
        connProps.putAll(propertiesFromPairs(propertyPairs));
        if (quoteIdentfiiers != null) {
            connProps.put("quote.sql.identifiers", quoteIdentfiiers.toString());
        }
        if (useHoldlockInMerge != null) {
            connProps.put(JdbcSinkConfig.MSSQL_USE_MERGE_HOLDLOCK, useHoldlockInMerge.toString());
        }
        return new JdbcSinkConfig(connProps);
    }

    protected void assertDecimalMapping(
            int scale,
            String expectedSqlType
    ) {
        assertMapping(expectedSqlType, Decimal.schema(scale));
    }

    protected void assertDateMapping(String expectedSqlType) {
        assertMapping(expectedSqlType, Date.SCHEMA);
    }

    protected void assertTimeMapping(String expectedSqlType) {
        assertMapping(expectedSqlType, Time.SCHEMA);
    }

    protected void assertTimestampMapping(String expectedSqlType) {
        assertMapping(expectedSqlType, Timestamp.SCHEMA);
    }

    protected void assertPrimitiveMapping(
            Schema.Type type,
            String expectedSqlType
    ) {
        assertMapping(expectedSqlType, type, null);
    }

    protected void assertMapping(
            String expectedSqlType,
            Schema schema
    ) {
        assertMapping(expectedSqlType, schema.type(), schema.name(), schema.parameters());
    }

    protected void assertMapping(
            String expectedSqlType,
            Schema.Type type,
            String schemaName,
            Map<String, String> schemaParams
    ) {
        SchemaBuilder schemaBuilder = new SchemaBuilder(type).name(schemaName);
        if (schemaParams != null) {
            for (Map.Entry<String, String> entry : schemaParams.entrySet()) {
                schemaBuilder.parameter(entry.getKey(), entry.getValue());
            }
        }
        SinkRecordField field = new SinkRecordField(schemaBuilder.build(), schemaName, false);
        String sqlType = dialect.getSqlType(field);
        assertEquals(expectedSqlType, sqlType);
    }

    protected void assertMapping(
            String expectedSqlType,
            Schema.Type type,
            String schemaName,
            String... schemaParamPairs
    ) {
        Map<String, String> schemaProps = propertiesFromPairs(schemaParamPairs);
        assertMapping(expectedSqlType, type, schemaName, schemaProps);
    }


    protected void assertColumnConverter(
            int jdbcType,
            String typeName,
            Schema schema,
            Class<?> clazz
    ) {
        ColumnMapping mapping = new ColumnMapping(
                new ColumnDefinition(
                        columnA,
                        jdbcType,
                        typeName,
                        clazz.getCanonicalName(),
                        Nullability.NOT_NULL,
                        Mutability.UNKNOWN,
                        0,
                        0,
                        false,
                        1,
                        false,
                        false,
                        false,
                        false,
                        false
                ),
                1,
                new Field(
                        "b",
                        1,
                        schema
                )
        );
        assertNotNull(dialect.columnConverterFor(
                mapping,
                mapping.columnDefn(),
                mapping.columnNumber(),
                true
        ));
    }


    protected Map<String, String> propertiesFromPairs(String... pairs) {
        Map<String, String> props = new HashMap<>();
        assertEquals("Expecting even number of properties but found " + pairs.length, 0,
                pairs.length % 2);
        for (int i = 0; i != pairs.length; ++i) {
            String key = pairs[i];
            String value = pairs[++i];
            props.put(key, value);
        }
        return props;
    }

    protected void assertStatements(
            String[] expected,
            List<String> actual
    ) {
        // TODO: Remove
        assertEquals(expected.length, actual.size());
        for (int i = 0; i != expected.length; ++i) {
            assertEquals(expected[i], actual.get(i));
        }
    }

    protected TableId tableId(String name) {
        return new TableId(null, null, name);
    }

    protected Collection<ColumnId> columns(
            TableId id,
            String... names
    ) {
        List<ColumnId> columns = new ArrayList<>();
        for (int i = 0; i != names.length; ++i) {
            columns.add(new ColumnId(id, names[i]));
        }
        return columns;
    }

    protected void verifyDataTypeMapping(
            String expected,
            Schema schema
    ) {
        SinkRecordField field = new SinkRecordField(schema, schema.name(), schema.isOptional());
        assertEquals(expected, dialect.getSqlType(field));
    }

    protected void verifyCreateOneColNoPk(String expected) {
        assertEquals(expected, dialect.buildCreateTableStatement(tableId, Arrays.asList(
                new SinkRecordField(Schema.INT32_SCHEMA, "col1", false)
        )));
    }

    protected void verifyCreateOneColOnePk(String expected) {
        assertEquals(expected, dialect.buildCreateTableStatement(tableId, Arrays.asList(
                new SinkRecordField(Schema.INT32_SCHEMA, "pk1", true)
        )));
    }

    protected void verifyCreateOneColOnePkAsString(String expected) {
        assertEquals(expected, dialect.buildCreateTableStatement(tableId, Arrays.asList(
                new SinkRecordField(Schema.STRING_SCHEMA, "pk1", true)
        )));
    }

    protected void verifyCreateThreeColTwoPk(String expected) {
        assertEquals(expected, dialect.buildCreateTableStatement(tableId, Arrays.asList(
                new SinkRecordField(Schema.INT32_SCHEMA, "pk1", true),
                new SinkRecordField(Schema.INT32_SCHEMA, "pk2", true),
                new SinkRecordField(Schema.INT32_SCHEMA, "col1", false)
        )));
    }

    protected void verifyAlterAddOneCol(String... expected) {
        assertArrayEquals(expected, dialect.buildAlterTable(tableId, Arrays.asList(
                new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "newcol1", false)
        )).toArray());
    }

    protected void verifyAlterAddTwoCols(String... expected) {
        assertArrayEquals(expected, dialect.buildAlterTable(tableId, Arrays.asList(
                new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "newcol1", false),
                new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "newcol2", false)
        )).toArray());
    }

}