package cn.synway.bigdata.midas.response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Collection;
import java.util.TimeZone;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.MidasStatement;
import cn.synway.bigdata.midas.domain.MidasDataTypeTestDataProvider.MidasDataTypeTestData;
import cn.synway.bigdata.midas.settings.MidasProperties;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

public class MidasResultSetTest {

    @DataProvider(name = "longArrays")
    public Object[][] longArrays() {
        return new Object[][]{
                {"[0]", new long[]{0}},
                {"[324000111222,123,-456]", new long[]{324000111222L, 123, -456}},
                {"[]", new long[]{}},
        };
    }

    @Test(dataProvider = "longArrays")
    public void toLongArrayTest(String str, long[] expected) throws Exception {
        Assert.assertEquals(
                MidasResultSet.toLongArray(ByteFragment.fromString(str)),
                expected
        );
    }

    MidasProperties props = new MidasProperties();
    TimeZone tz = TimeZone.getDefault();

    @Test
    public void withoutTotals() throws Exception {
        String response =
          "SiteName\tcount()\n" +
            "String\tUInt64\n" +
            "hello.com\t21209048\n" +
            "there.com\t49302091\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));
        assertEquals(21209048L, rs.getLong(2));

        rs.next();
        assertEquals("there.com", rs.getString(1));
        assertEquals(49302091L, rs.getLong(2));

        assertFalse(rs.next());
    }

    @Test
    public void withoutTotalsSingleColumn() throws Exception {
        String response =
          "SiteName\n" +
            "String\n" +
            "hello.com\n" +
            "there.com\n" +
            "\n" +
            "\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        MidasResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));

        rs.next();
        assertEquals("there.com", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        assertFalse(rs.next());
        try {
            rs.getTotals();
            fail("The getTotals shall fail when usesWithTotals is set to false.");
        } catch (IllegalStateException e) {
            // this is expected
        }
        assertFalse(rs.next());
    }

    @Test
    public void withTotals() throws Exception {
        String response = "SiteName\tcount()\n" +
          "String\tUInt64\n" +
          "hello.com\t21209048\n" +
          "there.com\t49302091\n" +
          "\n" +
          "\t70511139\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        MidasResultSet rs = buildResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));
        assertEquals(21209048L, rs.getLong(2));

        rs.next();
        assertEquals("there.com", rs.getString(1));
        assertEquals(49302091L, rs.getLong(2));

        assertFalse(rs.next());

        rs.getTotals();
        assertEquals("", rs.getString(1));
        assertEquals(70511139L, rs.getLong(2));
    }

    public void withTotalsAndEmptyStrings() throws Exception {
        String response = "SiteName\tCountry\n" +
          "String\tString\n" +
          "hello.com\tPoland\n" +
          "there.com\tUSA\n" +
          "\t\n" +
          "other.com\t\n" +
          "\n" +
          "\t\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        MidasResultSet rs = buildResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));
        assertEquals("Poland", rs.getString(2));


        rs.next();
        assertEquals("there.com", rs.getString(1));
        assertEquals("USA", rs.getString(2));

        rs.next();
        assertEquals("", rs.getString(1));
        assertEquals("", rs.getString(2));

        rs.next();
        assertEquals("other.com", rs.getString(1));
        assertEquals("", rs.getString(2));

        assertFalse(rs.next());

        rs.getTotals();
        assertEquals("", rs.getString(1));
        assertEquals(70511139L, rs.getLong(2));
    }

    @Test
    public void withTotalsSingleColumn() throws Exception {
        String response =
          "SiteName\n" +
            "String\n" +
            "hello.com\n" +
            "there.com\n" +
            "\n" +
            "other.com\n" +
            "\n" + // with totals separator row
            "\n"; // with totals values row

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));

        rs.next();
        assertEquals("there.com", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        rs.next();
        assertEquals("other.com", rs.getString(1));

        assertFalse(rs.next());
    }

    @Test
    public void withTotalsSingleIntColumn() throws Exception {
        String response =
          "Code\n" +
            "UInt64\n" +
            "1\n" +
            "2\n" +
            "3\n" +
            "\n" + // with totals separator row
            "0"; // with totals values row

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        MidasResultSet rs = buildResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals(1L, rs.getLong(1));

        rs.next();
        assertEquals(2L, rs.getLong(1));

        rs.next();
        assertEquals(3L, rs.getLong(1));

        assertFalse(rs.next());

        rs.getTotals();
        assertEquals(0L, rs.getLong(1));
    }

    @Test
    public void withTotalsSingleNullableColumn() throws Exception {
        String response =
          "SiteName\n" +
            "Nullable(String)\n" +
            "hello.com\n" +
            "there.com\n" +
            "\n" +
            "\\N\n" +
            "other.com\n" +
            "\n" + // with totals separator row
            "\\N\n";// with totals values row

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        MidasResultSet rs = buildResultSet(is, 1024, "db", "table", true, null, null, props);

        rs.next();
        assertEquals("hello.com", rs.getString(1));

        rs.next();
        assertEquals("there.com", rs.getString(1));

        rs.next();
        assertEquals("", rs.getString(1));

        rs.next();
        assertNull(rs.getString(1));

        rs.next();
        assertEquals("other.com", rs.getString(1));

        assertFalse(rs.next());

        rs.getTotals();
        assertNull(rs.getString(1));
    }

    @Test
    public void testIsLast() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                        "String\tUInt64\n" +
                        "hello.com\t21209048\n" +
                        "there.com\t49302091\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        assertFalse(rs.isLast());
        assertTrue(rs.next());
        assertFalse(rs.isLast());
        assertTrue(rs.next());
        assertTrue(rs.isLast());
        assertFalse(rs.next());
    }

    @Test
    public void testIsFirst() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                        "String\tUInt64\n" +
                        "hello.com\t21209048\n" +
                        "there.com\t49302091\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        assertFalse(rs.isFirst());
        assertTrue(rs.next());
        assertTrue(rs.isFirst());
        assertTrue(rs.next());
        assertFalse(rs.isFirst());
    }
    
    @Test
    public void testBeforeFirst() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                        "String\tUInt64\n" +
                        "hello.com\t21209048\n" +
                        "there.com\t49302091\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        assertTrue(rs.isBeforeFirst());
        assertTrue(rs.next());
        assertFalse(rs.isBeforeFirst());
        is.close();
    }
    
    @Test
    public void testIsAfterLast() throws Exception {
        String response =
                "SiteName\tcount()\n" +
                        "String\tUInt64\n" +
                        "hello.com\t21209048\n" +
                        "there.com\t49302091\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.next());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.next());
        assertTrue(rs.isAfterLast());
    }

    @Test
    public void testDecimalMetadata() throws Exception {
        String response =
            "sum(myMoney)\n" +
            "Decimal(38, 3)\n" +
            "12955152630.539";
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));
        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);
        rs.next();
        assertEquals(rs.getMetaData().getColumnType(1), Types.DECIMAL);
        assertEquals(rs.getMetaData().getColumnTypeName(1), "Decimal(38, 3)");
        assertEquals(rs.getMetaData().getColumnClassName(1), BigDecimal.class.getCanonicalName());
        assertEquals(rs.getObject(1), new BigDecimal("12955152630.539"));
        assertEquals(rs.getMetaData().getScale(1), 3);
        assertEquals(rs.getMetaData().getPrecision(1), 38);
    }

    @Test
    public void testArrayString() throws Exception {
        String response =
            "FOO\n"
          + "Array(String)\n"
          + "[foo,bar]\n";
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));
        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals("java.sql.Array", meta.getColumnClassName(1));
        rs.next();
        Object o = rs.getObject(1);
        assertTrue(Array.class.isAssignableFrom(o.getClass()),
            o.getClass().getCanonicalName());
        String[] s = (String[]) ((Array) o).getArray();
        assertEquals("foo", s[0]);
        assertEquals("bar", s[1]);
    }

    @Test
    public void test3dArrayString() throws Exception {
        String response =
            "FOO\n"
          + "Array(Array(Array(String)))\n"
          + "[[[a,b],[c,d]],[[e,f],[g,h]]]";
        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));
        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals("java.sql.Array", meta.getColumnClassName(1));
        rs.next();

        Object o = rs.getObject(1);
        assertTrue(Array.class.isAssignableFrom(o.getClass()), o.getClass().getCanonicalName());
        String[][][] actual = (String[][][]) ((Array) o).getArray();
        String[][][] expected = {{{"a", "b"}, {"c","d"}}, {{"e", "f"}, {"g", "h"}}};
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; ++i) {
            assertEquals(expected[i].length, actual[i].length);
            for (int j = 0; j < expected[i].length; ++j) {
                assertEquals(expected[i][j].length, actual[i][j].length);
                for (int k = 0; k < expected[i][j].length; ++k) {
                    assertEquals(expected[i][j][k], actual[i][j][k]);
                }
            }
        }
    }

    @Test
    public void testClassNamesObjects() throws Exception {
        String testData = MidasTypesTestData.buildTestString();
        ByteArrayInputStream is = new ByteArrayInputStream(testData.getBytes("UTF-8"));
        ResultSet rs = buildResultSet(is, testData.length(), "db", "table", false, null,
            TimeZone.getTimeZone("UTC"), props);
        rs.next();
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String typeName = meta.getColumnTypeName(i);
            String className = null;
            try {
                className = meta.getColumnClassName(i);
            } catch (Exception e) {
                fail("Unable to determine class name for " + typeName, e);
            }
            Class<?> clazz = Class.forName(className);
            assertNotNull(
                clazz,
                "Class not available. class name: " + className + ", type name: " + typeName);
            Object o = rs.getObject(i);
            if (o == null && meta.isNullable(i) > 0) {
                continue;
            }
            assertNotNull(
                o,
                "Object null. class name: " + className + ", type name: " + typeName);
            assertTrue(
                clazz.isAssignableFrom(rs.getObject(i).getClass()),
                "Class mismatch. class name: " + className + ", type name: " + typeName +
                    " object class: " + o.getClass().getCanonicalName());
        }
    }

    @Test
    public void testGetColumnNames() throws Exception {
        String response = "SiteName\tCountry\n" +
                "String\tString\n" +
                "hello.com\tPoland\n" +
                "there.com\tUSA\n" +
                "\t\n" +
                "other.com\t\n" +
                "\n" +
                "\t\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        MidasResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);
        String[] columnNames = rs.getColumnNames();
        assertEquals(2, columnNames.length);
        assertEquals("SiteName", columnNames[0]);
        assertEquals("Country", columnNames[1]);
    }

    /**
     * By jdbc specification
     *
     * If the value is SQL <code>NULL</code>, the value returned is <code>0</code>
     *
     * {@link java.sql.ResultSet#getByte(int)}
     * {@link java.sql.ResultSet#getShort(int)}
     * {@link java.sql.ResultSet#getInt(int)}
     * {@link java.sql.ResultSet#getLong(int)}
     * {@link java.sql.ResultSet#getFloat(int)}
     * {@link java.sql.ResultSet#getDouble(int)}
     *
     * If the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     *
     * {@link java.sql.ResultSet#getBigDecimal(int)}
     * {@link java.sql.ResultSet#getTime(int)}
     * {@link java.sql.ResultSet#getDate(int)}
     * {@link java.sql.ResultSet#getTimestamp(int)}
     * {@link java.sql.ResultSet#getURL(int)} unsupported now
     * {@link java.sql.ResultSet#getAsciiStream(int)} unsupported now
     */
    @Test
    public void testNulls() throws Exception {
        String response =
                "Type\n" +
                        "Nullable(Int8)\n" +
                        "\\N\n";

        ByteArrayInputStream is = new ByteArrayInputStream(response.getBytes("UTF-8"));

        ResultSet rs = buildResultSet(is, 1024, "db", "table", false, null, null, props);

        rs.next();
        //0
        assertEquals(0, rs.getByte(1));
        assertEquals(0, rs.getShort(1));
        assertEquals(0, rs.getInt(1));
        assertEquals(0, rs.getLong(1));
        assertEquals((float) 0, rs.getFloat(1));
        assertEquals((double)0, rs.getDouble(1));

        //null
        assertNull(rs.getBigDecimal(1));
        assertNull(rs.getTime(1));
        assertNull(rs.getDate(1));
        assertNull(rs.getTimestamp(1));

        //unsupported now
        //assertNull(rs.getURL(1));
        //assertNull(rs.getAsciiStream(1));

        assertFalse(rs.next());
    }

    private static MidasResultSet buildResultSet(InputStream is, int bufferSize, String db, String table, boolean usesWithTotals, MidasStatement statement, TimeZone timezone, MidasProperties properties) throws IOException {
    	return new MidasResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    }

    private static enum MidasTypesTestData {

        // SELECT name FROM system.data_type_families WHERE alias_to <> '' ORDER BY name ASC

        // AggregateFunction
        Array("Array(String)", "[foo, bar]", false, false),
        Date("Date", "2019-03-03", false, true),
        DateTime("DateTime", "2019-03-03 13:37:42", false, true),
        Decimal("Decimal(12,2)", "42.23", false, true),
        // Decimal128
        // Decimal32
        // Decimal64
        Enum16("Enum16('foo'=0,'bar'=1)", "FOO", false, true),
        Enum8("Enum8('foo'=0,'bar'=1)", "BAR", false, true),
        FixedString("FixedString(3)", "BAZ", false, true),
        Float32("Float32", "42.23", false, true),
        Float64("Float64", "42.23", false, true),
        IPv4("IPv4", "127.0.0.1", false, true),
        IPv6("IPv6", "::1", false, true),
        Int16("Int16", "1337", false, true),
        Int32("Int32", "1337", false, true),
        Int64("Int64", "1337", false, true),
        Int8("Int8", "42", false, true),
        // IntervalDay
        // IntervalHour
        // IntervalMinute
        // IntervalMonth
        // IntervalQuarter
        // IntervalSecond
        // IntervalWeek
        // IntervalYear
        // LowCardinality
        // Nested
        // Nothing
        // Nullable
        String("String", "foo", true, true),
        Tuple("Tuple(UInt8, String)", "(42, 'foo')", false, true),
        UInt16("UInt16", "42", false, true),
        UInt32("UInt32", "23", false, true),
        UInt64("UInt64", "1337", false, true),
        UInt8("UInt8", "1", false, true),
        UUID("UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", true, true);

        private final String typeName;
        private final String serializedValue;
        private final boolean lowCardinalityCandidate;
        private final boolean nullableCandidate;

        MidasTypesTestData(String typeName, String serializedValue,
            boolean lowCardinalityCandidate, boolean nullableCandidate)
        {
            this.typeName = typeName;
            this.serializedValue = serializedValue;
            this.lowCardinalityCandidate = lowCardinalityCandidate;
            this.nullableCandidate = nullableCandidate;
        }

        private static String buildTestString() {
            StringBuilder sb = new StringBuilder();
            // row 1: column names
            for (MidasTypesTestData t : values()) {
                sb.append(t.typeName)
                  .append("\t");
                if (t.nullableCandidate) {
                    sb.append("Nullable(")
                      .append(t.typeName)
                      .append(')')
                      .append("\t");
                }
                if (t.lowCardinalityCandidate) {
                    sb.append("LowCardinality(")
                      .append(t.typeName)
                      .append(')')
                      .append("\t");
                }
            }
            sb.replace(sb.length(), sb.length(), "\n");

            // row 2: type names
            sb.append(sb.substring(0, sb.length()));

            // row 3 : example data
            for (MidasTypesTestData t : values()) {
                sb.append(t.serializedValue)
                  .append("\t");
                if (t.nullableCandidate) {
                    sb.append("\\N\t");
                }
                if (t.lowCardinalityCandidate) {
                    sb.append(t.serializedValue)
                      .append("\t");
                }
            }
            sb.replace(sb.length(), sb.length(), "\n");
            return sb.toString();
        }
    }

    private static String buildTestString(Collection<MidasDataTypeTestData> testDataTypes) {
        StringBuilder sb = new StringBuilder();
        // row 1: column names
        for (MidasDataTypeTestData t : testDataTypes) {
            sb.append(t.getTypeName())
              .append("\t");
            if (t.isNullableCandidate()) {
                sb.append("Nullable(")
                  .append(t.getTypeName())
                  .append(')')
                  .append("\t");
            }
            if (t.isLowCardinalityCandidate()) {
                sb.append("LowCardinality(")
                  .append(t.getTypeName())
                  .append(')')
                  .append("\t");
            }
        }
        sb.replace(sb.length(), sb.length(), "\n");

        // row 2: type names
        sb.append(sb.substring(0, sb.length()));

        // row 3 : example data
        for (MidasDataTypeTestData t : testDataTypes) {
            sb.append(t.getTestValue())
              .append("\t");
            if (t.isNullableCandidate()) {
                sb.append("\\N\t");
            }
            if (t.isLowCardinalityCandidate()) {
                sb.append(t.getTestValue())
                  .append("\t");
            }
        }
        sb.replace(sb.length(), sb.length(), "\n");
        return sb.toString();
    }
}
