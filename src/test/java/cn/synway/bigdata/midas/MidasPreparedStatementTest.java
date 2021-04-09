package cn.synway.bigdata.midas;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;

import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.settings.MidasProperties;

import static org.testng.Assert.assertEquals;

public class MidasPreparedStatementTest {

    private static final String SQL_STATEMENT= "INSERT INTO foo (bar) VALUES (";

    @Test
    public void testSetBytesNull() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setBytes(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetBytesNormal() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setBytes(1, "foo".getBytes("UTF-8"));
        assertParamMatches(s, "'\\x66\\x6F\\x6F'");
    }

    @Test
    public void testSetBytesEmpty() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setBytes(1, "".getBytes("UTF-8"));
        assertParamMatches(s, "''");
    }

    @Test
    public void testSetNull() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setNull(1, Types.ARRAY);
        assertParamMatches(s, "null");
        s.setNull(1, Types.CHAR);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetBooleanTrue() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setBoolean(1, true);
        assertParamMatches(s, "1");
    }

    @Test
    public void testSetBooleanFalse() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setBoolean(1, false);
        assertParamMatches(s, "0");
    }

    @Test
    public void testSetByte() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setByte(1, (byte) -127);
        assertParamMatches(s, "-127");
    }

    @Test
    public void testSetShort() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setShort(1, (short) 42);
        assertParamMatches(s, "42");
    }

    @Test
    public void testSetInt() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setInt(1, 0);
        assertParamMatches(s, "0");
    }

    @Test
    public void testSetLong() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setLong(1, 1337L);
        assertParamMatches(s, "1337");
    }

    @Test
    public void testSetFloat() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setFloat(1, -23.42f);
        assertParamMatches(s, "-23.42");
    }

    @Test
    public void testSetDouble() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setDouble(1, Double.MIN_VALUE);
        assertParamMatches(s, "4.9E-324"); // will result in 0 in Float64
                                           // but parsing is OK
    }

    @Test
    public void testSetBigDecimalNull() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setBigDecimal(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetBigDecimalNormal() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setBigDecimal(1, BigDecimal.valueOf(-0.2342));
        assertParamMatches(s, "-0.2342");
    }

    @Test
    public void testSetStringNull() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setString(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetStringSimple() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setString(1, "foo");
        assertParamMatches(s, "'foo'");
    }

    @Test
    public void testSetStringEvil() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setString(1, "\"'\\x32");
        assertParamMatches(s, "'\"\\'\\\\x32'");
    }

    @Test
    public void testSetDateNull() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setDate(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetDateNormal() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setDate(1, new Date(1557168043000L));
        assertParamMatches(s, "'2019-05-06'");
    }

    @Test
    public void testSetDateOtherTimeZone() throws Exception {
        MidasPreparedStatement s = createStatement(
            TimeZone.getTimeZone("Asia/Tokyo"),
            new MidasProperties());
        s.setDate(1, new Date(1557168043000L));
        assertParamMatches(s, "'2019-05-06'");
    }

    @Test
    public void testSetDateOtherTimeZoneServerTime() throws Exception {
        MidasProperties props = new MidasProperties();
        props.setUseServerTimeZoneForDates(true);
        MidasPreparedStatement s = createStatement(
            TimeZone.getTimeZone("Asia/Tokyo"),
            props);
        s.setDate(1, new Date(1557168043000L));
        assertParamMatches(s, "'2019-05-07'");
    }

    @Test
    public void testSetTimeNull() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setTime(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetTimeNormal() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setTime(1, new Time(1557168043000L));
        assertParamMatches(s, "'2019-05-06 21:40:43'");
    }

    @Test
    public void testSetTimeNormalOtherTimeZone() throws Exception {
        MidasPreparedStatement s = createStatement(
            TimeZone.getTimeZone("America/Los_Angeles"),
            new MidasProperties());
        s.setTime(1, new Time(1557168043000L));
        assertParamMatches(s, "'2019-05-06 11:40:43'");
    }

    @Test
    public void testSetTimestampNull() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setTimestamp(1, null);
        assertParamMatches(s, "null");
    }

    @Test
    public void testSetTimestampNormal() throws Exception {
        MidasPreparedStatement s = createStatement();
        s.setTimestamp(1, new Timestamp(1557168043000L));
        assertParamMatches(s, "'2019-05-06 21:40:43'");
    }

    @Test
    public void testSetTimestampNormalOtherTimeZone() throws Exception {
        MidasPreparedStatement s = createStatement(
            TimeZone.getTimeZone("America/Los_Angeles"),
            new MidasProperties());
        s.setTimestamp(1, new Timestamp(1557168043000L));
        assertParamMatches(s, "'2019-05-06 11:40:43'");
    }

    private static void assertParamMatches(MidasPreparedStatement stmt, String expected) {
        assertEquals(stmt.asSql(), SQL_STATEMENT + expected + ")");
    }

    private static MidasPreparedStatement createStatement() throws Exception {
        return createStatement(
            TimeZone.getTimeZone("Europe/Moscow"),
            new MidasProperties());
    }

    private static MidasPreparedStatement createStatement(TimeZone timezone,
        MidasProperties props) throws Exception
    {
        return new MidasPreparedStatementImpl(
            Mockito.mock(CloseableHttpClient.class),
            Mockito.mock(MidasConnection.class),
            props,
            "INSERT INTO foo (bar) VALUES (?)",
            timezone,
            ResultSet.TYPE_FORWARD_ONLY);
    }

}
