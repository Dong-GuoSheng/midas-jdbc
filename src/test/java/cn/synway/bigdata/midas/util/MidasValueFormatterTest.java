package cn.synway.bigdata.midas.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.UUID;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class MidasValueFormatterTest {

    @Test
    public void testFormatBytesUUID() {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = ByteBuffer.allocate(16)
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits())
            .array();
        String formattedBytes = MidasValueFormatter.formatBytes(bytes);
        byte[] reparsedBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            reparsedBytes[i] =  (byte)
                ((Character.digit(formattedBytes.charAt(i * 4 + 2), 16) << 4)
                + Character.digit(formattedBytes.charAt(i * 4 + 3), 16));
        }
        assertEquals(reparsedBytes, bytes);
    }

    @Test
    public void testFormatBytesHelloWorld() throws Exception {
        byte[] bytes = "HELLO WORLD".getBytes("UTF-8");
        String formattedBytes = MidasValueFormatter.formatBytes(bytes);
        byte[] reparsedBytes = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            reparsedBytes[i] =  (byte)
                ((Character.digit(formattedBytes.charAt(i * 4 + 2), 16) << 4)
                + Character.digit(formattedBytes.charAt(i * 4 + 3), 16));
        }
        assertEquals(
            new String(reparsedBytes, Charset.forName("UTF-8")),
            "HELLO WORLD");
    }

    @Test
    public void testFormatBytesNull() {
        assertNull(MidasValueFormatter.formatBytes(null));
    }

    @Test
    public void testFormatBytesEmpty() {
        assertEquals(MidasValueFormatter.formatBytes(new byte[0]), "");
    }

    @Test
    public void testFormatObject() throws Exception {
        TimeZone tzUTC = TimeZone.getTimeZone("UTC");
        assertEquals(MidasValueFormatter.formatObject(Byte.valueOf("42"), tzUTC, tzUTC), "42");
        assertEquals(MidasValueFormatter.formatObject("foo", tzUTC, tzUTC), "foo");
        assertEquals(MidasValueFormatter.formatObject("f'oo\to", tzUTC, tzUTC), "f\\'oo\\to");
        assertEquals(MidasValueFormatter.formatObject(BigDecimal.valueOf(42.23), tzUTC, tzUTC), "42.23");
        assertEquals(MidasValueFormatter.formatObject(BigInteger.valueOf(1337), tzUTC, tzUTC), "1337");
        assertEquals(MidasValueFormatter.formatObject(Short.valueOf("-23"), tzUTC, tzUTC), "-23");
        assertEquals(MidasValueFormatter.formatObject(Integer.valueOf(1337), tzUTC, tzUTC), "1337");
        assertEquals(MidasValueFormatter.formatObject(Long.valueOf(-23L), tzUTC, tzUTC), "-23");
        assertEquals(MidasValueFormatter.formatObject(Float.valueOf(4.2f), tzUTC, tzUTC), "4.2");
        assertEquals(MidasValueFormatter.formatObject(Double.valueOf(23.42), tzUTC, tzUTC), "23.42");
        byte[] bytes = "HELLO WORLD".getBytes("UTF-8");
        assertEquals(MidasValueFormatter.formatObject(bytes, tzUTC, tzUTC),
            "\\x48\\x45\\x4C\\x4C\\x4F\\x20\\x57\\x4F\\x52\\x4C\\x44");
        Date d = new Date(1557136800000L);
        assertEquals(MidasValueFormatter.formatObject(d, tzUTC, tzUTC), "2019-05-06");
        Time t = new Time(1557136800000L);
        assertEquals(MidasValueFormatter.formatObject(t, tzUTC, tzUTC), "2019-05-06 10:00:00");
        Timestamp ts = new Timestamp(1557136800000L);
        assertEquals(MidasValueFormatter.formatObject(ts, tzUTC, tzUTC), "2019-05-06 10:00:00");
        assertEquals(MidasValueFormatter.formatObject(Boolean.TRUE, tzUTC, tzUTC), "1");
        UUID u = UUID.randomUUID();
        assertEquals(MidasValueFormatter.formatObject(u, tzUTC, tzUTC), u.toString());
        int[] ints = new int[] { 23, 42 };
        assertEquals(MidasValueFormatter.formatObject(ints, tzUTC, tzUTC), "[23,42]");
        assertEquals(MidasValueFormatter.formatObject(new Timestamp[] {ts, ts}, tzUTC, tzUTC),
            "['2019-05-06 10:00:00','2019-05-06 10:00:00']");

    }


}
