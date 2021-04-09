package cn.synway.bigdata.midas;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class MidasPreparedStatementParameterTest {

    @Test
    public void testNullParam() {
        MidasPreparedStatementParameter p0 =
            MidasPreparedStatementParameter.nullParameter();
        assertEquals(p0.getRegularValue(), "null");
        assertEquals(p0.getBatchValue(), "\\N");

        MidasPreparedStatementParameter p1 =
            MidasPreparedStatementParameter.nullParameter();
        assertEquals(p1, p0);
        assertSame(p1, p0);
    }

    @Test
    public void testArrayAndCollectionParam() {
        MidasPreparedStatementParameter p0 =
                MidasPreparedStatementParameter.fromObject(Arrays.asList("A", "B", "C"), TimeZone.getDefault(), TimeZone.getDefault());
        MidasPreparedStatementParameter p1 =
                MidasPreparedStatementParameter.fromObject(new String[]{"A", "B", "C"}, TimeZone.getDefault(), TimeZone.getDefault());
        assertEquals(p0.getRegularValue(), p1.getRegularValue());
        assertEquals(p0.getBatchValue(), p1.getBatchValue());
    }

    @Test
    public void testBooleanParam() {
        assertEquals(MidasPreparedStatementParameter.fromObject(Boolean.TRUE,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "1");
        assertEquals(MidasPreparedStatementParameter.fromObject(Boolean.FALSE,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "0");
    }

    @Test
    public void testNumberParam() {
        assertEquals(MidasPreparedStatementParameter.fromObject(10,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "10");
        assertEquals(MidasPreparedStatementParameter.fromObject(10.5,
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "10.5");
    }

    @Test
    public void testStringParam() {
        assertEquals(MidasPreparedStatementParameter.fromObject("someString",
            TimeZone.getDefault(), TimeZone.getDefault()).getRegularValue(), "'someString'");
    }
}
