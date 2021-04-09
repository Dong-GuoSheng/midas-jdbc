package cn.synway.bigdata.midas;

import cn.synway.bigdata.midas.util.MidasValueFormatter;

import java.util.TimeZone;

public final class MidasPreparedStatementParameter {

    private static final MidasPreparedStatementParameter NULL_PARAM =
        new MidasPreparedStatementParameter(null, false);

    private static final MidasPreparedStatementParameter TRUE_PARAM =
            new MidasPreparedStatementParameter("1", false);

    private static final MidasPreparedStatementParameter FALSE_PARAM =
            new MidasPreparedStatementParameter("0", false);

    private final String stringValue;
    private final boolean quoteNeeded;

    public static MidasPreparedStatementParameter fromObject(Object x,
        TimeZone dateTimeZone, TimeZone dateTimeTimeZone)
    {
        if (x == null) {
            return NULL_PARAM;
        }
        return new MidasPreparedStatementParameter(
            MidasValueFormatter.formatObject(x, dateTimeZone, dateTimeTimeZone),
            MidasValueFormatter.needsQuoting(x));
    }

    public static MidasPreparedStatementParameter nullParameter() {
        return NULL_PARAM;
    }

    public static MidasPreparedStatementParameter boolParameter(boolean value) {
        return value ? TRUE_PARAM : FALSE_PARAM;
    }

    public MidasPreparedStatementParameter(String stringValue,
        boolean quoteNeeded)
    {
        this.stringValue = stringValue == null
            ? MidasValueFormatter.NULL_MARKER
            : stringValue;
        this.quoteNeeded = quoteNeeded;
    }

    String getRegularValue() {
        return !MidasValueFormatter.NULL_MARKER.equals(stringValue)
            ? quoteNeeded
                ? "'" + stringValue + "'"
                : stringValue
            : "null";
    }

    String getBatchValue() {
        return stringValue;
    }


    @Override
    public String toString() {
        return stringValue;
    }

}