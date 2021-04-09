package cn.synway.bigdata.midas.response;

import java.util.TimeZone;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.domain.MidasDataType;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class MidasColumnInfoTest {

    @Test
    public void testParseNull() {
        try {
            MidasColumnInfo.parse(null, null);
            fail();
        } catch (NullPointerException npe) { /* expected */ }
    }

    @Test
    public void testDateTimeWithoutTimeZone() {
        MidasColumnInfo info = MidasColumnInfo.parse(
            "DateTime", "column");
        assertEquals(info.getMidasDataType(), MidasDataType.DateTime);
    }

    @Test
    public void testDateTimeWithEmptyTimeZone() {
        MidasColumnInfo info = MidasColumnInfo.parse(
            "DateTime()", "column");
        assertEquals(info.getMidasDataType(), MidasDataType.DateTime);
    }

    @Test
    public void testDateTimeArrayWithTimeZonee() {
        assertEquals(
            MidasColumnInfo.parse("Array(DateTime('America/Los_Angeles'))", "foo").getTimeZone(),
            TimeZone.getTimeZone("America/Los_Angeles"));
    }

    @Test
    public void testSpuriousArguments() {
        MidasColumnInfo info = MidasColumnInfo.parse(
            "Decimal(12, 3), 42)", "column");
        assertEquals(info.getScale(), 3);
    }

    @Test(dataProvider = "columnInfoParsingDataProvider")
    public void testParser(String input, MidasDataType dataType, MidasDataType arrayBaseType,
        boolean nullable, boolean lowCardinality, int precision, int scale, TimeZone timeZone,
        int arrayLevel)
    {
        MidasColumnInfo info = MidasColumnInfo.parse(input, "columnName");
        assertEquals(info.getMidasDataType(), dataType);
        assertEquals(info.getArrayBaseType(), arrayBaseType);
        assertEquals(info.isNullable(), nullable);
        assertEquals(info.isLowCardinality(), lowCardinality);
        assertEquals(info.getPrecision(), precision);
        assertEquals(info.getScale(), scale);
        assertEquals(info.getTimeZone(), timeZone);
        assertEquals(info.getArrayLevel(), arrayLevel);
        assertEquals(info.getOriginalTypeName(), input);
        assertEquals(info.getColumnName(), "columnName");
    }

    @SuppressWarnings("boxing")
    @DataProvider(name = "columnInfoParsingDataProvider")
    public Object[][] provideRegularParsingTestData() {
        return new Object[][] {
            { "Array(String)",
               MidasDataType.Array, MidasDataType.String, false, false, 0, 0, null, 1 },
            { "Array(Array(String))",
               MidasDataType.Array, MidasDataType.String, false, false, 0, 0, null, 2 },
            { "Array(Array(LowCardinality(Nullable(String))))",
               MidasDataType.Array, MidasDataType.String, true, true, 0, 0, null, 2 },
            { "Array(Decimal(12, 3))",
               MidasDataType.Array, MidasDataType.Decimal, false, false, 12, 3, null, 1 },
            { "Decimal(12, 3)",
               MidasDataType.Decimal, null, false, false, 12, 3, null, 0 },
            { "Decimal32(3)",
               MidasDataType.Decimal32, null, false, false, 9, 3, null, 0 },
            { "Decimal64(3)",
               MidasDataType.Decimal64, null, false, false, 18, 3, null, 0 },
            { "Decimal128(3)",
               MidasDataType.Decimal128, null, false, false, 38, 3, null, 0 },
            { "DateTime('Europe/Berlin')",
               MidasDataType.DateTime, null, false, false, 19, 0, TimeZone.getTimeZone("Europe/Berlin"), 0 },
            { "DateTime(Europe/Amsterdam)",
               MidasDataType.DateTime, null, false, false, 19, 0, TimeZone.getTimeZone("Europe/Amsterdam"), 0 },
            { "FixedString(42)",
               MidasDataType.FixedString, null, false, false, 42, 0, null, 0 },
            { "LowCardinality(Nullable(Int64))", // issue #325
               MidasDataType.Int64, null, true, true, 20, 0, null, 0 }
        };
    }

    @Test(dataProvider = "columnInfoParsingUnknownDataProvider")
    public void testParserUnknownDataTypes(String input, boolean nullable,
        boolean lowCardinality)
    {
        MidasColumnInfo info = MidasColumnInfo.parse(input, "columnName");
        assertEquals(info.getMidasDataType(), MidasDataType.Unknown);
        assertEquals(info.isNullable(), nullable);
        assertEquals(info.isLowCardinality(), lowCardinality);
    }

    @Test(dataProvider = "columnInfoParsingUnknownDataProvider")
    public void testParserUnknownArrayDataTypes(String input, boolean nullable,
        boolean lowCardinality)
    {
        MidasColumnInfo info = MidasColumnInfo.parse(
            "Array(" + input + ")", "columnName");
        assertEquals(info.getMidasDataType(), MidasDataType.Array);
        assertEquals(info.getArrayBaseType(), MidasDataType.Unknown);
        assertEquals(info.isNullable(), nullable);
        assertEquals(info.isLowCardinality(), lowCardinality);
    }

    @SuppressWarnings("boxing")
    @DataProvider(name = "columnInfoParsingUnknownDataProvider")
    public Object[][] provideUnknownDataTypeParserInput() {
        return new Object[][] {
            { "Nullable(NewPhantasyDataType(42))", true, false},
            { "NewPhantasyDataType", false, false},
            { "LowCardinality(NewPhantasyDataType)", false, true }
        };
    }

    @Test
    public void testCleanTypeName() {
        assertEquals(
            MidasColumnInfo.parse("Decimal(12,3)", "col").getCleanTypeName(),
            "Decimal(12,3)");
        assertEquals(
            MidasColumnInfo.parse("LowCardinality(Nullable(Decimal(12,3)))", "col").getCleanTypeName(),
            "Decimal(12,3)");
        assertEquals(
            MidasColumnInfo.parse("LowCardinality(Decimal(12,3))", "col").getCleanTypeName(),
            "Decimal(12,3)");
        assertEquals(
            MidasColumnInfo.parse("Nullable(Decimal(12,3))", "col").getCleanTypeName(),
            "Decimal(12,3)");
        assertEquals(
            MidasColumnInfo.parse("Array(LowCardinality(Nullable(Decimal(12,3))))", "col").getCleanTypeName(),
            "Array(Decimal(12,3))");
        assertEquals(
            MidasColumnInfo.parse("Array(Array(LowCardinality(Nullable(Decimal(12,3)))))", "col").getCleanTypeName(),
            "Array(Array(Decimal(12,3)))");
    }

    @Test(dataProvider = "columnInfoNullableTypeDefinitions")
    public void testTypeIsNullable(String typeDef, Boolean nullable) throws Exception {
        assertEquals(
            nullable.booleanValue(),
            MidasColumnInfo.parse(typeDef, "foo").isNullable());
    }

    @DataProvider(name = "columnInfoNullableTypeDefinitions")
    public Object[][] provideNullableTypeDefinitions() {
        return new Object[][] {
            {"DateTime", Boolean.FALSE},
            {"Float64", Boolean.FALSE},
            {"Nullable(Float64)", Boolean.TRUE},
            {"Nullable(DateTime)", Boolean.TRUE}
        };
    }

    @Test(dataProvider = "columnInfoScales")
    public void testGetScale(String typeDef, int scale) {
        assertEquals(
            MidasColumnInfo.parse(typeDef, "foo").getScale(),
            scale);
    }

    @DataProvider(name = "columnInfoScales")
    public Object[][] provideScaleTypeDefinitions() {
        return new Object[][] {
            {"DateTime", Integer.valueOf(0)},
            {"Int32", Integer.valueOf(0)},
            {"Array(String)", Integer.valueOf(0)},
            {"Nullable(Int32)", Integer.valueOf(0)},
            {"Nullable(DateTime)", Integer.valueOf(0)},
            {"Float64", Integer.valueOf(17)},
            {"Nullable(Float64)", Integer.valueOf(17)},
            {"Float32", Integer.valueOf(8)},
            {"Nullable(Float32)", Integer.valueOf(8)},
            {"Decimal(12,3)", Integer.valueOf(3)},
            {"Decimal(12, 3)", Integer.valueOf(3)},
            {"Decimal(12, )", Integer.valueOf(0)},
            {"Decimal(12, 0)", Integer.valueOf(0)},
            {"Nullable(Decimal(12,3)", Integer.valueOf(3)}
        };
    }

    @Test(dataProvider = "columnInfoPrecisions")
    public void testGetPrecision(String typeDef, int precision) {
        assertEquals(
            MidasColumnInfo.parse(typeDef, "foo").getPrecision(),
            precision);
    }


    @DataProvider(name = "columnInfoPrecisions")
    public Object[][] providePrecisionTypeDefinitions() {
        return new Object[][] {
            {"DateTime", Integer.valueOf(19)},
            {"DateTime('W-SU')", Integer.valueOf(19)},
            {"Date", Integer.valueOf(10)},
            {"UInt8", Integer.valueOf(3)},
            {"Int32", Integer.valueOf(11)},
            {"Float32", Integer.valueOf(8)},
            {"String", Integer.valueOf(0)},
            {"FixedString(12)", Integer.valueOf(12)},
            {"Enum8", Integer.valueOf(0)},
            {"Array(String)", Integer.valueOf(0)},
            {"Decimal(42,23)", Integer.valueOf(42)},
            {"Decimal(23)", Integer.valueOf(0)}, // illegal decimal def
            {"Nullable(Int32)", Integer.valueOf(11)},
            {"Nullable(DateTime)", Integer.valueOf(19)},
            {"Nullable(FixedString(4))", Integer.valueOf(4)},

        };
    }

}
