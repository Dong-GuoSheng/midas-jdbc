package cn.synway.bigdata.midas.domain;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.DataProvider;

public class MidasDataTypeTestDataProvider {

    private static MidasDataTypeTestData[] testData;

    @DataProvider(name = "MidasDataTypeStringsProvider")
    public static Object[][] provideSimpleDataTypes() {
        Object[][] myData = new Object[getTestData().length][2];
        for (int i = 0; i < getTestData().length; i++) {
            myData[i][0] = getTestData()[i].simpleTypeName;
            myData[i][1] = getTestData()[i].MidasDataType;
        }
        return myData;
    }

    public static List<MidasDataTypeTestData> provideDataTypes() {
        List<MidasDataTypeTestData> filtered = new ArrayList<MidasDataTypeTestData>();
        for (int i = 0; i < getTestData().length; i++) {
            MidasDataTypeTestData d = getTestData()[i];
            if (d.isCheckValue()) {
                filtered.add(d);
            }
        }
        return filtered;
    }

    private static MidasDataTypeTestData[] getTestData() {
        if (testData == null) {
            testData = initTestData();
        }
        return testData;
    }

    private static MidasDataTypeTestData[] initTestData() {
        return new MidasDataTypeTestData[] {
            create("IntervalYear", MidasDataType.IntervalYear, "IntervalYear", "42", true, true),
            create("IntervalQuarter", MidasDataType.IntervalQuarter, "IntervalQuarter", "42", true, true),
            create("IntervalDay", MidasDataType.IntervalDay, "IntervalDay", "42", true, true),
            create("IntervalWeek", MidasDataType.IntervalWeek, "IntervalWeek", "42", true, true),
            create("IntervalHour", MidasDataType.IntervalHour, "IntervalHour", "42", true, true),
            create("IntervalMinute", MidasDataType.IntervalMinute, "IntervalMinute", "42", true, true),
            create("Nested", MidasDataType.Nested),
            create("IntervalMonth", MidasDataType.IntervalMonth, "IntervalMonth", "42", true, true),
            create("Tuple", MidasDataType.Tuple, "Tuple(String, UInt32)", "('foo', 42)", true, true),
            create("AggregateFunction", MidasDataType.AggregateFunction),
            create("FixedString", MidasDataType.FixedString, "FixedString(6)", "FOOBAR", true, true),
            create("IntervalSecond", MidasDataType.IntervalSecond, "IntervalSecond", "42", true, true),
            create("UInt64", MidasDataType.UInt64, "UInt64", "42", true, true),
            create("Enum8", MidasDataType.Enum8, "Enum8(1 = 'foo', 2 = 'bar')", "foo", true, true),
            create("Int32", MidasDataType.Int32, "Int32", "-23", true, true),
            create("Int16", MidasDataType.Int16, "Int16", "-23", true, true),
            create("Int8", MidasDataType.Int8, "Int8", "-42", true, true),
            create("Date", MidasDataType.Date, "Date", "2019-05-02", true, true),
            create("UInt32", MidasDataType.UInt32, "UInt32", "42", true, true),
            create("UInt8", MidasDataType.UInt8, "UInt8", "23", true, true),
            create("Enum16", MidasDataType.Enum16, "Enum16(1 = 'foo', 2 = 'bar')", "foo", true, false),
            create("DateTime", MidasDataType.DateTime, "DateTime", "2019-05-02 13:37:00", true, true),
            create("UInt16", MidasDataType.UInt16, "UInt16", "42", true, true),
            create("Nothing", MidasDataType.Nothing),
            create("Array", MidasDataType.Array),
            create("Int64", MidasDataType.Int64, "Int64", "-42", true, true),
            create("Float32", MidasDataType.Float32, "Float32", "0.42", true, false),
            create("Float64", MidasDataType.Float64, "Float64", "-0.23", true, false),
            create("Decimal32", MidasDataType.Decimal32, "Decimal32(4)", "0.4242", true, false),
            create("Decimal64", MidasDataType.Decimal64, "Decimal64(4)", "1337.23", true, false),
            create("Decimal128", MidasDataType.Decimal128, "Decimal128(4)", "1337.23", true, false),
            create("UUID", MidasDataType.UUID, "UUID", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", true, false),
            create("String", MidasDataType.String, "String", "foo", true, true),
            create("Decimal", MidasDataType.Decimal, "Decimal(12,3)", "23.420", true, true),
            create("LONGBLOB", MidasDataType.String),
            create("MEDIUMBLOB", MidasDataType.String),
            create("TINYBLOB", MidasDataType.String),
            create("BIGINT", MidasDataType.Int64),
            create("SMALLINT", MidasDataType.Int16),
            create("TIMESTAMP", MidasDataType.DateTime),
            create("INTEGER", MidasDataType.Int32),
            create("INT", MidasDataType.Int32),
            create("DOUBLE", MidasDataType.Float64),
            create("MEDIUMTEXT", MidasDataType.String),
            create("TINYINT", MidasDataType.Int8),
            create("DEC", MidasDataType.Decimal),
            create("BINARY", MidasDataType.FixedString),
            create("FLOAT", MidasDataType.Float32),
            create("CHAR", MidasDataType.String),
            create("VARCHAR", MidasDataType.String),
            create("TEXT", MidasDataType.String),
            create("TINYTEXT", MidasDataType.String),
            create("LONGTEXT", MidasDataType.String),
            create("BLOB", MidasDataType.String),
            create("FANTASY", MidasDataType.Unknown, "Fantasy", "[42, 23]", true, true)
        };
    }

    private static MidasDataTypeTestData create(String simpleTypeName,
        MidasDataType MidasDataType, String typeName,
        String testValue, boolean nullableCandidate,
        boolean lowCardinalityCandidate)
    {
        return new MidasDataTypeTestData(simpleTypeName, MidasDataType,
            typeName, testValue, nullableCandidate, lowCardinalityCandidate);
    }

    private static MidasDataTypeTestData create(String simpleTypeName,
        MidasDataType MidasDataType)
    {
        return new MidasDataTypeTestData(simpleTypeName, MidasDataType,
            null, null, false, false);
    }

    public static final class MidasDataTypeTestData {

        private final String simpleTypeName;
        private final MidasDataType MidasDataType;
        private final String typeName;
        private final String testValue;
        private final boolean nullableCandidate;
        private final boolean lowCardinalityCandidate;

        MidasDataTypeTestData(String simpleTypeName,
            MidasDataType MidasDataType, String typeName,
            String testValue, boolean nullableCandidate,
            boolean lowCardinalityCandidate)
        {
            this.simpleTypeName = simpleTypeName;
            this.MidasDataType = MidasDataType;
            this.typeName = typeName;
            this.testValue = testValue;
            this.nullableCandidate = nullableCandidate;
            this.lowCardinalityCandidate = lowCardinalityCandidate;
        }

        private boolean isCheckValue() {
            return typeName != null;
        }

        public String getTypeName() {
            return typeName;
        }

        public boolean isNullableCandidate() {
            return nullableCandidate;
        }

        public boolean isLowCardinalityCandidate() {
            return lowCardinalityCandidate;
        }

        public String getTestValue() {
            return testValue;
        }

    }

}
