package cn.synway.bigdata.midas.domain;

import java.util.Locale;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MidasDataTypeTest {

    @Test(
        dataProvider = "MidasDataTypeStringsProvider",
        dataProviderClass = MidasDataTypeTestDataProvider.class
    )
    public void testFromDataTypeStringSimpleTypes(String typeName, MidasDataType result) {
        assertEquals(
            MidasDataType.fromTypeString(typeName),
            result,
            typeName);
        assertEquals(
            MidasDataType.fromTypeString(typeName.toUpperCase(Locale.ROOT)),
            result);
        assertEquals(
            MidasDataType.fromTypeString(typeName.toLowerCase(Locale.ROOT)),
            result);
    }

}
