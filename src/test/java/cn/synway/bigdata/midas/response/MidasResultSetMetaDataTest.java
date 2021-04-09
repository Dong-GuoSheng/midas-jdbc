package cn.synway.bigdata.midas.response;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MidasResultSetMetaDataTest {


  @Test
  public void testIsNullable() throws SQLException {

    MidasResultSet resultSet = mock(MidasResultSet.class);
    MidasColumnInfo[] types = new MidasColumnInfo[] {
        MidasColumnInfo.parse("DateTime", "column1"),
        MidasColumnInfo.parse("Nullable(Float64)", "column2")
    };
    when(resultSet.getColumns()).thenReturn(Arrays.asList(types));
    MidasResultSetMetaData resultSetMetaData = new MidasResultSetMetaData(resultSet);
    Assert.assertEquals(resultSetMetaData.isNullable(1), ResultSetMetaData.columnNoNulls);
    Assert.assertEquals(resultSetMetaData.isNullable(2), ResultSetMetaData.columnNullable);
  }

    @Test
    public void testIsNullableColumnTypeName() throws SQLException {

        MidasResultSet resultSet = mock(MidasResultSet.class);
        when(resultSet.getColumns()).thenReturn(Collections.singletonList(
            MidasColumnInfo.parse("Nullable(Float64)", "column1")));
        MidasResultSetMetaData resultSetMetaData = new MidasResultSetMetaData(resultSet);
        Assert.assertEquals(resultSetMetaData.getColumnTypeName(1), "Float64");
    }

    @Test
    public void testIsNullableSigned() throws SQLException {
        MidasResultSet resultSet = mock(MidasResultSet.class);
        MidasColumnInfo[] types = new MidasColumnInfo[]{
            MidasColumnInfo.parse("Nullable(Float64)", "column1"),
            MidasColumnInfo.parse("Nullable(UInt64)", "column2"),
            MidasColumnInfo.parse("Nullable(UFantasy)", "column3")
        };
        when(resultSet.getColumns()).thenReturn(Arrays.asList(types));
        MidasResultSetMetaData resultSetMetaData = new MidasResultSetMetaData(
            resultSet);
        Assert.assertTrue(resultSetMetaData.isSigned(1));
        Assert.assertFalse(resultSetMetaData.isSigned(2));
        Assert.assertFalse(resultSetMetaData.isSigned(3));
    }

    @Test
    public void testDateTimeWithTimeZone() throws SQLException {
        MidasResultSet resultSet = mock(MidasResultSet.class);
        when(resultSet.getColumns()).thenReturn(Collections.singletonList(
            MidasColumnInfo.parse("DateTime('W-SU')", "column1")));
        MidasResultSetMetaData resultSetMetaData = new MidasResultSetMetaData(
            resultSet);
        Assert.assertEquals(resultSetMetaData.getColumnTypeName(1), "DateTime('W-SU')");
        Assert.assertEquals(resultSetMetaData.getColumnType(1), Types.TIMESTAMP);
    }

}