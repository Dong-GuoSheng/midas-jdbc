package cn.synway.bigdata.midas.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import cn.synway.bigdata.midas.MidasConnection;
import cn.synway.bigdata.midas.MidasContainerForTest;
import cn.synway.bigdata.midas.MidasDataSource;
import cn.synway.bigdata.midas.MidasStatement;
import cn.synway.bigdata.midas.util.MidasRowBinaryStream;
import cn.synway.bigdata.midas.util.MidasStreamCallback;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;

import static org.testng.Assert.assertEquals;

public class NativeStreamTest {

    private MidasDataSource dataSource;
    private MidasConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        dataSource = MidasContainerForTest.newDataSource();
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
    public void testLowCardinality() throws Exception{
        final MidasStatement statement = connection.createStatement();
        connection.createStatement().execute("DROP TABLE IF EXISTS test.low_cardinality");
        connection.createStatement().execute(
            "CREATE TABLE test.low_cardinality (date Date, lowCardinality LowCardinality(String), string String) ENGINE = MergeTree(date, (date), 8192)"
        );

        // Code: 368, e.displayText() = DB::Exception: Bad cast from type DB::ColumnString to DB::ColumnLowCardinality
        if (connection.getMetaData().getDatabaseMajorVersion() <= 19) {
            return;
        }

        final Date date1 = new Date(1497474018000L);

        statement.sendNativeStream(
            "INSERT INTO test.low_cardinality (date, lowCardinality, string)",
            new MidasStreamCallback() {
                @Override
                public void writeTo(MidasRowBinaryStream stream) throws IOException {
                    stream.writeUnsignedLeb128(3); // Columns number
                    stream.writeUnsignedLeb128(1); // Rows number

                    stream.writeString("date"); // Column name
                    stream.writeString("Date");  // Column type
                    stream.writeDate(date1);  // value

                    stream.writeString("lowCardinality"); // Column name
                    stream.writeString("String");  // Column type
                    stream.writeString("string");  // value

                    stream.writeString("string"); // Column name
                    stream.writeString("String");  // Column type
                    stream.writeString("string");  // value
                }
            }
        );

        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM test.low_cardinality");

        Assert.assertTrue(rs.next());
        assertEquals(rs.getString("lowCardinality"), "string");
        assertEquals(rs.getString("string"), "string");
    }
}
