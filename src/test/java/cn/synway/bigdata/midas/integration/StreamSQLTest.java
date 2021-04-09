package cn.synway.bigdata.midas.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import cn.synway.bigdata.midas.MidasConnection;
import cn.synway.bigdata.midas.MidasContainerForTest;
import cn.synway.bigdata.midas.MidasDataSource;
import cn.synway.bigdata.midas.domain.MidasCompression;
import cn.synway.bigdata.midas.domain.MidasFormat;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.GZIPOutputStream;

public class StreamSQLTest {
    private MidasDataSource dataSource;
    private MidasConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        dataSource = MidasContainerForTest.newDataSource();
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
    public void simpleCSVInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.csv_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE test.csv_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().
                write()
                .sql("insert into test.csv_stream_sql format CSV")
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.csv_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    private InputStream getTSVStream(final int rowsCount) {
        return new InputStream() {
            private int si = 0;
            private String s = "";
            private int i = 0;

            private boolean genNextString() {
                if (i >= rowsCount) return false;
                si = 0;
                s = String.format("%d\txxxx%d\n", 1, i);
                i++;
                return true;
            }

            public int read() {
                if (si >= s.length()) {
                    if ( ! genNextString() ) {
                        return -1;
                    }
                }
                return s.charAt( si++ );
            }
        };
    }

    @Test
    public void multiRowTSVInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.tsv_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE test.tsv_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        final int rowsCount = 100000;

        connection.createStatement().
                write()
                .sql("insert into test.tsv_stream_sql format TSV")
                .data(getTSVStream(rowsCount), MidasFormat.TSV)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.tsv_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), rowsCount);
        Assert.assertEquals(rs.getInt("sum"), rowsCount);
        Assert.assertEquals(rs.getInt("uniq"), rowsCount);
    }

    private InputStream gzStream( InputStream is ) throws IOException
    {
        final int bufferSize = 16384;
        byte data[] = new byte[bufferSize];
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(os);
        BufferedInputStream es = new BufferedInputStream(is, bufferSize);
        int count;
        while ( ( count = es.read( data, 0, bufferSize) ) != -1 )
            gzipOutputStream.write( data, 0, count );
        es.close();
        gzipOutputStream.close();

        return new ByteArrayInputStream( os.toByteArray() );
    }

    @Test
    public void multiRowTSVInsertCompressed() throws SQLException, IOException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.tsv_compressed_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE test.tsv_compressed_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        final int rowsCount = 100000;

        InputStream gz = gzStream(getTSVStream(rowsCount));
        connection.createStatement().
                write()
                .sql("insert into test.tsv_compressed_stream_sql format TSV")
                .data(gz, MidasFormat.TSV, MidasCompression.gzip)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.tsv_compressed_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), rowsCount);
        Assert.assertEquals(rs.getInt("sum"), rowsCount);
        Assert.assertEquals(rs.getInt("uniq"), rowsCount);
    }


    @Test
    public void JSONEachRowInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.json_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE test.json_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "{\"value\":5,\"string_value\":\"6\"}\n{\"value\":1,\"string_value\":\"6\"}";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().
                write()
                .sql("insert into test.json_stream_sql")
                .data(inputStream, MidasFormat.JSONEachRow)
                .data(inputStream)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.json_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void JSONEachRowCompressedInsert() throws SQLException, IOException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.json_comressed_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE test.json_comressed_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "{\"value\":5,\"string_value\":\"6\"}\n{\"value\":1,\"string_value\":\"6\"}";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().
                write()
                .sql("insert into test.json_comressed_stream_sql")
                .data(inputStream, MidasFormat.JSONEachRow)
                .data(gzStream(inputStream))
                .dataCompression(MidasCompression.gzip)
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.json_comressed_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void CSVInsertCompressedIntoTable() throws SQLException, IOException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.csv_stream_compressed");
        connection.createStatement().execute(
                "CREATE TABLE test.csv_stream_compressed (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().
                write()
                .table("test.csv_stream_compressed")
                .format(MidasFormat.CSV)
                .dataCompression(MidasCompression.gzip)
                .data(gzStream(inputStream))
                .send();

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.csv_stream_compressed");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

}
