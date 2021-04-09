package cn.synway.bigdata.midas;


import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.impl.client.HttpClientBuilder;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.settings.MidasQueryParam;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MidasStatementTest {
    @Test
    public void testClickhousify() throws Exception {
        String sql = "SELECT ololo FROM ololoed;";
        assertEquals(MidasStatementImpl.clickhousifySql(sql), "SELECT ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql2 = "SELECT ololo FROM ololoed";
        assertEquals(MidasStatementImpl.clickhousifySql(sql2), "SELECT ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql3 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(MidasStatementImpl.clickhousifySql(sql3), "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes");

        String sql4 = "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;";
        assertEquals(MidasStatementImpl.clickhousifySql(sql4), "SELECT ololo FROM ololoed FORMAT TabSeparatedWithNamesAndTypes;");

        String sql5 = "SHOW ololo FROM ololoed;";
        assertEquals(MidasStatementImpl.clickhousifySql(sql5), "SHOW ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql6 = " show ololo FROM ololoed;";
        assertEquals(MidasStatementImpl.clickhousifySql(sql6), "show ololo FROM ololoed\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql7 = "SELECT ololo FROM ololoed \nFORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(MidasStatementImpl.clickhousifySql(sql7), "SELECT ololo FROM ololoed \nFORMAT TabSeparatedWithNamesAndTypes");

        String sql8 = "SELECT ololo FROM ololoed \n\n FORMAT TabSeparatedWithNamesAndTypes";
        assertEquals(MidasStatementImpl.clickhousifySql(sql8), "SELECT ololo FROM ololoed \n\n FORMAT TabSeparatedWithNamesAndTypes");

        String sql9 = "SELECT ololo FROM ololoed\n-- some comments one line";
        assertEquals(MidasStatementImpl.clickhousifySql(sql9), "SELECT ololo FROM ololoed\n-- some comments one line\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql10 = "SELECT ololo FROM ololoed\n-- some comments\ntwo line";
        assertEquals(MidasStatementImpl.clickhousifySql(sql10), "SELECT ololo FROM ololoed\n-- some comments\ntwo line\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql11 = "SELECT ololo FROM ololoed/*\nsome comments\ntwo line*/";
        assertEquals(MidasStatementImpl.clickhousifySql(sql11), "SELECT ololo FROM ololoed/*\nsome comments\ntwo line*/\nFORMAT TabSeparatedWithNamesAndTypes;");

        String sql12 = "SELECT ololo FROM ololoed\n// c style some comments one line";
        assertEquals(MidasStatementImpl.clickhousifySql(sql12), "SELECT ololo FROM ololoed\n// c style some comments one line\nFORMAT TabSeparatedWithNamesAndTypes;");

    }

    @Test
    public void testCredentials() throws SQLException, URISyntaxException {
        MidasProperties properties = new MidasProperties(new Properties());
        MidasProperties withCredentials = properties.withCredentials("test_user", "test_password");
        assertTrue(withCredentials != properties);
        assertNull(properties.getUser());
        assertNull(properties.getPassword());
        assertEquals(withCredentials.getUser(), "test_user");
        assertEquals(withCredentials.getPassword(), "test_password");

        MidasStatementImpl statement = new MidasStatementImpl(
            HttpClientBuilder.create().build(),
            null, withCredentials, ResultSet.TYPE_FORWARD_ONLY);

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        // we use Basic AUTH nowadays
        assertFalse(query.contains("password=test_password"));
        assertFalse(query.contains("user=test_user"));
    }

    @Test
    public void testMaxExecutionTime() throws Exception {
        MidasProperties properties = new MidasProperties();
        properties.setMaxExecutionTime(20);
        MidasStatementImpl statement = new MidasStatementImpl(HttpClientBuilder.create().build(),
            null, properties, ResultSet.TYPE_FORWARD_ONLY);
        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("max_execution_time=20"), "max_execution_time param is missing in URL");

        statement.setQueryTimeout(10);
        uri = statement.buildRequestUri(null, null, null, null, false);
        query = uri.getQuery();
        assertTrue(query.contains("max_execution_time=10"), "max_execution_time param is missing in URL");
    }

    @Test
    public void testMaxMemoryUsage() throws Exception {
        MidasProperties properties = new MidasProperties();
        properties.setMaxMemoryUsage(41L);
        MidasStatementImpl statement = new MidasStatementImpl(HttpClientBuilder.create().build(),
           null, properties, ResultSet.TYPE_FORWARD_ONLY);

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        String query = uri.getQuery();
        assertTrue(query.contains("max_memory_usage=41"), "max_memory_usage param is missing in URL");
    }

    @Test
    public void testAdditionalRequestParams() {
        MidasProperties properties = new MidasProperties();
        MidasStatementImpl statement = new MidasStatementImpl(
                HttpClientBuilder.create().build(),
                null,
                properties,
                ResultSet.TYPE_FORWARD_ONLY
        );

        statement.option("cache_namespace", "aaaa");
        URI uri = statement.buildRequestUri(
                null,
                null,
                null,
                null,
                false
        );
        String query = uri.getQuery();
        assertTrue(query.contains("cache_namespace=aaaa"), "cache_namespace param is missing in URL");

        uri = statement.buildRequestUri(
                null,
                null,
                null,
                ImmutableMap.of("cache_namespace", "bbbb"),
                false
        );
        query = uri.getQuery();
        assertTrue(query.contains("cache_namespace=bbbb"), "cache_namespace param is missing in URL");

        // check that statement level params are given to Writer
        assertEquals(statement.write().getRequestParams().get("cache_namespace"), "aaaa");
    }

    @Test
    public void testAdditionalDBParams() {
        MidasProperties properties = new MidasProperties();
        properties.setMaxThreads(1);

        MidasStatementImpl statement = new MidasStatementImpl(
                HttpClientBuilder.create().build(),
                null,
                properties,
                ResultSet.TYPE_FORWARD_ONLY
        );

        URI uri = statement.buildRequestUri(null, null, null, null, false);
        assertTrue(uri.getQuery().contains("max_threads=1"));

        // override on statement level
        statement.addDbParam(MidasQueryParam.MAX_THREADS, "2");

        uri = statement.buildRequestUri(null, null, null, null, false);
        assertTrue(uri.getQuery().contains("max_threads=2"));

        // override on method level
        uri = statement.buildRequestUri(null, null, Collections.singletonMap(MidasQueryParam.MAX_THREADS, "3"), null, false);
        assertTrue(uri.getQuery().contains("max_threads=3"));

        // check that statement level params are given to Writer
        assertEquals(statement.write().getAdditionalDBParams().get(MidasQueryParam.MAX_THREADS), "2");
    }

    @Test
    public void testIsSelect() {
        assertTrue(MidasStatementImpl.isSelect("SELECT 42"));
        assertTrue(MidasStatementImpl.isSelect("select 42"));
        assertFalse(MidasStatementImpl.isSelect("selectfoo"));
        assertTrue(MidasStatementImpl.isSelect("  SELECT foo"));
        assertTrue(MidasStatementImpl.isSelect("WITH foo"));
        assertTrue(MidasStatementImpl.isSelect("DESC foo"));
        assertTrue(MidasStatementImpl.isSelect("EXISTS foo"));
        assertTrue(MidasStatementImpl.isSelect("SHOW foo"));
        assertTrue(MidasStatementImpl.isSelect("-- foo\n SELECT 42"));
        assertTrue(MidasStatementImpl.isSelect("--foo\n SELECT 42"));
        assertFalse(MidasStatementImpl.isSelect("- foo\n SELECT 42"));
        assertTrue(MidasStatementImpl.isSelect("/* foo */ SELECT 42"));
        assertTrue(MidasStatementImpl.isSelect("/*\n * foo\n*/\n SELECT 42"));
        assertFalse(MidasStatementImpl.isSelect("/ foo */ SELECT 42"));
        assertFalse(MidasStatementImpl.isSelect("-- SELECT baz\n UPDATE foo"));
        assertFalse(MidasStatementImpl.isSelect("/* SELECT baz */\n UPDATE foo"));
        assertFalse(MidasStatementImpl.isSelect("/*\n UPDATE foo"));
        assertFalse(MidasStatementImpl.isSelect("/*"));
        assertFalse(MidasStatementImpl.isSelect("/**/"));
        assertFalse(MidasStatementImpl.isSelect(" --"));
        assertTrue(MidasStatementImpl.isSelect("explain select 42"));
        assertTrue(MidasStatementImpl.isSelect("EXPLAIN select 42"));
        assertFalse(MidasStatementImpl.isSelect("--EXPLAIN select 42\n alter"));
        assertTrue(MidasStatementImpl.isSelect("--\nEXPLAIN select 42"));
        assertTrue(MidasStatementImpl.isSelect("/*test*/ EXPLAIN select 42"));
    }

}
