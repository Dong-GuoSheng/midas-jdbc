package cn.synway.bigdata.midas;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.settings.MidasProperties;

public class MidasJdbcUrlParserTest {

    @Test
    public void testParseDashes() throws Exception {
        Properties props = new Properties();
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337/db-name-with-dash", new Properties());
        Assert.assertEquals(chProps.getDatabase(), "db-name-with-dash");
    }

    @Test
    public void testParseTrailingSlash() throws Exception {
        Properties props = new Properties();
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337/", new Properties());
        Assert.assertEquals(chProps.getDatabase(), "default");
    }

    @Test
    public void testParseDbInPathAndProps() throws Exception {
        MidasProperties props = new MidasProperties();
        props.setDatabase("database-name");
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337/database-name", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "database-name");
        Assert.assertEquals(chProps.getPath(), "/");
    }

    @Test
    public void testParseDbInPathAndProps2() throws Exception {
        MidasProperties props = new MidasProperties();
        props.setDatabase("database-name");
        props.setUsePathAsDb(false);
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337/database-name", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "database-name");
        Assert.assertEquals(chProps.getPath(), "/database-name");
    }

    @Test
    public void testParsePathDefaultDb() throws Exception {
        MidasProperties props = new MidasProperties();
        props.setPath("/path");
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337/", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "default");
        Assert.assertEquals(chProps.getPath(), "/path");
    }

    @Test
    public void testParsePathDefaultDb2() throws Exception {
        MidasProperties props = new MidasProperties();
        props.setPath("/path");
        props.setUsePathAsDb(false);
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "default");
        Assert.assertEquals(chProps.getPath(), "/"); //uri takes priority
    }

    @Test
    public void testParsePathAndDb() throws Exception {
        MidasProperties props = new MidasProperties();
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337/db?database=dbname", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "db");
        Assert.assertEquals(chProps.getPath(), "/");
    }

    @Test
    public void testParsePathAndDb2() throws Exception {
        MidasProperties props = new MidasProperties();
        props.setUsePathAsDb(false);
        MidasProperties chProps = MidasJdbcUrlParser.parse(
            "jdbc:midas://foo.yandex:1337/db?database=dbname", props.asProperties());
        Assert.assertEquals(chProps.getDatabase(), "dbname");
        Assert.assertEquals(chProps.getPath(), "/db");
    }

}
