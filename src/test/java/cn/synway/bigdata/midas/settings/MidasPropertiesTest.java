package cn.synway.bigdata.midas.settings;

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import cn.synway.bigdata.midas.MidasJdbcUrlParser;
import org.testng.Assert;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.BalancedMidasDataSource;
import cn.synway.bigdata.midas.MidasDataSource;

import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class MidasPropertiesTest {

    /**
     * Method {@link MidasJdbcUrlParser#parseUriQueryPart(URI, Properties)} returns instance
     * of {@link Properties} with defaults. These defaults may be missed if method
     * {@link java.util.Hashtable#get(Object)} is used for {@code Properties}.
     */
    @Test
    public void constructorShouldNotIgnoreDefaults() {
        Properties defaults = new Properties();
        String expectedUsername = "superuser";
        defaults.setProperty("user", expectedUsername);
        Properties propertiesWithDefaults = new Properties(defaults);

        MidasProperties MidasProperties = new MidasProperties(propertiesWithDefaults);
        Assert.assertEquals(MidasProperties.getUser(), expectedUsername);
    }

    @Test
    public void constructorShouldNotIgnoreMidasProperties() {
        int expectedConnectionTimeout = 1000;
        boolean isCompress = false;
        Integer maxParallelReplicas = 3;
        Integer maxPartitionsPerInsertBlock = 200;
        Long maxInsertBlockSize = 142L;
        Boolean insertDeduplicate = true;
        Boolean insertDistributedSync = true;
        Boolean anyJoinDistinctRightTableKeys = true;

        MidasProperties properties = new MidasProperties();
        properties.setConnectionTimeout( expectedConnectionTimeout );
        properties.setMaxParallelReplicas( maxParallelReplicas );
        properties.setMaxPartitionsPerInsertBlock( maxPartitionsPerInsertBlock );
        properties.setCompress( isCompress );
        properties.setMaxInsertBlockSize(maxInsertBlockSize);
        properties.setInsertDeduplicate(insertDeduplicate);
        properties.setInsertDistributedSync(insertDistributedSync);
        properties.setAnyJoinDistinctRightTableKeys(anyJoinDistinctRightTableKeys);

        MidasDataSource MidasDataSource = new MidasDataSource(
                "jdbc:midas://localhost:8123/test",
                properties
        );
        Assert.assertEquals(
                MidasDataSource.getProperties().getConnectionTimeout(),
                expectedConnectionTimeout
        );
        Assert.assertEquals(
                MidasDataSource.getProperties().isCompress(),
                isCompress
        );
        Assert.assertEquals(
                MidasDataSource.getProperties().getMaxParallelReplicas(),
                maxParallelReplicas
        );
        Assert.assertEquals(
                MidasDataSource.getProperties().getMaxPartitionsPerInsertBlock(),
                maxPartitionsPerInsertBlock
        );
        Assert.assertEquals(
                MidasDataSource.getProperties().getTotalsMode(),
                MidasQueryParam.TOTALS_MODE.getDefaultValue()
        );
        Assert.assertEquals(
            MidasDataSource.getProperties().getMaxInsertBlockSize(),
            maxInsertBlockSize
        );
        Assert.assertEquals(
            MidasDataSource.getProperties().getInsertDeduplicate(),
            insertDeduplicate
        );
        Assert.assertEquals(
            MidasDataSource.getProperties().getInsertDistributedSync(),
            insertDistributedSync
        );
        Assert.assertEquals(
            MidasDataSource.getProperties().getAnyJoinDistinctRightTableKeys(),
            anyJoinDistinctRightTableKeys
        );
    }

    @Test
    public void additionalParametersTest_Midas_datasource() {
        MidasDataSource MidasDataSource = new MidasDataSource("jdbc:midas://localhost:1234/ppc?compress=1&decompress=1&user=root");

        assertTrue(MidasDataSource.getProperties().isCompress());
        assertTrue(MidasDataSource.getProperties().isDecompress());
        assertEquals("root", MidasDataSource.getProperties().getUser());
    }

    @Test
    public void additionalParametersTest_balanced_Midas_datasource() {
        BalancedMidasDataSource MidasDataSource = new BalancedMidasDataSource("jdbc:midas://localhost:1234,another.host.com:4321/ppc?compress=1&decompress=1&user=root");

        assertTrue(MidasDataSource.getProperties().isCompress());
        assertTrue(MidasDataSource.getProperties().isDecompress());
        assertEquals("root", MidasDataSource.getProperties().getUser());
    }

    @Test
    public void booleanParamCanBeParsedAsZeroAndOne() throws Exception {
        Assert.assertTrue(new MidasProperties().isCompress());
        Assert.assertFalse(new MidasProperties(new Properties(){{setProperty("compress", "0");}}).isCompress());
        Assert.assertTrue(new MidasProperties(new Properties(){{setProperty("compress", "1");}}).isCompress());
    }

    @Test
    public void MidasQueryParamContainsMaxMemoryUsage() throws Exception {
        final MidasProperties MidasProperties = new MidasProperties();
        MidasProperties.setMaxMemoryUsage(43L);
        Assert.assertEquals(MidasProperties.asProperties().getProperty("max_memory_usage"), "43");
    }

    @Test
    public void maxMemoryUsageParamShouldBeParsed() throws Exception {
        final Properties driverProperties = new Properties();
        driverProperties.setProperty("max_memory_usage", "42");

        MidasDataSource ds = new MidasDataSource("jdbc:midas://localhost:8123/test", driverProperties);
        Assert.assertEquals(ds.getProperties().getMaxMemoryUsage(), Long.valueOf(42L), "max_memory_usage is missing");
    }

    @Test
    public void buildQueryParamsTest() {
        MidasProperties MidasProperties = new MidasProperties();
        MidasProperties.setInsertQuorumTimeout(1000L);
        MidasProperties.setInsertQuorum(3L);
        MidasProperties.setSelectSequentialConsistency(1L);
        MidasProperties.setMaxInsertBlockSize(42L);
        MidasProperties.setInsertDeduplicate(true);
        MidasProperties.setInsertDistributedSync(true);
        MidasProperties.setUser("myUser");
        MidasProperties.setPassword("myPassword");

        Map<MidasQueryParam, String> MidasQueryParams = MidasProperties.buildQueryParams(true);
        Assert.assertEquals(MidasQueryParams.get(MidasQueryParam.INSERT_QUORUM), "3");
        Assert.assertEquals(MidasQueryParams.get(MidasQueryParam.INSERT_QUORUM_TIMEOUT), "1000");
        Assert.assertEquals(MidasQueryParams.get(MidasQueryParam.SELECT_SEQUENTIAL_CONSISTENCY), "1");
        Assert.assertEquals(MidasQueryParams.get(MidasQueryParam.MAX_INSERT_BLOCK_SIZE), "42");
        Assert.assertEquals(MidasQueryParams.get(MidasQueryParam.INSERT_DEDUPLICATE), "1");
        Assert.assertEquals(MidasQueryParams.get(MidasQueryParam.INSERT_DISTRIBUTED_SYNC), "1");
        assertFalse(MidasQueryParams.containsKey(MidasQueryParam.USER));
        assertFalse(MidasQueryParams.containsKey(MidasQueryParam.PASSWORD));
    }

    @Test
    public void mergeMidasPropertiesTest() {
        MidasProperties MidasProperties1 = new MidasProperties();
        MidasProperties MidasProperties2 = new MidasProperties();
        MidasProperties1.setDatabase("click");
        MidasProperties1.setConnectionTimeout(13000);
        MidasProperties2.setSocketTimeout(15000);
        MidasProperties2.setUser("readonly");
        final MidasProperties merged = MidasProperties1.merge(MidasProperties2);
        // merge equals: MidasProperties1 overwrite with MidasProperties2's value or default not null value
        Assert.assertEquals(merged.getDatabase(),"click"); // using properties1, because properties1 not setting and
        // default value is null
        Assert.assertEquals(merged.getConnectionTimeout(),MidasConnectionSettings.CONNECTION_TIMEOUT.getDefaultValue());// overwrite with properties2's default value
        Assert.assertEquals(merged.getSocketTimeout(),15000);// using properties2
        Assert.assertEquals(merged.getUser(),"readonly"); // using properties2
    }

    @Test
    public void mergePropertiesTest() {
        MidasProperties MidasProperties1 = new MidasProperties();
        Properties properties2 = new Properties();
        MidasProperties1.setDatabase("click");
        MidasProperties1.setMaxThreads(8);
        MidasProperties1.setConnectionTimeout(13000);
        properties2.put(MidasConnectionSettings.SOCKET_TIMEOUT.getKey(), "15000");
        properties2.put(MidasQueryParam.DATABASE.getKey(), "house");
        final MidasProperties merged = MidasProperties1.merge(properties2);
        // merge equals: MidasProperties1 overwrite with properties in properties2 not including default value
        Assert.assertEquals( merged.getDatabase(),"house");// overwrite with properties2
        Assert.assertEquals(merged.getMaxThreads().intValue(),8);// using properties1
        Assert.assertEquals(merged.getConnectionTimeout(),13000);// using properties1
        Assert.assertEquals(merged.getSocketTimeout(),15000);// using properties2
    }
}
