package cn.synway.bigdata.midas;

import java.time.Duration;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.util.MidasVersionNumberUtil;

import static java.time.temporal.ChronoUnit.SECONDS;

public class MidasContainerForTest {
    private static final int HTTP_PORT = 8123;
    private static final int NATIVE_PORT = 9000;
    private static final int MYSQL_PORT = 3306;

    private static final String MidasVersion;
    private static final GenericContainer<?> MidasContainer;

    static {
        String imageTag = System.getProperty("MidasVersion");
        if (imageTag == null || (imageTag = imageTag.trim()).isEmpty()) {
            MidasVersion = imageTag = "";
        } else {
            if (MidasVersionNumberUtil.getMajorVersion(imageTag) == 0) {
                MidasVersion = "";
            } else {
                MidasVersion = imageTag;
            }
            imageTag = ":" + imageTag;
        }
        MidasContainer = new GenericContainer<>("yandex/Midas-server" + imageTag)
                .withExposedPorts(HTTP_PORT, NATIVE_PORT, MYSQL_PORT)
                .withClasspathResourceMapping(
                    "ru/yandex/Midas/users.d",
                    "/etc/Midas-server/users.d",
                    BindMode.READ_ONLY)
                .waitingFor(Wait.forHttp("/ping").forPort(HTTP_PORT).forStatusCode(200)
                    .withStartupTimeout(Duration.of(60, SECONDS)));

    }

    public static String getMidasVersion() {
        return MidasVersion;
    }

    public static GenericContainer<?> getMidasContainer() {
        return MidasContainer;
    }

    public static String getMidasHttpAddress() {
        return getMidasHttpAddress(false);
    }

    public static String getMidasHttpAddress(boolean useIPaddress) {
        return new StringBuilder()
                .append(useIPaddress ? MidasContainer.getContainerIpAddress() : MidasContainer.getHost())
                .append(':').append(MidasContainer.getMappedPort(HTTP_PORT)).toString();
    }

    public static MidasDataSource newDataSource() {
        return newDataSource(new MidasProperties());
    }

    public static MidasDataSource newDataSource(MidasProperties properties) {
        return newDataSource("jdbc:midas://" + getMidasHttpAddress(), properties);
    }

    public static MidasDataSource newDataSource(String url) {
        return newDataSource(url, new MidasProperties());
    }

    public static MidasDataSource newDataSource(String url, MidasProperties properties) {
        String baseUrl = "jdbc:midas://" + getMidasHttpAddress();
        if (url == null) {
            url = baseUrl;
        } else if (!url.startsWith("jdbc:")) {
            url = baseUrl + "/" + url;
        }

        return new MidasDataSource(url, properties);
    }

    public static BalancedMidasDataSource newBalancedDataSource(String... addresses) {
        return newBalancedDataSource(new MidasProperties(), addresses);
    }

    public static BalancedMidasDataSource newBalancedDataSource(MidasProperties properties,
            String... addresses) {
        return newBalancedDataSourceWithSuffix(null, properties, addresses);
    }

    public static BalancedMidasDataSource newBalancedDataSourceWithSuffix(String urlSuffix,
            MidasProperties properties, String... addresses) {
        StringBuilder url = new StringBuilder().append("jdbc:midas://");
        if (addresses == null || addresses.length == 0) {
            url.append(getMidasHttpAddress());
        } else {
            int position = url.length();
            for (int i = 0; i < addresses.length; i++) {
                url.append(',').append(addresses[i]);
            }
            url.deleteCharAt(position);
        }

        if (urlSuffix != null) {
            url.append('/').append(urlSuffix);
        }

        return new BalancedMidasDataSource(url.toString(), properties);
    }

    @BeforeSuite()
    public static void beforeSuite() {
        MidasContainer.start();
    }

    @AfterSuite()
    public static void afterSuite() {
        MidasContainer.stop();
    }
}
