package cn.synway.bigdata.midas.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Very simple version number parser. It is only needed for Midas driver
 * and database version numbers
 */
public final class MidasVersionNumberUtil {

    private static final Pattern VERSION_NUMBER_PATTERN =
        Pattern.compile("^\\s*(\\d+)\\.(\\d+).*");

    public static int getMajorVersion(String versionNumber) {
        Matcher m = VERSION_NUMBER_PATTERN.matcher(versionNumber);
        return m.matches() ? Integer.parseInt(m.group(1)) : 0;
    }

    public static int getMinorVersion(String versionNumber) {
        Matcher m = VERSION_NUMBER_PATTERN.matcher(versionNumber);
        return m.matches() ? Integer.parseInt(m.group(2)) : 0;
    }

    private MidasVersionNumberUtil() { /* do not instantiate util */ }
}
