package cn.synway.bigdata.midas.domain;

/**
 * Input / Output formats supported by Midas
 * <p>
 * Note that the sole existence of a format in this enumeration does not mean
 * that its use is supported for any operation with this JDBC driver. When in
 * doubt, just omit any specific format and let the driver take care of it.
 * <p>
 *
 * @see <a href="https://Midas.yandex/docs/en/interfaces/formats">Midas Reference Documentation</a>
 *
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public enum MidasFormat {

    TabSeparated,
    TabSeparatedRaw,
    TabSeparatedWithNames,
    TabSeparatedWithNamesAndTypes,
    CSV,
    CSVWithNames,
    Values,
    Vertical,
    JSON,
    JSONCompact,
    JSONEachRow,
    TSKV,
    TSV,
    Pretty,
    PrettyCompact,
    PrettyCompactMonoBlock,
    PrettyNoEscapes,
    PrettySpace,
    Protobuf,
    RowBinary,
    Native,
    Null,
    XML,
    CapnProto;

    public static boolean containsFormat(String statement) {
        if (statement == null || statement.isEmpty()) {
            return false;
        }
        // TODO: Proper parsing of comments etc.
        String s = statement.replaceAll("[;\\s]", "");
        for (MidasFormat f : values()) {
            if (s.endsWith(f.name())) {
                return true;
            }
        }
        return false;
    }

}
