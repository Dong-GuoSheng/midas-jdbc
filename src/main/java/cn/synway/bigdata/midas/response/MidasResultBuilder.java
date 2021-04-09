package cn.synway.bigdata.midas.response;

import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.util.guava.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/**
 * For building MidasResultSet by hands
 */
public class MidasResultBuilder {

    private final int columnsNum;
    private List<String> names;
    private List<String> types;
    private List<List<?>> rows = new ArrayList<List<?>>();
    private TimeZone timezone = TimeZone.getTimeZone("UTC");
    private boolean usesWithTotals;
    private MidasProperties properties = new MidasProperties();

    public static MidasResultBuilder builder(int columnsNum) {
        return new MidasResultBuilder(columnsNum);
    }

    private MidasResultBuilder(int columnsNum) {
        this.columnsNum = columnsNum;
    }

    public MidasResultBuilder names(String... names) {
        return names(Arrays.asList(names));
    }

    public MidasResultBuilder types(String... types) {
        return types(Arrays.asList(types));
    }

    public MidasResultBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row));
    }

    public MidasResultBuilder withTotals(boolean usesWithTotals) {
        this.usesWithTotals = usesWithTotals;
        return this;
    }

    public MidasResultBuilder names(List<String> names) {
        if (names.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + names.size());
        this.names = names;
        return this;
    }

    public MidasResultBuilder types(List<String> types) {
        if (types.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + types.size());
        this.types = types;
        return this;
    }

    public MidasResultBuilder addRow(List<?> row) {
        if (row.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + row.size());
        rows.add(row);
        return this;
    }

    public MidasResultBuilder timeZone(TimeZone timezone) {
        this.timezone = timezone;
        return this;
    }

    public MidasResultBuilder properties(MidasProperties properties) {
        this.properties = properties;
        return this;
    }

    public MidasResultSet build() {
        try {
            if (names == null) throw new IllegalStateException("names == null");
            if (types == null) throw new IllegalStateException("types == null");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            appendRow(names, baos);
            appendRow(types, baos);
            for (List<?> row : rows) {
                appendRow(row, baos);
            }

            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            return new MidasResultSet(inputStream, 1024, "system", "unknown", usesWithTotals, null, timezone, properties);
        } catch (IOException e) {
            throw new RuntimeException("Never happens", e);
        }
    }

    private void appendRow(List<?> row, ByteArrayOutputStream baos) throws IOException {
        for (int i = 0; i < row.size(); i++) {
            if (i != 0) baos.write('\t');
            appendObject(row.get(i), baos);
        }
        baos.write('\n');
    }

    private void appendObject(Object o, ByteArrayOutputStream baos) throws IOException {
        if (o == null) {
            baos.write('\\');
            baos.write('N');
        } else {
            String value;
            if (o instanceof Boolean) {
                if ((Boolean) o) {
                    value = "1";
                } else {
                    value = "0";
                }
            } else {
                value = o.toString();
            }
            ByteFragment.escape(value.getBytes(StreamUtils.UTF_8), baos);
        }
    }

}
