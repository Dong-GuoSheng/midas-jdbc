package cn.synway.bigdata.midas;

import cn.synway.bigdata.midas.domain.MidasCompression;
import cn.synway.bigdata.midas.domain.MidasFormat;
import cn.synway.bigdata.midas.util.MidasStreamCallback;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;
import cn.synway.bigdata.midas.util.MidasStreamHttpEntity;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

public class Writer extends ConfigurableApi<Writer> {

    private MidasFormat format = MidasFormat.TabSeparated;
    private MidasCompression compression = null;
    private String table = null;
    private String sql = null;
    private InputStreamProvider streamProvider = null;

    Writer(MidasStatementImpl statement) {
        super(statement);
    }

    /**
     * Specifies format for further insert of data via send()
     */
    public Writer format(MidasFormat format) {
        if (null == format) {
            throw new NullPointerException("Format can not be null");
        }
        this.format = format;
        return this;
    }

    /**
     * Set table name for data insertion
     *
     * @param table table name
     * @return this
     */
    public Writer table(String table) {
        this.sql = null;
        this.table = table;
        return this;
    }

    /**
     * Set SQL for data insertion
     *
     * @param sql in a form "INSERT INTO table_name [(X,Y,Z)] VALUES "
     * @return this
     */
    public Writer sql(String sql) {
        this.sql = sql;
        this.table = null;
        return this;
    }

    /**
     * Specifies data input stream
     */
    public Writer data(InputStream stream) {
        streamProvider = new HoldingInputProvider(stream);
        return this;
    }

    public Writer data(InputStream stream, MidasFormat format) {
        return format(format).data(stream);
    }

    /**
     * Shortcut method for specifying a file as an input
     */
    public Writer data(File input) {
        streamProvider = new FileInputProvider(input);
        return this;
    }

    public Writer data(InputStream stream, MidasFormat format, MidasCompression compression) {
        return dataCompression(compression).format(format).data(stream);
    }

    public Writer data(File input, MidasFormat format, MidasCompression compression) {
        return dataCompression(compression).format(format).data(input);
    }

    public Writer dataCompression(MidasCompression compression) {
        if (null == compression) {
            throw new NullPointerException("Compression can not be null");
        }
        this.compression = compression;
        return this;
    }

    public Writer data(File input, MidasFormat format) {
        return format(format).data(input);
    }

    /**
     * Method to call, when Writer is fully configured
     */
    public void send() throws SQLException {
        HttpEntity entity;
        try {
            InputStream stream;
            if (null == streamProvider || null == (stream = streamProvider.get())) {
                throw new IOException("No input data specified");
            }
            entity = new InputStreamEntity(stream);
        } catch (IOException err) {
            throw new SQLException(err);
        }
        send(entity);
    }

    private void send(HttpEntity entity) throws SQLException {
        statement.sendStream(this, entity);
    }

    /**
     * Allows to send stream of data to Midas
     *
     * @param sql    in a form of "INSERT INTO table_name (X,Y,Z) VALUES "
     * @param data   where to read data from
     * @param format format of data in InputStream
     * @throws SQLException
     */
    public void send(String sql, InputStream data, MidasFormat format) throws SQLException {
        sql(sql).data(data).format(format).send();
    }

    /**
     * Convenient method for importing the data into table
     *
     * @param table  table name
     * @param data   source data
     * @param format format of data in InputStream
     * @throws SQLException
     */
    public void sendToTable(String table, InputStream data, MidasFormat format) throws SQLException {
        table(table).data(data).format(format).send();
    }

    /**
     * Sends the data in RowBinary or in Native formats
     */
    public void send(String sql, MidasStreamCallback callback, MidasFormat format) throws SQLException {
        if (!(MidasFormat.RowBinary.equals(format) || MidasFormat.Native.equals(format))) {
            throw new SQLException("Wrong binary format - only RowBinary and Native are supported");
        }

        format(format).sql(sql).send(new MidasStreamHttpEntity(callback, statement.getConnection().getTimeZone(), statement.properties));
    }

    String getSql() {
        if (null != table) {
            return "INSERT INTO " + table + " FORMAT " + format;
        } else if (null != sql) {
            String result = sql;
            if (!MidasFormat.containsFormat(result)) {
                result += " FORMAT " + format;
            }
            return result;
        } else {
            throw new IllegalArgumentException("Neither table nor SQL clause are specified");
        }
    }

    private interface InputStreamProvider {
        InputStream get() throws IOException;
    }

    private static final class FileInputProvider implements InputStreamProvider {
        private final File file;

        private FileInputProvider(File file) {
            this.file = file;
        }

        @Override
        public InputStream get() throws IOException {
            return new FileInputStream(file);
        }
    }

    private static final class HoldingInputProvider implements InputStreamProvider {
        private final InputStream stream;

        private HoldingInputProvider(InputStream stream) {
            this.stream = stream;
        }

        @Override
        public InputStream get() throws IOException {
            return stream;
        }
    }

    public MidasCompression getCompression() {
        return compression;
    }
}
