package ia.jdbc.util;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

public class DBMetadata {

    public static Table buildTable(ResultSetMetaData metaData, String typeName) throws SQLException {
        Table table = new Table();
        table.name = typeName;
        table.columns = new Column[metaData.getColumnCount()];
        for (int i = 0; i < table.columns.length; i++) {
            Column c = new Column();
            c.name = metaData.getColumnName(i + 1);
            c.dataLength = metaData.getScale(i + 1);
            c.decimalSize = metaData.getPrecision(i + 1);
            c.dataType = mapToEsType(metaData.getColumnType(i + 1), c.dataLength, c.decimalSize);
            table.columns[i] = c;
        }
        return table;
    }

    public static class Column {
        public String name;
        public String dataType;
        public int dataLength;
        public int decimalSize;
        public boolean nullable = true;

        @Override
        public String toString() {
            return "Column{" +
                    "name='" + name + '\'' +

                    ", dataType=" + dataType +
                    ", dataLength=" + dataLength +
                    ", decimalSize=" + decimalSize +
                    ", nullable=" + nullable +
                    '}';
        }
    }

    public static class Table {
        public String name;
        public Column[] columns;

        @Override
        public String toString() {
            return "Table{" +
                    "name='" + name + '\'' +
                    ", columns=" + Arrays.toString(columns) +
                    '}';
        }
    }

    public static Table[] getTables(Function<String, Boolean> filter) throws Exception {
        String url = System.getenv("url");
        String user = System.getenv("user");
        String password = System.getenv("password");
        Connection con = DriverManager.getConnection(url, user, password);
        DatabaseMetaData metaData = con.getMetaData();
        List<Table> tableList = new ArrayList<>();
        ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
        while (tables.next()) {
            String tableName = tables.getString(3);
            if (filter.apply(tableName)) {
                Table t = new Table();
                t.name = tableName;
                t.columns = getColumns(metaData, t.name);
                tableList.add(t);
            } else {
                System.out.println("skipping table " + tableName);
            }
        }
        con.close();
        return tableList.toArray(new Table[tableList.size()]);
    }

    private static Column[] getColumns(DatabaseMetaData metaData, String tableName) throws Exception {
        List<Column> columnList = new ArrayList<>();
        ResultSet columns = metaData.getColumns(null, null, tableName, "%");

        while (columns.next()) {
            Column col = new Column();
            col.name = columns.getString(4);
            col.dataLength = columns.getInt(7);
            col.decimalSize = columns.getInt(9);
            col.dataType = mapToEsType(columns.getInt(5), col.dataLength, col.decimalSize);
            columnList.add(col);
        }

        return columnList.toArray(new Column[columnList.size()]);
    }

    private static String mapToEsType(int type, int dataLength, int decimalSize) throws SQLException {
        JDBCType jdbcType = JDBCType.valueOf(type);
        if (!dataTypeMapping.containsKey(jdbcType)) {
            throw new RuntimeException("unkonw jdbctype map for "
                    + jdbcType.getName() + " " + jdbcType.ordinal()
                    + "," + dataLength + "," + decimalSize);
        }
        if (jdbcType == JDBCType.NUMERIC && decimalSize != 0) {
            throw new RuntimeException("Numeric can't map to long");
        }
        return dataTypeMapping.get(jdbcType);
    }

    private static Map<JDBCType, String> dataTypeMapping = new HashMap<>();

    static {
        dataTypeMapping.put(JDBCType.BIGINT, "long");
        dataTypeMapping.put(JDBCType.VARCHAR, "string");
        dataTypeMapping.put(JDBCType.SMALLINT, "integer");
        dataTypeMapping.put(JDBCType.INTEGER, "integer");
        dataTypeMapping.put(JDBCType.TIMESTAMP, "date");
        dataTypeMapping.put(JDBCType.DECIMAL, "double");
        dataTypeMapping.put(JDBCType.CLOB, "binary");
        dataTypeMapping.put(JDBCType.CHAR, "string");
        dataTypeMapping.put(JDBCType.TINYINT, "boolean");
        dataTypeMapping.put(JDBCType.NUMERIC, "long");
        dataTypeMapping.put(JDBCType.BLOB, "binary");
    }
}
