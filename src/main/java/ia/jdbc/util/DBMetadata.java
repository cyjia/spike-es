package ia.jdbc.util;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

public class DBMetadata {

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
            JDBCType jdbcType = JDBCType.valueOf(columns.getInt(5));
            if (!dataTypeMapping.containsKey(jdbcType)) {
                throw new RuntimeException("unkonw jdbctype map for "
                        + jdbcType.getName() + " " + jdbcType.ordinal()
                        + "," + col.dataLength + "," + col.decimalSize);
            }
            if (jdbcType == JDBCType.NUMERIC && col.decimalSize != 0) {
                throw new RuntimeException("Numeric can't map to long");
            }
            col.dataType = dataTypeMapping.get(jdbcType);
            columnList.add(col);
        }

        return columnList.toArray(new Column[columnList.size()]);
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
