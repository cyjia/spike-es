package ia.jdbc.util;

public class ESMetadata {

    public static String getMapping(DBMetadata.Table table) {
        return getMapping(table.name, table);
    }

    /**
     * {typeName: {properties: { name: {type: string}}}}
     */
    public static String getMapping(String typeName, DBMetadata.Table table) {
        DBMetadata.Column[] columns = table.columns;
        String[] fields = new String[2 * columns.length];
        for (int i = 0; i < 2 * columns.length; i += 2) {
            DBMetadata.Column c = columns[i / 2];
            fields[i] = s(c.name);
            fields[i + 1] = o(s("type"), s(c.dataType));
        }
        return o(s(typeName), o(s("properties"), o(fields)));
    }

    private static String o(String... fields) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < fields.length; i += 2) {
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append(String.format("%s: %s", fields[i], fields[i + 1]));
        }
        return "{ " + sb.toString() + " }";
    }

    private static String s(String s) {
        return "\"" + s + "\"";
    }
}
