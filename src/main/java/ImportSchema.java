import ia.jdbc.util.DBMetadata;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class ImportSchema {

    public static void main(String[] args) throws Exception {
        String connectionUrl = System.getenv("url");
        String userName = System.getenv("user");
        String password = System.getenv("password");

        DBMetadata.Table[] tables = DBMetadata.getTables(connectionUrl, userName, password,
                (tableName) -> !tableName.startsWith("T_") && !tableName.startsWith("t_"));
        for (DBMetadata.Table t : tables) {
            File f = new File(String.format("/Users/CYJIA/scct-tables/%s.csv", t.name));
            OutputStreamWriter output = new OutputStreamWriter(new FileOutputStream(f));
            for (DBMetadata.Column c : t.columns) {
                output.write(String.format("%s,%s,%d,%d\n",
                        c.name, c.dataType, c.dataLength, c.decimalSize));
            }
            output.close();
            System.out.println("write one file " + t.name);
        }
    }
}
