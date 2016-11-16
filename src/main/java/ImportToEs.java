import ia.es.ESMetadata;
import ia.es.EsClient;
import ia.io.util.IOUtil;
import ia.jdbc.util.DB;
import ia.jdbc.util.DBMetadata;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImportToEs {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new RuntimeException("No tableName provided");
        }
        final String tableName = "doview";
        final String sql = IOUtil.inputStreamToText(ClassLoader.getSystemResourceAsStream("metadata/do_view.sql"));

        final DB db = new DB();
        long pageNum = 0;
        long pageSize = 50000;
        long successCount = importFromView(db, paginateSql(sql, pageNum, pageSize), tableName);
        while (successCount > 0) {
            pageNum++;
            successCount = importFromView(db, paginateSql(sql, pageNum, pageSize), tableName);
        }
    }

    private static String paginateSql(String sql, long pageNum, long pageSize) {
        return String.format("%s WHERE hu.id >= %d and hu.id < %d", sql, pageNum * pageSize, (pageNum + 1) * pageSize);
    }

    private static Long importFromView(DB db, String sql, String typeName) throws Exception {
        return db.query(sql, rs -> {
            try {
                ResultSetMetaData metaData = rs.getMetaData();
                DBMetadata.Table table = DBMetadata.buildTable(metaData, typeName);
                String mapping = ESMetadata.getMapping(table);
                EsClient esClient = new EsClient();
                String index = "prd" + table.name.toLowerCase();
                if (!esClient.isIndexExist(index)) {
                    CreateIndexResponse response = esClient.createIndex(index, table.name, mapping);
                }
                long processedCount = resultSetToESIndex(table, esClient, index, rs);
                esClient.close();
                return processedCount;
            } catch (SQLException e) {
                throw new RuntimeException("Error get metadata from result");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static long resultSetToESIndex(DBMetadata.Table t, EsClient esClient, String index, ResultSet resultSet) {
        try {
            final List<Map<String, Object>> bulkList = new ArrayList<>();
            long count = 0;
            while (resultSet.next()) {
                Map<String, Object> fields = new HashMap<>();
                for (DBMetadata.Column c : t.columns) {
                    fields.put(c.name, resultSet.getObject(c.name));
                }
                bulkList.add(fields);
                count++;
                if (bulkList.size() == 10000) {
                    System.out.println("processing 10000 docs");
                    esClient.bulkIndexDocuments(index, t.name, bulkList);
                    System.out.println("finished 10000 docs");
                    bulkList.clear();
                }
            }
            if (bulkList.size() > 0) {
                esClient.bulkIndexDocuments(index, t.name, bulkList);
                bulkList.clear();
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
