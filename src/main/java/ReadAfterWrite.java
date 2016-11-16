import ia.es.EsClient;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.get.GetField;

import java.util.HashMap;
import java.util.Map;

public class ReadAfterWrite {

    public static void main(String[] args) throws Exception {
        EsClient esClient = new EsClient();
        String index = "prdhandlingunitcsl";
        String type = "handlingUnitCsl";
        String id = "46228";
        for (int i = 0; i < 10; i++) {
            System.out.println("Round " + i);
            GetResponse oldDoc = esClient.getDocument(index, type, id);
            if (!oldDoc.isExists()) {
                throw new RuntimeException("Doc 4447839 should exist in " + index + "/" + "type");
            }

            String fieldName = "totalVolume";
            Object oldValue = getFieldValue(oldDoc, fieldName);
            Map<String, Object> newValues = new HashMap<>();
            newValues.put(fieldName, i * 100);
            newValues.put("id", id);
            esClient.indexDocument(index, type, id, newValues).getVersion();

            GetResponse newDoc = esClient.getDocument(index, type, id);
            System.out.println("oldVersion:" + oldDoc.getVersion() + ", newVersion:" + newDoc.getVersion());
            System.out.println("oldValue:" + oldValue +
                    ", newValue:" + getFieldValue(newDoc, fieldName));
        }

    }

    private static Object getFieldValue(GetResponse doc, String fieldName) {
        return doc.getSource().get(fieldName);
    }
}
