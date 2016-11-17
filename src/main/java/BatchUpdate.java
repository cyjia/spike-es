import ia.es.EsClient;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BatchUpdate {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        EsClient esClient = new EsClient();
        final List<UpdateRequest> requestList = new ArrayList<>();
        final String index = "prddoview";
        final String type = "doview";
        esClient.searchScroll(index, 65000, (rowNum, hit) -> {
            Map<String, Object> source = hit.getSource();
            source.put("productType", 8);
            requestList.add(
                    esClient.updateRequestBuilder(index, type, hit.getId())
                            .setDoc(source)
                            .request());
            if (requestList.size() == 1000) {
                bulkUpdate(esClient, requestList, "Finished 1000 " + rowNum);
            }
        });
        if (requestList.size() > 0) {
            bulkUpdate(esClient, requestList, "Finished 1000 ");
        }
        System.out.println("Time took:" + (System.currentTimeMillis() - start));
    }

    private static void bulkUpdate(EsClient esClient, List<UpdateRequest> requestList, String x2) {
        BulkRequestBuilder bulkRequestBuilder = esClient.getBulkRequestBuilder();
        requestList.forEach(bulkRequestBuilder::add);
        System.out.println("updating 1000");
        bulkRequestBuilder.execute().actionGet();
        System.out.println(x2);
        requestList.clear();
    }
}
