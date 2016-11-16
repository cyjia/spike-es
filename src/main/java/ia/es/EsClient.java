package ia.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class EsClient {
    private final TransportClient client;

    public EsClient() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "scct-pf-es01")
                .put("client.transport.sniff", true)
                .build();
        this.client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.116.61.63"), 9300));
    }

    public CreateIndexResponse createIndex(String index, String type, String mapping) throws Exception {
        return client.admin().indices()
                .prepareCreate(index)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 0))
                .addMapping(type, mapping).get();
    }

    public boolean isIndexExist(String index) throws Exception {
        IndicesAdminClient indices = client.admin().indices();
        return indices.exists(new IndicesExistsRequest(index)).get().isExists();
    }

    public IndexResponse indexDocument(String index, String mapping, String id, Map<String, Object> fields) {
        return client.prepareIndex(index, mapping, id)
                .setSource(fields).get();
    }

    public GetResponse getDocument(String index, String type, String id) {
        return client.prepareGet(index, type, id).get();
    }

    public void close() {
        this.client.close();
    }

    public BulkResponse bulkIndexDocuments(String index, String mapping, List<Map<String, Object>> bulkList) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkList.forEach(x -> {
            bulkRequestBuilder.add(
                    client.prepareIndex(index, mapping, x.get("id").toString()).setSource(x));
        });
        return bulkRequestBuilder.get();
    }

    public void bulkProcess(String index, String mapping, Stream<Map<String, Object>> docStream) throws Exception {
        BulkProcessor bulkProcessor = createBulkProcessor();
        docStream.forEachOrdered(x -> {
            bulkProcessor.add(client.prepareIndex(index, mapping, x.get("id").toString()).setSource(x).request());
        });
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
    }

    private BulkProcessor createBulkProcessor() {
        return BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        System.out.println("processing " + executionId);
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        System.out.println("finished processing " + executionId);
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println("failed processing " + executionId);
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(500, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }
}
