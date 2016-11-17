package ia.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class EsClient {
    private final TransportClient client;

    public EsClient() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "scct-pf-es01")
                .put("client.transport.sniff", true)
                .build();
        this.client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
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

    public void searchScroll(String index, int maxDoc, BiConsumer<Integer, SearchHit> hitConsumer) {
        TermQueryBuilder productType = termQuery("productType", "9999");
//        QueryBuilder qb = boolQuery()
//                .must(rangeQuery("podDate").from("2016-05-13T07:50:00.000Z"));
        SearchResponse response = client.prepareSearch(index)
                .addSort("podDate", SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(productType)
                .setSize(500).execute().actionGet();
        long max = maxDoc;
        int count = 0;
        while (response.getHits().getHits().length > 0) {
            System.out.println("took:" + response.getTook());
            System.out.println("total:" + response.getHits().getTotalHits());
            for (SearchHit hit : response.getHits().getHits()) {
                hitConsumer.accept(count, hit);
                count++;
            }
            if (count > max) {
                break;
            }
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        }
        client.prepareClearScroll()
                .addScrollId(response.getScrollId())
                .execute()
                .actionGet();
    }

    public void close() {
        this.client.close();
    }

    public BulkResponse bulkIndexDocuments(String index, String mapping, List<Map<String, Object>> bulkList) {
        BulkRequestBuilder bulkRequestBuilder = getBulkRequestBuilder();
        bulkList.forEach(x -> {
            bulkRequestBuilder.add(
                    client.prepareIndex(index, mapping, x.get("id").toString()).setSource(x));
        });
        return bulkRequestBuilder.get();
    }

    public BulkRequestBuilder getBulkRequestBuilder() {
        return client.prepareBulk();
    }

    public void bulkProcess(String index, String mapping, Stream<Map<String, Object>> docStream) throws Exception {
        BulkProcessor bulkProcessor = createBulkProcessor();
        docStream.forEachOrdered(x -> {
            bulkProcessor.add(client.prepareIndex(index, mapping, x.get("id").toString()).setSource(x).request());
        });
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
    }

    public UpdateRequestBuilder updateRequestBuilder(String index, String type, String id) {
        return client.prepareUpdate(index, type, id);
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
