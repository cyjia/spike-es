import ia.jdbc.util.DBMetadata;
import ia.jdbc.util.ESMetadata;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;

public class ImportToEs {
    public static class EsClient {
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
                            .put("index.number_of_replicas", 2))
                    .addMapping(type, mapping).get();
        }

        public void close() {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String connectionUrl = System.getenv("url");
        String userName = System.getenv("user");
        String password = System.getenv("password");
        DBMetadata.Table[] tables = DBMetadata.getTables(connectionUrl, userName, password,
                (tableName) -> tableName.equals("deliveryOdrHeaderBiz"));
        for (DBMetadata.Table t : tables) {
            String mapping = ESMetadata.getMapping(t);
            System.out.println(mapping);
            EsClient esClient = new EsClient();
            CreateIndexResponse response = esClient.createIndex("t2" + t.name.toLowerCase(), t.name, mapping);
            esClient.close();
        }
    }
}
