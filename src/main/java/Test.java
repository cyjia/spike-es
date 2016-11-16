import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;

public class Test {

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

        public void createIndex(String index, String mapping) throws Exception {
            CreateIndexResponse response = client.admin().indices()
                    .prepareCreate(index)
                    .setSettings(Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 2))
                    .addMapping(mapping, "").get();
        }
    }


    public static void main(String[] args) throws Exception {
        EsClient esc = new EsClient();
        esc.createIndex("t2deliveryodrkpi", "");
    }
}