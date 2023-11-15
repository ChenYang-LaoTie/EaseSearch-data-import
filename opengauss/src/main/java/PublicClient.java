import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

public class PublicClient {

    public static RestHighLevelClient restHighLevelClient;

    public static void CreateClientFormConfig(String configPath) throws Exception {
        Yaml yaml = new Yaml(new Constructor(YamlConfig.class));
        InputStream inputStream = new FileInputStream(configPath);

        YamlConfig yamlConfig = yaml.load(inputStream);
        File configFile = new File(configPath);
        if (configFile.exists()) {
            if (configFile.delete()) {
                System.out.println("File deleted successfully");
            } else {
                System.out.println("Failed to delete the file");
            }
        }

        if (yamlConfig.isUseCer()) {
            EsClientCer.create(
                    yamlConfig.getHost(),
                    yamlConfig.getPort(),
                    yamlConfig.getProtocol(),
                    5 * 1000,
                    5 * 1000,
                    30 * 1000,
                    yamlConfig.getUsername(),
                    yamlConfig.getPassword(),
                    yamlConfig.getCerFilePath(),
                    yamlConfig.getCerPassword()
            );
        } else {
            EsClient.create(
                    yamlConfig.getHost(),
                    yamlConfig.getPort(),
                    yamlConfig.getProtocol(),
                    5 * 1000,
                    5 * 1000,
                    30 * 1000,
                    yamlConfig.getUsername(),
                    yamlConfig.getPassword()
            );
        }
    }





    public static void makeIndex(String index, String mappingPath) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        if (exists) {
            return;
        }

        CreateIndexRequest request1 = new CreateIndexRequest(index);
        File mappingJson = FileUtils.getFile(mappingPath);
        String mapping = FileUtils.readFileToString(mappingJson, StandardCharsets.UTF_8);

        request1.mapping(mapping, XContentType.JSON);
        request1.setTimeout(TimeValue.timeValueMillis(1));

        restHighLevelClient.indices().create(request1, RequestOptions.DEFAULT);
    }

    public static void insert(Map<String, Object> data, String index) throws Exception {
        IndexRequest indexRequest = new IndexRequest(index).id((String) data.get("path")).source(data);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    }

    public static void deleteExpired(Set<String> idSet, String index) {
        try {
            long st = System.currentTimeMillis();
            int scrollSize = 500;//一次读取的doc数量
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());//读取全量数据
            searchSourceBuilder.size(scrollSize);
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10));//设置一次读取的最大连接时长
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(scroll);

            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            String scrollId = searchResponse.getScrollId();

            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                if (!idSet.contains(hit.getId())) {
                    DeleteRequest deleteRequest = new DeleteRequest(hit.getIndex(), hit.getId());
                    DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
                }
            }


            while (hits.length > 0) {
                SearchScrollRequest searchScrollRequestS = new SearchScrollRequest(scrollId);
                searchScrollRequestS.scroll(scroll);
                SearchResponse searchScrollResponseS = restHighLevelClient.scroll(searchScrollRequestS, RequestOptions.DEFAULT);
                scrollId = searchScrollResponseS.getScrollId();

                hits = searchScrollResponseS.getHits().getHits();
                for (SearchHit hit : hits) {
                    if (!idSet.contains(hit.getId())) {
                        DeleteRequest deleteRequest = new DeleteRequest(hit.getIndex(), hit.getId());
                        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
                    }
                }
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);

            restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

}
