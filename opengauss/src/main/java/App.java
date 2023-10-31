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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class App {
    private static final String TARGET_PATH = System.getenv("TARGET_PATH");

    private static RestHighLevelClient restHighLevelClient;

    private static String INDEX_PREFIX;

    private static final String APPLICATION_PATH = System.getenv("APPLICATION_PATH");


    public static void main(String[] args) {
        try {
            Yaml yaml = new Yaml(new Constructor(YamlConfig.class));
            InputStream inputStream = new FileInputStream(APPLICATION_PATH);

            YamlConfig yamlConfig = yaml.load(inputStream);

            INDEX_PREFIX = yamlConfig.getIndexPrefix();

            if (yamlConfig.isUseCer()) {
                restHighLevelClient = EsClientCer.create(
                        yamlConfig.getHost(),
                        yamlConfig.getPort(),
                        yamlConfig.getProtocol(),
                        1000,
                        1000,
                        1000,
                        yamlConfig.getUsername(),
                        yamlConfig.getPassword(),
                        yamlConfig.getCerFilePath(),
                        yamlConfig.getCerPassword()
                );
            } else {
                restHighLevelClient = EsClient.create(
                        yamlConfig.getHost(),
                        yamlConfig.getPort(),
                        yamlConfig.getProtocol(),
                        1000,
                        1000,
                        1000,
                        yamlConfig.getUsername(),
                        yamlConfig.getPassword()
                );
            }


            fileDate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }


    }


    public static void fileDate() {
        File indexFile = new File(TARGET_PATH);
        if (!indexFile.exists()) {
            System.out.printf("%s folder does not exist%n", indexFile.getPath());
            return;
        }

        System.out.println("begin to update document");

        Set<String> idSet = new HashSet<>();

        Collection<File> listFiles = FileUtils.listFiles(indexFile, new String[]{"md", "html"}, true);

        for (File paresFile : listFiles) {
            if (!paresFile.getName().startsWith("_")) {
                try {
                    Map<String, Object> escape = pares.parse(paresFile);
                    if (null != escape) {
                        insert(escape, INDEX_PREFIX + "_" + escape.get("lang"));
                        idSet.add((String) escape.get("path"));
                    }
                } catch (Exception e) {
                    System.out.println(paresFile.getPath());
                    System.out.println(e.getMessage());
                }
            }
        }

        System.out.println("start delete expired document");
        deleteExpired(idSet);
    }

    public static void insert(Map<String, Object> data, String index) throws Exception {
        IndexRequest indexRequest = new IndexRequest(index).id((String) data.get("path")).source(data);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    }

    public static void deleteExpired(Set<String> idSet) {
        try {
            long st = System.currentTimeMillis();
            int scrollSize = 500;//一次读取的doc数量
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());//读取全量数据
            searchSourceBuilder.size(scrollSize);
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10));//设置一次读取的最大连接时长
            SearchRequest searchRequest = new SearchRequest(INDEX_PREFIX + "_*");
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