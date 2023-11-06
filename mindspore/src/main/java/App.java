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
import java.util.*;

public class App {
    private static final String TARGET = System.getenv("TARGET");

    private static RestHighLevelClient restHighLevelClient;

    private static final String APPLICATION_PATH = System.getenv("APPLICATION_PATH");

    private static final String MAPPING_PATH = System.getenv("MAPPING_PATH");

    public static YamlConfig yamlConfig;


    public static void main(String[] args) {
        try {
            Yaml yaml = new Yaml(new Constructor(YamlConfig.class));
            InputStream inputStream = new FileInputStream(APPLICATION_PATH);

            yamlConfig = yaml.load(inputStream);

            if (yamlConfig.isUseCer()) {
                restHighLevelClient = EsClientCer.create(
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
                restHighLevelClient = EsClient.create(
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
            makeIndex(yamlConfig.getIndexPrefix() + "_zh", MAPPING_PATH);
            makeIndex(yamlConfig.getIndexPrefix() + "_en", MAPPING_PATH);
            fileDate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        System.out.println("import end");
        System.exit(0);
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


    public static void fileDate() throws Exception {
        File indexFile = new File(TARGET);
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
//                        insert(escape, yamlConfig.getIndexPrefix() + "_" + escape.get("lang"));
                        idSet.add((String) escape.get("path"));
                    } else {
                        System.out.println("parse null : " + paresFile.getPath());
                    }
                } catch (Exception e) {
                    System.out.println(paresFile.getPath());
                    System.out.println(e.getMessage());
                }
            }
        }

        System.out.println("__________________________");
        System.out.println(idSet.size());

//        List<Map<String, Object>> customizeEscape = pares.customizeData();
//        if (null == customizeEscape) {
//            return;
//        }

//        for (Map<String, Object> lm : customizeEscape) {
//            insert(lm, yamlConfig.getIndexPrefix() + "_" + lm.get("lang"));
//            idSet.add((String) lm.get("path"));
//        }
//
//        System.out.println("start delete expired document");
//        deleteExpired(idSet);
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
            SearchRequest searchRequest = new SearchRequest(yamlConfig.getIndexPrefix() + "_*");
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
