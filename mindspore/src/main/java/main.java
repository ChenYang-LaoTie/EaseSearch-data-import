import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;

public class main {
    private static final String TARGET_PATH = System.getenv("TARGET_PATH");

    private static RestHighLevelClient restHighLevelClient;

    private static final String INDEX_PREFIX = System.getenv("INDEX_PREFIX");

    private static final String APPLICATION_PATH = System.getenv("APPLICATION_PATH");

    private static class YamlConfig {
        private String host;
        private String username;
        private boolean useCer;
    }


    public static void main(String[] args) {
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = new FileInputStream(APPLICATION_PATH);
            YamlConfig yamlConfig = yaml.load(inputStream);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }



        restHighLevelClient = InitRestHighLevelClient("", "", "", 9200);
        if (null != restHighLevelClient) {
            return;
        }
        fileDate();

    }

    public static RestHighLevelClient InitRestHighLevelClient(String userName, String password, String host, int port) {
        try {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                    return true;
                }
            }).build();
            SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext, NoopHostnameVerifier.INSTANCE);
            RestHighLevelClient restClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(host, port, "https")).setHttpClientConfigCallback(
                            new RestClientBuilder.HttpClientConfigCallback() {
                                @Override
                                public HttpAsyncClientBuilder customizeHttpClient(
                                        HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                    httpAsyncClientBuilder.disableAuthCaching();
                                    httpAsyncClientBuilder.setSSLStrategy(sessionStrategy);
                                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                    return httpAsyncClientBuilder;
                                }
                            }).setRequestConfigCallback(
                            new RestClientBuilder.RequestConfigCallback() {
                                // 该方法接收一个RequestConfig.Builder对象，对该对象进行修改后然后返回。
                                @Override
                                public RequestConfig.Builder customizeRequestConfig(
                                        RequestConfig.Builder requestConfigBuilder) {
                                    return requestConfigBuilder.setConnectTimeout(5 * 1000) // 连接超时（默认为1秒）现在改为5秒
                                            .setSocketTimeout(30 * 1000);// 套接字超时（默认为30秒）现在改为30秒
                                }
                            }));
            return restClient;
        } catch (Exception e) {
            System.out.println("elasticsearch TransportClient create error!!: " + e);
            return null;
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

        try {
            System.out.println("begin to update customize data");
            List<Map<String, Object>> escape = pares.customizeData();
            if (null == escape) {
                System.out.println("get customize data error, do not update this part");
                return;
            }
            for (Map<String, Object> lm : escape) {
                insert(lm, INDEX_PREFIX + "_" + lm.get("lang"));
                idSet.add((String) lm.get("path"));
            }

        } catch (Exception e) {
            System.out.println("get customize: " + e.getMessage());
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
