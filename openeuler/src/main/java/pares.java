import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.io.FileUtils;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class pares {
    public static final String BASEPATH = System.getenv("TARGET") + "/";
    public static final String BLOG = "blog";
    public static final String BLOGS = "blogs";
    public static final String DOCS = "docs";
    public static final String NEWS = "news";
    public static final String OTHER = "other";
    public static final String MIGRATION = "migration";
    public static final String SHOWCASE = "showcase";
    public static final String EVENTS = "events";
    public static final String USERPRACTICE = "userPractice";


    public static Map<String, Object> parse(File file) throws Exception {
        String originalPath = file.getPath();
        String fileName = file.getName();
        String path = originalPath
                .replace("\\", "/")
                .replace(BASEPATH, "")
                .replace("\\\\", "/")
                .replace(".md", "")
                .replace(".html", "");

        String lang = path.substring(0, path.indexOf("/"));

        String type = path.substring(lang.length() + 1, path.indexOf("/", lang.length() + 1));
        if (!DOCS.equals(type)
                && !BLOG.equals(type)
                && !BLOGS.equals(type)
                && !NEWS.equals(type)
                && !SHOWCASE.equals(type)
                && !MIGRATION.equals(type)
                && !EVENTS.equals(type)
                && !USERPRACTICE.equals(type)) {
            type = OTHER;
            if (!fileName.equals("index.html")) {
                return null;
            }

        }
        if (type.equals(OTHER) || type.equals(SHOWCASE) || type.equals(MIGRATION)) {
            path = path.substring(0, path.length() - 5);
        }
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("lang", lang);
        jsonMap.put("type", type);
        jsonMap.put("articleName", fileName);
        jsonMap.put("path", path);

        String fileContent = FileUtils.readFileToString(file, StandardCharsets.UTF_8);

        if (fileName.endsWith(".html")) {
            parseHtml(jsonMap, fileContent);
        } else {
            if (DOCS.equals(type)) {
                parseDocsType(jsonMap, fileContent, fileName, path, type);
            } else {
                parseUnDocsType(jsonMap, fileContent);
            }
        }

        if (jsonMap.get("title") == "" && jsonMap.get("textContent") == "") {
            return null;
        }
        return jsonMap;
    }

    public static void parseHtml(Map<String, Object> jsonMap, String fileContent) {
        Document node = Jsoup.parse(fileContent);
        Elements titles = node.getElementsByTag("title");
        if (titles.size() > 0) {
            jsonMap.put("title", titles.first().text());
        }

        Elements elements = node.getElementsByTag("main");
        if (elements.size() > 0) {
            Element mainNode = elements.first();
            jsonMap.put("textContent", mainNode.text());
        }
    }

    public static void parseDocsType(Map<String, Object> jsonMap, String fileContent, String fileName, String path,
                                     String type) {
        Parser parser = Parser.builder().build();
        HtmlRenderer renderer = HtmlRenderer.builder().build();
        Node document = parser.parse(fileContent);
        Document node = Jsoup.parse(renderer.render(document));

        if (node.getElementsByTag("h1").size() > 0) {
            jsonMap.put("title", node.getElementsByTag("h1").first().text());
        } else {
            jsonMap.put("title", fileName);
        }

        if (node.getElementsByTag("a").size() > 0 && node.getElementsByTag("ul").size() > 0) {
            Element a = node.getElementsByTag("a").first();
            if (a.attr("href").startsWith("#")) {
                node.getElementsByTag("ul").first().remove();
            }
        }
        jsonMap.put("textContent", node.text());

        String version = path.replaceFirst(jsonMap.get("lang") + "/" + type + "/", "");
        version = version.substring(0, version.indexOf("/"));

        jsonMap.put("version", version);
    }

    public static void parseUnDocsType(Map<String, Object> jsonMap, String fileContent) {
        Parser parser = Parser.builder().build();
        HtmlRenderer renderer = HtmlRenderer.builder().build();
        String r = "";
        if (fileContent.contains("---")) {
            fileContent = fileContent.substring(fileContent.indexOf("---") + 3);
            if (fileContent.contains("---")) {
                r = fileContent.substring(0, fileContent.indexOf("---"));
                fileContent = fileContent.substring(fileContent.indexOf("---") + 3);
            }
        }

        Node document = parser.parse(fileContent);
        Document node = Jsoup.parse(renderer.render(document));
        jsonMap.put("textContent", node.text());

        Yaml yaml = new Yaml();
        Map<String, Object> ret = yaml.load(r);
        String key = "";
        Object value = "";
        for (Map.Entry<String, Object> entry : ret.entrySet()) {
            key = entry.getKey().toLowerCase(Locale.ROOT);
            value = entry.getValue();
            if (key.equals("date")) {
                // 需要处理日期不标准导致的存入ES失败的问题。
                String dateString = "";
                if (value.getClass().getSimpleName().equals("Date")) {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    dateString = format.format(value);
                } else {
                    dateString = value.toString();
                }
                Pattern pattern = Pattern.compile("\\D"); // 匹配所有非数字
                Matcher matcher = pattern.matcher(dateString);
                dateString = matcher.replaceAll("-");
                if (dateString.length() < 10) {
                    StringBuilder stringBuilder = new StringBuilder(dateString);
                    if (stringBuilder.charAt(7) != '-') {
                        stringBuilder.insert(5, "0");
                    }
                    if (stringBuilder.length() < 10) {
                        stringBuilder.insert(8, "0");
                    }
                    dateString = stringBuilder.toString();
                }
                value = dateString;
            }
            if (key.equals("author") && value instanceof String) {
                value = new String[]{value.toString()};
            }
            if (key.equals("head")) {
                continue;
            }
            jsonMap.put(key, value);
        }
        if (jsonMap.containsKey("date")) {
            jsonMap.put("archives", jsonMap.get("date").toString().substring(0, 7));
        }
    }

    public static List<Map<String, Object>> customizeData() {
        List<Map<String, Object>> r = new ArrayList<>();
        if (!setForum(r)) {
            System.out.println("Failed to add forum data");
            return null;
        }
        return null;
    }

    private static boolean setForum(List<Map<String, Object>> r) {
        String path = App.yamlConfig.getForumDomain() + "/latest.json?no_definitions=true&page=";

        String req = "";
        HttpURLConnection connection = null;
        String result;  // 返回结果字符串

        for (int i = 0; ; i++) {
            req = path + i;
            try {
                connection = sendHTTP(req, "GET");
                TimeUnit.SECONDS.sleep(30);
                if (connection.getResponseCode() == 200) {
                    result = ReadInput(connection.getInputStream());
                    if (!setData(result, r)) {
                        break;
                    }
                } else {
                    System.out.println(req + " - " + connection.getResponseCode());
                    return false;
                }
            } catch (IOException | InterruptedException e) {
                System.out.println("Connection failed, error is: " + e.getMessage());
                return false;
            } finally {
                if (null != connection) {
                    connection.disconnect();
                }
            }
        }
        return true;
    }

    private static boolean setData(String data, List<Map<String, Object>> r) {
        JSONObject post = JSON.parseObject(data);
        JSONObject topicList = post.getJSONObject("topic_list");
        JSONArray jsonArray = topicList.getJSONArray("topics");
        if (jsonArray.size() <= 0) {
            return false;
        }
        String path = "";
        HttpURLConnection connection = null;
        String result; // 返回结果字符串
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject topic = jsonArray.getJSONObject(i);
            String id = topic.getString("id");
            String slug = topic.getString("slug");
            path = String.format("%s/t/%s/%s.json?track_visit=true&forceLoad=true", App.yamlConfig.getForumDomain(), slug, id);
            try {
                connection = sendHTTP(path, "GET");
                if (connection.getResponseCode() == 200) {
                    result = ReadInput(connection.getInputStream());
                    JSONObject st = JSON.parseObject(result);
                    JSONObject postStream = st.getJSONObject("post_stream");
                    JSONArray posts = postStream.getJSONArray("posts");
                    JSONObject pt = posts.getJSONObject(0);

                    String cooked = pt.getString("cooked");
                    Parser parser = Parser.builder().build();
                    HtmlRenderer renderer = HtmlRenderer.builder().build();
                    Node document = parser.parse(cooked);
                    Document node = Jsoup.parse(renderer.render(document));
                    Map<String, Object> jsonMap = new HashMap<>();
                    jsonMap.put("textContent", node.text());
                    jsonMap.put("title", st.getString("title"));
                    jsonMap.put("eulerForumId", pt.get("id"));
                    jsonMap.put("type", "forum");
                    jsonMap.put("lang", "zh");
                    jsonMap.put("path", "/t/" + slug + "/" + id);

                    r.add(jsonMap);
                } else {
                    System.out.println(path + " - " + connection.getResponseCode());
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            } finally {
                if (null != connection) {
                    connection.disconnect();
                }
            }
        }
        return true;
    }

    public boolean setService(List<Map<String, Object>> r) {
        String path = System.getenv("SERVICE_URL");
        HttpURLConnection connection = null;
        try {
            connection = sendHTTP(path, "GET");
            if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
                return false;
            }
            Yaml yaml = new Yaml();
            List<Map<String, Object>> data = yaml.load(connection.getInputStream());
            for (Map<String, Object> datum : data) {
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("title", datum.get("name"));
                jsonMap.put("secondaryTitle", datum.get("introduction"));
                jsonMap.put("lang", datum.get("lang"));
                jsonMap.put("path", datum.get("path"));
                jsonMap.put("type", "service");

                StringBuilder textContentBuilder = new StringBuilder();
                if (datum.get("introduction") != null) {
                    textContentBuilder.append(datum.get("introduction")).append(" ");
                }
                if (datum.get("description") != null) {
                    textContentBuilder.append(datum.get("description"));
                }
                String textContent =  textContentBuilder.toString();

                jsonMap.put("textContent", textContent);
                r.add(jsonMap);
            }

        } catch (IOException e) {
            System.out.println("load yaml failed, error is: " + e.getMessage());
            return false;
        } finally {
            if (null != connection) {
                connection.disconnect();
            }
        }
        return true;
    }

    private static HttpURLConnection sendHTTP(String path, String method) throws IOException {
        URL url = new URL(path);
        HttpURLConnection connection = null;
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(60000);
        connection.setReadTimeout(60000);
        connection.connect();
        return connection;
    }

    private static String ReadInput(InputStream is) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuffer sbf = new StringBuffer();
        String temp = null;
        while ((temp = br.readLine()) != null) {
            sbf.append(temp);
        }
        try {
            br.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        try {
            is.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return sbf.toString();
    }
}
