import org.apache.commons.io.FileUtils;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Pares {

    public static final String BASEPATH = System.getenv("TARGET") + "/";
    public static final String DOCS = "docs";


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
        version = "no version";
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
                value = new String[] { value.toString() };
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
}
