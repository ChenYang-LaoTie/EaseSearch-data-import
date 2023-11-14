import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.*;

public class App {
    private static final String TARGET = System.getenv("TARGET");

    private static final String APPLICATION_PATH = System.getenv("APPLICATION_PATH");

    private static final String MAPPING_PATH = System.getenv("MAPPING_PATH");

    private static final String INDEX_PREFIX = "openeuler_articles";


    public static void main(String[] args) {
        try {
            PublicClient.CreateClientFormConfig(APPLICATION_PATH);
            PublicClient.makeIndex(INDEX_PREFIX + "_zh", MAPPING_PATH);
            PublicClient.makeIndex(INDEX_PREFIX + "_en", MAPPING_PATH);
            fileDate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        System.out.println("import end");
        System.exit(0);
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
                    Map<String, Object> escape = Pares.parse(paresFile);
                    if (null != escape) {
                        PublicClient.insert(escape, INDEX_PREFIX + "_" + escape.get("lang"));
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

        List<Map<String, Object>> customizeEscape = Pares.customizeData();
        if (null == customizeEscape) {
            return;
        }

        for (Map<String, Object> lm : customizeEscape) {
            PublicClient.insert(lm, INDEX_PREFIX + "_" + lm.get("lang"));
            idSet.add((String) lm.get("path"));
        }

        System.out.println("start delete expired document");
        PublicClient.deleteExpired(idSet, INDEX_PREFIX + "_*");
    }


}
