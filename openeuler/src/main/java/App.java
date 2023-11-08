import org.apache.commons.io.FileUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class App {
    private static final String TARGET = System.getenv("TARGET");

    private static final String APPLICATION_PATH = System.getenv("APPLICATION_PATH");

    private static final String MAPPING_PATH = System.getenv("MAPPING_PATH");

    public static YamlConfig yamlConfig;


    public static void main(String[] args) {

        try {
            Yaml yaml = new Yaml(new Constructor(YamlConfig.class));
            InputStream inputStream = new FileInputStream(APPLICATION_PATH);

            yamlConfig = yaml.load(inputStream);

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
            PublicClient.makeIndex(yamlConfig.getIndexPrefix() + "_zh", MAPPING_PATH);
            PublicClient.makeIndex(yamlConfig.getIndexPrefix() + "_en", MAPPING_PATH);
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
                        PublicClient.insert(escape, yamlConfig.getIndexPrefix() + "_" + escape.get("lang"));
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
            PublicClient.insert(lm, yamlConfig.getIndexPrefix() + "_" + lm.get("lang"));
            idSet.add((String) lm.get("path"));
        }

        System.out.println("start delete expired document");
        PublicClient.deleteExpired(idSet, yamlConfig.getIndexPrefix() + "_*");
    }


}
