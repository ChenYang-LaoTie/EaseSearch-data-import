import java.util.List;

public class YamlConfig {
    private String indexPrefix;
    private List<String> host;
    private int port;
    private String protocol;
    private String username;
    private String password;
    private boolean useCer;
    private String cerFilePath;
    private String cerPassword;

    private String mindsporeOfficial;


    public String getIndexPrefix() {
        return indexPrefix;
    }

    public void setIndexPrefix(String indexPrefix) {
        this.indexPrefix = indexPrefix;
    }

    public List<String> getHost() {
        return host;
    }

    public void setHost(List<String> host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isUseCer() {
        return useCer;
    }

    public void setUseCer(boolean useCer) {
        this.useCer = useCer;
    }

    public String getCerFilePath() {
        return cerFilePath;
    }

    public void setCerFilePath(String cerFilePath) {
        this.cerFilePath = cerFilePath;
    }

    public String getCerPassword() {
        return cerPassword;
    }

    public void setCerPassword(String cerPassword) {
        this.cerPassword = cerPassword;
    }

    public String getMindsporeOfficial() {
        return mindsporeOfficial;
    }

    public void setMindsporeOfficial(String mindsporeOfficial) {
        this.mindsporeOfficial = mindsporeOfficial;
    }
}