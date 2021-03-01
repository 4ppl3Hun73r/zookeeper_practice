package jiho.zookeeper.config;

public class ZookeeperConfig {
    public static final String host = "127.0.0.1:2181";
    public static final int sessionTimeout = 2000;

    public static final String clusterMaxThroughputPath = "/global-config/max-throughput";

    public static final String clientRootNodePath = "/client";
    public static final String clientNodePrefixPath = "/client-";

    public static final String leaderNodePath = "/leader";
}
