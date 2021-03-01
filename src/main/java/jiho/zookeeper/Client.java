package jiho.zookeeper;


import jiho.zookeeper.config.ZookeeperConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.*;

import com.fasterxml.jackson.databind.ObjectMapper;


@Slf4j
public class Client {
    private ZooKeeper zk;
    private Status status = new Status();
    private ObjectMapper objectMapper = new ObjectMapper();

    private String clientId;
    private String clientPath;
    private String leaderClientId;


    public Client() throws InterruptedException, IOException, KeeperException {
        connectZookeeper();

        // client로 등록한다, 정보에 status를 json으로 넣어 놓는다.
        this.clientPath = zk.create(ZookeeperConfig.clientRootNodePath + ZookeeperConfig.clientNodePrefixPath,
                objectMapper.writeValueAsBytes(status), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        // 외부에서 status정보가 변경되면 자신의 status 값을 수정하기 위해 watcher를 등록한다
        zk.addWatch(this.clientPath, clientStatusWatcher, AddWatchMode.PERSISTENT);
        this.clientId = clientPath.split("/")[2];

        // leader 처리
        if (zk.exists(ZookeeperConfig.leaderNodePath, false) == null) {
            zk.create(ZookeeperConfig.leaderNodePath, this.clientId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
                    , leaderCreateCallback, null);
            this.leaderClientId = this.clientId;
        } else {
            this.leaderClientId = new String(zk.getData(ZookeeperConfig.leaderNodePath, false, null));
            zk.addWatch(ZookeeperConfig.leaderNodePath, leaderStateWatcher, AddWatchMode.PERSISTENT);
        }

        log.info("{} Client is Ready, Leader Client is {}", clientId, leaderClientId);
    }

    /**
     * zookeeper 연결
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void connectZookeeper() throws IOException, InterruptedException {
        CountDownLatch connectionLatch = new CountDownLatch(1);
        this.zk = new ZooKeeper(ZookeeperConfig.host, ZookeeperConfig.sessionTimeout, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
        });
        connectionLatch.await(1, TimeUnit.MINUTES);
    }

    private void electNewLeader() {
        try {
            log.info("Elect a new leader");
            // client 목록을 가져온다.
            List<String> clientList = zk.getChildren(ZookeeperConfig.clientRootNodePath, false);
            log.info("Candidate list : {}", clientList);
            if (clientId.equals(clientList.get(0))) {
                // client 목록의 첫번째 client가 새로운 leader가 된다.
                zk.create(ZookeeperConfig.leaderNodePath, clientId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
                        , leaderCreateCallback, null);
            }
        } catch (Exception e) {
            log.error("Elect a leader Exception {}", e.getMessage(), e);
        }
    }

    private Watcher clientStatusWatcher = event -> {
        log.info("Status Change Watcher : {}", event);

        // status 값 변경이 되었으면 로컬 데이터를 업데이트 한다.
        if (Watcher.Event.EventType.NodeDataChanged == event.getType()) {
            try {
                log.info("Prev status : {}", status);
                byte[] bStatus = zk.getData(clientPath, false, null);
                status = objectMapper.readValue(bStatus, Status.class);
                log.info("Update status : {}", status);
            } catch (Exception e) {
                log.error("Status Change Watcher Exception : {}", e.getMessage(), e);
            }
        }
    };

    private Watcher leaderStateWatcher = event -> {
        log.info("Leader Client State Change : {}", event);

        try {
            switch (event.getType()) {
                case NodeCreated:
                    log.info("Prev leader is {}", leaderClientId);
                    leaderClientId = new String(zk.getData(ZookeeperConfig.leaderNodePath, false, null));
                    log.info("New leader is {}", leaderClientId);
                    break;
                case NodeDeleted:
                    log.info("{} is dead", leaderClientId);
                    electNewLeader();
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("Leader Event Error : {}", e.getMessage(), e);
        }

    };

    // 신규 리더가 선출되면 기본적으로 동작하는 로직
    private AsyncCallback.StringCallback leaderCreateCallback = new AsyncCallback.StringCallback() {
        private long clusterMaxThroughput = 100;

        private Watcher clientWatchWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.info("Client State Event : {}", event);
                Event.EventType eventType = event.getType();

                switch (eventType) {
                    case NodeCreated:
                        log.info("Join Client");
                        rebalanceClientThroughput();
                        break;
                    case NodeDeleted:
                        log.info("Detach Client");
                        rebalanceClientThroughput();
                        break;
                    default:
                        break;
                }
            }
        };

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            KeeperException.Code returnCode = KeeperException.Code.get(rc);

            if (KeeperException.Code.OK == returnCode) {
                try {
                    // 전체 throughput 데이터를 가져옴
                    this.clusterMaxThroughput = getClusterMaxThroughput();

                    // 자식 프로세스 상태 확인
                    // 자식들 throughput 처리
                    zk.addWatch(ZookeeperConfig.clientRootNodePath, clientWatchWatcher, AddWatchMode.PERSISTENT_RECURSIVE);

                    // cluster의 max throughput 변경 감지
                    zk.addWatch(ZookeeperConfig.clusterMaxThroughputPath, event -> {
                        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                            clusterMaxThroughput = getClusterMaxThroughput();
                            rebalanceClientThroughput();
                        }
                    }, AddWatchMode.PERSISTENT);

                    // 다른 client 들의 throughput 제
                    rebalanceClientThroughput();
                } catch (Exception e) {
                    log.error("add watch exception {}", e.getMessage(), e);
                }
            }
        }

        private void rebalanceClientThroughput() {
            try {
                List<String> clientList = zk.getChildren(ZookeeperConfig.clientRootNodePath, false);

                // 일단 간단한 로직으로 ...
                long eachThroughput = (long) Math.floor(clusterMaxThroughput / clientList.size());

                for (String client : clientList) {
                    String clientPath = ZookeeperConfig.clientRootNodePath + "/" + client;
                    byte[] bStatus = zk.getData(clientPath, false, null);
                    log.info("{} status is {}", client, new String(bStatus));
                    Status status = objectMapper.readValue(bStatus, Status.class);
                    status.setThroughput(eachThroughput);
                    zk.setData(clientPath, objectMapper.writeValueAsBytes(status), -1);
                }
            } catch (Exception e) {
                log.error("rebalance Exception : {}", e.getMessage(), e);
            }
        }
    };

    private long getClusterMaxThroughput() {
        byte[] bClusterMaxThroughput = new byte[0];
        try {
            bClusterMaxThroughput = zk.getData(ZookeeperConfig.clusterMaxThroughputPath, false, null);
        } catch (Exception e) {
            log.error("get Cluster Max Throughput Exception : {}", e.getMessage(), e);
        }
        return Long.parseLong(new String(bClusterMaxThroughput));
    }
}
