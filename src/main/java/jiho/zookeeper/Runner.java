package jiho.zookeeper;

import java.util.concurrent.CountDownLatch;

public class Runner {

    public static void main(String[] args) throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);

        new Client();

        cdl.await();
    }
}
