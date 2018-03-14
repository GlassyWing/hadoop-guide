import org.apache.commons.lang.ArrayUtils;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class CreateGroup implements Watcher {

	private static final int SESSION_TIMEOUT = 5000;

	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected)
			connectedSignal.countDown();
	}

	public void create(String groupName) throws KeeperException, InterruptedException {
		String path = "/" + groupName;
		String createPath = zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println("Created " + createPath);
	}

	public void close() throws InterruptedException {
		zk.close();
	}

	public static void main(String[] args) throws Exception {
		CreateGroup createGroup = new CreateGroup();
		createGroup.connect(args[0]);
		createGroup.create(args[1]);
		createGroup.close();
	}
}
