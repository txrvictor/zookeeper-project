import java.util.Date;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class TokenGenerator implements Watcher {

    static ZooKeeper zk = null;
    static String root = "/root";
    static Integer mutex = new Integer(-1);

    TokenGenerator(String address) {
        if (zk == null) {
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
    }

    synchronized public void process(WatchedEvent event) {
		synchronized (mutex) {
			mutex.notify();
		}
	}

    public static void main(String args[]) {
        // arg[0]: e.g. "localhost"
        TokenGenerator connector = new TokenGenerator(args[0]);
        
        // Elect a leader to start
     	Random rand = new Random();
     	int r = rand.nextInt(1000000);
        SyncPrimitive.Leader leader = new SyncPrimitive.Leader(zk, root, "/leader", r, mutex);
        try {
			boolean success = leader.elect();
			if (success) {
				
				// Get the lock
		        SyncPrimitive.Lock lock = new SyncPrimitive.Lock(zk, root, mutex);  
	            if (lock.lock()) {
	            	// lock compute shall execute the token methods
	            	lock.compute();
	            } else {
	                while (true) {
	                    // Waiting for a notification
	                }
	            }
			} else {
				while (true) {
					// Waiting for a notification
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

}