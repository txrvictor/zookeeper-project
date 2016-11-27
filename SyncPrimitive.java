import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.util.Date;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class SyncPrimitive implements Watcher {

	static ZooKeeper zk = null;
	static Integer mutex;
	static String root;

	SyncPrimitive(ZooKeeper zk, String root, Integer mutex) {
		if (SyncPrimitive.zk == null) {
			SyncPrimitive.zk = zk;
			SyncPrimitive.mutex = mutex;
			SyncPrimitive.root = root;
		}
	}
	
	public static void CreateEphemeralNode(String path, byte[] data) throws Exception {
		if (SyncPrimitive.zk != null)
			SyncPrimitive.zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

    public static void UpdateNode(String path, byte[] data) throws Exception {
    	if (SyncPrimitive.zk != null)
    		SyncPrimitive.zk.setData(path, data, zk.exists(path, true).getVersion());
    }

    public static void DeleteNode(String path) throws Exception {
    	if (SyncPrimitive.zk != null)
    		SyncPrimitive.zk.delete(path, zk.exists(path, true).getVersion());
    }

    synchronized public void process(WatchedEvent event) {
		synchronized (mutex) {
			SyncPrimitive.mutex.notify();
		}
	}

	/**
	 * Barrier
	 */
	static public class Barrier extends SyncPrimitive {
		int size;
		String name;

		/**
		 * Barrier constructor
		 *
		 * @param address
		 * @param root
		 * @param size
		 */
		Barrier(ZooKeeper zk, String root, Integer mutex, int size) {
			super(zk, root, mutex);
			this.size = size;

			// Create barrier node
			if (zk != null) {
				try {
					Stat s = zk.exists(root, false);
					if (s == null) {
						zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				} catch (KeeperException e) {
					System.out.println("Keeper exception when instantiating queue: " + e.toString());
				} catch (InterruptedException e) {
					System.out.println("Interrupted exception");
				}
			}

			// My node name
			try {
				name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
			} catch (UnknownHostException e) {
				System.out.println(e.toString());
			}
		}

		/**
		 * Join barrier
		 *
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		boolean enter() throws Exception {
			zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			while (true) {
				synchronized (SyncPrimitive.mutex) {
					List<String> list = zk.getChildren(root, true);
					if (list.size() < size) {
						SyncPrimitive.mutex.wait();
					} else {
						return true;
					}
				}
			}
		}

		/**
		 * Wait until all reach barrier
		 *
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		boolean leave() throws Exception {
			zk.delete(root + "/" + name, 0);
			while (true) {
				synchronized (SyncPrimitive.mutex) {
					List<String> list = zk.getChildren(root, true);
					if (list.size() > 0) {
						// verify date
						for (String zNode : list) {
							Stat stat = zk.exists(zNode, false);
							if (stat != null) {
								String dateStr = new String(zk.getData(zNode, false, stat), "UTF-8");
								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								if (new Date().after(sdf.parse(dateStr))) {
									SyncPrimitive.DeleteNode(zNode);
									return true;
								}
							}
						}
						SyncPrimitive.mutex.wait();
					} else {
						return true;
					}
				}
			}
		}
	}

	/**
	 * Lock
	 */
	static public class Lock extends SyncPrimitive {
		String pathName;

		Lock(ZooKeeper zk, String root, Integer mutex) {
			super(zk, root, mutex);

			// Create ZK node name
			if (zk != null) {
				try {
					Stat s = zk.exists(root, false);
					if (s == null) {
						zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				} catch (KeeperException e) {
					System.out.println("Keeper exception when instantiating queue: " + e.toString());
				} catch (InterruptedException e) {
					System.out.println("Interrupted exception");
				}
			}
		}

		boolean lock() throws KeeperException, InterruptedException {
			// Step 1
			pathName = zk.create(root + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("My path name is: " + pathName);
			// Steps 2 to 5
			return testMin();
		}

		boolean testMin() throws KeeperException, InterruptedException {
			while (true) {
				Integer suffix = new Integer(pathName.substring(12));
				// Step 2
				List<String> list = zk.getChildren(root, false);
				Integer min = new Integer(list.get(0).substring(5));
				System.out.println("List: " + list.toString());
				String minString = list.get(0);
				for (String s : list) {
					Integer tempValue = new Integer(s.substring(5));
					if (tempValue < min) {
						min = tempValue;
						minString = s;
					}
				}
				System.out.println("Suffix: " + suffix + ", min: " + min);
				// Step 3
				if (suffix.equals(min)) {
					System.out.println("Lock acquired for " + minString + "!");
					return true;
				}
				// Step 4
				// Wait for the removal of the next lowest sequence number
				Integer max = min;
				String maxString = minString;
				for (String s : list) {
					Integer tempValue = new Integer(s.substring(5));
					if (tempValue > max && tempValue < suffix) {
						max = tempValue;
						maxString = s;
					}
				}
				// Exists with watch
				Stat s = zk.exists(root + "/" + maxString, this);
				System.out.println("Watching " + root + "/" + maxString);
				// Step 5
				if (s != null) {
					// Wait for notification
					break;
				}
			}
			System.out.println(pathName + " is waiting for a notification!");
			return false;
		}

		synchronized public void process(WatchedEvent event) {
			synchronized (SyncPrimitive.mutex) {
				String path = event.getPath();
				if (event.getType() == Event.EventType.NodeDeleted) {
					System.out.println("Notification from " + path);
					try {
						if (testMin()) {
							// Step 5 (cont.) -> go to step 2 to check
							this.compute();
						} else {
							System.out.println("Not lowest sequence number! Waiting for a new notification.");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

		void compute() {
			System.out.println("Lock acquired!");
			try {
				Thread t = new Thread(new Runnable() {
					public void run() {

						// create sequencial node with date as metadata
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						// actual date plus one minute
						String dateStr = sdf.format(new Date(new Date().getTime() + 60000));

						try {
							SyncPrimitive.CreateEphemeralNode(SyncPrimitive.root, dateStr.getBytes("UTF-8"));
						
							// barrier the node deletion until date has passed
							Barrier b = new Barrier(zk, SyncPrimitive.root, 1, SyncPrimitive.mutex); // barrier of 1 node
							b.enter();
							
							while (!b.leave()) {
								try {
									Thread.sleep(30000);
								} catch (InterruptedException e) {}
							}
						
							System.out.println("Left barrier and deleted node");
						} catch (Exception e) {
							System.out.println("Error creating node!");
						}
					}
				});
				t.start();
				
				// Exits, which releases the ephemeral node (Unlock operation)
				System.out.println("Lock released!");
				System.exit(0);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Error, lock released!");
				System.exit(0);
			}
		}
	}

	/**
	 * Leader Election
	 */
	static public class Leader extends SyncPrimitive {
		String leader;
		String id; // Id of the leader
		String pathName;

		Leader(ZooKeeper zk, String root, String leader, int id, Integer mutex) {
			super(zk, root, mutex);
			this.leader = leader;
			this.id = new Integer(id).toString();
			// Create ZK node name
			if (zk != null) {
				try {
					// Create election znode
					Stat s1 = zk.exists(root, false);
					if (s1 == null) {
						zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
					// Checking for a leader
					Stat s2 = zk.exists(leader, false);
					if (s2 != null) {
						byte[] idLeader = zk.getData(leader, false, s2);
						System.out.println("Current leader with id: " + new String(idLeader));
					}

				} catch (KeeperException e) {
					System.out.println("Keeper exception when instantiating queue: " + e.toString());
				} catch (InterruptedException e) {
					System.out.println("Interrupted exception");
				}
			}
		}

		boolean elect() throws KeeperException, InterruptedException {
			this.pathName = zk.create(root + "/n-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("My path name is: " + pathName + " and my id is: " + id + "!");
			return check();
		}

		boolean check() throws KeeperException, InterruptedException {
			Integer suffix = new Integer(pathName.substring(12));
			while (true) {
				List<String> list = zk.getChildren(root, false);
				Integer min = new Integer(list.get(0).substring(5));
				System.out.println("List: " + list.toString());
				String minString = list.get(0);
				for (String s : list) {
					Integer tempValue = new Integer(s.substring(5));
					// System.out.println("Temp value: " + tempValue);
					if (tempValue < min) {
						min = tempValue;
						minString = s;
					}
				}
				System.out.println("Suffix: " + suffix + ", min: " + min);
				if (suffix.equals(min)) {
					this.leader();
					return true;
				}
				Integer max = min;
				String maxString = minString;
				for (String s : list) {
					Integer tempValue = new Integer(s.substring(5));
					// System.out.println("Temp value: " + tempValue);
					if (tempValue > max && tempValue < suffix) {
						max = tempValue;
						maxString = s;
					}
				}
				// Exists with watch
				Stat s = zk.exists(root + "/" + maxString, this);
				System.out.println("Watching " + root + "/" + maxString);
				// Step 5
				if (s != null) {
					// Wait for notification
					break;
				}
			}
			System.out.println(pathName + " is waiting for a notification!");
			return false;

		}

		synchronized public void process(WatchedEvent event) {
			synchronized (SyncPrimitive.mutex) {
				if (event.getType() == Event.EventType.NodeDeleted) {
					try {
						boolean success = check();
						if (success) {
							compute();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

		void leader() throws KeeperException, InterruptedException {
			System.out.println("Become a leader: " + id + "!");
			// Create leader znode
			Stat s2 = zk.exists(leader, false);
			if (s2 == null) {
				zk.create(leader, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} else {
				zk.setData(leader, id.getBytes(), 0);
			}
		}

		void compute() {
			System.out.println("I will die after 10 seconds!");
			try {
				new Thread();
				Thread.sleep(10000);
				System.out.println("Process " + id + " died!");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.exit(0);
		}
	}

}
