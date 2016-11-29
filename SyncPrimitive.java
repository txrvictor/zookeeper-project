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
	static String root = "/root";
	static String address;

	SyncPrimitive(String address) {
		SyncPrimitive.address = address;
		if (zk == null) {
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (Exception e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
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
		String barriersRoot;

		Barrier(int size) {
			super(SyncPrimitive.address);
			this.size = size;

			// Create barrier node
			if (zk != null) {
				barriersRoot = root + "/barriers";
				try {
					Stat s = zk.exists(barriersRoot, false);
					if (s == null) {
						zk.create(barriersRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
			String barrierNode = zk.create(barriersRoot + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("BARRIER UP: " + barrierNode);
			while (true) {
				synchronized (SyncPrimitive.mutex) {
					List<String> list = zk.getChildren(barriersRoot, true);
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
		boolean leave(String tokenNode) throws Exception {
			//zk.delete(barrierNode, 0);
			while (true) {
				synchronized (SyncPrimitive.mutex) {
					List<String> list = zk.getChildren(barriersRoot, true);
					// verify barriers and date
					if (list.size() > 0) {
						System.out.println("More than one barrier stills up, checking...");	
						for (String n : list) {
							String nodePath = barriersRoot + "/" + n;
							Stat stat = zk.exists(nodePath, false);
							if (stat != null) {							
								// Barriers could have more conditions, but
								// leaving just date expiration for this project			
								String dateStr = new String(zk.getData(tokenNode, false, stat), "UTF-8");
								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								if (new Date().after(sdf.parse(dateStr))) {
									System.out.print("Token expirated! Processing... ");
									
									// Delete the barrier
									SyncPrimitive.DeleteNode(nodePath);
									System.out.println("Deleted Barrier Node: " + nodePath);

									// Finally delete the token...
									SyncPrimitive.DeleteNode(tokenNode);
									System.out.println("Deleted the Expired Token Node: " + tokenNode);

									break;
						
								} else {
									System.out.println("Not expirated yet!");
									return false;
								}
							}
						}
						// left barrier
						return true;
					} else {
						// no barriers left
						System.out.println("No more barriers!");
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
		String locksRoot;
		
		Lock() {
			super(SyncPrimitive.address);

			// Create ZK node name
			if (zk != null) {
				locksRoot = root + "/locks";
				try {
					Stat s = zk.exists(locksRoot, false);
					if (s == null) {
						zk.create(locksRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
			pathName = zk.create(locksRoot + "/lock-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("My path name is: " + pathName);
			// Steps 2 to 5
			return testMin();
		}

		boolean testMin() throws KeeperException, InterruptedException {
			while (true) {
				Integer suffix = new Integer(pathName.substring(17));
				// Step 2
				List<String> list = zk.getChildren(locksRoot, false);
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
				Stat s = zk.exists(locksRoot + "/" + maxString, this);
				System.out.println("Watching " + locksRoot + "/" + maxString);
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
						
						System.out.println("Executing Lock Compute Thread....");

						// create sequencial node with date as metadata
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						
						Date currentTime = new Date();
						System.out.println("Current time: " + sdf.format(currentTime));
						
						// actual date plus 20 seconds minute
						String dateStr = sdf.format(new Date(currentTime.getTime() + 20000));
						System.out.println("Expiration time: " + dateStr);

						try {
							String tokenRoot = SyncPrimitive.root + "/tokens";
							Stat s = zk.exists(tokenRoot, false);
							if (s == null) {
								zk.create(tokenRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
							}

							// create the unique node
							String tokenNode = zk.create(tokenRoot + "/TOKEN-", dateStr.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
							System.out.println("Created the following TokenNode: " + tokenNode);
						
							// Enter the barrier and only leave when the token expires
							Barrier b = new Barrier(1); // barrier of 1 node
							b.enter();
							
							while (!b.leave(tokenNode)) {
								System.out.println("Waiting to leave the barrier...");
								try {
									Thread.sleep(5000);
								} catch (InterruptedException e) {
									
								}
					
							}
						
							System.out.println("Left barrier and deleted node");
							System.out.println("-- END OF CLIENT SESSION --");
							
							System.exit(0);
							
						} catch (Exception e) {
							System.out.println("Exception in processing: " + e);
						}
					}
				});
				t.start();
				
				// delete the node to unlock
				System.out.println("Lock released!");
				SyncPrimitive.DeleteNode(pathName);
				
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
		String electionRoot;

		Leader(String leader, int id) {
			super(SyncPrimitive.address);
			
			electionRoot = root + "/election";
			
			this.leader = leader;
			this.id = new Integer(id).toString();
			// Create ZK node name
			if (zk != null) {
				try {
					// Create election znode
					Stat s1 = zk.exists(electionRoot, false);
					if (s1 == null) {
						zk.create(electionRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
			this.pathName = zk.create(electionRoot + "/n-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("My path name is: " + pathName + " and my id is: " + id + "!");
			return check();
		}

		boolean check() throws KeeperException, InterruptedException {
			Integer suffix = new Integer(pathName.substring(17));
			while (true) {
				List<String> list = zk.getChildren(electionRoot, false);
				Integer min = new Integer(list.get(0).substring(5));
				System.out.println("List (root/election/): " + list.toString());
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
				Stat s = zk.exists(electionRoot + "/" + maxString, this);
				System.out.println("Watching " + electionRoot + "/" + maxString);
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
			// try again to get the lock now that nodes were deleted
			// and probably there is a new leader
	        Lock lock = new Lock();  
            try {
				if (lock.lock()) {
					// lock compute shall execute the token methods
					System.out.println("Going to compute the lock execution...");
					lock.compute();		            
				} else {
					System.out.println("Couldn't get a lock... wait");
				    while (true) {
				        // Waiting for a notification
				    }
				}
			} catch (Exception e) {
				System.out.println("Error: " + e);
			}
		}
	}

	public static void main(String args[]) {
        // arg[0]: e.g. "localhost"
        new SyncPrimitive(args[0]);
        String leaderPath = "/leader";

        // Check for a leader
        System.out.println("Checking for a leader...");
        try {
			Stat s = zk.exists(leaderPath, false);
			if (s != null) {
				byte[] idLeader = zk.getData(leaderPath, false, s);
				System.out.println("Leader exists: " + new String(idLeader));
			} 
			else {
				// Not found, elect a leader to start
				Random rand = new Random();
     			int r = rand.nextInt(1000000);
        		Leader leader = new Leader(leaderPath, r);
        		boolean success = leader.elect();			
				if (success) {
					System.out.println("New Leader elected!");
				} else {
					System.out.println("Couldn't elect a leader, wait for notification of mutex");
					while (true) {
						// Waiting for a notification (will exec compute() when mutex notify)
					}
				}
			}

			// Get the lock if we reached this point of code
			System.out.println("Now let's try to get the lock...");
	        Lock lock = new Lock();  
            if (lock.lock()) {
            	// lock compute shall execute the token methods
            	System.out.println("Going to compute the lock execution...");
            	lock.compute();
            } else {
            	System.out.println("Couldn't get a lock... wait");
                while (true) {
                    // Waiting for a notification of mutex
                }
            }
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }

}
