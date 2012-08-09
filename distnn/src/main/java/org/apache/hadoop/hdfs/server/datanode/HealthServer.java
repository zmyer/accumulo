/**
 * This server has several jobs:
 * 1. detect and respond to DataNode failure 
 * 2. report deleted blocks to DataNodes for removal
 * 3. balance the storage load across DataNodes
 * 4. coordinate cluster shutdown
 * 
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HealthProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;

public class HealthServer implements HealthProtocol {

	public static final int DEFAULT_PORT = 8020;
	private Server server;
	private Thread garbageBalanceThread;
	private boolean stopped = false;
	
	
	/**
	 * This class scans the namespace for orphaned blocks
	 * orphaned blocks are told to datanodes that host them so
	 * they can be deleted
	 * 
	 * checks for under-replicated blocks
	 * 
	 * it also checks for under- or over-loaded datanodes and 
	 * makes plans to rebalance the cluster
	 *
	 * the namespace could be enormous (shouldn't put in mem)
	 * the number of blocks could be enormous (shouldn't put in mem)
	 * the number of datanodes is up to 10k
	 * 
	 * @author aaron
	 *
	 */
	private class GarbageBalanceThread implements Runnable {

		@Override
		public void run() {
			while(!stopped) {
				
				
				
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		}
	}
	
	public HealthServer(Configuration conf) throws IOException {
		
		// scan the datanodes table to see which datanodes exist
		
		// start GarbageThread
		garbageBalanceThread = new Thread(new GarbageBalanceThread());
		garbageBalanceThread.start();
		
		// start RPC server to listen for requests from DataNodes
		InetSocketAddress socAddr =  NetUtils.createSocketAddr(FileSystem.getDefaultUri(conf).getAuthority(), DEFAULT_PORT);
		this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
				10, false, conf);

		this.server.start();
	}

	// ------- Health Protocol methods
	
	@Override
	public DatanodeCommand[] heartbeat(DatanodeRegistration reg) {
		// update this node's status as alive
		
		// check for any blocks this node is hosting that have been deleted
		
		
		return null;
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
	throws IOException {

		return versionID;
	}

	@Override
	public void errorReport(DatanodeRegistration registration, int errorCode,
			String msg) throws IOException {
		System.out.println(registration.getHost() + " " + errorCode + " " + msg);
		
	}
	
	// ------- end Health Protocol methods

	public void join() {
		try {
			this.server.join();
		} catch (InterruptedException ie) {
			stopped = true;
		}
	}

	public static void main() throws IOException {
		Configuration conf = new Configuration();

		HealthServer healthServer = new HealthServer(conf);
		healthServer.join();
	}
	
}
