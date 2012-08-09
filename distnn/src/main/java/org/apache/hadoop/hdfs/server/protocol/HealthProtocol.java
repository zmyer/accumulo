/**
 * this class is the portion of DatanodeProtocol that has to do with health
 */
package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface HealthProtocol extends VersionedProtocol {

	public static final long versionID = 0;

	public DatanodeCommand[] heartbeat(DatanodeRegistration reg);
	
	public void errorReport(DatanodeRegistration registration,
            int errorCode, 
            String msg) throws IOException;
	
	
}
