package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;

public interface FakeNameNode extends ClientProtocol, DatanodeProtocol {
  
}
