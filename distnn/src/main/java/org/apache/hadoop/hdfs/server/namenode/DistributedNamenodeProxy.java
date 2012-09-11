/**
 * To Do
 * 
 *  add hierarchical locking
 * 	add support for permissions
 * 	add support for leasing
 * 
 * finish namenode actions:
 * 
 * finish datanode actions:
 * 
 * 
 * 
 * store this kind of stuff in namespaceTable for each file:
 * 
 * Path path
 * long length
 * boolean isdir
 * short block_replication
 * long blocksize
 * long modification_time
 * long access_time
 * FsPermission permission
 * String owner
 * String group
 * 
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DNNConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorWatcher;

public class DistributedNamenodeProxy implements FakeNameNode {
  Executor executor = Executors.newSingleThreadExecutor();

  public static class ConnectInfo {
    public ConnectInfo(Configuration conf) {
      this.passwd = conf.get("dnn.user.password", "").getBytes();
      if (passwd.length == 0)
        throw new IllegalArgumentException("dnn.user.password not set");
      this.username = conf.get("dnn.user.username", "root");
      this.zookeepers = conf.get("dnn.zookeepers", "localhost"); 
      this.instance = conf.get("dnn.instance.name", "");
      if (instance.length() == 0)
        throw new IllegalArgumentException("dnn.instance.name not set");
    }
    public String username;
    public byte[] passwd;
    public String zookeepers;
    public String instance;
  }

  private static Logger log = Logger.getLogger(DistributedNamenodeProxy.class);
  
  private class Replicator {
    
    private HashSet<DatanodeInfo> targets;
    
    Replicator() {
      targets = new HashSet<DatanodeInfo>();
    }

    void start() {
      zookeeper.getData().usingWatcher(new CuratorWatcher() {
        @Override
        public void process(WatchedEvent event) throws Exception {
          scanDatanodes();
          synchronized (this) {
            this.notifyAll();
          }
        }});
    }
    
    DatanodeInfo[] getReplicationTargets(int replicationFactor) throws IOException {

      synchronized (this) {
        if (targets.size() == 0) {
          scanDatanodes();
        }
        while (targets.size() == 0) {
          try {
            wait(250);
          } catch (InterruptedException e) {
            //
          }
        }
      }
      
      List<DatanodeInfo> targetsCopy = new ArrayList<DatanodeInfo>();
      synchronized (targets) {
        targetsCopy.addAll(targets);
      }
      // pick nodes at random
      // TODO: take into account whether a datanode is too full to host another block
      Collections.shuffle(targetsCopy);
      
      if(targetsCopy.size() < 1)
        throw new IOException("unable to achieve required replication: too few datanodes running");
      
      targetsCopy = targetsCopy.subList(0, Math.min(replicationFactor, targetsCopy.size()));
      DatanodeInfo[] targetSetArray = targetsCopy.toArray(new DatanodeInfo[targetsCopy.size()]);
      return targetSetArray;
    }
    
    
    // rescan the datanodes table to keep the set of datanodes up to date
    // other client code can also help us remove nodes from this set when reporting
    // failed interactions with a datanode
    private void scanDatanodes() throws IOException {
      log.info("scanning datanodes table ..");
      HashSet<DatanodeInfo> updatedTargets = new HashSet<DatanodeInfo>();
      BatchScanner scanner = createBatchScanner(TABLES.DATANODES, new Range());
      COLUMNS.IPC_PORT.fetch(scanner);
      try {
        for (Entry<Key,Value> entry : scanner) {
          String nodeName = entry.getKey().getRow().toString();
          int ipcPort = Integer.parseInt(entry.getValue().toString());
          updatedTargets.add(new DatanodeInfo(new DatanodeID(nodeName, "", -1, ipcPort)));
        }
      } finally {
        scanner.close();
      }
      try {
        if (updatedTargets.isEmpty()) {
          log.info("scanning datanodes from zookeeper ..");
          for (String nodeName : zookeeper.getChildren().forPath(DNNConstants.DATANODES_PATH))
            updatedTargets.add(new DatanodeInfo(new DatanodeID(nodeName)));
        }
      } catch (Exception ex) {
        log.warn(ex, ex);
      }
      log.info("there are " + updatedTargets.size() + " datanodes");
      targets = updatedTargets;
    }
  } 
  
  static private BatchScanner createBatchScanner(Connector conn, String table, Range ... ranges) throws IOException {
    try {
      BatchScanner result = conn.createBatchScanner(table, Constants.NO_AUTHS, QUERY_THREADS);
      result.setRanges(Arrays.asList(ranges));
      return result;
    } catch (TableNotFoundException ex) {
      throw new IOException(ex);
    }
  }
  
  static private Scanner createScanner(Connector conn, String table, Range range) throws IOException {
    try {
      Scanner result = conn.createScanner(table, Constants.NO_AUTHS);
      result.setRange(range);
      return result;
    } catch (TableNotFoundException ex) {
      throw new IOException(ex);
    }
  }
  
  private static BatchWriter createBatchWriter(Connector conn, String table) throws IOException {
    try {
      return conn.createBatchWriter(table, 10*1000, 1000, 4);
    } catch (TableNotFoundException ex) {
      throw new IOException(ex);
    }
  }
  
  private static byte[] getParentPath(String src) {
    if(src.equals("/"))
      return "/".getBytes();
    
    String[] components = src.split(Path.SEPARATOR);
    
    StringBuilder sb = new StringBuilder();
    for(int i=0; i < components.length - 1; i++)
      sb.append(components[i] + "/");
    
    if(sb.length() > 1)
      sb.deleteCharAt(sb.length()-1);
    
    return sb.toString().getBytes();
  }
  
  public static String normalizePath(String src) {
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }
  
  private static void unimplemented(Object ... args) {
    Throwable t = new Throwable();
    String method = t.getStackTrace()[1].getMethodName();
    log.warn(method + " unimplemented, args: " + Arrays.asList(args), t);
  }
  
  private Random rand = new Random();
  private Replicator replicator = new Replicator();
  private final Connector conn;
  static final class TABLES {
    private final static String NAMESPACE = "namespace";
    private final static String BLOCKS = "blocks";
    private final static String DATANODES = "datanodes";
  }
  static final class FAMILIES {
    private final static Text INFO = new Text("info");
    private final static Text CHILDREN = new Text("children");
    private final static Text BLOCKS = new Text("blocks");
    private final static Text DATANODES = new Text("datanodes");
    private final static Text COMMAND = new Text("command");
  }
  static final class COLUMNS {
    private final static ColumnFQ REMAINING = new ColumnFQ(FAMILIES.INFO, new Text("remaining"));
    private final static ColumnFQ SIZE = new ColumnFQ(FAMILIES.INFO, new Text("size"));
    private final static ColumnFQ IS_DIR = new ColumnFQ(FAMILIES.INFO, new Text("isDir"));
    private final static ColumnFQ CAPACITY = new ColumnFQ(FAMILIES.INFO, new Text("capacity"));
    private final static ColumnFQ IPC_PORT = new ColumnFQ(FAMILIES.INFO, new Text("ipc_port"));
    private final static ColumnFQ USED = new ColumnFQ(FAMILIES.INFO, new Text("used"));
    private final static ColumnFQ REPLICATION = new ColumnFQ(FAMILIES.INFO, new Text("replication"));
    private final static ColumnFQ BLOCK_SIZE = new ColumnFQ(FAMILIES.INFO, new Text("blocksize"));
    private final static ColumnFQ MODIFICATION_TIME = new ColumnFQ(FAMILIES.INFO, new Text("mtime"));
    private final static ColumnFQ STORAGE_ID = new ColumnFQ(FAMILIES.INFO, new Text("storageID"));
    private final static ColumnFQ PERMISSION = new ColumnFQ(FAMILIES.INFO, new Text("permission"));
  }
  private final static Value blank = new Value(new byte[]{});
  
  private final static int QUERY_THREADS = 10;
  
  //private HealthProtocol healthServer;

  private long lastCapacity = -1;
  
  private long lastDfsUsed = -1;
  
  private long lastRemaining = -1;
  
  private final CuratorFramework zookeeper;
  
  public DistributedNamenodeProxy(CuratorFramework keeper, Configuration conf) throws IOException {
    log.info("========= Distributed Name Node Proxy init =========");
    ConnectInfo info = new ConnectInfo(conf);
    Instance instance = new ZooKeeperInstance(info.instance, info.zookeepers);
    zookeeper = keeper;
    try {
      this.conn = instance.getConnector(info.username, info.passwd);
    } catch (Exception e) {
      throw new IOException(e);
    }
    replicator.start();
    //		String healthNodeHost = config.get("healthnode");
    //		if(healthNodeHost == null)
    //			throw new IOException("error: no healthnode address specified. add one to core-site.xml");
    
    //InetSocketAddress healthNodeAddr = new InetSocketAddress(healthNodeHost, 9090);
    
    
    //healthServer = (HealthProtocol)RPC.getProxy(HealthProtocol.class,
    //		HealthProtocol.versionID, healthNodeAddr, config,
    //	        NetUtils.getSocketFactory(config, HealthProtocol.class));
    
  }
  
  
  @Override
  public void abandonBlock(Block b, String src, String holder)
      throws IOException {
    log.info("using abandonBlock");
    
    // find the block's position in the list (probably the last one)
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    bs.fetchColumnFamily(FAMILIES.BLOCKS);
    
    // delete it from the file
    Mutation m = new Mutation(new Text(src));
    try {
      for (Entry<Key,Value> entry : bs) {
        String cq = entry.getKey().getColumnQualifier().toString();
        String parts[] = cq.split("_");
        long block = Long.parseLong(parts[1]);
        if (b.getBlockId() == block) {
          m.putDelete(FAMILIES.BLOCKS, entry.getKey().getColumnQualifier());
        }
      }
    } finally {
      bs.close();
    }
    if (m.getUpdates().isEmpty()) {
      throw new IOException("Block " + b.getBlockId() + " not found to abandon for " + src);
    }
    
    // delete the block size and location information
    BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
    try {
      bw.addMutation(m);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        throw new RuntimeException(e);
      }
    }
    bw = createBatchWriter(TABLES.BLOCKS);
    try {
      Text row = new Text("" + b.getBlockId());
      bs = createBatchScanner(TABLES.BLOCKS, new Range(row));
      m = new Mutation(row);
      for (Entry<Key,Value> entry : bs) {
        m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
      }
    } finally {
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        throw new RuntimeException(e);
      }
      bs.close();
    }
  }
  
  @Override
  public LocatedBlock addBlock(String arg0, String arg1) throws IOException {
    return addBlock(arg0, arg1, new DatanodeInfo[0]);
  }
  
  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to).  Return an array that consists
   * of the block, plus a set of machines.  The first on this list should
   * be where the client writes data.  Subsequent items in the list must
   * be provided in the connection to the first datanode.
   *
   * Make sure the previous blocks have been reported by datanodes and
   * are replicated.  Will return an empty 2-elt array if we want the
   * client to "try again later".
   * @throws IOException 
   */
  @Override
  public LocatedBlock addBlock(String src, String clientName, DatanodeInfo[] excludeNodes)
      throws IOException {
    log.info("using addBlock " + src + " " + clientName);

    // get the last block ID and replication
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    bs.fetchColumnFamily(FAMILIES.BLOCKS);
    COLUMNS.REPLICATION.fetch(bs);
    
    // TODO: fetch from configuration
    int defaultReplication = 3;
    int replication = -1;
    int blockPos = 0;
    try {
      for (Entry<Key,Value> entry : bs) {
        if (entry.getKey().getColumnFamily().equals(FAMILIES.BLOCKS))
          blockPos++;
        if (COLUMNS.REPLICATION.hasColumns(entry.getKey()))
          replication = Integer.parseInt(entry.getValue().toString());
      }
    } finally {
      bs.close();
    }
    if (replication < 1) {
      replication = defaultReplication;
    }
    
    // create new blocks on data nodes
    //   zookeeper holds the negative numbered blocks
    long blockID = Math.abs(rand.nextLong());
    byte[] blockIDBytes = Long.toString(blockID).getBytes();
    
    Block b = new Block(blockID, 0, 0);
    
    // choose a set of nodes on which to replicate block
    DatanodeInfo[] targets = replicator.getReplicationTargets(replication); 
    log.info("replicating " + blockID + " to " + Arrays.asList(targets));
    
    // TODO: get a lease to the first
    // TODO: can we record all this in the namespace table?
    // i.e. do we ever need to lookup blocks without knowing the associated files?
    // blockReceived() doesn't know the file mapping
    
    // record block to host mapping and vice versa
    recordBlockHosts(blockIDBytes, targets);
    
    // record file to block mapping
    Mutation nameData = new Mutation(new Text(src.getBytes()));
    nameData.put(FAMILIES.BLOCKS, new Text(String.format("%08d_%d", blockPos, blockID).getBytes()), blank);
    BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
    try {
      try {
        bw.addMutation(nameData);
      } finally {
        bw.close();  
      }
    } catch (MutationsRejectedException ex) {
      throw new IOException(ex);
    }
    LocatedBlock newBlock = new LocatedBlock(b, targets);
    log.info("addBlock new block for " + src + " " + b.getBlockName());
    return newBlock;
  }
  
  @Override
  public LocatedBlock append(String src, String clientName)
      throws IOException {
    throw new NotImplementedException();
  }
  
  /** ------------ Data Node Protocol Methods -----------
   * 
   */
  
  @Override
  public void blockReceived(DatanodeRegistration registration, final Block[] blocks, String[] delHints) throws IOException {
    log.info("using blockReceived");
    SimpleTimer.getInstance().schedule(new TimerTask() {
      @Override
      public void run() {
        
        // for each block we should have recorded its existence already
        // we should also know about the datanode
        
        // update blocks table
        try {
          final BatchWriter bw = createBatchWriter(TABLES.BLOCKS);
          try {
            for(Block b : blocks) {
              if (ZookeeperNameNode.isZooBlockId(b.getBlockId()))
                continue;
              Mutation blockData = new Mutation(new Text(Long.toString(b.getBlockId())));
              COLUMNS.BLOCK_SIZE.put(blockData, new Value(Long.toString(b.getNumBytes()).getBytes()));
              bw.addMutation(blockData);
            }
          } finally {
            bw.close();
          }
          // update total file space ?
        } catch (Exception ex) {
          log.info(ex, ex);
        }
      }
    }, 0);
  }
  
  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration,
      long[] blocks) throws IOException {
    log.info("using blockReport");
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    Set<Long> current = new HashSet<Long>();
    for (int i = 0; i < blist.getNumberOfBlocks(); i++) {
      if (blist.getBlockId(i) > 0) {
        current.add(blist.getBlockId(i));
      }
    }
    log.info(registration.getName() + " reports blocks " + current);
    if (current.isEmpty())
      return null;
    BatchWriter bw = createBatchWriter(TABLES.BLOCKS);
    Mutation m = new Mutation(registration.getName());
    Scanner scan = createScanner(TABLES.DATANODES);
    scan.setRange(new Range(registration.getName()));
    scan.fetchColumnFamily(FAMILIES.BLOCKS);
    try {
      for (Entry<Key,Value> entry : scan) {
        long block = Long.parseLong(entry.getKey().getColumnQualifier().toString());
        if (!current.remove(block)) {
          // found some block, not in the blocklist, remove the entry
          m.putDelete(FAMILIES.BLOCKS, entry.getKey().getColumnQualifier());
        }
      }
      if (!m.getUpdates().isEmpty())
        bw.addMutation(m);
    } catch (Exception ex) {
      throw new IOException(ex);
    } finally {
      try {
        bw.close();
      } catch (MutationsRejectedException ex) {
        throw new IOException(ex);
      }
    }
    List<Block> deleteList = new ArrayList<Block>(current.size());
    for (Long id : current) {
      deleteList.add(new Block(id));
    }
    log.debug("Asking " + registration.getName() + " to delete blocks " + deleteList);
    return new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, deleteList.toArray(new Block[0]));
  }
  
  @Override
  public void blocksBeingWrittenReport(DatanodeRegistration arg0, long[] arg1) throws IOException {
    unimplemented(arg0, arg1);
  }
  
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    unimplemented(token);
  }
  
  @Override
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeID[] newtargets) throws IOException {
    log.info("using commitBlockSynchronization");
    
  }
  
  @Override
  public boolean complete(String src, String clientName) throws IOException {
    log.info("using complete " + src);
    
    // write complete status to namespace?
    // does this just help avoid mutations to existent complete files?
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    bs.fetchColumnFamily(FAMILIES.BLOCKS);
    List<Range> ranges = new ArrayList<Range>();
    try {
      for (Entry<Key,Value> entry : bs) {
        String orderBlock = entry.getKey().getColumnQualifier().toString();
        ranges.add(new Range(orderBlock.split("_")[1]));
      }
    } finally {
      bs.close();
    }
    if (ranges.isEmpty())
      return true;
    long fileSize = 0;
    BatchScanner blockScanner = createBatchScanner(TABLES.BLOCKS, ranges.toArray(new Range[]{}));
    COLUMNS.BLOCK_SIZE.fetch(blockScanner);
    fileSize = 0;
    int count = 0;
    try {
      for (Entry<Key,Value> entry : blockScanner) {
        log.info("Looking at block sizes " + entry.getKey() + " -> " + entry.getValue());
        long blockSize = Long.parseLong(new String(entry.getValue().get()));
        if (blockSize == 0) 
          break;
        fileSize += blockSize;
        count++;
      }
    } finally {
      blockScanner.close();
    }
    if (count != ranges.size()) {
      log.info("Did not read block sizes for all blocks on file " + src + " read " + count + " but expected " + ranges.size());
      return false;
    }
    
    // write size to namespace table
    Mutation fileSizePut = new Mutation(new Text(src.getBytes()));
    COLUMNS.SIZE.put(fileSizePut, new Value(Long.toString(fileSize).getBytes()));
    BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
    try {
      try {
        bw.addMutation(fileSizePut);
      } finally {
        bw.close();
      }
    } catch (MutationsRejectedException ex) {
      throw new IOException(ex);
    }
    return true;
  }
  
  public int computeDatanodeWork() {
    log.info("using computeDatanodeWork");
    // TODO: how is this number used by the caller?
    return 0;
  }
  
  static private void put(Mutation m, ColumnFQ cfq, String value) {
    cfq.put(m, new Value(value.getBytes()));
  }
  
  static private Value now() {
    return new Value(Long.toString(System.currentTimeMillis()).getBytes());
  }
  
  @Override
  public void create(String src, FsPermission masked, String clientName, boolean overwrite, boolean createParent, short replication, long blockSize) throws IOException {
    log.info("using create");
    
    // verify that parent directories exist
    byte[] parent = getParentPath(src);
    String isDirFlag = null;
    ColumnFQ srcColumn = new ColumnFQ(FAMILIES.CHILDREN, new Text(src));
    
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(parent)));
    COLUMNS.IS_DIR.fetch(bs);
    bs.fetchColumnFamily(FAMILIES.CHILDREN);
    try {
      for (Entry<Key,Value> entry : bs) {
        if (COLUMNS.IS_DIR.hasColumns(entry.getKey()))
        {
          isDirFlag = new String(entry.getValue().get());
        }
        
        if (srcColumn.hasColumns(entry.getKey())) {
          throw new IOException("file exists: " + src);
        }
      }
    } finally {
      bs.close();
    }
    if (isDirFlag == null) {
      if (!createParent)
        throw new IOException("Parent directory does not exist " + new Text(parent));
      this.mkdirs(new String(parent), FsPermission.getDefault());
    }
    if ("N".equals(isDirFlag)) {
      throw new IOException("Parent entry is not a directory " + new Text(parent));
    }
    
    // TODO: check access permissions
    
    // edit namespace table to create this file
    
    /*
     * not yet recorded:
     * 
     * long access_time
     * String owner
     * String group
     * Leases
     */
    
    // TODO: not atomic
    try {
      BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
      try {
        Mutation createRequest = new Mutation(new Text(src));
        COLUMNS.MODIFICATION_TIME.put(createRequest, now());
        put(createRequest, COLUMNS.REPLICATION, Short.toString(replication));
        put(createRequest, COLUMNS.BLOCK_SIZE, Long.toString(blockSize));
        put(createRequest, COLUMNS.PERMISSION, masked.toString());
        put(createRequest, COLUMNS.IS_DIR, "N");
        bw.addMutation(createRequest);
        
        // record existence of new file in parent dir now or on complete?
        Mutation childCreate = new Mutation(new Text(getParentPath(src)));
        // TODO: could store that this is a file in the Value
        childCreate.put(FAMILIES.CHILDREN, new Text(src.getBytes()), blank);
        bw.addMutation(childCreate);
      } finally {
        bw.close();
      }
    } catch (MutationsRejectedException ex) {
      throw new IOException(ex);
    }
    
    // TODO: verify replication
    
    // TODO: grant a lease to the client??
    // GFS grants the lease to the primary datanode, not the client
  }
  
  @Override
  public void create(String src, FsPermission masked, String clientName,
      boolean overwrite, short replication, long blockSize)
          throws IOException {
    create(src, masked, clientName, overwrite, true, replication, blockSize);
  }
  
  private BatchScanner createBatchScanner(String table, Range ... ranges) throws IOException {
    return createBatchScanner(conn, table, ranges);
  }
  
  private Scanner createScanner(String table) throws IOException {
    try {
      return conn.createScanner(table, Constants.NO_AUTHS);
    } catch (TableNotFoundException ex) {
      throw new IOException(ex);
    }
  }
  
  public BatchWriter createBatchWriter(String table) throws IOException {
    return createBatchWriter(conn, table);
  }
  
  @Override
  public boolean delete(String src) throws IOException {
    log.info("using delete");
    // NameNode code calls this with recursive=true as default
    return delete(src, true);
  }
  
  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    log.info("using delete " + src);
    // check permissions - how?
    
    byte[] parent = getParentPath(src);
    
    // determine whether this is a directory
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    COLUMNS.IS_DIR.fetch(bs);
    bs.fetchColumnFamily(FAMILIES.CHILDREN);
    
    String isDir_ = null;
    ArrayList<Text> children = new ArrayList<Text>();
    try {
      for (Entry<Key,Value> entry : bs) {
        if (COLUMNS.IS_DIR.hasColumns(entry.getKey())) {
          isDir_ = entry.getKey().getColumnQualifier().toString();
        } else if (entry.getKey().getColumnFamily().equals(FAMILIES.CHILDREN)) {
          children.add(entry.getKey().getColumnQualifier());
        }
      }
    } finally {
      bs.close();
    }
    if("Y".equals(isDir_) && !recursive && children.size() > 0)
      throw new IOException("can't delete directory. not empty");
    
    Mutation childDelete = new Mutation(new Text(parent));
    Text srcText = new Text(src);
    childDelete.putDelete(FAMILIES.CHILDREN, srcText);
    COLUMNS.MODIFICATION_TIME.put(childDelete, now());
    
    ArrayList<Mutation> deletes = new ArrayList<Mutation>();
    getDeletes(srcText, deletes);
    deletes.add(childDelete);
    
    // delete everything at once
    BatchWriter nw = createBatchWriter(TABLES.NAMESPACE);
    Set<Text> blocks = new HashSet<Text>();
    try {
      try {
        nw.addMutations(deletes);
        for (Mutation m : deletes) {
          for (ColumnUpdate update : m.getUpdates()) {
            byte cf[] = update.getColumnFamily();
            if (FAMILIES.BLOCKS.compareTo(cf, 0, cf.length) == 0) {
              blocks.add(new Text(new String(update.getColumnQualifier()).split("_", 2)[1]));
            }
          }
        }
      } finally {
        nw.close();
      }
      if (blocks.isEmpty())
        return true;
      log.info("deleting blocks "+ blocks);
      Map<String, List<String>> hostBlockMap = new HashMap<String, List<String>>();
      // Now remove the blocks
      BatchWriter bw = createBatchWriter(TABLES.BLOCKS);
      // scan back the entries that go with the blocks
      List<Range> ranges = new ArrayList<Range>();
      for (Text row : blocks) {
        ranges.add(new Range(row));
      }
      bs = createBatchScanner(TABLES.BLOCKS, ranges.toArray(new Range[0]));
      // delete everything that matches our block list
      for (Entry<Key,Value> entry : bs) {
        if (blocks.contains(entry.getKey().getRow())) {
          Mutation m = new Mutation(entry.getKey().getRow());
          m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
          bw.addMutation(m);
          if (entry.getKey().getColumnFamily().equals(FAMILIES.DATANODES)) {
            String host = entry.getKey().getColumnQualifier().toString();
            String block = entry.getKey().getRow().toString();
            List<String> blockList = null;
            if ((blockList = hostBlockMap.get(host)) == null) {
              hostBlockMap.put(host, blockList = new ArrayList<String>());
            }
            blockList.add(block);
          }
        }
      }
      bs.close();
      bw.close();
      log.info("Host -> block map " + hostBlockMap);

      // Create commands to remove the blocks on the datanodes at the next heartbeat
      bw = createBatchWriter(TABLES.DATANODES);
      for (Entry<String,List<String>> entry : hostBlockMap.entrySet()) {
        String host = entry.getKey();
        Block block[] = new Block[entry.getValue().size()];
        int i = 0;
        for (String blockString : entry.getValue()) {
          block[i++] = new Block(Long.parseLong(blockString), 0, 0);
        }
        Mutation m = new Mutation(host);
        DatanodeCommand cmd = new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, block);
        m.put(FAMILIES.COMMAND, new Text(UUID.randomUUID().toString()), new Value(serialize(cmd)));
        bw.addMutation(m);
      }
      bw.close();
    } catch (MutationsRejectedException ex) {
      throw new IOException(ex);
    } catch (Exception ex) {
      log.info(ex, ex);
    }
    // TODO: when to return false?
    return true;
  }
  
  @Override
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    log.info("using distributedUpgradeProgress");
    return null;
  }
  
  @Override
  public void errorReport(DatanodeRegistration registration, int errorCode,
      String msg) throws IOException {
    log.info("using errorReport");
    
  }
  
  @Override
  public void finalizeUpgrade() throws IOException {
    log.info("using finalizeUpgrade");
    
  }
  
  @Override
  public void fsync(String src, String client) throws IOException {
    log.info("using fsync");
    
  }
  
  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws IOException {
    log.info("using getBlockLocations: " + src + " " + offset + " " + length);
    
    ArrayList<LocatedBlock> locatedBlocks = new ArrayList<LocatedBlock>();
    
    // get blocks from namespace table
    Value fileSizeBytes = null;
    java.util.Map<Text, Value> IDs = new TreeMap<Text, Value>();
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    bs.fetchColumnFamily(FAMILIES.BLOCKS);
    bs.fetchColumnFamily(FAMILIES.INFO);
    try {
      log.info("getting blocks for " + src + " from namespace table");
      for (Entry<Key,Value> entry : bs) {
        if (COLUMNS.SIZE.hasColumns(entry.getKey())) {
            fileSizeBytes = entry.getValue();
        } else if (entry.getKey().getColumnFamily().equals(FAMILIES.BLOCKS)) {
          IDs.put(entry.getKey().getColumnQualifier(), entry.getValue());
        }
      }
    } finally {
      bs.close();
    }
    
    long fileLength = 0;
    
    if(fileSizeBytes == null)
      throw new IOException("file not found: " + src);
    
    fileLength = Long.parseLong(new String(fileSizeBytes.get()));
    log.info("File " + src + " is " +fileLength + " bytes long");
    
    // TODO: calculate which blocks we need for the offset and length
    // TODO: could store the actual long id in the value of the table
    if(IDs.size() == 0) {
      throw new IOException("file not found: " + src);
    }
    
    boolean underConst = false;
    fileLength = 0L;
    long blockOffset = 0L;
    for(Text id : IDs.keySet()) {
      // remove block position indicator
      String idString = id.toString().split("_")[1];
      
      String blockIDString = new String(idString);
      log.info("found block: " + blockIDString);
      
      // lookup host and length information for each block
      // TODO: can we join this data into the namespace table?
      log.info("getting host data for block ...");
      long blockSize = 0;
      ArrayList<DatanodeInfo> dni = new ArrayList<DatanodeInfo>();
      bs = createBatchScanner(TABLES.BLOCKS, new Range(idString));
      bs.fetchColumnFamily(FAMILIES.DATANODES);
      COLUMNS.BLOCK_SIZE.fetch(bs);
      try {
        for (Entry<Key,Value> entry : bs) {
          if (COLUMNS.BLOCK_SIZE.hasColumns(entry.getKey())) {
            blockSize = Long.parseLong(new String(entry.getValue().get()));
            fileLength += blockSize;
            if (blockSize == 0) {
              underConst = true;
            }
            log.info("got size " + blockSize + " for block " + blockIDString);
          } else if (entry.getKey().getColumnFamily().equals(FAMILIES.DATANODES)) {
            String host = entry.getKey().getColumnQualifier().toString();
            dni.add(new DatanodeInfo(new DatanodeID(host)));
            log.info("got host: " + new String(host) + " for block " + blockIDString);
          }
        }
      } finally {
        bs.close();
      }
      
      // TODO: add generation	
      locatedBlocks.add(
          new LocatedBlock(
              new Block(Long.parseLong(idString), blockSize, 0), 
              dni.toArray(new DatanodeInfo[dni.size()]),
              blockOffset
              )		
          );
      
      blockOffset += blockSize;
    }
    
    // TODO: sort locatedBlocks by network-distance from client
    log.info("Reporting file size of " + fileLength + " underConstruction = " + true);
//    BatchWriter bw = createBatchWriter(namespaceTable);
//    try {
//      Mutation m = new Mutation(src);
//      ColumnFQ.put(m, infoSize, new Value(Long.toString(fileLength).getBytes()));
//      bw.addMutation(m);
//      bw.close();
//    } catch (Exception ex) {
//      throw new IOException(ex);
//    }
    return new LocatedBlocks(fileLength, locatedBlocks, underConst);
  }
  
  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    log.info("using getContentSummary");
    if (!path.endsWith("/"))
      path += "/";
    long summary[] = {0, 0, 1};
    Text endRange = new Text(path);
    endRange.append(new byte[]{(byte)0xff}, 0, 1);
    Range range = new Range(new Text(path), true, endRange, false);
    Scanner scanner = createScanner(conn, TABLES.NAMESPACE, range);
    COLUMNS.SIZE.fetch(scanner);
    COLUMNS.IS_DIR.fetch(scanner);
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      if (COLUMNS.SIZE.hasColumns(key)) {
        summary [0] += Long.parseLong(entry.getValue().toString());
      }
      if (COLUMNS.IS_DIR.hasColumns(key)) {
        if ("Y".equals(entry.getValue().toString()))
          summary[2]++;
        else
          summary[1]++;
      }
    }
    return new ContentSummary(summary[0], summary[1], summary[2]);
  }
  
  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    log.info("using getDatanodeReport");
    return null;
  }
  
  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
    unimplemented(renewer);
    return null;
  }
  
  /**
   * recursively create delete objects for src and all children
   * 
   * @param src
   * @param recursive
   * @return
   * @throws IOException 
   */
  private void getDeletes(Text src, List<Mutation> deletes) throws IOException {
    
    // Maybe this list won't fit in memory?
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    Mutation m = new Mutation(src);
    try {
      for (Entry<Key,Value> entry : bs) {
        Text columnFamily = entry.getKey().getColumnFamily();
        if (columnFamily.equals(FAMILIES.CHILDREN)) {
          getDeletes(entry.getKey().getColumnQualifier(), deletes);
        }
        log.info("deleting " + src + " " + entry.getKey().getColumnFamily() + ":" + entry.getKey().getColumnQualifier());
        if (!src.equals("/"))
          m.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
      }
      if (!m.getUpdates().isEmpty())
        deletes.add(m);
    } finally {
      bs.close();
    }
  }
  
  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    log.info("using getFileInfo " + src);
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(src));
    try {
      return loadFileStatus(src, bs.iterator());
    } finally {
      bs.close();
    }
  }
  
  /**
   * This method is currently doing a lot of lookups ...
   */
  @Override
  public DirectoryListing getListing(String src, byte[] startAfter) throws IOException {
    log.info("using getListing " + src);
    // TODO: use startAfter and needLocation
    
    ArrayList<HdfsFileStatus> files = new ArrayList<HdfsFileStatus>();
    
    String isDirFlag = null;
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    bs.fetchColumnFamily(FAMILIES.CHILDREN);
    COLUMNS.IS_DIR.fetch(bs);
    List<String> children = new ArrayList<String>();
    try {
      for (Entry<Key,Value> entry : bs) {
        log.info("Looking at entry " + entry.getKey() + " -> " + entry.getValue());
        String value = new String(entry.getValue().get());
        if (COLUMNS.IS_DIR.hasColumns(entry.getKey())) {
          isDirFlag = value;
        } else if (entry.getKey().getColumnFamily().equals(FAMILIES.CHILDREN)) {
          children.add(entry.getKey().getColumnQualifier().toString());
        }
      }
    } finally {
      bs.close();
    }
    if(isDirFlag == null)
      throw new IOException("directory not found: " + src);
    
    if(isDirFlag.equals("N"))
      throw new IOException(src + " is not a directory");
    
    log.info("Looking at children " + children);
    for (String child : children) {
      bs = createBatchScanner(TABLES.NAMESPACE, new Range(child));
      try {
        HdfsFileStatus stat = loadFileStatus(child, bs.iterator());
        files.add(stat);
      } finally {
        bs.close();
      }
    }
    return new DirectoryListing(files.toArray(new HdfsFileStatus[files.size()]), 0);
  }
  
  @Override
  public long getPreferredBlockSize(String filename) throws IOException {
    log.info("using getPreferredBlockSize");
    return 0;
  }
  
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    log.info("using getProtocolVersion");
    return 0;
  }
  
  @Override
  public long[] getStats() throws IOException {
    log.info("using getStats");
    return null;
  }
  
  private HdfsFileStatus loadFileStatus(String src, Iterator<Entry<Key,Value>> fileResult) throws IOException {
    log.info("loadFileStatus " + src);
    String isDirFlag = null;
    long modification_time = 0;
    long blocksize = 0;
    int block_replication = 0;
    long length = 0;
    String permissionString = null;
    
    Text row = null;
    while (fileResult.hasNext()) {
      Entry<Key, Value> entry = fileResult.next();
      log.info("looking at file data " + entry.getKey() + " -> " + entry.getValue());
      String value = new String(entry.getValue().get());
      if (COLUMNS.IS_DIR.hasColumns(entry.getKey())) {
        isDirFlag = value;
      } else if (COLUMNS.SIZE.hasColumns(entry.getKey())) {
        length = Long.parseLong(value);
      } else if (COLUMNS.REPLICATION.hasColumns(entry.getKey())) {
        block_replication = Integer.parseInt(value);
      } else if (COLUMNS.BLOCK_SIZE.hasColumns(entry.getKey())) {
        blocksize = Long.parseLong(value);
      } else if (COLUMNS.MODIFICATION_TIME.hasColumns(entry.getKey())) {
        modification_time = Long.parseLong(value);
      } else if (COLUMNS.PERMISSION.hasColumns(entry.getKey())) {
        permissionString = new String(entry.getValue().get());
      }
      row = entry.getKey().getRow();
    }
    if (isDirFlag == null) {
      log.info("did not find is_dir for " + src);
      throw new FileNotFoundException(src);
    }
    
    boolean isdir = isDirFlag.equals("Y");
    FsPermission permission = FsPermission.getDefault();
    log.info("permission string " + permissionString);
    if (permissionString != null) {
      permission = FsPermission.valueOf((isdir ? "d":"-") + permissionString);
    }
    
    // TODO
    long access_time = modification_time;
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    String group = UserGroupInformation.getCurrentUser().getGroupNames()[0];
    return new HdfsFileStatus(length, isdir, block_replication, blocksize, modification_time, access_time, permission, user, group, 
        TextUtil.getBytes(row));
  }
  
  @Override
  public void metaSave(String filename) throws IOException {
    log.info("using metaSave");
    
  }
  
  @Override
  public boolean mkdirs(String src, FsPermission masked) throws IOException {
    log.info("using mkdirs ");
    
    if (!DFSUtil.isValidName(src)) {
      throw new IOException("Invalid directory name: " + src);
    }
    
    // TODO: check permissions
    
    src = normalizePath(src);
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(src)));
    COLUMNS.IS_DIR.fetch(bs);
    try {
      for (Entry<Key,Value> entry : bs) {
        if (COLUMNS.IS_DIR.hasColumns(entry.getKey())) {
          if (new String(entry.getValue().get()).equals("Y"))
            return true;
          else
            throw new IOException(src + "already exists and is not a directory");
        }
      }
    } finally {
      bs.close();
    }
    
    // TODO: get locks ...
    // verify parent path exists
    byte[] parentPath = getParentPath(src);
    
    bs = createBatchScanner(TABLES.NAMESPACE, new Range(new Text(parentPath)));
    COLUMNS.IS_DIR.fetch(bs);
    bs.fetchColumnFamily(FAMILIES.CHILDREN);
    String isDirString = null;
    try {
      for (Entry<Key,Value> entry : bs) {
        if (COLUMNS.IS_DIR.hasColumns(entry.getKey())) {
          isDirString = new String(entry.getValue().get());
        } else if (entry.getKey().getColumnFamily().equals(FAMILIES.CHILDREN)) {
        }
      }
    } finally {
      bs.close();
    }
    
    if(isDirString == null) 
      mkdirs(new String(parentPath), masked);
    else if(isDirString.equals("N")) 
      throw new IOException("error: parent " + src + " is not a directory");
    
    // edit namespace
    BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
    Mutation m = new Mutation(new Text(parentPath));
    m.put(FAMILIES.CHILDREN, new Text(src), blank);
    //String dirName = getDirName(src);
    try {
      try {
        bw.addMutation(m);
        m = new Mutation(new Text(src));
        COLUMNS.IS_DIR.put(m, new Value("Y".getBytes()));
        COLUMNS.MODIFICATION_TIME.put(m, now());
        bw.addMutation(m);
      } finally {
        bw.close();
      }
    } catch (MutationsRejectedException ex) {
      throw new IOException(ex);
    }
    return true;
  }
  
  @Override
  public long nextGenerationStamp(Block arg0, boolean arg1) throws IOException {
    unimplemented(arg0, arg1);
    return 0;
  }
  
  
  
  @Override
  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm)
      throws IOException {
    unimplemented(comm);
    return null;
  }
  
  private void recordBlockHosts(byte[] blockIDBytes, DatanodeInfo[] hosts) throws IOException {
    try {
      if(hosts.length == 0)
        return;
      
      Mutation blockData = new Mutation(new Text(blockIDBytes));
      for(int i=0; i < hosts.length; i++)
        blockData.put(FAMILIES.DATANODES, new Text(hosts[i].name.getBytes()), blank);
      BatchWriter bw = createBatchWriter(TABLES.BLOCKS);
      try {
        bw.addMutation(blockData);
      } finally {
        bw.close();
      }
      
      bw = createBatchWriter(TABLES.DATANODES);
      try {
        for(int i=0; i < hosts.length; i++) {
          Mutation host = new Mutation(new Text(hosts[i].name));
          host.put(FAMILIES.BLOCKS, new Text(blockIDBytes), blank);
          bw.addMutation(host);
        }
      } finally {
        bw.close();
      }
    } catch (MutationsRejectedException ex) {
      throw new IOException(ex);
    }
    
  }
  
  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    unimplemented(src, clientName);
    return false;
  }
  
  @Override
  public void refreshNodes() throws IOException {
    unimplemented();
  }
  
  @Override
  public DatanodeRegistration register(DatanodeRegistration registration)
      throws IOException {
    log.info("using register");
    
    // record this datanode's info
    try {
      if (conn != null) {
        BatchWriter bw = createBatchWriter(TABLES.DATANODES);
        Mutation reg = new Mutation(new Text(registration.name.getBytes()));
        COLUMNS.STORAGE_ID.put(reg, new Value(registration.storageID.getBytes()));
        try {
          try {
            bw.addMutation(reg);
          } finally {
            bw.close();
          }
        } catch (MutationsRejectedException ex) {
          throw new IOException(ex);
        }
      }
    } catch (Throwable ex) {
      log.info("Ignoring exception, maybe accumulo is not yet initialized? " + ex);
    }
    // clients get this info in a list of targets from addBlock()
    return registration;
  }
  
  private static class FileStatus {
    boolean exists;
    boolean isDir;
    public FileStatus(boolean exists, boolean isDir) {
      this.exists = exists;
      this.isDir = isDir;
    }
  }
  
  FileStatus getFileStatus(String src) throws IOException {
    FileStatus result = new FileStatus(false, false);
    BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(src));
    try {
      COLUMNS.IS_DIR.fetch(bs);
      for (Entry<Key,Value> entry : bs) {
        result.exists = true;
        if (new String(entry.getValue().get()).equals("Y")) {
          result.isDir = true;
        }
      }
    } finally {
      bs.close();
    }
    return result;
  }
  
  @Override
  public boolean rename(String src, String dst) throws IOException {
    log.info("using rename " + src + " -> " + dst);
    
    try {
      FileStatus srcStatus = getFileStatus(src);
      if (srcStatus.exists == false)
        throw new FileNotFoundException(src);
      if (srcStatus.isDir == true)
        return false;
      FileStatus dstStatus = getFileStatus(dst);
      String parent = new String(getParentPath(dst));
      FileStatus parentStatus = getFileStatus(parent);
      if (!dstStatus.exists && !parentStatus.exists) {
        throw new FileNotFoundException(dst);
      }
      if (!parentStatus.isDir) {
        throw new IOException(parent + " is not a directory");
      }
      if (dstStatus.isDir) {
        return rename(src, dst + "/" + basename(src));
      }
      // remove conflicting destination file, if any
      if (dstStatus.exists)
        delete(dst, true);
      // copy file information
      BatchScanner bs = createBatchScanner(TABLES.NAMESPACE, new Range(src));
      BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
      try {
        Mutation m = new Mutation(new Text(dst));
        Mutation s = new Mutation(new Text(src));
        for (Entry<Key,Value> entry: bs) {
          Key key = entry.getKey();
          m.put(key.getColumnFamily(), key.getColumnQualifier(), entry.getValue());
          s.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        }
        bw.addMutation(m);
        bw.addMutation(s);
        // remove child link in src's parent
        m = new Mutation(new Text(getParentPath(src)));
        m.putDelete(FAMILIES.CHILDREN, new Text(src));
        bw.addMutation(m);
        // add child link in dst's parent
        m = new Mutation(new Text(getParentPath(dst)));
        m.put(FAMILIES.CHILDREN, new Text(dst), blank);
      } finally {
        bs.close();
        bw.close();
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return true;
  }
  
  private String basename(String src) {
    return src.substring(src.lastIndexOf("/") + 1);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    unimplemented(token);
    return 0;
  }
  
  @Override
  public void renewLease(String clientName) throws IOException {
    unimplemented(clientName);
  }
  
  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    unimplemented((Object)blocks);
  }
  
  @Override
  public void saveNamespace() throws IOException {
    unimplemented();
  }
  
  private static class SendResult {
    List<DatanodeCommand> commands = new ArrayList<DatanodeCommand>();
    List<Mutation> deletes = new ArrayList<Mutation>();
  }
  
  // try to send a heartbeat.. if it times out, do nothing: we are probably recovering the metadata tables
  @Override
  public DatanodeCommand[] sendHeartbeat(final DatanodeRegistration registration,
      final long capacity, final long dfsUsed, final long remaining, final int xmitsInProgress,
      final int xceiverCount) throws IOException {
    
    FutureTask<SendResult> future = new FutureTask<SendResult>(new Callable<SendResult>() {
      @Override
      public SendResult call() throws Exception {
        SendResult result = new SendResult();
        
        log.info("using sendHeartbeat");
        if (!conn.tableOperations().exists(TABLES.DATANODES))
          return result;
        // update datanodes table with info
        // skip this if none of the numbers have changed
        // TODO: get last numbers from a lookup
        if(capacity != lastCapacity || 
            dfsUsed != lastDfsUsed ||
            remaining != lastRemaining) {
          try {
            BatchWriter bw = createBatchWriter(conn, TABLES.DATANODES);
            Mutation m = new Mutation(new Text(registration.name.getBytes()));
            COLUMNS.CAPACITY.put(m, new Value(Long.toString(capacity).getBytes()));
            COLUMNS.USED.put(m, new Value(Long.toString(dfsUsed).getBytes()));
            COLUMNS.IPC_PORT.put(m, new Value(Integer.toString(registration.getIpcPort()).getBytes()));
            COLUMNS.REMAINING.put(m, new Value(Long.toString(remaining).getBytes()));
            try {
              bw.addMutation(m);
            } finally {
              bw.close();
            }
          } catch (Exception ex) {
            log.error(ex, ex);
          }
        }
        lastCapacity = capacity;
        lastDfsUsed = dfsUsed;
        lastRemaining = remaining;
        // return a list of commands for the data node
        List<DatanodeCommand> commands = new ArrayList<DatanodeCommand>();
        try {
          BatchScanner bs = createBatchScanner(TABLES.DATANODES, new Range(registration.getName()));
          bs.fetchColumnFamily(FAMILIES.COMMAND);
          for (Entry<Key,Value> entry : bs) {
            Key key = entry.getKey();
            DatanodeCommand command = (DatanodeCommand)deserialize(entry.getValue().get());
            log.info("found datanode Command " + command);
            commands.add(command);
            Mutation m = new Mutation(key.getRow());
            m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
            result.deletes.add(m);
          }
          bs.close();
        } catch (Exception ex) {
          throw new IOException(ex);
        }

        return result;
      }
    });
    
    executor.execute(future);
    try {
      synchronized(future) {
        future.wait(1000);
      }
    } catch (InterruptedException ex) {
      // ignored
    }
    try {
      if (future.isDone()) {
        // Ok, we were able to do a heartbeat, so go ahead and delete the commands we're about to return
        // TODO: this could fail, too.
        SendResult result = future.get();
        BatchWriter bw = createBatchWriter(TABLES.DATANODES);
        bw.addMutations(result.deletes);
        bw.close();
        return result.commands.toArray(new DatanodeCommand[0]);
      }
    } catch (Exception ex) {
      log.error(ex, ex);
    }
    return new DatanodeCommand[0];
  }
  
  
  public static Object deserialize(byte[] data) throws IOException {
    ByteArrayInputStream streamer = new ByteArrayInputStream(data);
    DataInputStream deserializer = new DataInputStream(streamer);
    String className = deserializer.readUTF();
    try {
      Writable w = (Writable)DistributedNamenodeProxy.class.getClassLoader().loadClass(className).getConstructor().newInstance();
      w.readFields(deserializer);
      return w;
    } catch (Exception ex) {
      throw new IOException(ex);
    } finally {
      deserializer.close();
    }
  }
  
  public static byte[] serialize(Writable obj) throws IOException {
    ByteArrayOutputStream streamer = new ByteArrayOutputStream();
    DataOutputStream serializer = new DataOutputStream(streamer);
    serializer.writeUTF(obj.getClass().getName());
    obj.write(serializer);
    serializer.close();
    return streamer.toByteArray();
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    unimplemented(bandwidth);
  }
  
  @Override
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    unimplemented(src, username, groupname);    
  }
  
  @Override
  public void setPermission(String src, FsPermission permission)
      throws IOException {
    log.info("using setPermission");
    try {
      HdfsFileStatus fileInfo = getFileInfo(src);
      if (!fileInfo.getPermission().equals(permission)) {
        BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
        try {
          Mutation m = new Mutation(src);
          COLUMNS.PERMISSION.put(m, new Value(permission.toString().getBytes()));
          bw.addMutation(m);
        } finally {
          bw.close();
        }
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws IOException {
    unimplemented(path, namespaceQuota, diskspaceQuota);
  }
  
  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {
    log.info("using setReplicatoon");
    try {
      HdfsFileStatus fileInfo = getFileInfo(src);
      if (fileInfo.isDir())
        return false;
      
      if (fileInfo.getReplication() != replication) {
        BatchWriter bw = createBatchWriter(TABLES.NAMESPACE);
        try {
          Mutation m = new Mutation(src);
          COLUMNS.REPLICATION.put(m, new Value(Short.toString(replication).getBytes()));
          bw.addMutation(m);
        } finally {
          bw.close();
        }
      }
      return true;
    } catch (FileNotFoundException ex) {
      return false;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  @Override
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    unimplemented(action);
    return false;
  }
  
  
  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    unimplemented(src, mtime, atime);
  }
  
  @Override
  public NamespaceInfo versionRequest() throws IOException {
    throw new NotImplementedException();
  }
  
}
