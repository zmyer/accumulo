package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.permission.FsPermission;
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
import org.apache.hadoop.hdfs.server.namenode.DistributedNamenodeProxy.ConnectInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.CuratorFrameworkFactory.Builder;
import com.netflix.curator.retry.RetryUntilElapsed;

public class ZookeeperNameNode implements FakeNameNode {
  static private Logger log = Logger.getLogger(ZookeeperNameNode.class); 
  
  CuratorFramework keeper;
  Random random = new Random();
  
  public static class FileInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public FileInfo(long blocksize, long createTime, String permission, short replication, long size) {
      this.blocksize = blocksize;
      this.createTime = createTime;
      this.permission = permission;
      this.replication = replication;
      this.size = size;
    }
    public long blocksize;
    public long createTime;
    public String permission;
    public short replication;
    public long size;
  }
  
  public static class DirInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public DirInfo(long createTime) {
      this.createTime = createTime;
    }

    public long createTime;
  }
  
  public static class BlockInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public BlockInfo(long id, String[] datanodes, long size) {
      this.id = id;
      this.datanodes = datanodes;
      this.size = size;
      this.complete = false;
    }
    long id;
    String[] datanodes;
    long size;
    boolean complete;
  }
  
  static Pattern isRoot = Pattern.compile("/accumulo(|/instance_id.*|/version.*|/walogArchive|/wal(/.*|$)|/recovery.*|/tables$|/tables/(\\!0|0|1|2)(/.*|$))");
  
  static private boolean isZooName(String path) {
    boolean result = isRoot.matcher(path).matches();
    log.info("Looking at " + path + " isZooName " + result);
    return result;
  }
  static public boolean isZooBlockId(long blockId) {
    return blockId < 0;
  }
  
  private final URI uri;
  private DistributedNamenodeProxy dist = null;
  private final String instance;

  private long start = System.currentTimeMillis();
  
  private static URI getURI(Configuration conf) throws IOException {
    try {
      return new URI(conf.get("fs.default.name"));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }
  
  public ZookeeperNameNode(Configuration conf) throws IOException {
    this(conf, getURI(conf));
  }

  public ZookeeperNameNode(Configuration conf, URI uri) throws IOException {
    ConnectInfo info = new ConnectInfo(uri);
    instance = info.instance;
    Builder builder = CuratorFrameworkFactory.builder();
    builder.connectString(info.zookeepers);
    builder.retryPolicy(new RetryUntilElapsed(120*1000, 500));
    //builder.aclProvider(aclProvider);
    CuratorFramework client = builder.build();
    client.start();
    this.keeper = client;
    this.uri = uri;
    try {
      findDatanodes();
    } catch (Exception e) {
      // ignored
    }
  }
  
  private static void unimplemented(Object ... args) {
    Throwable t = new Throwable();
    String method = t.getStackTrace()[1].getMethodName();
    log.warn(method + " unimplemented, args: " + Arrays.asList(args), t);
  }
  
  private FakeNameNode dist() {
    try {
      if (dist == null) {
        String instanceId = new String(keeper.getData().forPath("/accumulo/instances/" + instance));
        log.info("Looking at instance " + instance + " id " + instanceId);
        String tservers = "/accumulo/" + instanceId + "/tservers";
        List<String> children = keeper.getChildren().forPath(tservers);
        boolean atLeastOneTserver = false;
        for (String child : children) {
          List<String> locks = keeper.getChildren().forPath(tservers + "/" + child);
          if (locks != null && locks.size() > 1) {
            atLeastOneTserver = true;
            break;
          }
        }
        if (atLeastOneTserver)
          dist = new DistributedNamenodeProxy(keeper, uri);
      }
    } catch (Exception ex) {
      log.warn(ex, ex);
    }
    return dist;
  }
  
  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
    log.info("getBlockLocations " + src);
    if (!isZooName(src))
      return dist().getBlockLocations(src, offset, length);
    
    log.info("getBlockLocation " + src + " offset " + offset + " length " + length);
    try {
      Map<String, BlockInfo> blocks = new TreeMap<String, BlockInfo>();
      String blockpath = DNNConstants.NAMESPACE_PATH + src;
      boolean underConstruction = false;
      for (String child : keeper.getChildren().forPath(blockpath)) {
        byte[] data = keeper.getData().forPath(blockpath + "/" + child);
        Object obj = deserialize(data);
        if (obj instanceof BlockInfo) {
          BlockInfo info = (BlockInfo)obj;
          data = keeper.getData().forPath(DNNConstants.BLOCKS_PATH + "/" + new Block(info.id).getBlockName());
          obj = deserialize(data);
          if (obj instanceof BlockInfo) {
            info = (BlockInfo)obj;
            blocks.put(child, info);
            log.info(src + " block " + info.id + " size " + info.size);
            if (!info.complete)
              underConstruction = true;
          }
        }
      }
      Map<String,DatanodeRegistration> datanodes = findDatanodes();
      log.info("Got " + blocks.size() + " blocks for " + src);
      List<LocatedBlock> lblocks = new ArrayList<LocatedBlock>();
      long currentOffset = 0;
      for (Entry<String,BlockInfo> entry : blocks.entrySet()) {
        BlockInfo binfo = entry.getValue();
        DatanodeInfo[] info = new DatanodeInfo[binfo.datanodes.length];
        for (int j = 0; j < info.length; j++) {
          DatanodeRegistration dn = datanodes.get(binfo.datanodes[j]);
          if (dn != null)
            info[j] = new DatanodeInfo(new DatanodeID(binfo.datanodes[j], dn.getStorageID(), dn.getInfoPort(), dn.getIpcPort()));
          else
            info[j] = new DatanodeInfo(new DatanodeID(binfo.datanodes[j]));
        }
        log.info("Found " + entry.getKey() + " "+ info.length + " locations for block " + binfo.id);
        if (currentOffset >= offset && currentOffset < offset + length)
          lblocks.add(new LocatedBlock(new Block(binfo.id, binfo.size, 0), info, currentOffset));
        currentOffset += binfo.size;
      }
      log.info("Returning fileLength " + currentOffset + " for " + src);
      return new LocatedBlocks(currentOffset, lblocks, underConstruction);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
    if (data.length == 0)
      return new DirInfo(System.currentTimeMillis());
    ByteArrayInputStream streamer = new ByteArrayInputStream(data);
    ObjectInputStream deserializer = new ObjectInputStream(streamer);
    try{ 
      return deserializer.readObject();
    } finally {
      deserializer.close();
    }
  }
  
  public static byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream streamer = new ByteArrayOutputStream();
    ObjectOutputStream serializer = new ObjectOutputStream(streamer);
    serializer.writeObject(obj);
    serializer.close();
    return streamer.toByteArray();
  }
  
  @Override
  public void create(String src, FsPermission masked, String clientName, boolean overwrite, boolean createParent, short replication, long blockSize)
      throws IOException {
    log.info("creating " + src);
    if (!isZooName(src)) {
      dist().create(src, masked, clientName, overwrite, createParent, replication, blockSize);
      return;
    }
    try {
      FileInfo fileInfo = new FileInfo(blockSize, System.currentTimeMillis(), masked.toString(), replication, 0);
      byte[] data = serialize(fileInfo);
      String path = DNNConstants.NAMESPACE_PATH + src;
      try {
        byte[] current = keeper.getData().forPath(path);
        log.info("Current value for " + src + " is " + new Text(current));
        if (overwrite) {
          keeper.setData().forPath(path, data);
        } else {
          throw new FileAlreadyExistsException(src);
        }
      } catch (KeeperException.NoNodeException node) {
        if (createParent) {
          keeper.create().creatingParentsIfNeeded().forPath(path, data);
        } else {
          keeper.create().forPath(path, data);
        }
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  @Override
  public void create(String src, FsPermission masked, String clientName, boolean overwrite, short replication, long blockSize) throws IOException {
    create(src, masked, clientName, overwrite, true, replication, blockSize);
  }
  
  @Override
  public LocatedBlock append(String src, String clientName) throws IOException {
    throw new NotImplementedException();
  }
  
  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    unimplemented(src, clientName);
    return true;
  }
  
  @Override
  public boolean setReplication(String src, short replication) throws IOException {
    unimplemented(src, replication);
    return true;
  }
  
  @Override
  public void setPermission(String src, FsPermission permission) throws IOException {
    unimplemented(src, permission);
  }
  
  @Override
  public void setOwner(String src, String username, String groupname) throws IOException {
    unimplemented(src, username, groupname);
  }
  
  @Override
  public void abandonBlock(Block b, String src, String holder) throws IOException {
    log.info("abandonBlock " + b.getBlockId() + ": " + src);
    if (!isZooName(src)) { 
      dist().abandonBlock(b, src, holder);
      return;
    }
    try {
      String path = DNNConstants.NAMESPACE_PATH + src;
      for (String child : keeper.getChildren().forPath(path)) {
        byte[] blockData = keeper.getData().forPath(path + "/" + child);
        Object obj = deserialize(blockData);
        if (obj instanceof BlockInfo) {
          BlockInfo info = (BlockInfo) obj;
          if (info.id == b.getBlockId()) {
            keeper.delete().forPath(path + "/" + child);
            try {
              keeper.delete().forPath(DNNConstants.BLOCKS_PATH + "/" + b.getBlockName());
            } catch (KeeperException.NoNodeException ex) {
              // ignored
            }
            return;
          }
        }
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    throw new IOException("Unexpected error abandoning block " + b.getBlockId() + " block not found!");
  }
  
  @Override
  public LocatedBlock addBlock(String src, String clientName) throws IOException {
    return addBlock(src, clientName, new DatanodeInfo[]{});
  }
  
  @Override
  public LocatedBlock addBlock(String src, String clientName, DatanodeInfo[] excludedNodes) throws IOException {
    log.info("addBlock " + src);
    if (!isZooName(src)) 
      return dist().addBlock(src, clientName, excludedNodes);
    // get the list of online data nodes
    Map<String, DatanodeRegistration> nodes;
    try {
      nodes = findDatanodes();
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    short defaultReplication = 3; // TODO: read config
    short replication = -1;
    try {
      String path = DNNConstants.NAMESPACE_PATH + src;
      byte data[] = keeper.getData().forPath(path);
      HdfsFileStatus status = decodeFile(path, data);
      replication = status.getReplication();
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    if (replication < 0)
      replication = defaultReplication;
    
    List<String> randomList = new ArrayList<String>(nodes.keySet());
    Collections.shuffle(randomList);
    
    // DistibutedNameNode holds the positive blocks
    long blockID = -Math.abs(random.nextLong());
    Block b = new Block(blockID, 0, 0);
    List<String> replicas = randomList.subList(0, Math.min(replication, randomList.size()));
    List<DatanodeInfo> targets = new ArrayList<DatanodeInfo>();
    for (String replica : replicas) {
      targets.add(new DatanodeInfo(new DatanodeID(replica)));
    }
    recordBlock(src, b, targets);
    LocatedBlock newBlock = new LocatedBlock(b, targets.toArray(new DatanodeInfo[targets.size()]));
    log.info("added block " + b + " on " + targets + " for " + src);
    return newBlock;
  }

  private void recordBlock(String src, Block b, List<DatanodeInfo> targets) throws IOException {
    try {
      String[] datanodes = new String[targets.size()];
      for (int i = 0; i < targets.size(); i++) {
        datanodes[i] = targets.get(i).name;
      }
      BlockInfo blockInfo = new BlockInfo(b.getBlockId(), datanodes, 0);
      byte[] data = serialize(blockInfo);
      for (String datanode : datanodes) {
        keeper.create().forPath(DNNConstants.DATANODES_PATH + "/" + datanode + "/blocks/" + b.getBlockName(), data);
      }
      keeper.create().forPath(DNNConstants.BLOCKS_PATH + "/" + b.getBlockName(), data);
      keeper.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(DNNConstants.NAMESPACE_PATH + src + "/blocks-", data);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  private synchronized Map<String, DatanodeRegistration> findDatanodes() throws IOException {
    Map<String, DatanodeRegistration> nodes = new HashMap<String, DatanodeRegistration>();
    try {
      List<String> children = keeper.getChildren().forPath(DNNConstants.DATANODES_PATH);
      for (String child : children) {
        byte[] data = keeper.getData().forPath(DNNConstants.DATANODES_PATH + "/" + child);
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream ds = new DataInputStream(bais);
        DatanodeRegistration registration = new DatanodeRegistration();
        registration.readFields(ds);
        nodes.put(child, registration);
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return nodes;
  }
  
  @Override
  public boolean complete(String src, String clientName) throws IOException {
    log.info("using complete " + src);
    if (!isZooName(src))
      return dist().complete(src, clientName);
    String path = DNNConstants.NAMESPACE_PATH + src;
    try {
      long length = 0;
      for (String child : keeper.getChildren().forPath(path)) {
        Object object = deserialize(keeper.getData().forPath(path + "/" + child));
        if (object instanceof BlockInfo) {
          BlockInfo info = (BlockInfo)object;
          Block block = new Block(info.id);
          info = (BlockInfo)deserialize(keeper.getData().forPath(DNNConstants.BLOCKS_PATH + "/" + block.getBlockName()));
          log.info("Block size for " + info.id + " is " + info.size);
          length += info.size;
          if (!info.complete)
            return false;
        }
      }
      FileInfo info = (FileInfo)deserialize(keeper.getData().forPath(path));
      info.size = length;
      keeper.setData().forPath(path, serialize(info));
      log.info("updated file length of " + src + " to " + length);
      return true;
    } catch (Exception ex) {
      log.error(ex, ex);
      return false;
    }
  }
  
  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    unimplemented((Object[])blocks);
  }

  private Object getInfo(String path) throws Exception {
    byte data[];
    try {
      data = keeper.getData().forPath(path);
    } catch (KeeperException.NoNodeException ex) {
      return null;
    }
    if (data == null) return null;
    return deserialize(data);
  }
  
  private void recursivelyCopy(String src, String dst) throws Exception {
    byte[] data = keeper.getData().forPath(src);
    keeper.create().forPath(dst, data);
    List<String> children = keeper.getChildren().forPath(src);
    Collections.sort(children);
    for (String child : children) {
      recursivelyCopy(src + "/" + child, dst + "/" + child);
    }
  }
  
  @Override
  public boolean rename(String src, String dst) throws IOException {
    log.info("rename " + src + " -> " + dst);
    if (isZooName(src) != isZooName(dst))
      throw new IOException("You cannot rename files across zookeeper metadata and accumulo metadata");
    if (!isZooName(src))
      return dist().rename(src, dst);
    try {
      Object srcInfo = getInfo(DNNConstants.NAMESPACE_PATH + src);
      Object dstInfo = getInfo(DNNConstants.NAMESPACE_PATH + dst);
      String parent = getParent(dst);
      Object parentInfo = getInfo(DNNConstants.NAMESPACE_PATH + parent);
      if (srcInfo == null)
        throw new FileNotFoundException(src);
      if (!(srcInfo instanceof FileInfo))
        throw new IOException(src + " is a directory");
      if (parentInfo == null)
        throw new IOException(parent + " does not exist");
      if (!(parentInfo instanceof DirInfo))
        throw new IOException(parent + " is not a directory");
      if (dstInfo != null) {
        if (dstInfo instanceof DirInfo)
          return rename(src, dst + "/" + basename(src));
        else
          delete(dst);
      }
      String nsSrc = DNNConstants.NAMESPACE_PATH + src;
      recursivelyCopy(nsSrc, DNNConstants.NAMESPACE_PATH + dst);
      recursivelyDelete(nsSrc, false);
      return true;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  private static String basename(String src) {
    return src.substring(src.lastIndexOf("/") + 1);
  }

  private static String getParent(String dst) {
    return dst.substring(0, dst.lastIndexOf("/"));
  }

  @Override
  public boolean delete(String src) throws IOException {
    return delete(src, true);
  }
  
  @Override
  public boolean delete(String src, boolean recursive) throws IOException {
    log.info("attempting delete " + src);
    if (!isZooName(src))
      return dist().delete(src);
    
    try {
      recursivelyDelete(DNNConstants.NAMESPACE_PATH + src, true);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return true;
  }
  
  private void recursivelyDelete(String path, boolean removeBlocks) throws Exception {
    log.info("deleting " + path);
    List<String> children = null;
    try {
      children = keeper.getChildren().forPath(path);
    } catch (KeeperException.NoNodeException ex) {
      return;
    }
    log.info("children of " + path + " is " + children);
    Object obj = deserialize(keeper.getData().forPath(path));
    if (removeBlocks && obj instanceof FileInfo) {
      // create the datanode command to (eventually) delete the blocks
      Map<String, List<Long>> hostToBlockMap = new HashMap<String, List<Long>>();
      for (String child : children) {
        Object childObject = deserialize(keeper.getData().forPath(path + "/" + child));
        if (childObject instanceof BlockInfo) {
          BlockInfo block = (BlockInfo)childObject;
          for (String node : block.datanodes) {
            List<Long> blocks = hostToBlockMap.get(node);
            if (blocks == null)
              hostToBlockMap.put(node, blocks = new ArrayList<Long>());
            blocks.add(block.id);
          }
          String blockPath = DNNConstants.BLOCKS_PATH + "/" + new Block(block.id, 0, 0).getBlockName();
          log.info("deleting " + blockPath);
          keeper.delete().forPath(blockPath);
        }
      }
      for (Entry<String,List<Long>> entry : hostToBlockMap.entrySet()) {
        String host = entry.getKey();
        List<Block> blocks = new ArrayList<Block>();
        for (Long blockId : entry.getValue())
          blocks.add(new Block(blockId, 0, 0));
        DatanodeCommand cmd = new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, blocks.toArray(new Block[0]));
        byte[] data = DistributedNamenodeProxy.serialize(cmd);
        keeper.create().forPath(DNNConstants.DATANODES_PATH + "/" + host + "/commands/" + UUID.randomUUID().toString(), data);
      }
    }
    for (String child : children) {
      recursivelyDelete(path + "/" + child, removeBlocks);
    }
    log.info("deleting " + path);
    keeper.delete().forPath(path);
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked) throws IOException {
    log.info("mkdirs " + src);
    if (!isZooName(src))
      return dist().mkdirs(src, masked);
    
    try {
      DirInfo dirInfo = new DirInfo(System.currentTimeMillis());
      byte[] data = serialize(dirInfo);
      String path = DNNConstants.NAMESPACE_PATH + src;
      keeper.create().creatingParentsIfNeeded().forPath(path, data);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return true;
  }
  
  HdfsFileStatus decodeFile(String name, byte[] data) throws IOException, ClassNotFoundException {
    Object obj;
    try {
      obj = deserialize(data);
    } catch (Exception ex) {
      log.error(ex, ex);
      return null;
    }
    if (obj instanceof FileInfo) {
      FileInfo fileInfo = (FileInfo)obj;
      
      long length = fileInfo.size;
      boolean isdir = false;
      int block_replication = fileInfo.replication;
      long blocksize = fileInfo.blocksize;
      long modification_time = fileInfo.createTime;
      long access_time = fileInfo.createTime;
      FsPermission permission = FsPermission.valueOf("-" + fileInfo.permission);
      String owner = "hdfs";
      String group = "supergroup";
      byte[] path = name.getBytes();
      log.info(String.format("length %d isdir %s replication %d path %s", length, isdir, block_replication, new String(path)));
      return new HdfsFileStatus(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group, path);
    }
    if (obj instanceof DirInfo) {
      DirInfo dirInfo = (DirInfo)obj;
      long length = 0;
      boolean isdir = true;
      int block_replication = 0;
      long blocksize = 0;
      long modification_time = dirInfo.createTime;
      long access_time = dirInfo.createTime;
      FsPermission permission = FsPermission.getDefault();
      String owner = "hdfs";
      String group = "supergroup";
      byte[] path = name.getBytes();
      log.info(String.format("length %d isdir %s replication %d path %s", length, isdir, block_replication, new String(path)));
      return new HdfsFileStatus(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group, path);
    }
    return null;
  }
  
  @Override
  public DirectoryListing getListing(String src, byte[] startAfter) throws IOException {
    log.info("getListing " + src);
    if (!isZooName(src))
      return dist().getListing(src, startAfter);
    
    try {
      String basePath = DNNConstants.NAMESPACE_PATH + src;
      List<String> children = keeper.getChildren().forPath(basePath);
      List<HdfsFileStatus> stats = new ArrayList<HdfsFileStatus>();
      for (String child : children) {
        byte data[] = keeper.getData().forPath(basePath + "/" + child);
        HdfsFileStatus status = decodeFile(child, data);
        if (status != null)
          stats.add(status);
      }
      return new DirectoryListing(stats.toArray(new HdfsFileStatus[]{}), 0);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  @Override
  public void renewLease(String clientName) throws IOException {
    log.info("renewLease " + clientName);
  }
  
  @Override
  public long[] getStats() throws IOException {
    unimplemented();
    return null;
  }
  
  @Override
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type) throws IOException {
    unimplemented(type);
    return null;
  }
  
  @Override
  public long getPreferredBlockSize(String filename) throws IOException {
    unimplemented(filename);
    return 0;
  }
  
  @Override
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    unimplemented(action);
    return false;
  }
  
  @Override
  public void saveNamespace() throws IOException {
   unimplemented();
  }
  
  @Override
  public void refreshNodes() throws IOException {
    unimplemented();
  }
  
  @Override
  public void finalizeUpgrade() throws IOException {
    unimplemented();
  }
  
  @Override
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) throws IOException {
    unimplemented();
    return null;
  }
  
  @Override
  public void metaSave(String filename) throws IOException {
    unimplemented(filename);
  }
  
  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    unimplemented(bandwidth);
  }
  
  @Override
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    log.info("getFileInfo " + src);
    if (!isZooName(src))
      return dist().getFileInfo(src);
    
    try {
      byte[] data = keeper.getData().forPath(DNNConstants.NAMESPACE_PATH + src);
      HdfsFileStatus result = decodeFile(src, data);
      if (result != null) {
        log.info("Returning info for file " + result.getLocalName());
      } else {
        log.info("No file " + src);
      }
      return result;
    } catch (KeeperException.NoNodeException e) {
      // ignore
    } catch (Exception e) {
      log.error(e, e);
    }
    return null;
  }
  
  @Override
  public ContentSummary getContentSummary(String path) throws IOException {
    unimplemented(path);
    return null;
  }
  
  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws IOException {
    unimplemented(path, namespaceQuota, diskspaceQuota);
  }
  
  @Override
  public void fsync(String src, String client) throws IOException {
    unimplemented(src, client);
  }
  
  @Override
  public void setTimes(String src, long mtime, long atime) throws IOException {
    unimplemented(src, mtime, atime);
  }
  
  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
    unimplemented(renewer);
    return null;
  }
  
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    unimplemented(token);
    return 0;
  }
  
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    unimplemented(token);
  }
  
  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    unimplemented(protocol, clientVersion);
    return 0;
  }
  
  @Override
  public DatanodeRegistration register(DatanodeRegistration registration) throws IOException {
    log.info("register " + registration);
    if (keeper != null) {
      log.info("registering in zookeeper as " + registration.name);
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DataOutputStream data = new DataOutputStream(stream);
      registration.write(data);
      data.close();
      try {
        try {
          for (String name : new String[]{DNNConstants.DNN, DNNConstants.DATANODES_PATH}) {
            keeper.create().forPath(name);
          }
        } catch (KeeperException.NodeExistsException ex) {
          // expected
        }
        String path = DNNConstants.DATANODES_PATH + "/" + registration.name;
        try {
          keeper.setData().forPath(path, stream.toByteArray());
        } catch (KeeperException.NoNodeException e) {
          keeper.create().forPath(path, stream.toByteArray());
          keeper.create().forPath(path + "/blocks");
          keeper.create().forPath(path + "/commands");
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    findDatanodes();
    FakeNameNode dist = dist();
    if (dist != null) {
      dist.register(registration);
    }
    return registration;
  }
  
  @Override
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration, long capacity, long dfsUsed, long remaining, int xmitsInProgress, int xceiverCount)
      throws IOException {
    log.info("sendHeartbeat " + registration);
    List<DatanodeCommand> commands = new ArrayList<DatanodeCommand>();
    try {
      String commandsPath = DNNConstants.DATANODES_PATH + "/" + registration.getName() + "/commands";
      for (String child : keeper.getChildren().forPath(commandsPath)) {
        byte[] data = keeper.getData().forPath(commandsPath + "/" + child);
        commands.add((DatanodeCommand)DistributedNamenodeProxy.deserialize(data));
        keeper.delete().forPath(commandsPath + "/" + child);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    findDatanodes();
    if (dist != null) {
      DatanodeCommand[] cmds = dist.sendHeartbeat(registration, capacity, dfsUsed, remaining, xmitsInProgress, xceiverCount);
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
          commands.add(cmd);
        }
      }
    }
    return commands.toArray(new DatanodeCommand[0]);
  }
  
  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration, long[] blocks) throws IOException {
    log.info("blockReport " + registration);
    if (dist != null) {
      return dist.blockReport(registration, blocks);
    }
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    Set<Long> current = new HashSet<Long>();
    for (int i = 0; i < blist.getNumberOfBlocks(); i++) {
      if (isZooBlockId(blist.getBlockId(i))) {
        current.add(blist.getBlockId(i));
      }
    }
    log.info(registration.name + " reports " + current);
    return null;
  }
  
  @Override
  public void blocksBeingWrittenReport(DatanodeRegistration registration, long[] blocks) throws IOException {
    unimplemented(registration, new BlockListAsLongs(blocks));
  }
  
  @Override
  public void blockReceived(DatanodeRegistration registration, Block[] blocks, String[] delHints) throws IOException {
    log.info("blockRecieved " + Arrays.asList(blocks));
    FakeNameNode dist = dist();
    for (Block block : blocks) {
      if (!isZooBlockId(block.getBlockId()) && dist == null) {
        throw new IOException("blockReceived for distributed name node, but there are no data nodes yet! " + block.getBlockId());
      }
      if (isZooBlockId(block.getBlockId())) {
        String path = DNNConstants.BLOCKS_PATH + "/" + block.getBlockName();
        try {
          BlockInfo info = (BlockInfo)deserialize(keeper.getData().forPath(path));
          info.size = block.getNumBytes();
          info.complete = true;
          byte[] data = serialize(info);
          keeper.setData().forPath(path, data);
          log.info("Block size updated on " + block + " to " + info.size);
        } catch (Exception e) {
          log.error(e, e);
        }
      }
    }
    if (dist != null)
      dist.blockReceived(registration, blocks, delHints);
  }
  
  @Override
  public void errorReport(DatanodeRegistration registration, int errorCode, String msg) throws IOException {
    unimplemented(registration, errorCode, msg);
  }
  
  @Override
  public NamespaceInfo versionRequest() throws IOException {
    log.info("using versionRequest");
    // TODO: find out how to get namespace id
    // could store this in the info of the / entry
    NamespaceInfo nsi = new NamespaceInfo(384837986, 0, 0);
    //throw new RuntimeException();
    return nsi;
  }
  
  @Override
  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    unimplemented(comm);
    return null;
  }
  
  @Override
  public long nextGenerationStamp(Block block, boolean fromNN) throws IOException {
    unimplemented(block, fromNN);
    return 0;
  }
  
  @Override
  public void commitBlockSynchronization(Block block, long newgenerationstamp, long newlength, boolean closeFile, boolean deleteblock, DatanodeID[] newtargets)
      throws IOException {
    unimplemented(block, newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
  }
  
}
