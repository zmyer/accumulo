package org.apache.hadoop.hdfs.server.namenode;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DNNConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.DistributedNamenodeProxy.ConnectInfo;
import org.apache.log4j.Logger;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.CuratorFrameworkFactory.Builder;
import com.netflix.curator.retry.RetryUntilElapsed;

public class SwitchingNameNode {
  private static final Logger log = Logger.getLogger(SwitchingNameNode.class);
  static Pattern isRoot = Pattern.compile("/accumulo(|/instance_id.*|/version.*|/walogArchive|/recovery.*|/tables$|/tables/(\\!0|0|1|2)(/.*|$))");
  
  static private boolean isZooName(String path) {
    boolean result = isRoot.matcher(path).matches();
    log.info("Looking at " + path + " isZooName " + result);
    return result;
  }
  
  public static FakeNameNode create(final Configuration conf) {
    try {
      URI uri = new URI(conf.get("fs.default.name"));
      ConnectInfo info = new ConnectInfo(uri);
      Builder builder = CuratorFrameworkFactory.builder().namespace(DNNConstants.DNN);
      builder.connectString(info.zookeepers);
      builder.retryPolicy(new RetryUntilElapsed(120*1000, 500));
      //builder.aclProvider(aclProvider);
      CuratorFramework client = builder.build();
      client.start();
      ZookeeperNameNode zoo = new ZookeeperNameNode(client);
      return SwitchingNameNode.create(zoo, info);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  
  public static FakeNameNode create(final FakeNameNode zoonode, final ConnectInfo info) {
    
    return (FakeNameNode) Proxy.newProxyInstance(SwitchingNameNode.class.getClassLoader(), new Class<?>[] {FakeNameNode.class}, new InvocationHandler() {
      FakeNameNode distributed = null;
      
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        log.info("calling " + method.getName());
        if (method.getName().equals("toString")) {
          return "FakeNameNode " + zoonode + " dynamic name node@ " + info.instance;
        }
        try {
          // handle versionRequest with the zoonode
          if (method.getName().equals("versionRequest")) {
            return method.invoke(zoonode, args);
          }
          // split blocks manually to the separate nodes
          if (method.getName().equals("blockReport")) {
            long[] blocks = (long[])args[1];
            long[] zooblocks = new long[blocks.length];
            long[] distblocks = new long[blocks.length];
            int zoocount = 0;
            int distcount = 0;
            for (long block : blocks)
              if (block < 0)
                zooblocks[zoocount++] = block;
              else
                distblocks[distcount++] = block;
            zooblocks = Arrays.copyOf(zooblocks, zoocount);
            distblocks = Arrays.copyOf(distblocks, distcount);
            Object result = null;
            if (zooblocks.length > 0) {
              args[1] = zooblocks;
              log.info("Calling zoo blockReport with " + zooblocks.length + " blocks");
              result = method.invoke(zoonode, args);
            }
            if (distblocks.length > 0 && distributed != null) {
              args[1] = distblocks;
              log.info("Calling dist blockReport with " + distblocks.length + " blocks");
              result = method.invoke(distributed, args);
            }
            return result;
          }
          if (method.getName().equals("blockReceived")) {
            Block[] blocks = (Block[])args[1];
            List<Block> zooblocks = new ArrayList<Block>();
            List<Block> distblocks = new ArrayList<Block>();
            for (Block block : blocks) {
              if (block.getBlockId() < 0)
                zooblocks.add(block);
              else
                distblocks.add(block);
            }
            Object result = null;
            if (!zooblocks.isEmpty()) {
              args[1] = zooblocks.toArray(new Block[0]);
              result = method.invoke(zoonode, args);
            }
            if (distributed != null && !distblocks.isEmpty()) {
              args[1] = distblocks.toArray(new Block[0]);
              result = method.invoke(distributed, args);
            }
            return result;
          }
          // dispatch on filename
          if (args != null && args.length > 0 && (args[0] instanceof String)) {
            if (isZooName((String) args[0]))
              return method.invoke(zoonode, args);
          }
          // try to send to the dnn, then to znn
          synchronized (this) {
            if (distributed == null) {
              try {
                Instance zinst = new ZooKeeperInstance(info.instance, info.zookeepers);
                Connector conn = zinst.getConnector(info.username, info.passwd);
                distributed = new DistributedNamenodeProxy(conn);
              } catch (Exception ex) {
                log.warn("error invoking " + method.getName() + ", invoking zookeeper version");
                return method.invoke(zoonode, args);
              }
            }
          }
          return method.invoke(distributed, args);
        } catch (InvocationTargetException ex) {
          throw ex.getCause();
        }
      }
    });
    
  }
}
