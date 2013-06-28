/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate.curator;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;

public class CuratorUtil {
  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }
  
  public static String getNodeName(ChildData node) {
    return getNodeName(node.getPath());
  }
  
  public static String getNodeName(String nodePath) {
    return new File(nodePath).getName();
  }
  
  public static String getNodeParent(ChildData node) {
    return getNodeParent(node.getPath());
  }
  
  public static String getNodeParent(String nodePath) {
    return new File(nodePath).getParent();
  }
  
  public static void recursiveDelete(CuratorFramework curator, final String pathRoot, int version) throws KeeperException, InterruptedException {
    PathUtils.validatePath(pathRoot);
    
    List<String> tree = listSubTreeBFS(curator, pathRoot);
    for (int i = tree.size() - 1; i >= 0; --i) {
      // Delete the leaves first and eventually get rid of the root
      try {
        curator.delete().withVersion(version).forPath(tree.get(i));
      } catch (Exception e) {
        throw CuratorUtil.manageException(e);
      } // Delete all versions of the node
    }
  }
  
  private static List<String> listSubTreeBFS(CuratorFramework curator, final String pathRoot) throws KeeperException, InterruptedException {
    Deque<String> queue = new LinkedList<String>();
    List<String> tree = new ArrayList<String>();
    queue.add(pathRoot);
    tree.add(pathRoot);
    while (true) {
      String node = queue.pollFirst();
      if (node == null) {
        break;
      }
      List<String> children;
      try {
        children = curator.getChildren().forPath(node);
      } catch (Exception e) {
        throw CuratorUtil.manageException(e);
      }
      for (final String child : children) {
        final String childPath = node + "/" + child;
        queue.add(childPath);
        tree.add(childPath);
      }
    }
    return tree;
  }
  
  public static class LockID {
    public long eid;
    public String path;
    public String node;
    
    public LockID(String root, String serializedLID) {
      String sa[] = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');
      
      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }
      
      if (lastSlash == 0)
        path = root;
      else
        path = root + "/" + sa[0].substring(0, lastSlash);
      node = sa[0].substring(lastSlash + 1);
      eid = new BigInteger(sa[1], 16).longValue();
    }
    
    public LockID(String path, String node, long eid) {
      this.path = path;
      this.node = node;
      this.eid = eid;
    }
    
    public String serialize(String root) {
      
      return path.substring(root.length()) + "/" + node + "$" + Long.toHexString(eid);
    }
    
    @Override
    public String toString() {
      return " path = " + path + " node = " + node + " eid = " + Long.toHexString(eid);
    }
  }
  
  public static byte[] getLockData(CuratorCaches zc, String path) {
    
    List<ChildData> children = zc.getChildren(path);
    
    if (children == null || children.size() == 0) {
      return null;
    }
    
    children = new ArrayList<ChildData>(children);
    Collections.sort(children);
    
    return children.get(0).getData();
  }
  
  public static boolean isLockHeld(ZooKeeper zk, LockID lid) throws KeeperException, InterruptedException {
    while (true) {
      try {
        List<String> children = zk.getChildren(lid.path, false);
        
        if (children.size() == 0) {
          return false;
        }
        
        Collections.sort(children);
        
        String lockNode = children.get(0);
        if (!lid.node.equals(lockNode))
          return false;
        
        Stat stat = zk.exists(lid.path + "/" + lid.node, false);
        return stat != null && stat.getEphemeralOwner() == lid.eid;
      } catch (KeeperException.ConnectionLossException ex) {
        UtilWaitThread.sleep(1000);
      }
    }
  }

  /**
   * Fluffer class. Right now keep it generic but as I probe Curator I can make exceptions better
   */
  public static RuntimeException manageException(Exception e) throws KeeperException, InterruptedException {
    if (e instanceof KeeperException)
      throw (KeeperException) e;
    if (e instanceof InterruptedException)
      throw (InterruptedException) e;
    return new RuntimeException(e);
  }
}
