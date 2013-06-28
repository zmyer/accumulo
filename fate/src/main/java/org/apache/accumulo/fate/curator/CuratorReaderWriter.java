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

import java.security.SecurityPermission;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class CuratorReaderWriter extends CuratorReader {
  private static SecurityPermission ZOOWRITER_PERMISSION = new SecurityPermission("zookeeperWriterPermission");
  
  protected CuratorReaderWriter(String zooKeepers, int sessionTimeout, String scheme, byte[] auth) {
    super(constructCurator(zooKeepers, sessionTimeout, scheme, auth));
  }
  
  private static CuratorReaderWriter instance = null;
  
  public static synchronized CuratorReaderWriter getInstance(String zookeepers, int timeInMillis, String scheme, byte[] auth) {
    if (instance == null)
      instance = new CuratorReaderWriter(zookeepers, timeInMillis, scheme, auth);
    return instance;
  }
  
  private static CuratorFramework constructCurator(String zooKeepers, int sessionTimeout, String scheme, byte[] auth) {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(ZOOWRITER_PERMISSION);
    }
    return CuratorSession.getSession(zooKeepers, sessionTimeout, scheme, auth);
  }
  
  public void recursiveDelete(String zPath) throws KeeperException, InterruptedException {
    recursiveDelete(zPath, -1);
  }
  
  @Deprecated
  public void recursiveDelete(String zPath, int version) throws KeeperException, InterruptedException {
    CuratorUtil.recursiveDelete(getCurator(), zPath, version);
  }
  
  public static final List<ACL> PRIVATE;
  private static final List<ACL> PUBLIC;
  static {
    PRIVATE = new ArrayList<ACL>();
    PRIVATE.addAll(Ids.CREATOR_ALL_ACL);
    PUBLIC = new ArrayList<ACL>();
    PUBLIC.addAll(PRIVATE);
    PUBLIC.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
  }
  
  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL, SEQUENTIAL
  }
  
  private String putData(String zPath, byte[] data, CreateMode mode, int version, NodeExistsPolicy policy, List<ACL> acls) throws KeeperException,
      InterruptedException {
    if (policy == null)
      policy = NodeExistsPolicy.FAIL;
    
    CuratorFramework curator = getCurator();
    try {
      boolean exists = curator.checkExists().forPath(zPath) != null;

      
      if (!exists || policy.equals(NodeExistsPolicy.SEQUENTIAL)) {
        return curator.create().withMode(mode).withACL(acls).forPath(zPath, data);
      }
      else if (policy.equals(NodeExistsPolicy.OVERWRITE)) {
        curator.setData().withVersion(version).forPath(zPath, data);
        return zPath;
      }
      return null;
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  /**
   * Create a persistent node with the default ACL
   * 
   * @return true if the node was created or altered; false if it was skipped
   */
  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putPersistentData(zPath, data, -1, policy);
  }
  
  public boolean putPersistentDataWithACL(String zPath, byte[] data, NodeExistsPolicy policy, List<ACL> acls)
      throws KeeperException, InterruptedException {
    return putData(zPath, data, CreateMode.PERSISTENT, -1, policy, acls) != null;
  }
  
  public boolean putPersistentData(String zPath, byte[] data, int version, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putData(zPath, data, CreateMode.PERSISTENT, -1, policy, PUBLIC) != null;
  }
  
  public boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    return putData(zPath, data, CreateMode.PERSISTENT, -1, policy, PRIVATE) != null;
  }
  
  public String putPersistentSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return putData(zPath, data, CreateMode.PERSISTENT_SEQUENTIAL, -1, NodeExistsPolicy.SEQUENTIAL, PUBLIC);
  }
  
  public boolean putEphemeralData(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return putData(zPath, data, CreateMode.EPHEMERAL, -1, null, PUBLIC) != null;
  }
  
  public String putEphemeralSequential(String zPath, byte[] data) throws KeeperException, InterruptedException {
    return putData(zPath, data, CreateMode.EPHEMERAL_SEQUENTIAL, -1, NodeExistsPolicy.SEQUENTIAL, PUBLIC);
  }
  
  public void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    Stat stat = null;
    if (!exists(source))
      throw KeeperException.create(KeeperException.Code.NONODE, source);
    if (exists(destination)) {
      switch (policy) {
        case OVERWRITE:
          break;
        case SKIP:
        case SEQUENTIAL:
          return;
        case FAIL:
        default:
          throw KeeperException.create(KeeperException.Code.NODEEXISTS, source);
      }
    }
    
    stat = new Stat();
    byte[] data = getData(source, stat);
    if (stat.getEphemeralOwner() == 0) {
      if (data == null)
        throw KeeperException.create(KeeperException.Code.NONODE, source);
      putPersistentData(destination, data, policy);
      if (stat.getNumChildren() > 0)
        for (String child : getChildren(source))
          recursiveCopyPersistent(source + "/" + child, destination + "/" + child, policy);
    }
  }
  
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    try {
      getCurator().delete().withVersion(version).forPath(path);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  public interface Mutator {
    byte[] mutate(byte[] currentValue) throws Exception;
  }
  
  public byte[] mutate(String zPath, byte[] createValue, boolean privateACL, Mutator mutator) throws Exception {
    if (createValue != null) {
      byte[] data = getData(zPath);
      if (data == null) {
        if (privateACL)
          putPrivatePersistentData(zPath, createValue, NodeExistsPolicy.FAIL);
        else
          putPersistentData(zPath, createValue, NodeExistsPolicy.FAIL);
        return createValue;
      }
    }
    
    Stat stat = new Stat();
    byte[] data = getData(zPath, stat);
    data = mutator.mutate(data);
    if (data == null)
      return data;
    if (privateACL)
      putPrivatePersistentData(zPath, createValue, NodeExistsPolicy.OVERWRITE);
    else
      putPersistentData(zPath, createValue, NodeExistsPolicy.OVERWRITE);
    return data;
  }
  
  public boolean isLockHeld(CuratorUtil.LockID lockID) throws KeeperException, InterruptedException {
    try {
      return CuratorUtil.isLockHeld(getCurator().getZookeeperClient().getZooKeeper(), lockID);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  public void mkdirs(String path) throws KeeperException, InterruptedException {
    try {
      getCurator().create().creatingParentsIfNeeded().forPath(path);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
}
