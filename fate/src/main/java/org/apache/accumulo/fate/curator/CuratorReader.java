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

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class CuratorReader {
  
  protected String keepers;
  protected int timeout;
  
  private CuratorFramework curator;
  
  public CuratorReader(String zooKeepers, int sessionTimeout) {
    this(CuratorSession.getSession(zooKeepers, sessionTimeout, null, null));
  }
  
  public CuratorReader(String zooKeepers, int sessionTimeout, String scheme, byte[] auth) {
    this(CuratorSession.getSession(zooKeepers, sessionTimeout, scheme, auth));
  }
  
  public CuratorReader(CuratorFramework curator) {
    this.curator = curator;
  }
  
  @Deprecated
  public CuratorFramework getCurator() {
    return curator;
  }
  
  public byte[] getData(String zPath) throws KeeperException, InterruptedException {
    try {
      return getCurator().getData().forPath(zPath);
    } catch (Exception e) {
      try {
        throw CuratorUtil.manageException(e);
      } catch (KeeperException.NoNodeException nne) {
        return null;
      }
    }
  }
  
  public byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException {
    try {
      return getCurator().getData().storingStatIn(stat).forPath(zPath);
    } catch (Exception e) {
      try {
        throw CuratorUtil.manageException(e);
      } catch (KeeperException.NoNodeException nne) {
        return null;
      }
    }
  }
  
  public Stat getStatus(String zPath) throws KeeperException, InterruptedException {
    try {
      return getCurator().checkExists().forPath(zPath);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  public Stat getStatus(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
    try {
      return getCurator().checkExists().usingWatcher(watcher).forPath(zPath);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  public List<String> getChildren(String zPath) throws KeeperException, InterruptedException {
    try {
      return getCurator().getChildren().forPath(zPath);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  public List<String> getChildren(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
    try {
      return getCurator().getChildren().usingWatcher(watcher).forPath(zPath);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  public boolean exists(String zPath) throws KeeperException, InterruptedException {
    return getStatus(zPath) != null;
  }
  
  public boolean exists(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
    return getStatus(zPath, watcher) != null;
  }
  
  public void sync(final String path) throws KeeperException, InterruptedException {
    try {
      getCurator().sync().forPath(path);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
  
  public List<ACL> getACL(String zPath) throws KeeperException, InterruptedException {
    try {
      return getCurator().getACL().forPath(zPath);
    } catch (Exception e) {
      throw CuratorUtil.manageException(e);
    }
  }
}
