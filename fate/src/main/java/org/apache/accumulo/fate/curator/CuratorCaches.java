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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.log4j.Logger;

/**
 * Caches values stored in zookeeper and keeps them up to date as they change in zookeeper.
 * 
 */
public class CuratorCaches {
  private static final Logger log = Logger.getLogger(CuratorCaches.class);
  
  private HashMap<String,NodeCache> nodeCache;
  private HashMap<String,PathChildrenCache> childrenCache;
  
  private CuratorFramework curator;
  
  public CuratorCaches(String zooKeepers, int sessionTimeout) {
    this(CuratorSession.getSession(zooKeepers, sessionTimeout));
  }
  
  public CuratorCaches(CuratorFramework curator) {
    this.curator = curator;
    this.nodeCache = new HashMap<String,NodeCache>();
    this.childrenCache = new HashMap<String,PathChildrenCache>();
  }
  
  public synchronized List<ChildData> getChildren(final String zPath) {
    return getChildren(zPath, null);
  }
  
  public synchronized List<ChildData> getChildren(String zPath, PathChildrenCacheListener listener) {
    PathChildrenCache cache = childrenCache.get(zPath);
    if (cache == null) {
      cache = new PathChildrenCache(curator, zPath, true);
      if (listener != null) {
        cache.getListenable().addListener(listener);
      }
      try {
        log.debug("Starting cache against " + zPath + (listener!=null? " using listener " + listener:""));
        cache.start(StartMode.BUILD_INITIAL_CACHE);
        // I'll do it myself!
        if (listener != null)
          for (ChildData cd : cache.getCurrentData()) {
            listener.childEvent(curator, new PathChildrenCacheEvent(Type.INITIALIZED, cd));
          }
        
        // Because parent's children are being watched, we don't need to cache the individual node
        // UNLESS we have a listener on it
        for (ChildData child : cache.getCurrentData()) {
          NodeCache childCache = nodeCache.get(child.getPath());
          if (childCache != null && childCache.getListenable().size() == 0) {
            log.debug("Removing cache " + childCache.getCurrentData().getPath() + " because parent cache was added");
            childCache.close();
            nodeCache.remove(child.getPath());
          }
        }
      } catch (Exception e) {
        log.error(e, e);
        try {
          cache.close();
        } catch (IOException e1) {
          // We're already in a bad state at this point, I think, but just in case
          log.error(e, e);
        }
        return null;
      }
      childrenCache.put(zPath, cache);
    } else if (listener != null) {
      log.debug("LISTENER- cache is null for path " + zPath + ", but got listener " + listener.getClass() + ". this is a broken case!");
    }
    return cache.getCurrentData();
  }
  
  public List<String> getChildKeys(final String zPath) {
    List<String> toRet = new ArrayList<String>();
    for (ChildData child : getChildren(zPath)) {
      toRet.add(CuratorUtil.getNodeName(child));
    }
    return toRet;
  }
  
  public synchronized ChildData get(final String zPath) {
    NodeCache cache = nodeCache.get(zPath);
    if (cache == null) {
      PathChildrenCache cCache = childrenCache.get(CuratorUtil.getNodeParent(zPath));
      if (cCache != null) {
        return cCache.getCurrentData(zPath);
      }
      cache = new NodeCache(curator, zPath);
      try {
        cache.start(true);
      } catch (Exception e) {
        log.error(e, e);
        try {
          cache.close();
        } catch (IOException e1) {
          // We're already in a bad state at this point, I think, but just in case
          log.error(e, e);
        }
        return null;
      }
      nodeCache.put(zPath, cache);
    }
    
    return cache.getCurrentData();
  }
  
  private synchronized void remove(String zPath) {
    if (log.isTraceEnabled())
      log.trace("removing " + zPath + " from cache");
    NodeCache nc = nodeCache.get(zPath);
    if (nc != null) {
      try {
        nc.close();
      } catch (IOException e) {
        log.error(e, e);
      }
    }
    
    PathChildrenCache pc = childrenCache.get(zPath);
    if (pc != null) {
      try {
        pc.close();
      } catch (IOException e) {
        log.error(e, e);
      }
    }
    
    nodeCache.remove(zPath);
    childrenCache.remove(zPath);
  }
  
  public synchronized void clear() {
    for (NodeCache nc : nodeCache.values()) {
      try {
        nc.close();
      } catch (IOException e) {
        log.error(e, e);
      }
    }
    for (PathChildrenCache pc : childrenCache.values()) {
      try {
        pc.close();
      } catch (IOException e) {
        log.error(e, e);
      }
    }
    
    nodeCache.clear();
    childrenCache.clear();
  }
  
  public CuratorFramework getCurator() {
    return curator;
  }
  
  public synchronized void clear(String zPath) {
    List<String> pathsToRemove = new ArrayList<String>();
    for (Iterator<String> i = nodeCache.keySet().iterator(); i.hasNext();) {
      String path = i.next();
      if (path.startsWith(zPath))
        pathsToRemove.add(path);
    }
    
    for (Iterator<String> i = childrenCache.keySet().iterator(); i.hasNext();) {
      String path = i.next();
      if (path.startsWith(zPath))
        pathsToRemove.add(path);
    }
    
    for (String path : pathsToRemove)
      remove(path);
  }
  
  private static Map<String,CuratorCaches> instances = new HashMap<String,CuratorCaches>();
  
  public static synchronized CuratorCaches getInstance(String zooKeepers, int sessionTimeout) {
    String key = zooKeepers + ":" + sessionTimeout;
    CuratorCaches zc = instances.get(key);
    if (zc == null) {
      zc = new CuratorCaches(zooKeepers, sessionTimeout);
      instances.put(key, zc);
    }
    
    return zc;
  }
}
