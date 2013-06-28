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
package org.apache.accumulo.server.master.state.tables;

import java.security.SecurityPermission;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.curator.CuratorReaderWriter.Mutator;
import org.apache.accumulo.fate.curator.CuratorReaderWriter.NodeExistsPolicy;
import org.apache.accumulo.fate.curator.CuratorUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.curator.CuratorReaderWriter;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class TableManager {
  private static SecurityPermission TABLE_MANAGER_PERMISSION = new SecurityPermission("tableManagerPermission");
  
  private static final Logger log = Logger.getLogger(TableManager.class);
  private static final Set<TableObserver> observers = Collections.synchronizedSet(new HashSet<TableObserver>());
  private static final Map<String,TableState> tableStateCache = Collections.synchronizedMap(new HashMap<String,TableState>());
  
  private static TableManager tableManager = null;
  
  private final Instance instance;
  private ZooCache zooStateCache;
  
  public static void prepareNewTableState(String instanceId, String tableId, String tableName, TableState state, NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    // state gets created last
    String zTablePath = Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId;
    CuratorReaderWriter zoo = CuratorReaderWriter.getInstance();
    zoo.putPersistentData(zTablePath, new byte[0], existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_CONF, new byte[0], existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_NAME, tableName.getBytes(), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_STATE, state.name().getBytes(), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_FLUSH_ID, "0".getBytes(), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_COMPACT_ID, "0".getBytes(), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_COMPACT_CANCEL_ID, "0".getBytes(), existsPolicy);
  }
  
  public synchronized static TableManager getInstance() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TABLE_MANAGER_PERMISSION);
    }
    if (tableManager == null)
      tableManager = new TableManager();
    return tableManager;
  }
  
  private TableManager() {
    instance = HdfsZooInstance.getInstance();
    zooStateCache = new ZooCache();
    setupListeners();
    updateTableStateCache();
  }
  
  public TableState getTableState(String tableId) {
    return tableStateCache.get(tableId);
  }
  
  public static class IllegalTableTransitionException extends Exception {
    private static final long serialVersionUID = 1L;
    
    final TableState oldState;
    final TableState newState;
    
    public IllegalTableTransitionException(TableState oldState, TableState newState) {
      this.oldState = oldState;
      this.newState = newState;
    }
    
    public TableState getOldState() {
      return oldState;
    }
    
    public TableState getNewState() {
      return newState;
    }
    
  }
  
  public synchronized void transitionTableState(final String tableId, final TableState newState) {
    String statePath = ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;
    
    try {
      CuratorReaderWriter.getInstance().mutate(statePath, (byte[]) newState.name().getBytes(), false, new Mutator() {
        @Override
        public byte[] mutate(byte[] oldData) throws Exception {
          TableState oldState = TableState.UNKNOWN;
          if (oldData != null)
            oldState = TableState.valueOf(new String(oldData));
          boolean transition = true;
          // +--------+
          // v |
          // NEW -> (ONLINE|OFFLINE)+--- DELETING
          switch (oldState) {
            case NEW:
              transition = (newState == TableState.OFFLINE || newState == TableState.ONLINE);
              break;
            case ONLINE: // fall-through intended
            case UNKNOWN:// fall through intended
            case OFFLINE:
              transition = (newState != TableState.NEW);
              break;
            case DELETING:
              // Can't transition to any state from DELETING
              transition = false;
              break;
          }
          if (!transition)
            throw new IllegalTableTransitionException(oldState, newState);
          log.debug("Transitioning state for table " + tableId + " from " + oldState + " to " + newState);
          return newState.name().getBytes();
        }
      });
    } catch (Exception e) {
      log.fatal("Failed to transition table to state " + newState);
      throw new RuntimeException(e);
    }
  }
  
  private void updateTableStateCache() {
    synchronized (tableStateCache) {
      for (ChildData tableId : zooStateCache.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES))
        if (zooStateCache.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + CuratorUtil.getNodeName(tableId) + Constants.ZTABLE_STATE) != null)
          updateTableStateCache(CuratorUtil.getNodeName(tableId));
    }
  }
  
  // tableId argument for better debug statements
  private TableState updateTableStateCache(ChildData node, String tableId) {
    synchronized (tableStateCache) {
      TableState tState = TableState.UNKNOWN;
      byte[] data = node.getData();
      if (data != null) {
        String sState = new String(data);
        try {
          tState = TableState.valueOf(sState);
          log.debug("updateTableStateCache reporting " + tableId + " with state " + tState + " based on " + new String(data));
        } catch (IllegalArgumentException e) {
          log.error("Unrecognized state for table with tableId=" + tableId + ": " + sState);
        }
        tableStateCache.put(tableId, tState);
      }
      return tState;
    }
  }
  
  public TableState updateTableStateCache(String tableId) {
    return updateTableStateCache(zooStateCache.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE), tableId);
  }
  
  public void addTable(String tableId, String tableName, NodeExistsPolicy existsPolicy) throws KeeperException, InterruptedException {
    prepareNewTableState(instance.getInstanceID(), tableId, tableName, TableState.NEW, existsPolicy);
    updateTableStateCache(tableId);
  }
  
  public void cloneTable(String srcTable, String tableId, String tableName, Map<String,String> propertiesToSet, Set<String> propertiesToExclude,
      NodeExistsPolicy existsPolicy) throws KeeperException, InterruptedException {
    prepareNewTableState(instance.getInstanceID(), tableId, tableName, TableState.NEW, existsPolicy);
    String srcTablePath = Constants.ZROOT + "/" + instance.getInstanceID() + Constants.ZTABLES + "/" + srcTable + Constants.ZTABLE_CONF;
    String newTablePath = Constants.ZROOT + "/" + instance.getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF;
    CuratorReaderWriter.getInstance().recursiveCopyPersistent(srcTablePath, newTablePath, NodeExistsPolicy.OVERWRITE);
    
    for (Entry<String,String> entry : propertiesToSet.entrySet())
      TablePropUtil.setTableProperty(tableId, entry.getKey(), entry.getValue());
    
    for (String prop : propertiesToExclude)
      CuratorReaderWriter.getInstance().recursiveDelete(
          Constants.ZROOT + "/" + instance.getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF + "/" + prop);
    
    updateTableStateCache(tableId);
  }
  
  public void removeTable(String tableId) throws KeeperException, InterruptedException {
    synchronized (tableStateCache) {
      tableStateCache.remove(tableId);
      CuratorReaderWriter.getInstance().recursiveDelete(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE);
      CuratorReaderWriter.getInstance().recursiveDelete(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId);
    }
  }
  
  public boolean addObserver(TableObserver to) {
    synchronized (observers) {
      synchronized (tableStateCache) {
        to.initialize(Collections.unmodifiableMap(tableStateCache));
        return observers.add(to);
      }
    }
  }
  
  public boolean removeObserver(TableObserver to) {
    return observers.remove(to);
  }
  
  // Sets up cache listeners for the zookeeper cache
  private void setupListeners() {
    zooStateCache.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES, new AllTablesListener());
  }
  
  // This just manages the listeners for each table. Let the table listener do the heavy lifting
  private class AllTablesListener implements PathChildrenCacheListener {
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
      if (event.getType().equals(Type.CHILD_ADDED)) {
        zooStateCache.getChildren(event.getData().getPath(), new TableListener());
      } else if (event.getType().equals(Type.CHILD_REMOVED)) {
        zooStateCache.clear(event.getData().getPath());
      }
    }
  }
  
  private class TableListener implements PathChildrenCacheListener {
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
      if (event.getData().getPath().endsWith(Constants.ZTABLE_STATE)) {
        String tableId = CuratorUtil.getNodeName(CuratorUtil.getNodeParent(event.getData().getPath()));
        String msg = null;
        switch (event.getType()) {
          case CHILD_ADDED:
          case INITIALIZED:
            msg = "Initializing ";
          case CHILD_UPDATED:
            if (msg == null)
              msg = "Updating ";
            TableState state = updateTableStateCache(event.getData(), tableId);
            log.debug(msg + tableId + " to state " + state);
            break;
          case CHILD_REMOVED:
            tableStateCache.remove(tableId);
            log.debug("Table " + tableId + " removed.");
            break;
          default:
            log.debug("Unhandled state " + event.getType() + " encountered for table " + tableId + ". Ignoring.");
        }
      }
    }
  }
  
  /*
   * private static boolean verifyTabletAssignments(String tableId) { log.info( "Sending message to load balancer to verify assignment of tablets with tableId="
   * + tableId); // Return true only if transitions to other states did not interrupt // this process. (like deleting the table) return true; }
   * 
   * private static synchronized boolean unloadTable(String tableId) { int loadedTabletCount = 0; while (loadedTabletCount > 0) { // wait for tables to be
   * unloaded } log.info("Table unloaded. tableId=" + tableId); return true; }
   * 
   * private static void cleanupDeletedTable(String tableId) { log.info("Sending message to cleanup the deleted table with tableId=" + tableId); }
   * 
   * switch (tState) { case NEW: // this should really only happen before the watcher // knows about the table log.error("Unexpected transition to " + tState +
   * " @ " + event); break;
   * 
   * case LOADING: // a table has started coming online or has pending // migrations (maybe?) if (verifyTabletAssignments(tableId))
   * TableState.transition(instance, tableId, TableState.ONLINE); break; case ONLINE: log.trace("Table online with tableId=" + tableId); break;
   * 
   * case DISABLING: if (unloadTable(tableId)) TableState.transition(instance, tableId, TableState.DISABLED); break; case DISABLED:
   * log.trace("Table disabled with tableId=" + tableId); break;
   * 
   * case UNLOADING: unloadTable(tableId); TableState.transition(instance, tableId, TableState.OFFLINE); case OFFLINE: break;
   * 
   * case DELETING: unloadTable(tableId); cleanupDeletedTable(tableId); break;
   * 
   * default: log.error("Unrecognized transition to " + tState + " @ " + event); }
   */
}
