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
package org.apache.accumulo.server.master;

import static org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy.SKIP;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.curator.CuratorUtil;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.AddressUtil;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NotEmptyException;

public class LiveTServerSet {
  
  public interface Listener {
    void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added);
  }
  
  private static final Logger log = Logger.getLogger(LiveTServerSet.class);
  
  private final Listener cback;
  private final Instance instance;
  private final AccumuloConfiguration conf;
  private ZooCache zooCache;
  
  public class TServerConnection {
    private final InetSocketAddress address;
    
    public TServerConnection(InetSocketAddress addr) throws TException {
      address = addr;
    }
    
    private String lockString(ZooLock mlock) {
      return mlock.getLockID().serialize(ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK);
    }
    
    public void assignTablet(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.loadTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void unloadTablet(ZooLock lock, KeyExtent extent, boolean save) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.unloadTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift(), save);
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public TabletServerStatus getTableMap(boolean usePooledConnection) throws TException, ThriftSecurityException {
      
      if (usePooledConnection == true)
        throw new UnsupportedOperationException();
      
      TTransport transport = ThriftUtil.createTransport(address, conf);
      
      try {
        TabletClientService.Client client = ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);
        return client.getTabletServerStatus(Tracer.traceInfo(), SecurityConstants.getSystemCredentials());
      } finally {
        if (transport != null)
          transport.close();
      }
    }
    
    public void halt(ZooLock lock) throws TException, ThriftSecurityException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.halt(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void fastHalt(ZooLock lock) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.fastHalt(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void flush(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.flush(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), tableId,
            startRow == null ? null : ByteBuffer.wrap(startRow), endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void chop(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.chop(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void splitTablet(ZooLock lock, KeyExtent extent, Text splitPoint) throws TException, ThriftSecurityException, NotServingTabletException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.splitTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), extent.toThrift(),
            ByteBuffer.wrap(splitPoint.getBytes(), 0, splitPoint.getLength()));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void flushTablet(ZooLock lock, KeyExtent extent) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.flushTablet(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), extent.toThrift());
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public void compact(ZooLock lock, String tableId, byte[] startRow, byte[] endRow) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        client.compact(Tracer.traceInfo(), SecurityConstants.getSystemCredentials(), lockString(lock), tableId,
            startRow == null ? null : ByteBuffer.wrap(startRow), endRow == null ? null : ByteBuffer.wrap(endRow));
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
    public boolean isActive(long tid) throws TException {
      TabletClientService.Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), address, conf);
      try {
        return client.isActive(Tracer.traceInfo(), tid);
      } finally {
        ThriftUtil.returnClient(client);
      }
    }
    
  }
  
  static class TServerInfo {
    TServerConnection connection;
    TServerInstance instance;
    
    TServerInfo(TServerInstance instance, TServerConnection connection) {
      this.connection = connection;
      this.instance = instance;
    }
  };
  
  // Map from tserver master service to server information
  private Map<String,TServerInfo> current = new HashMap<String,TServerInfo>();
  private Map<String,Long> locklessServers = new HashMap<String,Long>();
  
  public LiveTServerSet(Instance instance, AccumuloConfiguration conf, Listener cback) {
    this.cback = cback;
    this.instance = instance;
    this.conf = conf;
    
  }
  
  public synchronized ZooCache getZooCache() {
    if (zooCache == null)
      zooCache = new ZooCache();
    return zooCache;
  }
  
  public synchronized void startListeningForTabletServerChanges() {
    SimpleTimer.getInstance().schedule(new Runnable() {
      @Override
      public void run() {
        synchronized (locklessServers) {
          if (!locklessServers.isEmpty()) {
            List<String> toRemove = new ArrayList<String>();
            for (Entry<String,Long> entry : locklessServers.entrySet()) {
              if (System.currentTimeMillis() - entry.getValue() > 600000) {
                deleteServerNode(entry.getKey());
                toRemove.add(entry.getKey());
              }
            }
            locklessServers.keySet().removeAll(toRemove);
          }
        }
      }
    }, 0, 5000);
    
    Collection<ChildData> result = getZooCache().getChildren(ZooUtil.getRoot(instance) + Constants.ZTSERVERS, serversListener);
    log.debug("Attaching SERVERSLISTENER to " + (ZooUtil.getRoot(instance) + Constants.ZTSERVERS) + " - received " + result);
  }
  
  private void deleteServerNode(String server) {
    try {
      getZooCache().getCurator().delete().forPath(ZooUtil.getRoot(instance) + Constants.ZTSERVERS + '/' + server);
    } catch (NotEmptyException ex) {
      // race condition: tserver created the lock after our last check; we'll see it at the next check
    } catch (NoNodeException nne) {
      // someone else deleted it
    } catch (Exception e) {
      // Some other curator exception, we don't care that much here.
      log.error(e,e);
    }
  }
  
  private ServersDirectoryListener serversListener = new ServersDirectoryListener(this);
  private TServerLockListener lockListener = new TServerLockListener(this);
  
  private class ServersDirectoryListener implements PathChildrenCacheListener {
    LiveTServerSet liveTServerSet;
    
    public ServersDirectoryListener(LiveTServerSet liveTServerSet) {
      this.liveTServerSet = liveTServerSet;
    }
    
    @Override
    public void childEvent(CuratorFramework curator, PathChildrenCacheEvent event) throws Exception {
      final Set<TServerInstance> doomed = new HashSet<TServerInstance>();
      log.debug("SERVERSLISTENER - Received event " + event.getType() + " for node " + event.getData().getPath());

      String server = CuratorUtil.getNodeName(event.getData());
      TServerInfo info = current.get(server);
      
      switch (event.getType()) {
        case INITIALIZED:
        case CHILD_ADDED:
        case CHILD_UPDATED:
          getZooCache().getChildren(event.getData().getPath(), lockListener);
          break;
        case CHILD_REMOVED:
          getZooCache().clear(event.getData().getPath());
          if (info != null) {
            doomed.add(info.instance);
            current.remove(server);
          }
          break;
        default:
          log.debug("Unhandled state " + event.getType() + " encountered for tserver manager. Ignoring.");
      }
      if (!doomed.isEmpty())
        liveTServerSet.cback.update(liveTServerSet, doomed, Collections.<TServerInstance> emptySet());
    }
  }
  
  private class TServerLockListener implements PathChildrenCacheListener {
    LiveTServerSet liveTServerSet;
    
    public TServerLockListener(LiveTServerSet liveTServerSet) {
      this.liveTServerSet = liveTServerSet;
    }
    
    @Override
    public void childEvent(CuratorFramework curator, PathChildrenCacheEvent event) throws Exception {
      final Set<TServerInstance> updates = new HashSet<TServerInstance>();
      final Set<TServerInstance> doomed = new HashSet<TServerInstance>();
      log.debug("LOCKLISTENER - Received event " + event.getType() + " for node " + event.getData().getPath());

      String server = CuratorUtil.getNodeName(CuratorUtil.getNodeParent(event.getData()));
      TServerInfo info = current.get(server);
      
      switch (event.getType()) {
        case INITIALIZED:
        case CHILD_ADDED:
        case CHILD_UPDATED:
          synchronized (locklessServers) {
            locklessServers.remove(server);
          }
          ServerServices services = new ServerServices(new String(event.getData().getData()));
          InetSocketAddress client = services.getAddress(ServerServices.Service.TSERV_CLIENT);
          InetSocketAddress addr = AddressUtil.parseAddress(server);
          TServerInstance instance = new TServerInstance(client, event.getData().getStat().getEphemeralOwner());
          
          if (info == null) {
            updates.add(instance);
            current.put(server, new TServerInfo(instance, new TServerConnection(addr)));
          } else if (!info.instance.equals(instance)) {
            doomed.add(info.instance);
            updates.add(instance);
            current.put(server, new TServerInfo(instance, new TServerConnection(addr)));
          }
          break;
        case CHILD_REMOVED:
          synchronized (locklessServers) {
            locklessServers.put(server, System.currentTimeMillis());
          }
          break;
        default:
          log.debug("Unhandled state " + event.getType() + " encountered for tserver lock manager. Ignoring.");
      }

      if (!doomed.isEmpty() || !updates.isEmpty())
        liveTServerSet.cback.update(liveTServerSet, doomed, updates);
    }
  }
  
  public synchronized TServerConnection getConnection(TServerInstance server) throws TException {
    if (server == null)
      return null;
    TServerInfo serverInfo = current.get(server.hostPort());
    // lock was lost?
    if (serverInfo == null)
      return null;
    // instance changed?
    if (!serverInfo.instance.equals(server))
      return null;
    TServerConnection result = serverInfo.connection;
    return result;
  }
  
  public synchronized Set<TServerInstance> getCurrentServers() {
    HashSet<TServerInstance> result = new HashSet<TServerInstance>();
    for (TServerInfo c : current.values()) {
      result.add(c.instance);
    }
    log.debug("Returning " + result + " for current tservers");
    return result;
  }
  
  public synchronized int size() {
    return current.size();
  }
  
  public synchronized TServerInstance find(String serverName) {
    TServerInfo serverInfo = current.get(serverName);
    if (serverInfo != null) {
      return serverInfo.instance;
    }
    return null;
  }
  
  public synchronized boolean isOnline(String serverName) {
    return current.containsKey(serverName);
  }
  
  public synchronized void remove(TServerInstance server) {
    current.remove(server.hostPort());
    
    log.info("Removing zookeeper lock for " + server);
    String zpath = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + server.hostPort();
    try {
      ZooReaderWriter.getRetryingInstance().recursiveDelete(zpath, SKIP);
    } catch (Exception e) {
      String msg = "error removing tablet server lock";
      log.fatal(msg, e);
      Halt.halt(msg, -1);
    }
    getZooCache().clear(zpath);
  }
}
