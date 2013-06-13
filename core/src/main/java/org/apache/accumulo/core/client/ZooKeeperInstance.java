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
package org.apache.accumulo.core.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.fate.curator.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.apache.curator.x.discovery.strategies.StickyStrategy;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * <p>
 * An implementation of instance that looks in zookeeper to find information needed to connect to an instance of accumulo.
 * 
 * <p>
 * The advantage of using zookeeper to obtain information about accumulo is that zookeeper is highly available, very responsive, and supports caching.
 * 
 * <p>
 * Because it is possible for multiple instances of accumulo to share a single set of zookeeper servers, all constructors require an accumulo instance name.
 * 
 * If you do not know the instance names then run accumulo org.apache.accumulo.server.util.ListInstances on an accumulo server.
 * 
 */

public class ZooKeeperInstance implements Instance {
  
  private static final Logger log = Logger.getLogger(ZooKeeperInstance.class);
  
  private String instanceId = null;
  private String instanceName = null;
  
  private final CuratorFramework curator;
  // http://curator.incubator.apache.org/curator-x-discovery/index.html
  private ServiceDiscovery<String> discovery;
  private ServiceProvider<String> rootService;
  private ServiceProvider<String> masterService;
  
  private final int zooKeepersSessionTimeOut;
  
  /**
   * 
   * @param instanceName
   *          The name of specific accumulo instance. This is set at initialization time.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   */
  
  public ZooKeeperInstance(String instanceName, String zooKeepers) {
    this(instanceName, zooKeepers, (int) AccumuloConfiguration.getDefaultConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT));
  }
  
  /**
   * 
   * @param instanceName
   *          The name of specific accumulo instance. This is set at initialization time.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   * @param sessionTimeout
   *          zoo keeper session time out in milliseconds.
   */
  
  public ZooKeeperInstance(String instanceName, String zooKeepers, int sessionTimeout) {
    ArgumentChecker.notNull(instanceName, zooKeepers);
    this.instanceName = instanceName;
    this.zooKeepersSessionTimeOut = sessionTimeout;
    
    // Need to create curator for getInstanceId
    curator = constructCurator(zooKeepers, sessionTimeout).usingNamespace(Constants.ZROOT + Constants.ZINSTANCES);
    this.instanceId = getInstanceID();
    
    // And now that we have the ID, we can set the namespace
    curator.usingNamespace(Constants.ZROOT + '/' + getInstanceID());
    setupDiscoveries(curator);
  }
  
  /**
   * 
   * @param instanceId
   *          The UUID that identifies the accumulo instance you want to connect to.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   */
  
  public ZooKeeperInstance(UUID instanceId, String zooKeepers) {
    this(instanceId, zooKeepers, (int) AccumuloConfiguration.getDefaultConfiguration().getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT));
  }
  
  /**
   * 
   * @param instanceId
   *          The UUID that identifies the accumulo instance you want to connect to.
   * @param zooKeepers
   *          A comma separated list of zoo keeper server locations. Each location can contain an optional port, of the format host:port.
   * @param sessionTimeout
   *          zoo keeper session time out in milliseconds.
   */
  
  public ZooKeeperInstance(UUID instanceId, String zooKeepers, int sessionTimeout) {
    ArgumentChecker.notNull(instanceId, zooKeepers);
    this.instanceId = instanceId.toString();
    this.zooKeepersSessionTimeOut = sessionTimeout;
    curator = constructCurator(zooKeepers, sessionTimeout).usingNamespace(Constants.ZROOT + '/' + getInstanceID());
    
    setupDiscoveries(curator);
  }
  
  private CuratorFramework constructCurator(String zookeeperConnectString, int sessionTimeoutMs) {
    return CuratorFrameworkFactory.builder().canBeReadOnly(true).sessionTimeoutMs(sessionTimeoutMs).retryPolicy(CuratorUtil.retry)
        .connectString(zookeeperConnectString).build();
  }
  
  private void setupDiscoveries(CuratorFramework curator2) {
    try {
      discovery = ServiceDiscoveryBuilder.builder(String.class).client(curator).basePath(Constants.ZROOT_TABLET_LOCATION).build();
      discovery.start();
      rootService = discovery.serviceProviderBuilder().serviceName(Constants.ZROOT_CURATOR_SERVICE)
          .providerStrategy(new StickyStrategy<String>(new RandomStrategy<String>())).build();
      masterService = discovery.serviceProviderBuilder().serviceName(Constants.MASTER_CURATOR_SERVICE)
          .providerStrategy(new StickyStrategy<String>(new RandomStrategy<String>())).build();
      rootService.start();
      masterService.start();
    } catch (Exception e) {
      // We should have encountered any known Zookeeper issues by now.
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public String getInstanceID() {
    if (instanceId == null) {
      // want the instance id to be stable for the life of this instance object, so only get it once
      // And this will ONLY be invoked once iff the constructors using instanceName are used
      // And the namespace will already be set to the instance path
      byte[] iidb;
      try {
        iidb = curator.getData().forPath(instanceName);
      } catch (Exception e) {
        throw new RuntimeException("Instance name " + instanceName
            + " does not exist in zookeeper.  Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.", e);
      }
      
      instanceId = new String(iidb);
    }
    
    try {
      if (curator.usingNamespace(Constants.ZROOT).checkExists().forPath(instanceId) == null) {
        if (instanceName == null)
          throw new RuntimeException("Instance id " + instanceId + " does not exist in zookeeper");
        throw new RuntimeException("Instance id " + instanceId + " pointed to by the name " + instanceName + " does not exist in zookeeper");
      }
    } catch (Exception e) {
      // Should only happen if things are in a very bad state, I think
      throw new RuntimeException(e);
    }
    
    return instanceId;
  }
  
  @Override
  public List<String> getMasterLocations() {
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up master location using curator service discovery.");
    String loc;
    try {
      loc = masterService.getInstance().getPayload();
    } catch (Exception e) {
      opTimer.stop("Failed to find master location in curator discovery service");
      // Zookeeper errors are handles, big ones hit already. This is probably very bad?
      log.error(e,e);
      return Collections.emptyList();
    }
    opTimer.stop("Found master at " + (loc == null ? null : new String(loc)) + " in %DURATION%");
    
    if (loc == null) {
      return Collections.emptyList();
    }
    
    return Collections.singletonList(new String(loc));
  }
  
  @Override
  public String getRootTabletLocation() {
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Looking up root tablet location using curator service discovery.");
    String loc;
    try {
      loc = rootService.getInstance().getPayload();
    } catch (Exception e) {
      opTimer.stop("Failed to find root tablet in curator discovery service");
      // Zookeeper errors are handles, big ones hit already. This is probably very bad?
      log.error(e,e);
      return null;
    }
    opTimer.stop("Found root tablet at " + (loc == null ? null : new String(loc)) + " in %DURATION%");
    return loc;
  }
  
  @Override
  public String getInstanceName() {
    if (instanceName == null)
      instanceName = lookupInstanceName(curator, UUID.fromString(getInstanceID()));
    
    return instanceName;
  }
  
  @Override
  public String getZooKeepers() {
    return curator.getZookeeperClient().getCurrentConnectionString();
  }
  
  @Override
  public int getZooKeepersSessionTimeOut() {
    return zooKeepersSessionTimeOut;
  }
  
  @Override
  @Deprecated
  public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
  }
  
  @Override
  @Deprecated
  public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, ByteBufferUtil.toBytes(pass));
  }
  
  @Override
  public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    return getConnector(CredentialHelper.create(principal, token, getInstanceID()));
  }
  
  @SuppressWarnings("deprecation")
  private Connector getConnector(TCredentials credential) throws AccumuloException, AccumuloSecurityException {
    return new ConnectorImpl(this, credential);
  }
  
  @Override
  @Deprecated
  public Connector getConnector(String principal, byte[] pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(principal, new PasswordToken(pass));
  }
  
  private AccumuloConfiguration conf = null;
  
  @Override
  public AccumuloConfiguration getConfiguration() {
    if (conf == null)
      conf = AccumuloConfiguration.getDefaultConfiguration();
    return conf;
  }
  
  @Override
  public void setConfiguration(AccumuloConfiguration conf) {
    this.conf = conf;
  }
  
  /**
   * Given a zooCache and instanceId, look up the instance name.
   * 
   * @param curator
   * @param instanceId
   * @return the instance name
   */
  public static String lookupInstanceName(CuratorFramework curator, UUID instanceId) {
    ArgumentChecker.notNull(curator, instanceId);
    curator = curator.usingNamespace(Constants.ZROOT);
    try {
      for (String name : curator.getChildren().forPath(Constants.ZINSTANCES)) {
        String instanceNamePath = Constants.ZINSTANCES + "/" + name;
        UUID iid = UUID.fromString(new String(curator.getData().forPath(instanceNamePath)));
        if (iid.equals(instanceId)) {
          return name;
        }
      }
      return null;
    } catch (Exception e) {
      // Should only happen if things are in a very bad state, I think
      log.error(e,e);
      return null;
    }
  }
  
  /**
   * To be moved to server code. Only lives here to support certain client side utilities to minimize command-line options.
   */
  @Deprecated
  public static String getInstanceIDFromHdfs(Path instanceDirectory) {
    try {
      FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), AccumuloConfiguration.getSiteConfiguration());
      FileStatus[] files = null;
      try {
        files = fs.listStatus(instanceDirectory);
      } catch (FileNotFoundException ex) {
        // ignored
      }
      log.debug("Trying to read instance id from " + instanceDirectory);
      if (files == null || files.length == 0) {
        log.error("unable obtain instance id at " + instanceDirectory);
        throw new RuntimeException("Accumulo not initialized, there is no instance id at " + instanceDirectory);
      } else if (files.length != 1) {
        log.error("multiple potential instances in " + instanceDirectory);
        throw new RuntimeException("Accumulo found multiple possible instance ids in " + instanceDirectory);
      } else {
        String result = files[0].getPath().getName();
        return result;
      }
    } catch (IOException e) {
      throw new RuntimeException("Accumulo not initialized, there is no instance id at " + instanceDirectory, e);
    }
  }
  
  @Deprecated
  @Override
  public Connector getConnector(org.apache.accumulo.core.security.thrift.AuthInfo auth) throws AccumuloException, AccumuloSecurityException {
    return getConnector(auth.user, auth.password);
  }
}
