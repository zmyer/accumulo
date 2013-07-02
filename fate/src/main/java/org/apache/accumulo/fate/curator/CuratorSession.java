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

import java.util.HashMap;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

class CuratorSession {
  
  private static final Logger log = Logger.getLogger(CuratorSession.class);
  
  private static RetryPolicy retry = new ExponentialBackoffRetry(1000, 5);
  
  private static Map<String,CuratorFramework> sessions = new HashMap<String,CuratorFramework>();
  
  private static String sessionKey(String keepers, int timeout, String scheme, byte[] auth) {
    return keepers + ":" + timeout + ":" + (scheme == null ? "" : scheme) + ":" + (auth == null ? "" : new String(auth));
  }
  
  private static CuratorFramework constructCurator(String zookeeperConnectString, int sessionTimeoutMs, String namespace, String scheme, byte[] bytes) {
    CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().sessionTimeoutMs(sessionTimeoutMs).retryPolicy(retry)
        .connectString(zookeeperConnectString);
    if (scheme != null && bytes != null)
      builder = builder.authorization(scheme, bytes);
    if (namespace != null)
      builder = builder.namespace(namespace);
    
    final CuratorFramework toRet = builder.build();
    toRet.start();
    return toRet;
  }
  
  public static synchronized CuratorFramework getSession(String zooKeepers, int timeout, String scheme, byte[] auth) {
    
    String sessionKey = sessionKey(zooKeepers, timeout, scheme, auth);
    
    // a read-only session can use a session with authorizations, so cache a copy for it w/out auths
    String readOnlySessionKey = sessionKey(zooKeepers, timeout, null, null);
    CuratorFramework curator = sessions.get(sessionKey);
    if (curator != null && curator.getState() == CuratorFrameworkState.STOPPED) {
      if (auth != null && sessions.get(readOnlySessionKey) == curator)
        sessions.remove(readOnlySessionKey);
      curator = null;
      sessions.remove(sessionKey);
    }
    
    if (curator == null) {
      log.debug("Connecting to " + zooKeepers + " with timeout " + timeout + " with auth " + (auth==null? "null":new String(auth)));
      curator = constructCurator(zooKeepers, timeout, null, scheme, auth);
      sessions.put(sessionKey, curator);
      if (auth != null && !sessions.containsKey(readOnlySessionKey))
        sessions.put(readOnlySessionKey, curator);
    } 
    
    return curator;
  }
}
