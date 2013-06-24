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
package org.apache.accumulo.server.conf;

import org.apache.accumulo.fate.curator.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.log4j.Logger;

class TableConfWatcher implements PathChildrenCacheListener {
  private static final Logger log = Logger.getLogger(TableConfWatcher.class);
  private TableConfiguration tableConfig;

  TableConfWatcher(TableConfiguration tableConfiguration) {
    tableConfig = tableConfiguration;
  }
  
  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    if (log.isTraceEnabled())
      log.trace("WatchEvent : " + event.getData().getPath() + " " + event.getType());
    String key = CuratorUtil.getNodeName(event.getData());
    
    switch (event.getType()) {
      case INITIALIZED:
      case CHILD_ADDED:
      case CHILD_UPDATED:
      case CHILD_REMOVED:
        tableConfig.propertyChanged(key);
        break;
      default:
        log.debug("Unhandled state " + event.getType() + " encountered for table " + tableConfig.getTableId() + ". Ignoring.");
    }
  }
}
