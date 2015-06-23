/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupTracker extends ZooKeeperNodeTracker {
  private static final Log LOG = LogFactory.getLog(GroupTracker.class);

  private List<Listener> listeners = Collections.synchronizedList(new ArrayList<Listener>());
  private GroupSerDe groupSerDe = new GroupSerDe();
  private volatile Map<String, GroupInfo> groupMap = new HashMap<String, GroupInfo>();
  private volatile Map<String, GroupInfo> serverMap = new HashMap<String, GroupInfo>();
  private RegionServerTracker rsTracker;
  private volatile boolean started = false;

  /**
   * Constructs a new ZK node tracker.
   * <p/>
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   */
  public GroupTracker(ZooKeeperWatcher watcher, Abortable abortable) throws IOException {
    //TODO make period configurable
    super(watcher,
        ZKUtil.joinZNode(watcher.baseZNode, GroupInfoManager.groupZNode),
        abortable!=null?abortable:new PersistentAbortable(10000));
    if(abortable == null) {
      ((PersistentAbortable)this.abortable).setGroupTracker(this);
    }
    rsTracker = new RegionServerTracker(watcher, abortable, this);
    try {
      ZKUtil.listChildrenAndWatchThem(watcher, node);
      rsTracker.start();
    } catch (KeeperException e) {
      throw new IOException("Failed to start RS tracker", e);
    }
  }

  public void addListener(Listener listener) {
    listeners.add(listener);
  }

  public void removeListener(Listener listener) {
    listeners.remove(listener);
  }

  @Override
  public synchronized void start() {
    super.start();
    started = true;
  }

  @Override
  public void nodeCreated(String path) {
    if (path.equals(node)) {
      refresh();
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (path.equals(node)) {
      nodeCreated(path);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.startsWith(node)) {
      refresh();
    }
  }

  public void blockUntilReady(int timeout) throws InterruptedException, IOException {
    blockUntilAvailable(timeout, false);
    if(getData(false) != null) {
      refresh(false);
    }
  }

  private void refresh() {
    try {
      refresh(false);
    } catch (IOException e) {
      this.abortable.abort("Failed to read group znode", e);
    }
  }

  private synchronized void refresh(boolean force) throws IOException {
    List<ServerName> onlineRS = rsTracker.getOnlineServers();
    Set<String> hostPorts = new HashSet<String>();
    for(ServerName entry: onlineRS) {
      hostPorts.add(entry.getHostAndPort());
    }
    Map<String, GroupInfo> tmpGroupMap = new HashMap<String, GroupInfo>();
    Map<String, GroupInfo> tmpServerMap = new HashMap<String, GroupInfo>();
    for(GroupInfo groupInfo: listGroups()) {
      tmpGroupMap.put(groupInfo.getName(), groupInfo);
      for(String server: groupInfo.getServers()) {
        tmpServerMap.put(server, groupInfo);
        hostPorts.remove(server);
      }
    }
    GroupInfo groupInfo = tmpGroupMap.get(GroupInfo.DEFAULT_GROUP);
    groupInfo.addAllServers(hostPorts);
    for(String entry: hostPorts) {
      tmpServerMap.put(entry, groupInfo);
    }

    //when reading sync on "this" if groupMap<->serverMap
    //invariant needs to be guaranteed
    groupMap = tmpGroupMap;
    serverMap = tmpServerMap;

    Map<String, GroupInfo> map = getGroupMap();
    for(Listener listener : listeners) {
      listener.groupMapChanged(map);
    }
  }

  private List<GroupInfo> listGroups() throws IOException {
    return groupSerDe.retrieveGroupList(watcher, node);
  }

  public GroupInfo getGroup(String name) {
    GroupInfo groupInfo = groupMap.get(name);
    return groupInfo;
  }

  public GroupInfo getGroupOfServer(String hostPort) {
    GroupInfo groupInfo = serverMap.get(hostPort);
    return groupInfo;
  }

  public Map<String, GroupInfo> getGroupMap() {
    return Collections.unmodifiableMap(groupMap);
  }

  public interface Listener {
    public void groupMapChanged(Map<String, GroupInfo> groupMap);
  }


  /**
   * This class is copied for RegionServerTracker
   * We need our own since the other one was tied to ServerManager
   * and thus the master
   */
  private static class RegionServerTracker extends ZooKeeperListener {
    private static final Log LOG = LogFactory.getLog(RegionServerTracker.class);
    private volatile List<ServerName> regionServers = new ArrayList<ServerName>();
    private Abortable abortable;
    private GroupTracker groupTracker;

    public RegionServerTracker(ZooKeeperWatcher watcher,
        Abortable abortable, GroupTracker groupTracker) {
      super(watcher);
      this.abortable = abortable;
      this.groupTracker = groupTracker;
    }

    public void start() throws KeeperException, IOException {
      watcher.registerListener(this);
      refresh();
    }

    private void add(final List<String> servers) throws IOException {
      List<ServerName> temp = new ArrayList<ServerName>();
      for (String n: servers) {
        ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(n));
        temp.add(sn);
      }
      regionServers = temp;
      //we're refreshing groups, since default membership
      //is dynamic and new servers may end up as new default group members
      refreshGroups();
    }

    private void remove(final ServerName sn) {
      List<ServerName> temp = new ArrayList<ServerName>();
      for(ServerName el: regionServers) {
        if(!sn.equals(el)) {
          temp.add(el);
        }
      }
      regionServers = temp;
      refreshGroups();
    }

    private void refreshGroups() {
      if(groupTracker.started && groupTracker.getData(false) != null) {
        groupTracker.refresh();
      }
    }

    public void refresh() throws KeeperException, IOException {
      List<String> servers =
        ZKUtil.listChildrenAndWatchThem(watcher, watcher.rsZNode);
      add(servers);
    }

    @Override
    public void nodeDeleted(String path) {
      if (path.startsWith(watcher.rsZNode)) {
        String serverName = ZKUtil.getNodeName(path);
        LOG.info("RegionServer ephemeral node deleted, processing expiration [" +
          serverName + "]");
        ServerName sn = ServerName.parseServerName(serverName);
        remove(sn);
      }
    }

    @Override
    public void nodeChildrenChanged(String path) {
      if (path.equals(watcher.rsZNode)) {
        try {
          List<String> servers =
            ZKUtil.listChildrenAndWatchThem(watcher, watcher.rsZNode);
          add(servers);
        } catch (IOException e) {
          abortable.abort("Unexpected zk exception getting RS nodes", e);
        } catch (KeeperException e) {
          abortable.abort("Unexpected zk exception getting RS nodes", e);
        }
      }
    }

    /**
     * Gets the online servers.
     * @return list of online servers
     */
    public List<ServerName> getOnlineServers() {
      return regionServers;
    }
  }

  private static class Refresher extends Thread {
    private final static Log LOG = LogFactory.getLog(Refresher.class);
    private GroupTracker groupTracker;
    private volatile boolean isRunning = true;
    private int period;

    public Refresher(GroupTracker groupTracker, int period) {
      this.groupTracker = groupTracker;
      this.period = period;
      this.setDaemon(true);
    }

    public boolean isRunning() {
      return isRunning;
    }

    @Override
    public void run() {
      while(true) {
        try {
          groupTracker.rsTracker.refresh();
          groupTracker.refresh(true);
          LOG.info("Recovery refresh successful");
          isRunning = false;
          return;
        } catch (IOException e) {
          LOG.warn("Failed to refresh", e);
        } catch (KeeperException e) {
          LOG.warn("Failed to refresh", e);
        }
        try {
          Thread.sleep(period);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  private static class PersistentAbortable implements Abortable {
    private final Log LOG = LogFactory.getLog(Abortable.class);
    private Refresher refresher;
    private GroupTracker groupTracker;
    private int period;


    public PersistentAbortable(int period) {
      this.period = period;
    }

    public void setGroupTracker(GroupTracker groupTracker) {
      this.groupTracker = groupTracker;
    }

    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("Launching referesher because of abort: "+why, e);
      if(refresher == null || !refresher.isRunning()) {
        refresher = new Refresher(groupTracker, period);
      }
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }
}
