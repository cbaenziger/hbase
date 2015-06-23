/**
 * Copyright The Apache Software Foundation
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is an implementation of {@link GroupInfoManager}. Which makes
 * use of an HBase table as the persistence store for the group information.
 * It also makes use of zookeeper to store group information needed
 * for bootstrapping during offline mode.
 */
public class GroupInfoManagerImpl implements GroupInfoManager, ServerListener {
	private static final Log LOG = LogFactory.getLog(GroupInfoManagerImpl.class);

  /** Table descriptor for <code>hbase:rsgroup</code> catalog table */
  private final static HTableDescriptor GROUP_TABLE_DESC;
  static {
    GROUP_TABLE_DESC = new HTableDescriptor(GROUP_TABLE_NAME_BYTES);
    GROUP_TABLE_DESC.addFamily(new HColumnDescriptor(META_FAMILY_BYTES));
    GROUP_TABLE_DESC.setMaxFileSize(1l << 32);
  }

  //Access to this map should always be synchronized.
  private volatile Map<String, GroupInfo> groupMap;
  private volatile Map<TableName, String> tableMap;
  private MasterServices master;
  private HTable groupTable;
  private ZooKeeperWatcher watcher;
  private GroupStartupWorker groupStartupWorker;
  //contains list of groups that were last flushed to persistent store
  private volatile Set<String> prevGroups;
  private GroupSerDe groupSerDe;
  private DefaultServerUpdater defaultServerUpdater;


  public GroupInfoManagerImpl(MasterServices master) throws IOException {
    this.groupMap = Collections.EMPTY_MAP;
    this.tableMap = Collections.EMPTY_MAP;
    groupSerDe = new GroupSerDe();
    this.master = master;
    this.watcher = master.getZooKeeper();
    groupStartupWorker = new GroupStartupWorker(this, master);
    prevGroups = new HashSet<String>();
    refresh();
    groupStartupWorker.start();
    defaultServerUpdater = new DefaultServerUpdater(this);
    master.getServerManager().registerListener(this);
    defaultServerUpdater.start();
  }

	/**
	 * Adds the group.
	 *
	 * @param groupInfo the group name
	 */
  @Override
  public synchronized void addGroup(GroupInfo groupInfo) throws IOException {
		if (groupMap.get(groupInfo.getName()) != null ||
        groupInfo.getName().equals(GroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group already exists: "+groupInfo.getName());
    }
    Map<String, GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.put(groupInfo.getName(), groupInfo);
    flushConfig(newGroupMap);
	}

  @Override
  public synchronized boolean moveServers(Set<String> hostPorts, String srcGroup, String dstGroup) throws IOException {
    GroupInfo src = new GroupInfo(getGroup(srcGroup));
    GroupInfo dst = new GroupInfo(getGroup(dstGroup));
    boolean foundOne = false;
    for(String el: hostPorts) {
      foundOne = src.removeServer(el) || foundOne;
      dst.addServer(el);
    }

    Map<String,GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);

    flushConfig(newGroupMap);
    return foundOne;
  }

  /**
	 * Gets the group info of server.
	 *
	 * @param hostPort the server
	 * @return An instance of GroupInfo.
	 */
  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
		for (GroupInfo info : groupMap.values()) {
			if (info.containsServer(hostPort)){
				return info;
			}
		}
		return getGroup(GroupInfo.DEFAULT_GROUP);
	}

  /**
   * Gets the group information.
   * 
   * @param groupName
   *          the group name
   * @return An instance of GroupInfo
   */
  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
    GroupInfo groupInfo = groupMap.get(groupName);
    return groupInfo;
  }



  @Override
  public String getGroupOfTable(TableName tableName) throws IOException {
    return tableMap.get(tableName);
  }

  @Override
  public synchronized void moveTables(Set<TableName> tableNames, String groupName) throws IOException {
    if (groupName != null && !groupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is a special group");
    }
    Map<String,GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    for(TableName tableName: tableNames) {
      if (tableMap.containsKey(tableName)) {
        GroupInfo src = new GroupInfo(groupMap.get(tableMap.get(tableName)));
        src.removeTable(tableName);
        newGroupMap.put(src.getName(), src);
      }
      if(groupName != null) {
        GroupInfo dst = new GroupInfo(newGroupMap.get(groupName));
        dst.addTable(tableName);
        newGroupMap.put(dst.getName(), dst);
      }
    }

    flushConfig(newGroupMap);
  }


  /**
	 * Delete a region server group.
	 *
	 * @param groupName the group name
	 * @throws java.io.IOException Signals that an I/O exception has occurred.
	 */
  @Override
  public synchronized void removeGroup(String groupName) throws IOException {
    if (!groupMap.containsKey(groupName) || groupName.equals(GroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is a reserved group");
    }
    Map<String,GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.remove(groupName);
    flushConfig(newGroupMap);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    List<GroupInfo> list = Lists.newLinkedList(groupMap.values());
    return list;
  }

  @Override
  public boolean isOnline() {
    return groupStartupWorker.isOnline();
  }

  @Override
  public synchronized void refresh() throws IOException {
    refresh(false);
  }

  private synchronized void refresh(boolean forceOnline) throws IOException {
    List<GroupInfo> groupList = new LinkedList<GroupInfo>();

    //overwrite anything read from zk, group table is source of truth
    //if online read from GROUP table
    if (forceOnline || isOnline()) {
      LOG.debug("Refershing in Online mode.");
      if (groupTable == null) {
        groupTable = new HTable(master.getConfiguration(), GROUP_TABLE_NAME_BYTES);
      }
      groupList.addAll(groupSerDe.retrieveGroupList(groupTable));
    } else {
      LOG.debug("Refershing in Offline mode.");
      String groupBasePath = ZKUtil.joinZNode(watcher.baseZNode, groupZNode);
      groupList.addAll(groupSerDe.retrieveGroupList(watcher, groupBasePath));
    }

    //refresh default group, prune
    NavigableSet<TableName> orphanTables = new TreeSet<TableName>();
    for(String entry: master.getTableDescriptors().getAll().keySet()) {
      orphanTables.add(TableName.valueOf(entry));
    }
    for(TableName table : GroupBasedLoadBalancer.SPECIAL_TABLES) {
      orphanTables.add(table);
    }
    for(GroupInfo group: groupList) {
      if(!group.getName().equals(GroupInfo.DEFAULT_GROUP)) {
        orphanTables.removeAll(group.getTables());
      }
    }

    //This is added to the last of the list
    //so it overwrites the default group loaded
    //from region group table or zk
    groupList.add(new GroupInfo(GroupInfo.DEFAULT_GROUP,
            new TreeSet<String>(getDefaultServers()),
            orphanTables));


    //populate the data
    HashMap<String, GroupInfo> newGroupMap = Maps.newHashMap();
    HashMap<TableName, String> newTableMap = Maps.newHashMap();
    for (GroupInfo group : groupList) {
      newGroupMap.put(group.getName(), group);
      for(TableName table: group.getTables()) {
        newTableMap.put(table, group.getName());
      }
    }
    groupMap = Collections.unmodifiableMap(newGroupMap);
    tableMap = Collections.unmodifiableMap(newTableMap);

    prevGroups.clear();
    prevGroups.addAll(groupMap.keySet());
	}

  private synchronized Map<TableName,String> flushConfigTable(Map<String,GroupInfo> newGroupMap) throws IOException {
    Map<TableName,String> newTableMap = Maps.newHashMap();
    Put put = new Put(ROW_KEY);
    Delete delete = new Delete(ROW_KEY);

    //populate deletes
    for(String groupName : prevGroups) {
      if(!newGroupMap.containsKey(groupName)) {
        delete.deleteColumns(META_FAMILY_BYTES, Bytes.toBytes(groupName));
      }
    }

    //populate puts
    for(GroupInfo groupInfo : newGroupMap.values()) {
      RSGroupProtos.GroupInfo proto = groupSerDe.toProto(groupInfo);
      put.add(META_FAMILY_BYTES,
          Bytes.toBytes(groupInfo.getName()),
          proto.toByteArray());
      for(TableName entry: groupInfo.getTables()) {
        newTableMap.put(entry, groupInfo.getName());
      }
    }

    RowMutations rowMutations = new RowMutations(ROW_KEY);
    if(put.size() > 0) {
      rowMutations.add(put);
    }
    if(delete.size() > 0) {
      rowMutations.add(delete);
    }
    if(rowMutations.getMutations().size() > 0) {
      groupTable.mutateRow(rowMutations);
    }
    return newTableMap;
  }

	private synchronized void flushConfig(Map<String,GroupInfo> newGroupMap) throws IOException {
    Map<TableName, String> newTableMap;
    //this should only not enter during startup
    if(!isOnline()) {
      LOG.error("Still in Offline mode.");
      throw new IOException("Still in Offline mode.");
    }

    newTableMap = flushConfigTable(newGroupMap);

    //make changes visible since it has been
    //persisted in the source of truth
    groupMap = Collections.unmodifiableMap(newGroupMap);
    tableMap = Collections.unmodifiableMap(newTableMap);


    try {
      //Write zk data first since that's what we'll read first
      String groupBasePath = ZKUtil.joinZNode(watcher.baseZNode, groupZNode);
      ZKUtil.createAndFailSilent(watcher, groupBasePath, ProtobufUtil.PB_MAGIC);

      List<ZKUtil.ZKUtilOp> zkOps = new ArrayList<ZKUtil.ZKUtilOp>(newGroupMap.size());
      for(String groupName : prevGroups) {
        if(!newGroupMap.containsKey(groupName)) {
          String znode = ZKUtil.joinZNode(groupBasePath, groupName);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        }
      }


      for(GroupInfo groupInfo : newGroupMap.values()) {
        String znode = ZKUtil.joinZNode(groupBasePath, groupInfo.getName());
        RSGroupProtos.GroupInfo proto = groupSerDe.toProto(groupInfo);
        LOG.debug("Updating znode: "+znode);
        ZKUtil.createAndFailSilent(watcher, znode);
        zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        zkOps.add(ZKUtil.ZKUtilOp.createAndFailSilent(znode,
            ProtobufUtil.prependPBMagic(proto.toByteArray())));
      }
      LOG.debug("Writing ZK GroupInfo count: " + zkOps.size());

      ZKUtil.multiOrSequential(watcher, zkOps, false);
    } catch (KeeperException e) {
      LOG.error("Failed to write to groupZNode", e);
      master.abort("Failed to write to groupZNode", e);
      throw new IOException("Failed to write to groupZNode",e);
    }

    prevGroups.clear();
    prevGroups.addAll(newGroupMap.keySet());
  }

  private List<ServerName> getOnlineRS() throws IOException {
    if (master != null) {
      return master.getServerManager().getOnlineServersList();
    }
    try {
      LOG.debug("Reading online RS from zookeeper");
      List<ServerName> servers = new LinkedList<ServerName>();
      for (String el: ZKUtil.listChildrenNoWatch(watcher, watcher.rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
      return servers;
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list from zookeeper", e);
    }
  }

  private List<String> getDefaultServers() throws IOException {
    List<String> defaultServers = new LinkedList<String>();
    for(ServerName server : getOnlineRS()) {
      String hostPort = server.getHostAndPort();
      boolean found = false;
      for(GroupInfo groupInfo : groupMap.values()) {
        if(!GroupInfo.DEFAULT_GROUP.equals(groupInfo.getName()) &&
            groupInfo.containsServer(hostPort)) {
          found = true;
          break;
        }
      }
      if(!found) {
        defaultServers.add(hostPort);
      }
    }
    return defaultServers;
  }

  private synchronized void updateDefaultServers(NavigableSet<String> hostPort) throws IOException {
    if(!isOnline()) {
      LOG.info("Offline mode. Skipping update of default servers");
      return;
    }
    GroupInfo info = groupMap.get(GroupInfo.DEFAULT_GROUP);
    GroupInfo newInfo = new GroupInfo(info.getName(), hostPort, info.getTables());
    HashMap<String, GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.put(newInfo.getName(), newInfo);
    flushConfig(newGroupMap);
  }

  @Override
  public void serverAdded(ServerName serverName) {
    defaultServerUpdater.serverChanged();
  }

  @Override
  public void serverRemoved(ServerName serverName) {
    defaultServerUpdater.serverChanged();
  }

  private static class DefaultServerUpdater extends Thread {
    private static final Log LOG = LogFactory.getLog(DefaultServerUpdater.class);
    private GroupInfoManagerImpl mgr;
    private boolean hasChanged = false;

    public DefaultServerUpdater(GroupInfoManagerImpl mgr) {
      this.mgr = mgr;
    }

    public void run() {
      List<String> prevDefaultServers = new LinkedList<String>();
      while(!mgr.master.isAborted() || !mgr.master.isStopped()) {
        try {
          LOG.info("Updating default servers.");
          List<String> servers = mgr.getDefaultServers();
          Collections.sort(servers);
          if(!servers.equals(prevDefaultServers)) {
            mgr.updateDefaultServers(new TreeSet<String>(servers));
            prevDefaultServers = servers;
            LOG.info("Updated with servers: "+servers.size());
          }
          try {
            synchronized (this) {
              if(!hasChanged) {
                wait();
              }
              hasChanged = false;
            }
          } catch (InterruptedException e) {
          }
        } catch (IOException e) {
          LOG.warn("Failed to update default servers", e);
        }
      }
    }

    public void serverChanged() {
      synchronized (this) {
        hasChanged = true;
        this.notify();
      }
    }
  }


  private static class GroupStartupWorker extends Thread {
    private static final Log LOG = LogFactory.getLog(GroupStartupWorker.class);

    private Configuration conf;
    private volatile boolean isOnline = false;
    private MasterServices masterServices;
    private GroupInfoManagerImpl groupInfoManager;

    public GroupStartupWorker(GroupInfoManagerImpl groupInfoManager,
                              MasterServices masterServices) {
      this.conf = masterServices.getConfiguration();
      this.masterServices = masterServices;
      this.groupInfoManager = groupInfoManager;
      setName(GroupStartupWorker.class.getName()+"-"+masterServices.getServerName());
      setDaemon(true);
    }

    @Override
    public void run() {
      if(waitForGroupTableOnline()) {
        LOG.info("GroupBasedLoadBalancer is now online");
      }
    }

    public boolean waitForGroupTableOnline() {
      final List<HRegionInfo> foundRegions = new LinkedList<HRegionInfo>();
      final List<HRegionInfo> assignedRegions = new LinkedList<HRegionInfo>();
      final AtomicBoolean found = new AtomicBoolean(false);
      final TableStateManager tsm = masterServices.getTableStateManager();
      boolean createSent = false;
      while (!found.get() && isMasterRunning()) {
        foundRegions.clear();
        assignedRegions.clear();
        found.set(true);
        try {

          final HConnection conn = HConnectionManager.getConnection(conf);
          final HTable nsTable =new HTable(TableName.NAMESPACE_TABLE_NAME, conn);
          final HTable groupTable = new HTable(GroupInfoManager.GROUP_TABLE_NAME, conn);
          boolean rootMetaFound =
              masterServices.getMetaTableLocator().verifyMetaRegionLocation(
                  conn,
                  masterServices.getZooKeeper(),
                  1);
          final AtomicBoolean nsFound = new AtomicBoolean(false);
          if (rootMetaFound) {

            MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitorBase() {
              @Override
              public boolean processRow(Result row) throws IOException {
                HRegionInfo info = HRegionInfo.getHRegionInfo(row);
                if (info != null) {
                  Cell serverCell =
                      row.getColumnLatestCell(HConstants.CATALOG_FAMILY,
                          HConstants.SERVER_QUALIFIER);
                  if (Bytes.equals(GROUP_TABLE_NAME_BYTES, info.getTableName()) &&
                      serverCell != null) {
                    ServerName sn =
                        ServerName.parseVersionedServerName(CellUtil.cloneValue(serverCell));
                    if (sn == null) {
                      found.set(false);
                    } else if (tsm.isTableState(GROUP_TABLE_NAME, TableState.State.ENABLED)) {
                      try {
                        HBaseProtos.RegionSpecifier regionSpecifier =
                            HBaseProtos.RegionSpecifier.newBuilder()
                                .setValue(ByteString.copyFrom(row.getRow()))
                                .setType(
                                    HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
                                .build();
                        ClientProtos.ClientService.BlockingInterface rs = conn.getClient(sn);
                        ClientProtos.GetRequest req =
                            ClientProtos.GetRequest.newBuilder()
                                .setRegion(regionSpecifier)
                                .setGet(ProtobufUtil.toGet(new Get(ROW_KEY))).build();
                        rs.get(null, req);
                        assignedRegions.add(info);
                      } catch(Exception ex) {
                        LOG.debug("Caught exception while verifying group region", ex);
                      }
                    }
                    foundRegions.add(info);
                  }
                  if (TableName.NAMESPACE_TABLE_NAME.equals(info.getTable())) {
                    Cell cell = row.getColumnLatestCell(HConstants.CATALOG_FAMILY,
                        HConstants.SERVER_QUALIFIER);
                    ServerName sn = null;
                    if(cell != null) {
                      sn = ServerName.parseVersionedServerName(CellUtil.cloneValue(cell));
                    }
                    if (tsm.isTableState(TableName.NAMESPACE_TABLE_NAME,
                        TableState.State.ENABLED)) {
                      try {
                        HBaseProtos.RegionSpecifier regionSpecifier =
                            HBaseProtos.RegionSpecifier.newBuilder()
                                .setValue(ByteString.copyFrom(row.getRow()))
                                .setType(
                                    HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
                                .build();
                        ClientProtos.ClientService.BlockingInterface rs = conn.getClient(sn);
                        ClientProtos.GetRequest req =
                            ClientProtos.GetRequest.newBuilder()
                                .setRegion(regionSpecifier)
                                    .setGet(ProtobufUtil.toGet(new Get(ROW_KEY))).build();
                        rs.get(null, req);
                        nsFound.set(true);
                      } catch(Exception ex) {
                        LOG.debug("Caught exception while verifying group region", ex);
                      }
                    }
                  }
                }
                return true;
              }
            };
            MetaScanner.metaScan(conf, visitor);
            // if no regions in meta then we have to create the table
            if (foundRegions.size() < 1 && rootMetaFound && !createSent && nsFound.get()) {
              groupInfoManager.createGroupTable(masterServices);
              createSent = true;
            }
            LOG.info("Group table: " + GROUP_TABLE_NAME + " isOnline: " + found.get()
                + ", regionCount: " + foundRegions.size() + ", assignCount: "
                + assignedRegions.size() + ", rootMetaFound: "+rootMetaFound);
            found.set(found.get() && assignedRegions.size() == foundRegions.size()
                && foundRegions.size() > 0);
          } else {
            LOG.info("Waiting for catalog tables to come online");
            found.set(false);
          }
          if (found.get()) {
            LOG.debug("With group table online, refreshing cached information.");
            groupInfoManager.refresh(true);
            isOnline = true;
            //flush any inconsistencies between ZK and HTable
            groupInfoManager.flushConfig(groupInfoManager.groupMap);
          }
        } catch(Exception e) {
          found.set(false);
          LOG.warn("Failed to perform check", e);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.info("Sleep interrupted", e);
        }
      }
      return found.get();
    }

    public boolean isOnline() {
      return isOnline;
    }

    private boolean isMasterRunning() {
      return !masterServices.isAborted() && !masterServices.isStopped();
    }
  }

  private void createGroupTable(MasterServices masterServices) throws IOException {
    HRegionInfo newRegions[] = new HRegionInfo[]{
          new HRegionInfo(GROUP_TABLE_DESC.getTableName(), null, null)};
    //we need to create the table this way to bypass
    //checkInitialized
    masterServices.getExecutorService()
        .submit(new CreateTableHandler(
            masterServices,
            masterServices.getMasterFileSystem(),
            GROUP_TABLE_DESC,
            masterServices.getConfiguration(),
            newRegions,
            masterServices).prepare());
    //wait for region to be online
    int tries = 600;
    while(masterServices.getAssignmentManager().getRegionStates()
          .getRegionServerOfRegion(newRegions[0]) == null && tries > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException("Wait interrupted", e);
      }
      tries--;
    }
    if(tries <= 0) {
      throw new IOException("Failed to create group table.");
    }
  }

}
