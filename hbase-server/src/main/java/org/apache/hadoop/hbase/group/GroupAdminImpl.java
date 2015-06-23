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

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.metrics.util.MBeanUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Service to support Region Server Grouping (HBase-6721)
 * This should be installed as a Master CoprocessorEndpoint
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GroupAdminImpl implements GroupAdmin {
  private static final Log LOG = LogFactory.getLog(GroupAdminImpl.class);

  private final long threadKeepAliveTimeInMillis = 1000;
  private int threadMax = 1;
  private BlockingQueue<Runnable> threadQ;
  private MasterServices master;
  private ExecutorService executorService;
  //List of servers that are being moved from one group to another
  //Key=host:port,Value=targetGroup
  ConcurrentMap<String,String> serversInTransition =
      new ConcurrentHashMap<String,String>();

  public GroupAdminImpl(MasterServices master) {
    this.master = master;
    threadQ = new LinkedBlockingDeque<Runnable>();
    threadMax = master.getConfiguration().getInt("hbase.group.executor.threads", 1);
    executorService = new ThreadPoolExecutor(threadMax, threadMax,
        threadKeepAliveTimeInMillis, TimeUnit.MILLISECONDS, threadQ);
    registerMBean();
  }

  @Override
  public NavigableSet<TableName> listTablesOfGroup(String groupName) throws IOException {
    return getGroupInfoManager().getGroup(groupName).getTables();
  }


  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
    return getGroupInfoManager().getGroup(groupName);
  }


  @Override
  public GroupInfo getGroupInfoOfTable(TableName tableName) throws IOException {
    String groupName = getGroupInfoManager().getGroupOfTable(tableName);
    if (groupName == null) {
      if(master.getTableDescriptors().get(tableName) == null) {
        throw new ConstraintException("Table "+tableName+" does not exist");
      }
      throw new ConstraintException("Table "+tableName+" has no group");
    }
    return getGroupInfoManager().getGroup(groupName);
  }

  @Override
  public void moveServers(Set<String> servers, String targetGroup)
      throws IOException {
    if (servers == null) {
      throw new DoNotRetryIOException(
          "The list of servers cannot be null.");
    }
    if (StringUtils.isEmpty(targetGroup)) {
      throw new DoNotRetryIOException("The target group cannot be null.");
    }
    if(servers.size() < 1) {
      return;
    }
    //check that it's a valid host and port
    for(String server: servers) {
      String splits[] = server.split(":",2);
      if(splits.length < 2)
        throw new DoNotRetryIOException("Server list contains not a valid <HOST>:<PORT> entry");
      Integer.parseInt(splits[1]);
    }

    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      //we only allow a move from a single source group
      //so this should be ok
      GroupInfo srcGrp = manager.getGroupOfServer(servers.iterator().next());
      //only move online servers (from default)
      //or servers from other groups
      //this prevents bogus servers from entering groups
      if(GroupInfo.DEFAULT_GROUP.equals(srcGrp.getName())) {
        Set<String> onlineServers = new HashSet<String>();
        for(ServerName server: master.getServerManager().getOnlineServers().keySet()) {
          onlineServers.add(server.getHostAndPort());
        }
        for(String el: servers) {
          if(!onlineServers.contains(el)) {
            throw new DoNotRetryIOException(
                "Server "+el+" is not an online server in default group.");
          }
        }
      }

      if(srcGrp.getServers().size() <= servers.size() &&
          srcGrp.getTables().size() > 0) {
        throw new DoNotRetryIOException("Cannot leave a group "+srcGrp.getName()+
            " that contains tables " +"without servers.");
      }
      GroupMoveServerWorker.MoveServerPlan plan =
          new GroupMoveServerWorker.MoveServerPlan(servers, targetGroup);
      GroupMoveServerWorker worker = null;
      try {
        worker = new GroupMoveServerWorker(master, serversInTransition, getGroupInfoManager(), plan);
        executorService.submit(worker);
        LOG.info("GroupMoveServerWorkerSubmitted: "+plan.getTargetGroup());
      } catch(Exception e) {
        LOG.error("Failed to submit GroupMoveServerWorker", e);
        if (worker != null) {
          worker.complete();
        }
        throw new DoNotRetryIOException("Failed to submit GroupMoveServerWorker",e);
      }
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    if (tables == null) {
      throw new ConstraintException(
          "The list of servers cannot be null.");
    }
    if(tables.size() < 1) {
      LOG.debug("moveTables() passed an empty set. Ignoring.");
      return;
    }
    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      if(targetGroup != null) {
        GroupInfo destGroup = manager.getGroup(targetGroup);
        if(destGroup == null) {
          throw new ConstraintException("Target group does not exist: "+targetGroup);
        }
        if(destGroup.getServers().size() < 1) {
          throw new ConstraintException("Target group must have at least one server.");
        }
      }

      for(TableName table : tables) {
        String srcGroup = manager.getGroupOfTable(table);
        if(srcGroup != null && srcGroup.equals(targetGroup)) {
          throw new ConstraintException("Source group is the same as target group for table "+table+" :"+srcGroup);
        }
      }

      manager.moveTables(tables, targetGroup);
    }
    for(TableName table: tables) {
      for(HRegionInfo region:
          master.getAssignmentManager().getRegionStates().getRegionsOfTable(table)) {
        master.getAssignmentManager().unassign(region);
      }
    }
  }

  @Override
  public void addGroup(String name) throws IOException {
    getGroupInfoManager().addGroup(new GroupInfo(name));
  }

  @Override
  public void removeGroup(String name) throws IOException {
    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      GroupInfo groupInfo = getGroupInfoManager().getGroup(name);
      if(groupInfo == null) {
        throw new DoNotRetryIOException("Group "+name+" does not exist");
      }
      int tableCount = groupInfo.getTables().size();
      if (tableCount > 0) {
        throw new DoNotRetryIOException("Group "+name+" must have no associated tables: "+tableCount);
      }
      int serverCount = groupInfo.getServers().size();
      if(serverCount > 0) {
        throw new DoNotRetryIOException("Group "+name+" must have no associated servers: "+serverCount);
      }
      for(NamespaceDescriptor ns: master.listNamespaceDescriptors()) {
        String nsGroup = ns.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP);
        if(nsGroup != null &&  nsGroup.equals(name)) {
          throw new DoNotRetryIOException("Group "+name+" is referenced by namespace: "+ns.getName());
        }
      }
      manager.removeGroup(name);
    }
  }

  @Override
  public boolean balanceGroup(String groupName) throws IOException {
    ServerManager serverManager = master.getServerManager();
    AssignmentManager assignmentManager = master.getAssignmentManager();
    LoadBalancer balancer = master.getLoadBalancer();

    boolean balancerRan;
    synchronized (balancer) {
      // Only allow one balance run at at time.
      Map<String, RegionState> groupRIT = groupGetRegionsInTransition(groupName);
      if (groupRIT.size() > 0) {
        LOG.debug("Not running balancer because " +
          groupRIT.size() +
          " region(s) in transition: " +
          StringUtils.abbreviate(
              master.getAssignmentManager().getRegionStates().getRegionsInTransition().toString(),
              256));
        return false;
      }
      if (serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): " +
            serverManager.getDeadServers());
        return false;
      }

      //We balance per group instead of per table
      List<RegionPlan> plans = new ArrayList<RegionPlan>();
      for(Map.Entry<TableName, Map<ServerName, List<HRegionInfo>>> tableMap:
          getGroupAssignmentsByTable(groupName).entrySet()) {
        LOG.info("Creating partial plan for table "+tableMap.getKey()+": "+tableMap.getValue());
        List<RegionPlan> partialPlans = balancer.balanceCluster(tableMap.getValue());
        LOG.info("Partial plan for table "+tableMap.getKey()+": "+partialPlans);
        if (partialPlans != null) {
          plans.addAll(partialPlans);
        }
      }
      long startTime = System.currentTimeMillis();
      balancerRan = plans != null;
      if (plans != null && !plans.isEmpty()) {
        LOG.info("Group balance "+groupName+" starting with plan count: "+plans.size());
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          assignmentManager.balance(plan);
        }
        LOG.info("Group balance "+groupName+" completed after "+(System.currentTimeMillis()-startTime)+" seconds");
      }
    }
    return balancerRan;
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return getGroupInfoManager().listGroups();
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return getGroupInfoManager().getGroupOfServer(hostPort);
  }

  @Override
  public Map<String, String> listServersInTransition() throws IOException {
    return Collections.unmodifiableMap(serversInTransition);
  }

  @InterfaceAudience.Private
  public GroupInfoManager getGroupInfoManager() throws IOException {
    return ((GroupBasedLoadBalancer)master.getLoadBalancer()).getGroupInfoManager();
  }

  private Map<String, RegionState> groupGetRegionsInTransition(String groupName)
      throws IOException {
    Map<String, RegionState> rit = Maps.newTreeMap();
    AssignmentManager am = master.getAssignmentManager();
    GroupInfo groupInfo = getGroupInfo(groupName);
    for(TableName tableName : groupInfo.getTables()) {
      for(HRegionInfo regionInfo: am.getRegionStates().getRegionsOfTable(tableName)) {
        RegionState state =
            master.getAssignmentManager().getRegionStates().getRegionTransitionState(regionInfo);
        if(state != null) {
          rit.put(regionInfo.getEncodedName(), state);
        }
      }
    }
    return rit;
  }

  private Map<TableName, Map<ServerName, List<HRegionInfo>>>
      getGroupAssignmentsByTable(String groupName) throws IOException {
    Map<TableName, Map<ServerName, List<HRegionInfo>>> result = Maps.newHashMap();
    GroupInfo groupInfo = getGroupInfo(groupName);
    Map<TableName, Map<ServerName, List<HRegionInfo>>> assignments = Maps.newHashMap();
    for(Map.Entry<HRegionInfo, ServerName> entry:
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
      TableName currTable = entry.getKey().getTable();
      ServerName currServer = entry.getValue();
      HRegionInfo currRegion = entry.getKey();
      if(groupInfo.getTables().contains(currTable)) {
        if(!assignments.containsKey(entry.getKey().getTable())) {
          assignments.put(currTable, new HashMap<ServerName, List<HRegionInfo>>());
        }
        if(!assignments.get(currTable).containsKey(currServer)) {
          assignments.get(currTable).put(currServer, new ArrayList<HRegionInfo>());
        }
        assignments.get(currTable).get(currServer).add(currRegion);
      }
    }

    Map<ServerName, List<HRegionInfo>> serverMap = Maps.newHashMap();
    for(ServerName serverName: master.getServerManager().getOnlineServers().keySet()) {
      if(groupInfo.getServers().contains(serverName.getHostAndPort())) {
        serverMap.put(serverName, Collections.EMPTY_LIST);
      }
    }

    //add all tables that are members of the group
    for(TableName tableName : groupInfo.getTables()) {
      if(assignments.containsKey(tableName)) {
        result.put(tableName, new HashMap<ServerName, List<HRegionInfo>>());
        result.get(tableName).putAll(serverMap);
        result.get(tableName).putAll(assignments.get(tableName));
        LOG.debug("Adding assignments for "+tableName+": "+assignments.get(tableName));
      }
    }

    return result;
  }

  void registerMBean() {
    MXBeanImpl mxBeanInfo =
        MXBeanImpl.init(this, master);
    MBeanUtil.registerMBean("Group", "Group", mxBeanInfo);
    LOG.info("Registered Group MXBean");
  }
}
