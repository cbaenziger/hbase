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

package org.apache.hadoop.hbase.master.balancer.grouploadbalancer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

/**
 * This load balancer partitions servers and tables into groups. Then, within each group, it uses
 * another load balancer to balance within each group.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class GroupBasedLoadBalancer extends BaseLoadBalancer {

  private static final Log LOG = LogFactory.getLog(GroupBasedLoadBalancer.class);

  private MasterServices masterServices;
  private GroupInfoManagerImpl groupInfoManager;
  private LoadBalancer internalBalancer;

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    this.groupInfoManager = new GroupInfoManagerImpl(conf);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    super.onConfigurationChange(conf);
    setConf(conf);
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {

    // don't balance master
    if (masterServerName != null && clusterState.containsKey(masterServerName)) {
      clusterState = new HashMap<>(clusterState);
      clusterState.remove(masterServerName);
    }

    // see if master regions need to be balanced
    List<RegionPlan> regionPlans = balanceMasterRegions(clusterState);
    if (regionPlans != null) {
      return regionPlans;
    }

    // we need to add all the servers and tables that were not placed in any groups into the default
    // group
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
      ServerName serverName = entry.getKey();
      List<HRegionInfo> regions = entry.getValue();

      String defaultGroupName = this.groupInfoManager.getDefaultGroupName();

      try {
        if (this.groupInfoManager.getGroupOfServer(serverName.getHostAndPort()) == null) {
          this.groupInfoManager.getGroupInfo(defaultGroupName)
              .addServer(serverName.getHostAndPort());
        }
      } catch (Exception exp) {
        LOG.debug("Error putting server " + serverName.getHostAndPort() + " in the default group.");
      }

      for (HRegionInfo region : regions) {
        try {
          if (this.groupInfoManager.getGroupOfTable(region.getTable()) == null) {
            this.groupInfoManager.getGroupInfo(defaultGroupName).addTable(region.getTable());
          }
        } catch (Exception exp) {
          LOG.debug("Error putting region " + region + " in the default group.");
        }
      }
    }

    Map<ServerName, List<HRegionInfo>> correctedClusterState = correctAssignments(clusterState);
    regionPlans = new ArrayList<RegionPlan>();

    // Balance regions group by group
    try {
      for (GroupInfo currentGroupInfo : this.groupInfoManager.listGroups()) {
        Map<ServerName, List<HRegionInfo>> groupClusterState = new HashMap<>();
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : correctedClusterState.entrySet()) {
          ServerName serverName = entry.getKey();
          List<HRegionInfo> regions = entry.getValue();
          if (this.groupInfoManager.getGroupOfServer(serverName.getHostAndPort())
              == currentGroupInfo) {
            groupClusterState.put(serverName, new LinkedList<HRegionInfo>());
          }

          for (HRegionInfo region : regions) {
            if (this.groupInfoManager.getGroupOfTable(region.getTable()) == currentGroupInfo) {
              groupClusterState.get(serverName).add(region);
            }
          }
        }

        List<RegionPlan> groupPlans =
            this.internalBalancer.balanceCluster(groupClusterState);
        if (groupPlans != null) {
          regionPlans.addAll(groupPlans);
        }
      }
    } catch (IOException exp) {
      LOG.warn("Exception while balancing cluster.", exp);
      regionPlans.clear();
    }
    return regionPlans;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    Map<ServerName, List<HRegionInfo>> assignments = new HashMap<>();
    ListMultimap<String, HRegionInfo> regionMap = LinkedListMultimap.create();
    ListMultimap<String, ServerName> serverMap = LinkedListMultimap.create();
    try {
      generateGroupMaps(regions, servers, regionMap, serverMap);
      for (String groupName : regionMap.keySet()) {
        if (regionMap.get(groupName).size() > 0) {
          Map<ServerName, List<HRegionInfo>> result = this.internalBalancer
              .roundRobinAssignment(regionMap.get(groupName), serverMap.get(groupName));
          if (result != null) {
            assignments.putAll(result);
          }
        }
      }
    } catch (HBaseIOException exp) {
      LOG.warn("Error with round robin assignments.", exp);
    }
    return assignments;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
      List<ServerName> servers) {
    try {
      Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<>();
      ListMultimap<String, HRegionInfo> groupToRegion = ArrayListMultimap.create();
      List<HRegionInfo> misplacedRegions = getMisplacedRegions(regions);
      for (HRegionInfo region : regions.keySet()) {
        if (!misplacedRegions.contains(region)) {
          String groupName = groupInfoManager.getGroupOfTable(region.getTable()).getName();
          groupToRegion.put(groupName, region);
        }
      }

      // Now the "groupToRegion" map has only the regions which have correct assignments
      for (String groupName : groupToRegion.keySet()) {
        Map<HRegionInfo, ServerName> currentAssignmentMap = new TreeMap<>();
        List<HRegionInfo> regionList = groupToRegion.get(groupName);
        for (HRegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        GroupInfo groupInfo = groupInfoManager.getGroupInfo(groupName);
        List<ServerName> candidateList = filterServersForGroupInfo(groupInfo, servers);
        Map<ServerName, List<HRegionInfo>> assignmentsFromInternalBalancer =
            this.internalBalancer.retainAssignment(currentAssignmentMap, candidateList);
        for (Map.Entry<ServerName, List<HRegionInfo>> entry :
            assignmentsFromInternalBalancer.entrySet()) {
          ServerName serverName = entry.getKey();
          List<HRegionInfo> regionListFromInternalBalancer = entry.getValue();
          if (!assignments.containsKey(serverName)) {
            assignments.put(serverName, regionListFromInternalBalancer);
          } else {
            List<HRegionInfo> existingRegionList = assignments.get(serverName);
            existingRegionList.addAll(regionListFromInternalBalancer);
            assignments.put(serverName, existingRegionList);
          }
        }
      }

      for (HRegionInfo region : misplacedRegions) {
        GroupInfo groupInfo = groupInfoManager.getGroupOfTable(region.getTable());
        List<ServerName> candidateList = filterServersForGroupInfo(groupInfo, servers);
        ServerName serverName = this.internalBalancer.randomAssignment(region, candidateList);
        if (serverName != null && !assignments.containsKey(serverName)) {
          assignments.put(serverName, new ArrayList<HRegionInfo>());
        } else if (serverName != null) {
          assignments.get(serverName).add(region);
        } else {
          // if no server is available to assign, assign it to a server in the default group
          NavigableSet<String> defaultServersString =
              groupInfoManager.getGroupInfo(groupInfoManager.getDefaultGroupName()).getServers();
          List<ServerName> serverNameList = new ArrayList<>();
          while (defaultServersString.iterator().hasNext()) {
            serverNameList.add(ServerName.parseServerName(defaultServersString.iterator().next()));
          }
          ServerName randomServerFromDefaultGroup =
              this.internalBalancer.randomAssignment(region, serverNameList);
          if (!assignments.containsKey(randomServerFromDefaultGroup)) {
            assignments.put(randomServerFromDefaultGroup, new ArrayList<HRegionInfo>());
          }
        }
      }
      return assignments;
    } catch (Exception exp) {
      LOG.warn("Failed to do retain assignment.", exp);
    }
    return null;
  }

  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    try {
      Map<HRegionInfo, ServerName> assignments = new HashMap<>();
      ListMultimap<String, HRegionInfo> regionMap = LinkedListMultimap.create();
      ListMultimap<String, ServerName> serverMap = LinkedListMultimap.create();
      generateGroupMaps(regions, servers, regionMap, serverMap);
      for (String groupName : regionMap.keySet()) {
        if (regionMap.get(groupName).size() > 0) {
          assignments.putAll(this.internalBalancer
              .immediateAssignment(regionMap.get(groupName), serverMap.get(groupName)));
        }
      }
      return assignments;
    } catch (Exception exp) {
      LOG.warn("Failed to do immediate assignment.", exp);
    }
    return null;
  }

  /**
   * Populates regionMap and serverMap so that regions and servers of the same group are together.
   *
   * @param regions        a list of all the regions
   * @param servers a list of all the servers
   * @param regionMap      a mapping of group names as a string to the regions it contains
   * @param serverMap      a mapping of group names as a string to the servers it contains
   */
  private void generateGroupMaps(List<HRegionInfo> regions, List<ServerName> servers,
      ListMultimap<String, HRegionInfo> regionMap, ListMultimap<String, ServerName> serverMap) {
    try {
      // put all regions in regionMap
      for (HRegionInfo region : regions) {
        GroupInfo groupInfo = groupInfoManager.getGroupOfTable(region.getTable());
        String groupName = (groupInfo == null) ? null : groupInfo.getName();
        // if a table doesn't belong to a group put it in the default group
        if (groupName == null) {
          groupName = groupInfoManager.getDefaultGroupName();
          groupInfoManager.getGroupInfo(groupName).addTable(region.getTable());
        }
        regionMap.put(groupName, region);
      }

      // put all servers in serverMap
      for (ServerName serverName : servers) {
        GroupInfo groupInfo = groupInfoManager.getGroupOfServer(serverName.getHostAndPort());
        String groupName = (groupInfo == null) ? null : groupInfo.getName();
        // if a table doesn't belong in a group put it in the default group
        if (groupName == null) {
          groupName = groupInfoManager.getDefaultGroupName();
          groupInfoManager.getGroupInfo(groupName).addServer(serverName.getHostAndPort());
        }
        serverMap.put(groupName, serverName);
      }
    } catch (IOException exp) {
      LOG.warn("Failed to generate group maps.", exp);
    }
  }

  private List<HRegionInfo> getMisplacedRegions(Map<HRegionInfo, ServerName> regions)
      throws IOException {
    List<HRegionInfo> misplacedRegions = new ArrayList<>();
    for (HRegionInfo region : regions.keySet()) {
      ServerName assignedServer = regions.get(region);
      GroupInfo groupInfo = groupInfoManager.getGroupOfTable(region.getTable());
      if (groupInfo == null) {
        String defaultGroupName = groupInfoManager.getDefaultGroupName();
        GroupInfo defaultGroupInfo = groupInfoManager.getGroupInfo(defaultGroupName);
        defaultGroupInfo.addTable(region.getTable());
        groupInfo = defaultGroupInfo;
      }
      if (!groupInfo.containsServer(assignedServer.getHostAndPort())) {
        LOG.warn("Found misplaced region: " + region.getRegionNameAsString() + " on server: "
            + assignedServer + " found in group: " + groupInfoManager
            .getGroupOfServer(assignedServer.getHostAndPort()) + " outside of group " +
            groupInfo.getName());
        misplacedRegions.add(region);
      }
    }
    return misplacedRegions;
  }

  /**
   * If a region is assigned to a server in the wrong group, unassign it so it is assigned to a
   * a server in he right one.
   *
   * @param existingAssignments a map of servers and the regions that it holds
   * @return a map of servers and regions within it that belongs to the same group
   */
  private Map<ServerName, List<HRegionInfo>> correctAssignments(
      Map<ServerName, List<HRegionInfo>> existingAssignments) {
    Map<ServerName, List<HRegionInfo>> correctAssignments = new TreeMap<>();
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : existingAssignments.entrySet()) {
      ServerName serverName = entry.getKey();
      List<HRegionInfo> regions = entry.getValue();
      correctAssignments.put(serverName, new LinkedList<HRegionInfo>());
      for (HRegionInfo region : regions) {
        GroupInfo groupRegionShouldBelongsTo = null;
        try {
          groupRegionShouldBelongsTo = groupInfoManager.getGroupOfTable(region.getTable());
        } catch (Exception e) {
          LOG.debug("Error getting information for region " + region, e);
        }
        GroupInfo groupRegionActuallyBelongsTo = null;
        try {
          groupRegionActuallyBelongsTo =
              groupInfoManager.getGroupOfServer(serverName.getHostAndPort());
        } catch (Exception e) {
          LOG.debug("Error getting information for server " + serverName, e);
        }
        if (groupRegionActuallyBelongsTo != groupRegionShouldBelongsTo) {
          // unassign it so it is assigned to a server in the right group
          this.masterServices.getAssignmentManager().unassign(region);
        } else {
          correctAssignments.get(serverName).add(region);
        }
      }
    }
    return correctAssignments;
  }

  private List<ServerName> filterServersForGroupInfo(GroupInfo groupInfo,
      List<ServerName> servers) {
    List<ServerName> serversForGroupInfo = new ArrayList<>();
    for (ServerName serverName : servers) {
      try {
        if (groupInfoManager.getGroupOfServer(serverName.getHostAndPort()) == groupInfo) {
          serversForGroupInfo.add(serverName);
        }
      } catch (IOException exp) {
        LOG.warn("Could not get group for server: " + serverName);
      }
    }
    return serversForGroupInfo;
  }
}