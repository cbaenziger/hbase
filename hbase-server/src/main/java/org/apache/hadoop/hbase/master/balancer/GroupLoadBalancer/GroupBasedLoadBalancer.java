/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer.grouploadbalancer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;

/**
 * This load balancer partitions servers and tables into groups. Then, within each group, it uses
 * another load balancer to balance within each group.
 * The configuration for the groups is set within the hbase-site.xml file. Within hbase-site.xml:
 * "hbase.master.loadbalancer.class" needs to be set to
 * "org.apache.hadoop.hbase.master.balancer.grouploadbalancer.GroupLoadBalancer" for this load
 * balancer to work.
 * "hbase.master.balancer.grouploadbalancer.groups" configures the names of the groups, separated
 * with a ";" eg. "group1;group2" creates two groups, named group1 and group2.
 * "hbase.master.balancer.grouploadbalancer.defaultgroup" configures the name of the default group.
 * Note that the defaultgroup must be a pre-existing group defined in
 * "hbase.master.balancer.grouploadbalancer.groups". eg. "group1" sets group1 to be the default
 * group.
 * To put servers in groups, you need to create a property named
 * "hbase.master.balancer.grouploadbalancer.servergroups." + groupName, and set it's value to the
 * IP address and port of the server in a comma separated list.
 * Note that the IP address and the port over the server is separated by a ",", not a ":".
 * eg. "hbase.master.balancer.grouploadbalancer.servergroups.group1" with a value
 * "10.255.196.145,60020;10.255.196.145,60021" will put two servers in group1.
 * To put tables in groups, you need to create a property named
 * "hbase.master.balancer.grouploadbalancer.tablegroups." + groupName, and set it's value to the
 * name of the table in a comma separated list. You must specify it's namespace in here also.
 * eg. "hbase.master.balancer.grouploadbalancer.tablegroups.group1" with a value
 * "my_ns:namespace_table;test" will put two servers in group1. Note that "namespace_table" is under
 * the namespace "my_ns".
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG) public class GroupBasedLoadBalancer
    extends BaseLoadBalancer {

  private static final Log LOG = LogFactory.getLog(GroupBasedLoadBalancer.class);

  private GroupInfoManagerImpl groupInfoManager;
  private LoadBalancer internalBalancer;
  private MasterServices masterServices;

  @Override public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }

  @Override public Configuration getConf() {
    return this.config;
  }

  @Override public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    this.groupInfoManager = new GroupInfoManagerImpl(conf);
  }

  @Override public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterMap) {

    LOG.info("**************** using group based load balancer *******************");

    // don't balance master
    if (masterServerName != null && clusterMap.containsKey(masterServerName)) {
      clusterMap = new HashMap<>(clusterMap);
      clusterMap.remove(masterServerName);
    }

    // see if master regions need to be balanced
    List<RegionPlan> regionsToReturn = balanceMasterRegions(clusterMap);
    if (regionsToReturn != null) {
      return regionsToReturn;
    }

    // we need to add all the servers and tables that were not placed in the default group
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterMap.entrySet()) {
      ServerName serverName = entry.getKey();
      List<HRegionInfo> regions = entry.getValue();

      String defaultGroupName = this.groupInfoManager.getDefaultGroupName();

      try {
        if (this.groupInfoManager.getGroupOfServer(serverName.getHostAndPort()) == null) {
          this.groupInfoManager.getGroup(defaultGroupName).addServer(serverName.getHostAndPort());
        }
      } catch (Exception e) {
        LOG.debug("Error putting server " + serverName.getHostAndPort() + " in the default group.");
      }

      for (HRegionInfo region : regions) {
        try {
          if (this.groupInfoManager.getGroupOfTable(region.getTable()) == null) {
            this.groupInfoManager.getGroup(defaultGroupName).addTable(region.getTable());
          }
        } catch (Exception e) {
          LOG.debug("Error putting region " + region + " in the default group.");
        }
      }
    }

    Map<ServerName, List<HRegionInfo>> correctedClusterMap = correctAssignments(clusterMap);
    regionsToReturn = new ArrayList<>();

    // Balance regions group by group
    try {
      for (GroupInfo currentGroupInfo : this.groupInfoManager.listGroups()) {
        Map<ServerName, List<HRegionInfo>> groupClusterMap = new HashMap<>();
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : correctedClusterMap.entrySet()) {
          ServerName serverName = entry.getKey();
          List<HRegionInfo> regions = entry.getValue();
          if (this.groupInfoManager.getGroupOfServer(serverName.getHostAndPort())
              == currentGroupInfo) {
            groupClusterMap.put(serverName, new LinkedList<HRegionInfo>());
          }

          for (HRegionInfo region : regions) {
            if (this.groupInfoManager.getGroupOfTable(region.getTable()) == currentGroupInfo) {
              groupClusterMap.get(serverName).add(region);
            }
          }
        }

        List<RegionPlan> groupRegionsToReturn =
            this.internalBalancer.balanceCluster(groupClusterMap);
        if (groupRegionsToReturn != null) {
          regionsToReturn.addAll(groupRegionsToReturn);
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception while balancing cluster.", e);
      regionsToReturn.clear();
    }

    return regionsToReturn;
  }

  @Override public void initialize() throws HBaseIOException {

    LOG.info("**************** initializing group based load balancer *******************");

    // TODO: Get internalBalan cer class from config
    this.internalBalancer = new SimpleLoadBalancer();
    this.internalBalancer.setClusterStatus(clusterStatus);
    this.internalBalancer.setMasterServices(masterServices);
    this.internalBalancer.setConf(config);
    this.internalBalancer.initialize();
    if (this.groupInfoManager == null) {
      try {
        this.groupInfoManager = new GroupInfoManagerImpl(this.config);
      } catch (Exception e) {
        throw new HBaseIOException("Failed to load group info manager.", e);
      }
    }
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {

    LOG.info("**************** group based load balancer roundRobinAssignment *******************");

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
    } catch (HBaseIOException e) {
      LOG.warn("Error with round robin assignments.", e);
    }
    return assignments;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
      List<ServerName> servers) {

    LOG.info("**************** group based load balancer retainAssignment *******************");

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
        GroupInfo groupInfo = groupInfoManager.getGroup(groupName);
        List<ServerName> candidateList = filterOfflineServers(groupInfo, servers);
        for (HRegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        assignments.putAll(
            this.internalBalancer.retainAssignment(currentAssignmentMap, candidateList));
      }

      for (HRegionInfo region : misplacedRegions) {
        String groupName = groupInfoManager.getGroupOfTable(region.getTable()).getName();
        GroupInfo groupInfo = groupInfoManager.getGroup(groupName);
        List<ServerName> candidateList = filterOfflineServers(groupInfo, servers);
        ServerName serverName = this.internalBalancer.randomAssignment(region, candidateList);
        if (serverName != null && !assignments.containsKey(serverName)) {
          assignments.put(serverName, new ArrayList<HRegionInfo>());
        } else if (serverName != null) {
          assignments.get(serverName).add(region);
        } else {
          // if no server is available to assign, assign it to a server in the default group
          NavigableSet<String> defaultServersString =
              groupInfoManager.getGroup(groupInfoManager.getDefaultGroupName()).getServers();
          List<ServerName> serverNameList = new ArrayList<>();
          while (defaultServersString.iterator().hasNext()) {
            serverNameList.add(ServerName.parseServerName(defaultServersString.iterator().next()));
          }
          ServerName randomServerFromDefaultGroup = this.internalBalancer.randomAssignment(region, serverNameList);
          if (!assignments.containsKey(randomServerFromDefaultGroup)) {
            assignments.put(randomServerFromDefaultGroup, new ArrayList<HRegionInfo>());
          }
        }
      }
      return assignments;
    } catch (Exception e) {
      LOG.warn("Failed to do retain assignment.", e);
    }

    return null;
  }

  @Override public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {

    LOG.info("**************** group based load balancer immediateAssignment *******************");

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
    } catch (Exception e) {
      LOG.warn("Failed to do immediate assignment.", e);
    }
    return null;
  }

  @Override
  public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {

    LOG.info("**************** group based load balancer randomAssignment *******************");
//    LOG.info("this.groupInfoManager " + this.groupInfoManager);
//
//    LOG.info("regionInfo " + regionInfo);
//    LOG.info("servers " + servers);

    if (servers != null && servers.contains(masterServerName)) {
      if (shouldBeOnMaster(regionInfo)) {
        return masterServerName;
      }
      servers = new ArrayList<>(servers);
      // Guarantee not to put other regions on master
      servers.remove(masterServerName);
    }

    try {
      ListMultimap<String, HRegionInfo> regionMap = LinkedListMultimap.create();
      ListMultimap<String, ServerName> serverMap = LinkedListMultimap.create();
      generateGroupMaps(Lists.newArrayList(regionInfo), servers, regionMap, serverMap);

      LOG.info("regionMap " + regionMap);
      LOG.info("serverMap " + serverMap);

      List<ServerName> filteredServers = serverMap.get(regionMap.keySet().iterator().next());
      return this.internalBalancer.randomAssignment(regionInfo, filteredServers);
    } catch(Exception e) {
      LOG.warn("Failed to do random assignment.", e);
    }
    return null;
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
        String groupName = (groupInfo == null)? null : groupInfo.getName();
        // if a table doesn't belong to a group put it in the default group
        if (groupName == null) {
          groupName = groupInfoManager.getDefaultGroupName();
          groupInfoManager.getGroup(groupName).addTable(region.getTable());
          LOG.info("The region " + region +
              " was not put in a group, so it was placed in the default group");
        }
        regionMap.put(groupName, region);
      }

      // put all servers in serverMap
      for (ServerName serverName : servers) {
        GroupInfo groupInfo = groupInfoManager.getGroupOfServer(serverName.getHostAndPort());
        String groupName = (groupInfo == null)? null : groupInfo.getName();
        // if a table doesn't belong in a group put it in the default group
        if (groupName == null) {
          groupName = groupInfoManager.getDefaultGroupName();
          groupInfoManager.getGroup(groupName).addServer(serverName.getHostAndPort());
          LOG.info("The server " + serverName +
              " was not put in a group, so it was placed in the default group");
        }
        serverMap.put(groupName, serverName);
      }
    } catch (IOException e) {
      LOG.warn("Failed to generate group maps.", e);
    }
  }

  private List<HRegionInfo> getMisplacedRegions(Map<HRegionInfo, ServerName> regions)
      throws IOException {

    List<HRegionInfo> misplacedRegions = new ArrayList<>();

    for (HRegionInfo region : regions.keySet()) {
      ServerName assignedServer = regions.get(region);
      GroupInfo groupInfo =
          groupInfoManager.getGroup(groupInfoManager.getGroupOfTable(region.getTable()).getName());

      if (assignedServer != null && groupInfo == null || !groupInfo
          .containsServer(assignedServer.getHostAndPort())) {
        LOG.warn("Found misplaced region: " + region.getRegionNameAsString() + " on server: "
            + assignedServer + " found in group: " + groupInfoManager
            .getGroupOfServer(assignedServer.getHostAndPort()) + " outside of group " +
            groupInfo.getName());
        misplacedRegions.add(region);
      }
    }
    return misplacedRegions;
  }

  private List<ServerName> filterOfflineServers(
      GroupInfo groupInfo, List<ServerName> servers) {

    if (groupInfo != null) {
      ArrayList<ServerName> onlineServers = new ArrayList<>();
      for (String server : groupInfo.getServers()) {
        for (ServerName serverName : servers) {
          if (ServerName.isSameHostnameAndPort(serverName, ServerName.parseServerName(server))) {
            onlineServers.add(serverName);
          }
        }
      }
      return onlineServers;
    }
    return Collections.EMPTY_LIST;
  }

}
