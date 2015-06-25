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

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;

/**
 * This load balancer partitions servers and tables into groups. Then, within each group, it uses
 * another load balancer to balance within each group.
 *
 * The configuration for the groups is set within the hbase-site.xml file. Within hbase-site.xml:
 *
 * "hbase.master.loadbalancer.class" needs to be set to
 * "org.apache.hadoop.hbase.master.balancer.grouploadbalancer.GroupLoadBalancer" for this load
 * balancer to work.
 *
 * "hbase.master.balancer.grouploadbalancer.groups" configures the names of the groups, separated
 * with a ";" eg. "group1;group2" creates two groups, named group1 and group2.
 *
 * "hbase.master.balancer.grouploadbalancer.defaultgroup" configures the name of the default group.
 * Note that the defaultgroup must be a pre-existing group defined in
 * "hbase.master.balancer.grouploadbalancer.groups". eg. "group1" sets group1 to be the default
 * group.
 *
 * To put servers in groups, you need to create a property named
 * "hbase.master.balancer.grouploadbalancer.servergroups." + groupName, and set it's value to the
 * IP address and port of the server in a comma separated list.
 * Note that the IP address and the port over the server is separated by a ",", not a ":".
 * eg. "hbase.master.balancer.grouploadbalancer.servergroups.group1" with a value
 * "10.255.196.145,60020;10.255.196.145,60021" will put two servers in group1.
 *
 * To put tables in groups, you need to create a property named
 * "hbase.master.balancer.grouploadbalancer.tablegroups." + groupName, and set it's value to the
 * name of the table in a comma separated list. You must specify it's namespace in here also.
 * eg. "hbase.master.balancer.grouploadbalancer.tablegroups.group1" with a value
 * "my_ns:namespace_table;test" will put two servers in group1. Note that "namespace_table" is under
 * the namespace "my_ns".
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class GroupBasedLoadBalancer extends BaseLoadBalancer {

  private static final Log LOG = LogFactory.getLog(GroupLoadBalancer.class);

  private GroupInfoManagerImpl groupInfoManager;
  private LoadBalancer internalBalancer;
  private MasterServices masterServices;

  @Override
  public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    this.groupInfoManager = new GroupInfoManagerImpl(conf);
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterMap) {

    LOG.info("**************** USING GROUP LOAD BALANCER *******************");

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
      List<HRegionInfo> hriList = entry.getValue();

      String defaultGroupName = this.groupInfoManager.getDefaultGroupName();

      try {
        if (this.groupInfoManager.getGroupOfServer(serverName.getHostAndPort()) == null) {
          this.groupInfoManager.getGroup(defaultGroupName).addServer(serverName.getHostAndPort());
        }
      } catch (Exception e) {
        LOG.debug("Error putting server " + serverName.getHostAndPort() + " in the default group.");
      }

      for (HRegionInfo hri : hriList) {
        try {
          if (this.groupInfoManager.getGroupOfTable(hri.getTable()) == null) {
            this.groupInfoManager.getGroup(defaultGroupName).addTable(hri.getTable());
          }
        } catch (Exception e) {
          LOG.debug("Error putting region " + hri + " in the default group.");
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
          List<HRegionInfo> hriList = entry.getValue();
          if (this.groupInfoManager.getGroupOfServer(serverName.getHostAndPort())
              == currentGroupInfo) {
            groupClusterMap.put(serverName, new LinkedList<HRegionInfo>());
          }

          for (HRegionInfo hri : hriList) {
            if (this.groupInfoManager.getGroupOfTable(hri.getTable()) == currentGroupInfo) {
              groupClusterMap.get(serverName).add(hri);
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

  @Override
  public void initialize() throws HBaseIOException {
    this.internalBalancer = new SimpleLoadBalancer();
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  /**
   * If a region is assigned to a server in the wrong group, unassign it so it is assigned to a
   * a server in the right one.
   * @param existingAssignments a map of servers and the regions that it holds
   * @return a map of servers and regions within it that belongs to the same group
   */
  private Map<ServerName, List<HRegionInfo>> correctAssignments(
      Map<ServerName, List<HRegionInfo>> existingAssignments) {

    Map<ServerName, List<HRegionInfo>> correctAssignments = new TreeMap<>();

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : existingAssignments.entrySet()) {

      ServerName serverName = entry.getKey();
      List<HRegionInfo> hriList = entry.getValue();

      correctAssignments.put(serverName, new LinkedList<HRegionInfo>());

      for (HRegionInfo hri : hriList) {

        GroupInfo groupRegionShouldBelongsTo = null;
        try {
          groupRegionShouldBelongsTo = groupInfoManager.getGroupOfTable(hri.getTable());
        } catch (Exception e) {
          LOG.debug("Error getting information for region " + hri, e);
        }
        GroupInfo groupRegionActuallyBelongsTo = null;
        try {
          groupRegionActuallyBelongsTo = groupInfoManager.getGroupOfServer(serverName.getHostAndPort());
        } catch (Exception e) {
          LOG.debug("Error getting information for server " + serverName, e);
        }

        if (groupRegionActuallyBelongsTo != groupRegionShouldBelongsTo) {
          // unassign it so it is assigned to a server in the right group
          this.masterServices.getAssignmentManager().unassign(hri);
        } else {
          correctAssignments.get(serverName).add(hri);
        }

      }
    }

    return correctAssignments;

  }

}
