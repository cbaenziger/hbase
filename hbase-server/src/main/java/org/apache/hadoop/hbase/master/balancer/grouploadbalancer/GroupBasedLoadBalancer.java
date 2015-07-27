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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
}