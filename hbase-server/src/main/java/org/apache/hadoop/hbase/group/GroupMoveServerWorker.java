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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.MasterServices;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a worker class responsible for moving a set of servers
 * from source group to target group. Supplied servers must be part
 * of the source group.
 *
 * Servers are then moved to a temporary transition group. Any
 * regions are then unassigned from the server. Once the servers
 * are drained of any regions the servers are then moved to the
 * destination group.
 */
public class GroupMoveServerWorker implements Runnable {
	private static final Log LOG = LogFactory.getLog(GroupMoveServerWorker.class);

  private MasterServices master;
  private MoveServerPlan plan;
  private String transGroup;
  private String sourceGroup;
  private GroupInfoManager groupManager;
  private Map<String,String> serversInTransition;
  private volatile boolean succeeded;

  public GroupMoveServerWorker(Server master, Map<String, String> serversInTransition,
                               GroupInfoManager groupManager,
                               MoveServerPlan plan) throws IOException {
    this.serversInTransition = serversInTransition;
    this.groupManager = groupManager;
    this.master = (MasterServices)master;
    this.plan = plan;

    synchronized (serversInTransition) {
      //check server list
      sourceGroup = groupManager.getGroupOfServer(plan.getServers().iterator().next()).getName();
      if(groupManager.getGroup(plan.getTargetGroup()) == null) {
        throw new ConstraintException("Target group does not exist: "+plan.getTargetGroup());
      }
      for(String server: plan.getServers()) {
        if (serversInTransition.containsKey(server)) {
          throw new DoNotRetryIOException(
              "Server list contains a server that is already being moved: "+server);
        }
        String tmpGroup = groupManager.getGroupOfServer(server).getName();
        if (sourceGroup != null && !tmpGroup.equals(sourceGroup)) {
          throw new DoNotRetryIOException(
              "Move server request should only come from one source group. "+
              "Expecting only "+sourceGroup+" but contains "+tmpGroup);
        }
      }
      if(sourceGroup.equals(plan.getTargetGroup())) {
        throw new ConstraintException(
            "Target group is the same as source group: "+plan.getTargetGroup());
      }
      //update the servers as in transition
      for(String server: plan.getServers()) {
        serversInTransition.put(server, plan.getTargetGroup());
      }
      if (!sourceGroup.startsWith(GroupInfo.TRANSITION_GROUP_PREFIX)) {
        transGroup = GroupInfo.TRANSITION_GROUP_PREFIX+
            System.currentTimeMillis()+"_"+sourceGroup+"-"+plan.getTargetGroup();
        groupManager.addGroup(new GroupInfo(transGroup));
      }
      groupManager.moveServers(plan.getServers(), sourceGroup,
          transGroup!=null?transGroup:plan.getTargetGroup());
    }
  }

  @Override
  public void run() {
    String name = "GroupMoveServer-"+transGroup+"-"+plan.getTargetGroup();
    Thread.currentThread().setName(name);
    try {
      boolean found;
      do {
        LOG.debug(name+" is awake");
        found = false;
        for(String rs: plan.getServers()) {
          List<HRegionInfo> regions = getOnlineRegions(rs);
          LOG.info("Unassigining "+regions.size()+" regions from server "+rs);
          if(regions.size() > 0) {
            //TODO bulk unassign
            for(HRegionInfo region: regions) {
              master.getAssignmentManager().unassign(region);
            }
            found = true;
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted", e);
        }
      } while(found);
      succeeded = true;
      LOG.info("Move server done: "+sourceGroup+"->"+plan.getTargetGroup());
    } catch(Exception e) {
      succeeded = false;
      LOG.error("Caught exception while running", e);
    }
    try {
      complete();
    } catch (IOException e) {
      succeeded = false;
      LOG.error("Failed to complete move", e);
    }
  }

  private List<HRegionInfo> getOnlineRegions(String hostPort) throws IOException {
    List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for(Map.Entry<HRegionInfo, ServerName> el:
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
      if (el.getValue().getHostAndPort().equals(hostPort)) {
        regions.add(el.getKey());
      }
    }
    return regions;
  }

  static class MoveServerPlan {
    private Set<String> servers;
    private String targetGroup;

    public MoveServerPlan(Set<String> servers, String targetGroup) {
      this.servers = servers;
      this.targetGroup = targetGroup;
    }

    public Set<String> getServers() {
      return servers;
    }

    public String getTargetGroup() {
      return targetGroup;
    }
  }

  public void complete() throws IOException {
    try {
      String tmpSourceGroup = sourceGroup;
      if (transGroup != null) {
        tmpSourceGroup = transGroup;
        LOG.debug("Moving "+plan.getServers().size()+
            " servers from transition group: "+transGroup+" to final group: "+plan.getTargetGroup());
      }
      if (succeeded) {
        groupManager.moveServers(plan.getServers(), tmpSourceGroup, plan.getTargetGroup());
        if (transGroup != null) {
          groupManager.removeGroup(transGroup);
          LOG.debug("Move done "+plan.getServers().size()+
              " servers from transition group: "+transGroup+" to final group: "+plan.getTargetGroup());
        }
        LOG.debug("Move done "+plan.getServers().size()+
            " servers from source group: "+sourceGroup+" to final group: "+plan.getTargetGroup());
      } else {
        //rollback
        groupManager.moveServers(plan.getServers(), tmpSourceGroup, sourceGroup);
        if (transGroup != null) {
          groupManager.removeGroup(transGroup);
          LOG.debug("Rollback done "+plan.getServers().size()+
              " servers from transition group: "+transGroup+" to old group: "+sourceGroup);
        }
      }
    } finally {
      //remove servers in transition
      synchronized(serversInTransition) {
        for(String server: plan.getServers()) {
          serversInTransition.remove(server);
        }
      }
    }
  }
}
