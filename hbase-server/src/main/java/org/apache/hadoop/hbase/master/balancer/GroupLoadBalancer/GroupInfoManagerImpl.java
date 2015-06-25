/**
 * Copyright The Apache Software Foundation
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

import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.ServerListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is an implementation of {@link GroupInfoManager}. Which makes
 * use of an HBase table as the persistence store for the group information.
 * It also makes use of zookeeper to store group information needed
 * for bootstrapping during offline mode.
 */
public class GroupInfoManagerImpl implements GroupInfoManager, ServerListener {
  private static final Log LOG = LogFactory.getLog(GroupInfoManagerImpl.class);

  private static final String GROUPS = "hbase.master.balancer.grouploadbalancer.groups";
  private static final String DEFAULT_GROUP =
      "hbase.master.balancer.grouploadbalancer.defaultgroup";
  private static final String SERVER_GROUPS_PREFIX =
      "hbase.master.balancer.grouploadbalancer.servergroups.";
  private static final String TABLE_GROUPS_PREFIX =
      "hbase.master.balancer.grouploadbalancer.tablegroups.";

  private static final String GROUP_DELIMITER = ";";

  // Access to this map should always be synchronized.
  private volatile Map<String, GroupInfo> groupMap;
  private String defaultGroupName;

  public GroupInfoManagerImpl(Configuration conf) {
    this.groupMap = new HashMap<>();
    populateGroupInfoManagerFromConf(conf);
  }

  /**
   * Adds a group.
   *
   * @param groupInfo the group name
   * @throws IOException
   */
  @Override public synchronized void addGroup(GroupInfo groupInfo) {
    if (groupMap.containsKey(groupInfo.getName())) {
      throw new IllegalArgumentException("Group already exists: " + groupInfo.getName());
    }
    groupMap.put(groupInfo.getName(), groupInfo);
  }

  /**
   * This method is not supported in this implementation.
   */
  @Override
  public synchronized boolean moveServers(Set<String> hostPorts, String srcGroup, String dstGroup)
      throws IOException {
    throw new IllegalArgumentException("This method should not be called");
  }

  /**
   * Gets the group info of a server.
   *
   * @param hostPort the IP address and port of a server
   * @return GroupInfo object of the group that the server belongs to
   * @throws IOException
   */
  @Override public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    for (GroupInfo groupInfo : groupMap.values()) {
      if (groupInfo.containsServer(hostPort)) {
        return groupInfo;
      }
    }
    return null;
  }

  /**
   * Gets the information of the group.
   *
   * @param groupName the group name
   * @return An instance of GroupInfo
   * @throws IOException
   */
  @Override public GroupInfo getGroup(String groupName) throws IOException {
    GroupInfo groupInfo = groupMap.get(groupName);
    return groupInfo;
  }

  /**
   * Gets the group info of a table.
   *
   * @param tableName the name of the table
   * @return GroupInfo object of the group that the table belongs to
   * @throws IOException
   */
  @Override public GroupInfo getGroupOfTable(TableName tableName) throws IOException {
    for (GroupInfo groupInfo : this.groupMap.values()) {
      if (groupInfo.containsTable(tableName)) {
        return groupInfo;
      }
    }
    return null;
  }

  /**
   * This method is not supported in this implementation.
   */
  @Override public synchronized void moveTables(Set<TableName> tableNames, String groupName)
      throws IOException {
    throw new IllegalArgumentException("This method should not be called.");
  }

  /**
   * This method is not supported in this implementation.
   */
  @Override public synchronized void removeGroup(String groupName) throws IOException {
    throw new IllegalArgumentException("This method should not be called.");
  }

  /**
   * Gets information about all the groups.
   *
   * @return a list of GroupInfo
   * @throws IOException
   */
  @Override public List<GroupInfo> listGroups() throws IOException {
    List<GroupInfo> list = Lists.newLinkedList(groupMap.values());
    return list;
  }

  /**
   * This method is not support in this implementation.
   */
  @Override public boolean isOnline() {
    throw new IllegalArgumentException("This method should not be called.");
  }

  /**
   * This method is not support in this implementation.
   */
  @Override public synchronized void refresh() throws IOException {
    throw new IllegalArgumentException("This method should not be called.");
  }

  /**
   * This method is not support in this implementation.
   */
  @Override public void serverAdded(ServerName serverName) {
  }

  /**
   * This method is not support in this implementation.
   */
  @Override public void serverRemoved(ServerName serverName) {
    throw new IllegalArgumentException("This method should not be called.");
  }

  public String getDefaultGroupName() {
    return this.defaultGroupName;
  }

  /**
   * Populates this object from data in hbase-site.xml
   * @param config the configuration file
   * @throws IOException
   */
  private void populateGroupInfoManagerFromConf(Configuration config) {

    String groupNamesString = config.get(GROUPS);
    String[] groupNamesArray = groupNamesString.split(GROUP_DELIMITER);

    // Build group configurations
    for (String groupName : groupNamesArray) {

      if (groupName.length() < 1) {
        throw new IllegalArgumentException("Group name cannot be null.");
      }

      if (this.groupMap.containsKey(groupName)) {
        throw new IllegalArgumentException("Group name already exists.");
      }

      GroupInfo groupInfo = new GroupInfo(groupName);

      String serverConfig = config.get(SERVER_GROUPS_PREFIX + groupName);
      String tableConfig = config.get(TABLE_GROUPS_PREFIX + groupName);

      if (serverConfig == null) {
        throw new IllegalArgumentException("No servers defined for the group: " + groupName);
      }
      if (tableConfig == null) {
        throw new IllegalArgumentException("No tables defined for the group: " + groupName);
      }

      String[] serversArray = serverConfig.split(GROUP_DELIMITER);
      String[] tablesArray = tableConfig.split(GROUP_DELIMITER);

      for (String serverNameString : serversArray) {
        groupInfo.addServer(serverNameString);
      }

      for (String tableNameString : tablesArray) {
        groupInfo.addTable(TableName.valueOf(tableNameString));
      }

      addGroup(groupInfo);

    }

    this.defaultGroupName = config.get(DEFAULT_GROUP);
    if (this.defaultGroupName == null) {
      throw new IllegalArgumentException("Default group name cannot be null.");
    }
    if (!this.groupMap.containsKey(this.defaultGroupName)) {
      throw new IllegalArgumentException("Default group name must be a pre-existing group name.");
    }
  }

}

