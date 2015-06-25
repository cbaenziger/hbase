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

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.TableName;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Stores the group information of region server groups.
 */
public class GroupInfo implements Serializable {

  public static final String DEFAULT_GROUP = "default";
  public static final String NAMESPACEDESC_PROP_GROUP = "hbase.rsgroup.name";
  public static final String TABLEDESC_PROP_GROUP = "hbase.rsgroup.name";
  public static final String TRANSITION_GROUP_PREFIX = "_transition_";

  private String name;
  private NavigableSet<String> servers;
  private NavigableSet<TableName> tables;

  public GroupInfo(String name) {
    this.name = name;
    this.servers = new TreeSet<>();
    this.tables = new TreeSet<>();
  }

  public GroupInfo(GroupInfo src) {
    name = src.getName();
    servers = Sets.newTreeSet(src.getServers());
    tables = Sets.newTreeSet(src.getTables());
  }

  /**
   * Get group name.
   *
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   * Adds the server to the group.
   *
   * @param hostPort the server
   */
  public void addServer(String hostPort){
    servers.add(hostPort);
  }

  /**
   * Adds a group of servers.
   *
   * @param hostPort the servers
   */
  public void addAllServers(Collection<String> hostPort){
    servers.addAll(hostPort);
  }

  /**
   * @param hostPort
   * @return true, if a server with hostPort is found
   */
  public boolean containsServer(String hostPort) {
    return servers.contains(hostPort);
  }

  /**
   * Get list of servers.
   *
   * @return
   */
  public NavigableSet<String> getServers() {
    return servers;
  }

  /**
   * Remove a server from this group.
   *
   * @param hostPort
   */
  public boolean removeServer(String hostPort) {
    return servers.remove(hostPort);
  }

  /**
   * Set of tables that are members of this group
   * @return
   */
  public NavigableSet<TableName> getTables() {
    return tables;
  }

  public void addTable(TableName table) {
    tables.add(table);
  }

  public void addAllTables(Collection<TableName> arg) {
    tables.addAll(arg);
  }

  public boolean containsTable(TableName table) {
    return tables.contains(table);
  }

  public boolean removeTable(TableName table) {
    return tables.remove(table);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("GroupName:");
    sb.append(this.name);
    sb.append(", ");
    sb.append(" Servers:");
    sb.append(this.servers);
    return sb.toString();

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GroupInfo groupInfo = (GroupInfo) o;

    if (!name.equals(groupInfo.name)) return false;
    if (!servers.equals(groupInfo.servers)) return false;
    if (!tables.equals(groupInfo.tables)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = servers.hashCode();
    result = 31 * result + tables.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }

}