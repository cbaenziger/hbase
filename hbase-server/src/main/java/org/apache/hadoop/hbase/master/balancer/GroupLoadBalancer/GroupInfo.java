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

import java.io.Serializable;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Stores the group information of region server groups.
 */
public class GroupInfo implements Serializable {

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
   * Get the name of the group.
   *
   * @return the group name
   */
  public String getName() {
    return this.name;
  }

  /**
   * Adds the server to the group.
   *
   * @param hostPort the server
   */
  public void addServer(String hostPort){
    this.servers.add(hostPort);
  }

  /**
   * Adds a collection of servers to the group.
   *
   * @param hostPort the collection of servers
   */
  public void addAllServers(Collection<String> hostPort){
    this.servers.addAll(hostPort);
  }

  /**
   * Checks to see if the group contains the server.
   *
   * @param hostPort the IP address and port of server
   * @return true, if a server with hostPort is found
   */
  public boolean containsServer(String hostPort) {
    return this.servers.contains(hostPort);
  }

  /**
   * Get list of servers in the group.
   *
   * @return a list of servers
   */
  public NavigableSet<String> getServers() {
    return this.servers;
  }

  /**
   * Get all the tables in the group.
   *
   * @return a list of tables
   */
  public NavigableSet<TableName> getTables() {
    return this.tables;
  }

  /**
   * Add a table to the group.
   *
   * @param table the table
   */
  public void addTable(TableName table) {
    this.tables.add(table);
  }

  /**
   * Add a collection of tables to the group.
   *
   * @param tables the tables
   */
  public void addAllTables(Collection<TableName> tables) {
    this.tables.addAll(tables);
  }

  /**
   * Check to see if a group contains a table
   *
   * @param table the table
   * @return returns true if the group contains the table
   */
  public boolean containsTable(TableName table) {
    return this.tables.contains(table);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GroupName:");
    sb.append(this.name);
    sb.append(", ");
    sb.append(" Servers:");
    sb.append(this.servers);
    sb.append(" Tables: ");
    sb.append(this.tables);
    return sb.toString();

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GroupInfo groupInfo = (GroupInfo) o;

    return this.name.equals(groupInfo.name) &&
        this.servers.equals(groupInfo.servers) &&
        this.tables.equals(groupInfo.tables);
  }

  @Override
  public int hashCode() {
    int result = servers.hashCode();
    result = 31 * result + tables.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }

}