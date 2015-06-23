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

import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public interface MXBean {

  public Map<String,List<String>> getServersByGroup() throws IOException;

  public List<GroupInfoBean> getGroups() throws IOException;

  public Map<String,String> getServersInTransition() throws IOException;

  public static class GroupInfoBean {

    private String name;
    private List<String> servers;
    private List<String> tables;

    //Need this to convert NavigableSet to List
    public GroupInfoBean(GroupInfo groupInfo) {
      this.name = groupInfo.getName();
      this.servers = new LinkedList<String>();
      this.servers.addAll(groupInfo.getServers());
      this.tables = new LinkedList<String>();
      for(TableName tableName: groupInfo.getTables()) {
        this.tables.add(tableName.getNameAsString());
      }
    }

    public String getName() {
      return name;
    }

    public List<String> getServers() {
      return servers;
    }

    public List<String> getTables() {
      return tables;
    }
  }

}
