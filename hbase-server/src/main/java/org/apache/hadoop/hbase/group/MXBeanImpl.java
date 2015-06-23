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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MXBeanImpl implements MXBean {
  private static final Log LOG = LogFactory.getLog(MXBeanImpl.class);

  private static MXBeanImpl instance = null;

  private GroupAdmin groupAdmin;
  private MasterServices master;

  public synchronized static MXBeanImpl init(
      final GroupAdmin groupAdmin,
      MasterServices master) {
    if (instance == null) {
      instance = new MXBeanImpl(groupAdmin, master);
    }
    return instance;
  }

  protected MXBeanImpl(final GroupAdmin groupAdmin,
      MasterServices master) {
    this.groupAdmin = groupAdmin;
    this.master = master;
  }

  @Override
  public Map<String, List<String>> getServersByGroup() throws IOException {
    Map<String, List<String>> data = new HashMap<String, List<String>>();
    for (final ServerName entry :
      master.getServerManager().getOnlineServersList()) {
      GroupInfo groupInfo = groupAdmin.getGroupOfServer(entry.getHostAndPort());
      if(!data.containsKey(groupInfo.getName())) {
        data.put(groupInfo.getName(), new LinkedList<String>());
      }
      data.get(groupInfo.getName()).add(entry.getHostAndPort());
    }
    return data;
  }

  @Override
  public List<GroupInfoBean> getGroups() throws IOException {
    LinkedList list = new LinkedList();
    for(GroupInfo group: groupAdmin.listGroups()) {
      list.add(new GroupInfoBean(group));
    }
    return list;
  }

  @Override
  public Map<String, String> getServersInTransition() throws IOException {
    return groupAdmin.listServersInTransition();
  }

}
