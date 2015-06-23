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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Client used for managing region server group information.
 */
@InterfaceAudience.Public
public class GroupAdminClient implements GroupAdmin {
  private RSGroupProtos.RSGroupService.BlockingInterface proxy;
	private static final Log LOG = LogFactory.getLog(GroupAdminClient.class);
  private int operationTimeout;
  private GroupSerDe serDe = new GroupSerDe();

  public GroupAdminClient(Configuration conf) throws IOException {
    proxy =
        RSGroupProtos.RSGroupService.newBlockingStub(new HBaseAdmin(conf).coprocessorService());
    operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
            HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  @Override
  public NavigableSet<TableName> listTablesOfGroup(String groupName) throws IOException {
    try {
      TreeSet<TableName> result = Sets.newTreeSet();
      List<HBaseProtos.TableName> tables =
          proxy.listTablesOfGroup(null,
              RSGroupProtos.ListTablesOfGroupRequest.newBuilder().setGroupName(groupName).build())
                  .getTableNameList();
      for(HBaseProtos.TableName entry: tables) {
        result.add(ProtobufUtil.toTableName(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
    try {
      RSGroupProtos.GetGroupInfoResponse resp =
        proxy.getGroupInfo(null,
                  RSGroupProtos.GetGroupInfoRequest.newBuilder().setGroupName(groupName).build());
      if(resp.hasGroupInfo()) {
        return serDe.toPojo(resp.getGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public GroupInfo getGroupInfoOfTable(TableName tableName) throws IOException {
    RSGroupProtos.GetGroupInfoOfTableRequest request =
        RSGroupProtos.GetGroupInfoOfTableRequest.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();

    try {
      return serDe.toPojo(proxy.getGroupInfoOfTable(null, request).getGroupInfo());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void moveServers(Set<String> servers, String targetGroup) throws IOException {
    RSGroupProtos.MoveServersRequest request =
        RSGroupProtos.MoveServersRequest.newBuilder()
            .setTargetGroup(targetGroup)
            .addAllServers(servers).build();

    try {
      proxy.moveServers(null, request);
      waitForTransitions(servers);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    RSGroupProtos.MoveTablesRequest.Builder builder =
        RSGroupProtos.MoveTablesRequest.newBuilder()
            .setTargetGroup(targetGroup);
    for(TableName tableName: tables) {
      builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    try {
      proxy.moveTables(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void addGroup(String groupName) throws IOException {
    RSGroupProtos.AddGroupRequest request =
        RSGroupProtos.AddGroupRequest.newBuilder()
            .setGroupName(groupName).build();
    try {
      proxy.addGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void removeGroup(String name) throws IOException {
    RSGroupProtos.RemoveGroupRequest request =
        RSGroupProtos.RemoveGroupRequest.newBuilder()
            .setGroupName(name).build();
    try {
      proxy.removeGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public boolean balanceGroup(String name) throws IOException {
    RSGroupProtos.BalanceGroupRequest request =
        RSGroupProtos.BalanceGroupRequest.newBuilder()
            .setGroupName(name).build();

    try {
      return proxy.balanceGroup(null, request).getBalanceRan();
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    try {
      List<RSGroupProtos.GroupInfo> resp =
          proxy.listGroups(null, RSGroupProtos.ListGroupsRequest.newBuilder().build())
              .getGroupInfoList();
      List<GroupInfo> result = new ArrayList<GroupInfo>(resp.size());
      for(RSGroupProtos.GroupInfo entry: resp) {
        result.add(serDe.toPojo(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    RSGroupProtos.GetGroupOfServerRequest request =
        RSGroupProtos.GetGroupOfServerRequest.newBuilder()
            .setServer(hostPort).build();
    try {
      return serDe.toPojo(
          proxy.getGroupOfServer(null, request).getGroupInfo());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public Map<String, String> listServersInTransition() throws IOException {
    try {
      List<HBaseProtos.NameStringPair> resp =
          proxy.listServersInTransition(null, RSGroupProtos.ListServersInTransitionRequest
          .newBuilder().build()).getTransitionsList();
      Map<String,String> result = Maps.newHashMap();
      for(HBaseProtos.NameStringPair entry: resp) {
        result.put(entry.getName(), entry.getValue());
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  private void waitForTransitions(Set<String> servers) throws IOException {
    long endTime = EnvironmentEdgeManager.getDelegate().currentTime()+operationTimeout;
    boolean found;
    do {
      found = false;
      for(String server: listServersInTransition().keySet()) {
        found = found || servers.contains(server);
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.debug("Sleep interrupted", e);

      }
    } while(found && EnvironmentEdgeManager.getDelegate().currentTime() <= endTime);
    if (found) {
      throw new DoNotRetryIOException("Timed out while Waiting for server transition to finish.");
    }
  }
}
