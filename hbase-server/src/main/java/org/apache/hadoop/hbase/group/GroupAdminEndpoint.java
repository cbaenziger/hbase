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

import com.google.common.collect.Sets;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

/**
 * Service to support Region Server Grouping (HBase-6721)
 * This should be installed as a Master CoprocessorEndpoint
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GroupAdminEndpoint
    implements RSGroupProtos.RSGroupService.Interface, CoprocessorService, Coprocessor {
  private static final Log LOG = LogFactory.getLog(GroupAdminEndpoint.class);

  private AccessController accessController;
  private MasterCoprocessorEnvironment menv;
  private MasterServices master;
  private GroupAdmin groupAdmin;
  private GroupSerDe serDe;

  @Override
  public void start(CoprocessorEnvironment env) {
    menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
    groupAdmin = new GroupAdminImpl(master);
    serDe = new GroupSerDe();
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  @Override
  public Service getService() {
    return RSGroupProtos.RSGroupService.newReflectiveService(this);
  }

  //PB endpoints

  @Override
  public void listTablesOfGroup(RpcController controller, RSGroupProtos.ListTablesOfGroupRequest request, RpcCallback<RSGroupProtos.ListTablesOfGroupResponse> done) {
    RSGroupProtos.ListTablesOfGroupResponse response = null;
    try {
      RSGroupProtos.ListTablesOfGroupResponse.Builder builder =
          RSGroupProtos.ListTablesOfGroupResponse.newBuilder();
      NavigableSet<TableName> tables = groupAdmin.listTablesOfGroup(request.getGroupName());
      for(TableName tableName: tables) {
        builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void getGroupInfo(RpcController controller, RSGroupProtos.GetGroupInfoRequest request, RpcCallback<RSGroupProtos.GetGroupInfoResponse> done) {
    RSGroupProtos.GetGroupInfoResponse response = null;
    try {
      RSGroupProtos.GetGroupInfoResponse.Builder builder =
          RSGroupProtos.GetGroupInfoResponse.newBuilder();
      GroupInfo groupInfo = groupAdmin.getGroupInfo(request.getGroupName());
      if(groupInfo != null) {
        builder.setGroupInfo(serDe.toProto(groupInfo));
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void getGroupInfoOfTable(RpcController controller,
                                  RSGroupProtos.GetGroupInfoOfTableRequest request,
                                  RpcCallback<RSGroupProtos.GetGroupInfoOfTableResponse> done) {
    RSGroupProtos.GetGroupInfoOfTableResponse response = null;
    try {
      RSGroupProtos.GetGroupInfoOfTableResponse.Builder builder =
          RSGroupProtos.GetGroupInfoOfTableResponse.newBuilder();
      GroupInfo groupInfo = groupAdmin.getGroupInfoOfTable(ProtobufUtil.toTableName(request.getTableName()));
      response = builder.setGroupInfo(serDe.toProto(groupInfo)).build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void moveServers(RpcController controller, RSGroupProtos.MoveServersRequest request, RpcCallback<RSGroupProtos.MoveServersResponse> done) {
    RSGroupProtos.MoveServersResponse response = null;
    try {
      requireAdmin("moveServers");

      RSGroupProtos.MoveServersResponse.Builder builder =
          RSGroupProtos.MoveServersResponse.newBuilder();
      groupAdmin.moveServers(Sets.newHashSet(request.getServersList()), request.getTargetGroup());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void moveTables(RpcController controller, RSGroupProtos.MoveTablesRequest request, RpcCallback<RSGroupProtos.MoveTablesResponse> done) {
    RSGroupProtos.MoveTablesResponse response = null;
    try {
      requireAdmin("moveTables");

      RSGroupProtos.MoveTablesResponse.Builder builder =
          RSGroupProtos.MoveTablesResponse.newBuilder();
      Set<TableName> tables = new HashSet<TableName>(request.getTableNameList().size());
      for(HBaseProtos.TableName tableName: request.getTableNameList()) {
        tables.add(ProtobufUtil.toTableName(tableName));
      }
      groupAdmin.moveTables(tables, request.getTargetGroup());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void addGroup(RpcController controller, RSGroupProtos.AddGroupRequest request, RpcCallback<RSGroupProtos.AddGroupResponse> done) {
    RSGroupProtos.AddGroupResponse response = null;
    try {
      requireAdmin("addGroup");

      RSGroupProtos.AddGroupResponse.Builder builder =
          RSGroupProtos.AddGroupResponse.newBuilder();
      groupAdmin.addGroup(request.getGroupName());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void removeGroup(RpcController controller, RSGroupProtos.RemoveGroupRequest request, RpcCallback<RSGroupProtos.RemoveGroupResponse> done) {
    RSGroupProtos.RemoveGroupResponse response = null;
    try {
      requireAdmin("removeGroup");

      RSGroupProtos.RemoveGroupResponse.Builder builder =
          RSGroupProtos.RemoveGroupResponse.newBuilder();
      groupAdmin.removeGroup(request.getGroupName());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void balanceGroup(RpcController controller, RSGroupProtos.BalanceGroupRequest request, RpcCallback<RSGroupProtos.BalanceGroupResponse> done) {
    RSGroupProtos.BalanceGroupResponse response = null;
    try {
      requireAdmin("balanceGroup");

      RSGroupProtos.BalanceGroupResponse.Builder builder =
          RSGroupProtos.BalanceGroupResponse.newBuilder();
      builder.setBalanceRan(groupAdmin.balanceGroup(request.getGroupName()));
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void listGroups(RpcController controller, RSGroupProtos.ListGroupsRequest request, RpcCallback<RSGroupProtos.ListGroupsResponse> done) {
    RSGroupProtos.ListGroupsResponse response = null;
    try {
      RSGroupProtos.ListGroupsResponse.Builder builder =
          RSGroupProtos.ListGroupsResponse.newBuilder();
      for(GroupInfo groupInfo: groupAdmin.listGroups()) {
        builder.addGroupInfo(serDe.toProto(groupInfo));
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void getGroupOfServer(RpcController controller, RSGroupProtos.GetGroupOfServerRequest request, RpcCallback<RSGroupProtos.GetGroupOfServerResponse> done) {
    RSGroupProtos.GetGroupOfServerResponse response = null;
    try {
      RSGroupProtos.GetGroupOfServerResponse.Builder builder =
          RSGroupProtos.GetGroupOfServerResponse.newBuilder();
      GroupInfo groupInfo = groupAdmin.getGroupOfServer(request.getServer());
      response = builder.setGroupInfo(serDe.toProto(groupInfo)).build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void listServersInTransition(RpcController controller, RSGroupProtos.ListServersInTransitionRequest request, RpcCallback<RSGroupProtos.ListServersInTransitionResponse> done) {
    RSGroupProtos.ListServersInTransitionResponse response = null;
    try {
      RSGroupProtos.ListServersInTransitionResponse.Builder builder =
          RSGroupProtos.ListServersInTransitionResponse.newBuilder();
      for(Map.Entry<String,String> entry: groupAdmin.listServersInTransition().entrySet()) {
        builder.addTransitions(HBaseProtos.NameStringPair.newBuilder()
            .setName(entry.getKey())
            .setValue(entry.getValue()).build());
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  void requireAdmin(String method) throws IOException {
    if(getAccessController() != null) {
      getAccessController().requirePermission(method, AccessControlLists.ACL_TABLE_NAME, null, null,
          Permission.Action.ADMIN);
    }
  }

  AccessController getAccessController() {
    if(accessController == null) {
      accessController = (AccessController)menv.getMasterServices()
        .getMasterCoprocessorHost().findCoprocessor(AccessController.class.getName());
    }
    return accessController;
  }

  void setGroupAdmin(GroupAdmin groupAdmin) {
    this.groupAdmin = groupAdmin;
  }

  GroupAdmin getGroupAdmin() {
    return groupAdmin;
  }
}
