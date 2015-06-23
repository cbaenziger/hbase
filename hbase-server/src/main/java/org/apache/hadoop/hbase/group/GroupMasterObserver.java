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
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import java.io.IOException;

/**
 * This class is a required component to enable Region Server Groups.
 * It must be installed as a system coprocessor on the master.
 */
public class GroupMasterObserver extends BaseMasterObserver {
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(GroupMasterObserver.class);

  private MasterCoprocessorEnvironment menv;
  private GroupAdminEndpoint groupAdminEndpoint;
  private GroupAdmin groupAdmin;
  private MasterServices master;

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    menv = (MasterCoprocessorEnvironment)ctx;
    master = menv.getMasterServices();
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    boolean bypassSecurity = false;
    String groupName = desc.getValue(GroupInfo.TABLEDESC_PROP_GROUP);
    if(groupName == null) {
      groupName =
          master.getNamespaceDescriptor(desc.getTableName().getNamespaceAsString())
              .getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP);
      bypassSecurity = true;
    } else {
      //we remove the property since it is ephemeral
      desc.remove(GroupInfo.TABLEDESC_PROP_GROUP);
    }
    if(groupName == null) {
      bypassSecurity = true;
      groupName = GroupInfo.DEFAULT_GROUP;
    }
    GroupInfo groupInfo = getGroupAdmin().getGroupInfo(groupName);
    if(groupInfo == null) {
      throw new ConstraintException("Group "+groupName+" does not exist.");
    }
    if(!groupInfo.containsTable(desc.getTableName())) {
      //Bypass security check if group assignment is taken from namespace
      if(!bypassSecurity) {
        groupAdminEndpoint.requireAdmin("moveTables(preCreateTable)");
      }
      LOG.debug("Pre-moving table "+desc.getTableName()+" to group "+groupName);
      getGroupAdmin().moveTables(Sets.newHashSet(desc.getTableName()), groupName);
    }
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                TableName tableName) throws IOException {
    try {
      GroupInfo group = getGroupAdmin().getGroupInfoOfTable(tableName);
      if(group != null) {
        LOG.debug("Removing deleted table from table group "+group.getName());
        getGroupAdmin().moveTables(Sets.newHashSet(tableName), null);
      }
    } catch (ConstraintException ex) {
      LOG.debug("Failed to perform group information cleanup for table: "+tableName, ex);
    } catch (IOException ex) {
      LOG.debug("Failed to perform group information cleanup for table: "+tableName, ex);
    }
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
    if(!ns.getName().equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR) &&
       !ns.getName().equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR) &&
        ns.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP) == null) {
      throw new ConstraintException("Non-reserved namespaces must associate with a group");
    }
    String group = ns.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP);
    if(group != null && getGroupAdmin().getGroupInfo(group) == null) {
      throw new ConstraintException("Region server group "+group+" does not exit");
    }
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
    preCreateNamespace(ctx, ns);
    NamespaceDescriptor curr = master.getNamespaceDescriptor(ns.getName());
    if(curr.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP) != null &&
      !curr.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP)
          .equals(ns.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP))) {
      throw new ConstraintException("Region server group affiliation can only be set once.");
    }
  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, HBaseProtos.SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
    preCreateTable(ctx, hTableDescriptor, null);
  }

  private GroupAdminEndpoint getGroupAdminEndpoint() {
    if(groupAdminEndpoint == null) {
      groupAdminEndpoint = (GroupAdminEndpoint)
          menv.getMasterServices().getMasterCoprocessorHost()
              .findCoprocessor(GroupAdminEndpoint.class.getName());
    }
    return groupAdminEndpoint;
  }

  private GroupAdmin getGroupAdmin() {
    if(groupAdmin == null) {
      groupAdmin = getGroupAdminEndpoint().getGroupAdmin();
    }
    return groupAdmin;
  }

}
