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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

//TODO do better encapsulation of SerDe logic from GroupInfoManager and GroupTracker
class GroupSerDe {
  private static final Log LOG = LogFactory.getLog(GroupSerDe.class);

  public GroupSerDe() {

  }

  public List<GroupInfo> retrieveGroupList(HTable groupTable) throws IOException {
    List<GroupInfo> groupInfoList = Lists.newArrayList();
    Result result = groupTable.get(new Get(GroupInfoManager.ROW_KEY));
    if(!result.isEmpty()) {
      NavigableMap<byte[],NavigableMap<byte[],byte[]>> dataMap = result.getNoVersionMap();
      for(byte[] groupName: dataMap.get(GroupInfoManager.META_FAMILY_BYTES).keySet()) {
        RSGroupProtos.GroupInfo proto =
            RSGroupProtos.GroupInfo.parseFrom(
                dataMap.get(GroupInfoManager.META_FAMILY_BYTES).get(groupName));
        groupInfoList.add(toPojo(proto));
      }
    }
    return groupInfoList;
  }

  public List<GroupInfo> retrieveGroupList(ZooKeeperWatcher watcher,
                                           String groupBasePath) throws IOException {
    List<GroupInfo> groupInfoList = Lists.newArrayList();
    //Overwrite any info stored by table, this takes precedence
    try {
      if(ZKUtil.checkExists(watcher, groupBasePath) != -1) {
        for(String znode: ZKUtil.listChildrenAndWatchForNewChildren(watcher, groupBasePath)) {
          byte[] data = ZKUtil.getData(watcher, ZKUtil.joinZNode(groupBasePath, znode));
          if(data.length > 0) {
            ProtobufUtil.expectPBMagicPrefix(data);
            ByteArrayInputStream bis = new ByteArrayInputStream(
                data, ProtobufUtil.lengthOfPBMagic(), data.length);
            groupInfoList.add(toPojo(RSGroupProtos.GroupInfo.parseFrom(bis)));
          }
        }
        LOG.debug("Read ZK GroupInfo count:" + groupInfoList.size());
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to read groupZNode",e);
    } catch (DeserializationException e) {
      throw new IOException("Failed to read groupZNode",e);
    } catch (InterruptedException e) {
      throw new IOException("Failed to read groupZNode",e);
    }
    return groupInfoList;
  }

  public GroupInfo toPojo(RSGroupProtos.GroupInfo proto) {
    GroupInfo groupInfo = new GroupInfo(proto.getName());
    groupInfo.addAllServers(proto.getServersList());
    for(HBaseProtos.TableName pTableName: proto.getTablesList()) {
      groupInfo.addTable(ProtobufUtil.toTableName(pTableName));
    }
    return groupInfo;
  }

  public RSGroupProtos.GroupInfo toProto(GroupInfo pojo) {
    List<HBaseProtos.TableName> tables =
        new ArrayList<HBaseProtos.TableName>(pojo.getTables().size());
    for(TableName arg: pojo.getTables()) {
      tables.add(ProtobufUtil.toProtoTableName(arg));
    }
    return RSGroupProtos.GroupInfo.newBuilder().setName(pojo.getName())
        .addAllServers(pojo.getServers())
        .addAllTables(tables).build();
  }
}
