/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.master.balancer;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.grouploadbalancer.GroupBasedLoadBalancer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;

import java.util.*;

@Category({ MediumTests.class })
public class TestGroupBasedLoadBalancer extends BalancerTestBase {
  private static GroupBasedLoadBalancer loadBalancer;
  private static Log LOG = LogFactory.getLog(TestGroupBasedLoadBalancer.class);
  private static Configuration conf;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    loadBalancer = new GroupBasedLoadBalancer();
    MasterServices services = Mockito.mock(HMaster.class);
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    Mockito.when(services.getAssignmentManager()).thenReturn(am);
    loadBalancer.setMasterServices(services);
  }

  @Test
  public void testBalanceCluster() throws Exception {

    // Create a configuration where 1 table is not grouped
    conf = HBaseConfiguration.create();
    conf.set("hbase.master.balancer.grouploadbalancer.groups", "test_group1;test_group2");
    conf.set("hbase.master.balancer.grouploadbalancer.internalbalancerclass",
        "org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer");
    conf.set("hbase.master.balancer.grouploadbalancer.defaultgroup", "test_group1");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.test_group1",
        "10.255.196.145:60001;10.255.196.145:60011");
    conf.set("hbase.master.balancer.grouploadbalancer.servergroups.test_group2",
        "10.255.196.145:60002;10.255.196.145:60012");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.test_group1",
        "test_table_1;test_table_2;test_table_3;");
    conf.set("hbase.master.balancer.grouploadbalancer.tablegroups.test_group2",
        "test_table_5;test_table_6;test_table_7;test_table_8");
    loadBalancer.setConf(conf);
    loadBalancer.initialize();

    // Create four test region servers
    Random random = new Random();
    long randomTimeDelta = (long) random.nextInt(60000);
    long currentTimestamp = System.currentTimeMillis();
    ServerName serverName1 = ServerName.valueOf("10.255.196.145:60001", currentTimestamp);
    ServerName serverName2 =
        ServerName.valueOf("10.255.196.145:60011", currentTimestamp + randomTimeDelta);
    ServerName serverName3 =
        ServerName.valueOf("10.255.196.145:60002", currentTimestamp + randomTimeDelta * 2);
    ServerName serverName4 =
        ServerName.valueOf("10.255.196.145:60012", currentTimestamp + randomTimeDelta * 3);

    // Create 8 test tables
    TableName tableName1 = TableName.valueOf("test_table_1");
    TableName tableName2 = TableName.valueOf("test_table_2");
    TableName tableName3 = TableName.valueOf("test_table_3");
    TableName tableName4 = TableName.valueOf("test_table_4");
    TableName tableName5 = TableName.valueOf("test_table_5");
    TableName tableName6 = TableName.valueOf("test_table_6");
    TableName tableName7 = TableName.valueOf("test_table_7");
    TableName tableName8 = TableName.valueOf("test_table_8");
    HRegionInfo region1 = new HRegionInfo(tableName1);
    HRegionInfo region2 = new HRegionInfo(tableName2);
    HRegionInfo region3 = new HRegionInfo(tableName3);
    HRegionInfo region4 = new HRegionInfo(tableName4);
    HRegionInfo region5 = new HRegionInfo(tableName5);
    HRegionInfo region6 = new HRegionInfo(tableName6);
    HRegionInfo region7 = new HRegionInfo(tableName7);
    HRegionInfo region8 = new HRegionInfo(tableName8);

    // Create a cluster where 2 out of the 8 regions are on servers of the wrong group
    Map<ServerName, List<HRegionInfo>> testCluster = new HashMap<>();
    List<HRegionInfo> regionList1 = new ArrayList<>();
    List<HRegionInfo> regionList2 = new ArrayList<>();
    List<HRegionInfo> regionList3 = new ArrayList<>();
    List<HRegionInfo> regionList4 = new ArrayList<>();
    regionList1.add(region8);
    regionList1.add(region1);
    regionList1.add(region2);
    regionList1.add(region3);
    regionList3.add(region4);
    regionList3.add(region5);
    regionList3.add(region6);
    regionList3.add(region7);
    testCluster.put(serverName1, regionList1);
    testCluster.put(serverName2, regionList2);
    testCluster.put(serverName3, regionList3);
    testCluster.put(serverName4, regionList4);

    List<RegionPlan> regionPlanList = loadBalancer.balanceCluster(testCluster);

    // Sort the regions based on the source server
    Collections.sort(regionPlanList, new Comparator<RegionPlan>() {
      @Override public int compare(RegionPlan o1, RegionPlan o2) {
        return o1.getSource().compareTo(o2.getSource());
      }
    });

    // Each group should be moving exactly 1 region from the server with the smaller port number
    // to the server with the larger port number in the same group
    assertTrue(regionPlanList.size() == 2);
    assertTrue(regionPlanList.get(0).getSource().getPort() == 60001);
    assertTrue(regionPlanList.get(0).getDestination().getPort() == 60011);
    assertTrue(regionPlanList.get(1).getSource().getPort() == 60002);
    assertTrue(regionPlanList.get(1).getDestination().getPort() == 60012);

  }
}
