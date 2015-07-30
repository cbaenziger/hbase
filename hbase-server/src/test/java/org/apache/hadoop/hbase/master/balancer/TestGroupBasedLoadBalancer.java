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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertTrue;


@Category({ MediumTests.class })
public class TestGroupBasedLoadBalancer extends BalancerTestBase {
  private static GroupBasedLoadBalancer loadBalancer;
  private static Log LOG = LogFactory.getLog(TestGroupBasedLoadBalancer.class);
  private static Configuration conf;
  private static ServerName serverName1;
  private static ServerName serverName2;
  private static ServerName serverName3;
  private static ServerName serverName4;
  private static HRegionInfo region1;
  private static HRegionInfo region2;
  private static HRegionInfo region3;
  private static HRegionInfo region4;
  private static HRegionInfo region5;
  private static HRegionInfo region6;
  private static HRegionInfo region7;
  private static HRegionInfo region8;


  @BeforeClass
  public static void beforeAllTests() throws Exception {
    loadBalancer = new GroupBasedLoadBalancer();
    MasterServices services = Mockito.mock(HMaster.class);
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    Mockito.when(services.getAssignmentManager()).thenReturn(am);
    loadBalancer.setMasterServices(services);


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
    serverName1 = ServerName.valueOf("10.255.196.145:60001", currentTimestamp);
    serverName2 = ServerName.valueOf("10.255.196.145:60011", currentTimestamp + randomTimeDelta);
    serverName3 = ServerName.valueOf("10.255.196.145:60002", currentTimestamp + randomTimeDelta * 2);
    serverName4 = ServerName.valueOf("10.255.196.145:60012", currentTimestamp + randomTimeDelta * 3);

    // Create 8 test tables
    TableName tableName1 = TableName.valueOf("test_table_1");
    TableName tableName2 = TableName.valueOf("test_table_2");
    TableName tableName3 = TableName.valueOf("test_table_3");
    TableName tableName4 = TableName.valueOf("test_table_4");
    TableName tableName5 = TableName.valueOf("test_table_5");
    TableName tableName6 = TableName.valueOf("test_table_6");
    TableName tableName7 = TableName.valueOf("test_table_7");
    TableName tableName8 = TableName.valueOf("test_table_8");
    region1 = new HRegionInfo(tableName1);
    region2 = new HRegionInfo(tableName2);
    region3 = new HRegionInfo(tableName3);
    region4 = new HRegionInfo(tableName4);
    region5 = new HRegionInfo(tableName5);
    region6 = new HRegionInfo(tableName6);
    region7 = new HRegionInfo(tableName7);
    region8 = new HRegionInfo(tableName8);
  }

  @Test
  public void testBalanceCluster() throws Exception {

    // Create a map where 2 out of the 8 regions are on servers of the wrong group and within
    // a group the region placement is unbalanced
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

  @Test
  public void testRoundRobinAssignment() throws Exception {

    List<HRegionInfo> regions = new ArrayList<>();
    regions.add(region1);
    regions.add(region2);
    regions.add(region3);
    regions.add(region4);
    regions.add(region5);
    regions.add(region6);
    regions.add(region7);
    regions.add(region8);
    List<ServerName> servers = new ArrayList<>();
    servers.add(serverName1);
    servers.add(serverName2);
    servers.add(serverName3);
    servers.add(serverName4);

    Map<ServerName, List<HRegionInfo>> assignment =
        loadBalancer.roundRobinAssignment(regions, servers);

    Set<String> tablesGroup1ShouldContain = new HashSet<>();
    Set<String> tablesGroup2ShouldContain = new HashSet<>();

    tablesGroup1ShouldContain.add("test_table_1");
    tablesGroup1ShouldContain.add("test_table_2");
    tablesGroup1ShouldContain.add("test_table_3");
    tablesGroup1ShouldContain.add("test_table_4");

    tablesGroup2ShouldContain.add("test_table_5");
    tablesGroup2ShouldContain.add("test_table_6");
    tablesGroup2ShouldContain.add("test_table_7");
    tablesGroup2ShouldContain.add("test_table_8");

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : assignment.entrySet()) {

      ServerName serverNameFromAssignment = entry.getKey();
      List<HRegionInfo> regionsFromAssignment = entry.getValue();
      // Sort the regions based on the source server
      Collections.sort(regionsFromAssignment, new Comparator<HRegionInfo>() {
        @Override public int compare(HRegionInfo o1, HRegionInfo o2) {
          return o1.getRegionNameAsString().compareTo(o2.getRegionNameAsString());
        }
      });

      assertTrue(regionsFromAssignment.size() == 2);

      HRegionInfo regionFromAssignment1 = regionsFromAssignment.get(0);
      HRegionInfo regionFromAssignment2 = regionsFromAssignment.get(1);

      // Assert that the regions were put in servers of the right group
      if (serverNameFromAssignment.getPort() == 60001
          || serverNameFromAssignment.getPort() == 60011) {
        assertTrue(
            tablesGroup1ShouldContain.contains(regionFromAssignment1.getTable().getNameAsString()));
        assertTrue(
            tablesGroup1ShouldContain.contains(regionFromAssignment2.getTable().getNameAsString()));
      } else {
        assertTrue(
            tablesGroup2ShouldContain.contains(regionFromAssignment1.getTable().getNameAsString()));
        assertTrue(
            tablesGroup2ShouldContain.contains(regionFromAssignment2.getTable().getNameAsString()));
      }
    }
  }

  @Test
  public void testRetainAssignment() throws Exception {

    // Create a map where 2 out of the 8 regions are on servers of the wrong group and within
    // a group the region placement is unbalanced
    Map<HRegionInfo, ServerName> regions = new HashMap<>();
    regions.put(region8, serverName1);
    regions.put(region1, serverName1);
    regions.put(region2, serverName1);
    regions.put(region3, serverName1);
    regions.put(region4, serverName3);
    regions.put(region5, serverName3);
    regions.put(region6, serverName3);
    regions.put(region7, serverName3);

    List<ServerName> servers = new ArrayList<>(); servers.add(serverName1);
    servers.add(serverName2); servers.add(serverName3); servers.add(serverName4);

    Set<String> tablesGroup1ShouldContain = new HashSet<>();
    Set<String> tablesGroup2ShouldContain = new HashSet<>();

    tablesGroup1ShouldContain.add("test_table_1");
    tablesGroup1ShouldContain.add("test_table_2");
    tablesGroup1ShouldContain.add("test_table_3");
    tablesGroup1ShouldContain.add("test_table_4");

    tablesGroup2ShouldContain.add("test_table_5"); tablesGroup2ShouldContain.add("test_table_6");
    tablesGroup2ShouldContain.add("test_table_7"); tablesGroup2ShouldContain.add("test_table_8");

    Map<ServerName, List<HRegionInfo>> assignments =
        loadBalancer.retainAssignment(regions, servers);

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : assignments.entrySet()) {
      ServerName serverNameFromAssignment = entry.getKey();
      List<HRegionInfo> regionsFromAssignment = entry.getValue();
      if (serverNameFromAssignment.getPort() == 60001
          || serverNameFromAssignment.getPort() == 60011) {
        if (serverNameFromAssignment.getPort() == 60001) {
          // this server has to have at least 3 regions since it started off with 3 correct regions
          // and retainAssignment prioritizes not moving regions around within servers of the right
          // group over groups being balanced
          assertTrue(regionsFromAssignment.size() >= 3);
        } for (HRegionInfo region : regionsFromAssignment) {
          assertTrue(tablesGroup1ShouldContain.contains(region.getTable().getNameAsString()));
        }
      } else if (serverNameFromAssignment.getPort() == 60002
          || serverNameFromAssignment.getPort() == 60012) {
        if (serverNameFromAssignment.getPort() == 60002) {
          // this server has to have at least 3 regions since it started off with 3 correct regions
          // and retainAssignment prioritizes not moving regions around within servers of the right
          // group over groups being balanced
          assertTrue(regionsFromAssignment.size() >= 3);
        } for (HRegionInfo region : regionsFromAssignment) {
          assertTrue(tablesGroup2ShouldContain.contains(region.getTable().getNameAsString()));
        }
      }
    }
  }

  @Test
  public void testImmediateAssignment() throws Exception {

    List<HRegionInfo> regions = new ArrayList<>();
    regions.add(region1);
    regions.add(region2);
    regions.add(region3);
    regions.add(region4);
    regions.add(region5);
    regions.add(region6);
    regions.add(region7);
    regions.add(region8);
    List<ServerName> servers = new ArrayList<>();
    servers.add(serverName1);
    servers.add(serverName2);
    servers.add(serverName3);
    servers.add(serverName4);

    Map<ServerName, List<HRegionInfo>> assignment =
        loadBalancer.roundRobinAssignment(regions, servers);

    Set<String> tablesGroup1ShouldContain = new HashSet<>();
    Set<String> tablesGroup2ShouldContain = new HashSet<>();

    tablesGroup1ShouldContain.add("test_table_1");
    tablesGroup1ShouldContain.add("test_table_2");
    tablesGroup1ShouldContain.add("test_table_3");
    tablesGroup1ShouldContain.add("test_table_4");

    tablesGroup2ShouldContain.add("test_table_5");
    tablesGroup2ShouldContain.add("test_table_6");
    tablesGroup2ShouldContain.add("test_table_7");
    tablesGroup2ShouldContain.add("test_table_8");

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : assignment.entrySet()) {

      ServerName serverNameFromAssignment = entry.getKey();
      List<HRegionInfo> regionsFromAssignment = entry.getValue();
      // Sort the regions based on the source server
      Collections.sort(regionsFromAssignment, new Comparator<HRegionInfo>() {
        @Override public int compare(HRegionInfo o1, HRegionInfo o2) {
          return o1.getRegionNameAsString().compareTo(o2.getRegionNameAsString());
        }
      });

      assertTrue(regionsFromAssignment.size() == 2);

      HRegionInfo regionFromAssignment1 = regionsFromAssignment.get(0);
      HRegionInfo regionFromAssignment2 = regionsFromAssignment.get(1);

      // Assert that the regions were put in servers of the right group
      if (serverNameFromAssignment.getPort() == 60001
          || serverNameFromAssignment.getPort() == 60011) {
        assertTrue(
            tablesGroup1ShouldContain.contains(regionFromAssignment1.getTable().getNameAsString()));
        assertTrue(
            tablesGroup1ShouldContain.contains(regionFromAssignment2.getTable().getNameAsString()));
      } else {
        assertTrue(
            tablesGroup2ShouldContain.contains(regionFromAssignment1.getTable().getNameAsString()));
        assertTrue(
            tablesGroup2ShouldContain.contains(regionFromAssignment2.getTable().getNameAsString()));
      }
    }
  }
}
