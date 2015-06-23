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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@Category({MediumTests.class})
public class TestGroups extends TestGroupsBase {
  protected static final Log LOG = LogFactory.getLog(TestGroups.class);
  private static HMaster master;
  private static GroupAdminEndpoint groupEndpoint;
  private static boolean init = false;


  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().set(
        HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        GroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.ZOOKEEPER_USEMULTI,
        true);
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.master.classes",
        GroupMasterObserver.class.getName() + "," +
            GroupAdminEndpoint.class.getName());
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE);
    TEST_UTIL.getConfiguration().set(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
        ""+NUM_SLAVES_BASE);

    admin = TEST_UTIL.getHBaseAdmin();
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster)cluster).getMaster();
    groupEndpoint =
        (GroupAdminEndpoint)master.getMasterCoprocessorHost().findCoprocessor(GroupAdminEndpoint
            .class.getName());

    //wait for balancer to come online
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized() &&
            ((GroupBasedLoadBalancer) master.getLoadBalancer()).isOnline();
      }
    });
    admin.setBalancerRunning(false,true);
    groupAdmin = new VerifyingGroupAdminClient(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeMethod() throws Exception {
    if(!init) {
      init = true;
      afterMethod();
    }

  }

  @After
  public void afterMethod() throws Exception {
    deleteTableIfNecessary();
    deleteNamespaceIfNecessary();
    deleteGroups();

    int missing = NUM_SLAVES_BASE + 1 - cluster.getClusterStatus().getServers().size();
    LOG.info("Restoring servers: "+missing);
    for(int i=0; i<missing; i++) {
      ((MiniHBaseCluster)cluster).startRegionServer();
    }

    groupAdmin.addGroup("master");
    groupAdmin.moveServers(
        Sets.newHashSet(((MiniHBaseCluster)cluster).getMaster().getServerName().getHostAndPort()),
        "master");
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting for cleanup to finish "+groupAdmin.listGroups());
        //Might be greater since moving servers back to default
        //is after starting a server

        return groupAdmin.getGroupInfo(GroupInfo.DEFAULT_GROUP).getServers().size()
           == NUM_SLAVES_BASE;
      }
    });
  }

  @Test
  public void testJmx() throws Exception {
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Iterator<ObjectName> it = mBeanServer.queryNames(new ObjectName("hadoop:name=Group,service=Group"), null).iterator();
    //verify it was loaded properly
    assertEquals("hadoop:name=Group,service=Group", it.next().getCanonicalName());

    final MXBeanImpl info = MXBeanImpl.init(groupAdmin, master);
    GroupInfo defaultGroup = groupAdmin.getGroupInfo(GroupInfo.DEFAULT_GROUP);
    assertEquals(2, info.getGroups().size());
    assertEquals(defaultGroup.getName(), info.getGroups().get(0).getName());
    assertEquals(defaultGroup.getServers(), Sets.newTreeSet(info.getGroups().get(0).getServers()));
    assertEquals(defaultGroup.getServers(), Sets.newTreeSet(info.getServersByGroup().get(GroupInfo.DEFAULT_GROUP)));
    assertEquals(0, info.getServersInTransition().size());

    GroupInfo barGroup = addGroup(groupAdmin, "bar", 3);
    TableName tableName1 = TableName.valueOf(tablePrefix+"_testJmx1");
    TableName tableName2 = TableName.valueOf(tablePrefix+"_testJmx2");
    TEST_UTIL.createTable(tableName1, Bytes.toBytes("f"));
    TEST_UTIL.createTable(tableName2, Bytes.toBytes("f"));
    groupAdmin.moveTables(Sets.newHashSet(tableName2), barGroup.getName());
    assertEquals(3, info.getGroups().size());

    int defaultIndex = -1;
    int barIndex = -1;

    for(int i=0; i<info.getGroups().size(); i++) {
      MXBean.GroupInfoBean bean = info.getGroups().get(i);
      if(bean.getName().equals(defaultGroup.getName())) {
        defaultIndex = i;
      }
      else if(bean.getName().equals(barGroup.getName())) {
        barIndex = i;
      }
    }

    defaultGroup = groupAdmin.getGroupInfo(GroupInfo.DEFAULT_GROUP);
    assertEquals(defaultGroup.getName(),
        info.getGroups().get(defaultIndex).getName());
    for(TableName entry: defaultGroup.getTables()) {
      assertTrue(info.getGroups().get(defaultIndex).getTables().contains(entry.getNameAsString()));
    }
    assertEquals(defaultGroup.getTables().size(),
        info.getGroups().get(defaultIndex).getTables().size());
    assertEquals(defaultGroup.getServers(),
        Sets.newTreeSet(info.getGroups().get(defaultIndex).getServers()));

    barGroup = groupAdmin.getGroupInfo(barGroup.getName());
    assertEquals(barGroup.getName(),
        info.getGroups().get(barIndex).getName());
    for(TableName entry: barGroup.getTables()) {
      assertTrue(info.getGroups().get(barIndex).getTables().contains(entry.getNameAsString()));
    }
    assertEquals(barGroup.getTables().size(),
        info.getGroups().get(barIndex).getTables().size());
    assertEquals(barGroup.getServers(),
        Sets.newTreeSet(info.getGroups().get(barIndex).getServers()));
  }

  @Test
  public void testBasicStartUp() throws IOException {
    GroupInfo defaultInfo = groupAdmin.getGroupInfo(GroupInfo.DEFAULT_GROUP);
    assertEquals(4, defaultInfo.getServers().size());
    // Assignment of root and meta regions.
    int count = master.getAssignmentManager().getRegionStates().getRegionAssignments().size();
    //3 meta,namespace, group
    assertEquals(3, count);
  }

  @Test
  public void testMoveServerWorker() throws Exception {
    LOG.info("testMoveServerWorker");

    //create groups and assign servers
    addGroup(groupAdmin, "bar", 3);
    groupAdmin.addGroup("foo");

    GroupInfo barGroup = groupAdmin.getGroupInfo("bar");
    GroupInfo fooGroup = groupAdmin.getGroupInfo("foo");
    assertEquals(3, barGroup.getServers().size());
    assertEquals(0, fooGroup.getServers().size());

    //test failure in move server worker
    MasterServices mockedMaster = Mockito.spy(master);
    doThrow(new RuntimeException("testMoveServerWorker")).when(mockedMaster).getAssignmentManager();
    Map<String,String> serversInTransition = new HashMap<String,String>();

    Runnable failedRunnable =
        new GroupMoveServerWorker(mockedMaster,
            serversInTransition,
            ((GroupAdminImpl)groupEndpoint.getGroupAdmin()).getGroupInfoManager(),
            new GroupMoveServerWorker.MoveServerPlan(barGroup.getServers(), fooGroup.getName()));
    Thread failedMoveServerThread = new Thread(failedRunnable);
    failedMoveServerThread.start();
    failedMoveServerThread.join();
    verify(mockedMaster,atLeastOnce()).getAssignmentManager();
    barGroup = groupAdmin.getGroupInfo("bar");
    assertEquals(0, serversInTransition.size());
    assertEquals(3, barGroup.getServers().size());
    assertEquals(4, groupAdmin.listGroups().size());
  }

  @Test
  public void testNamespaceCreateAndAssign() throws Exception {
    LOG.info("testNamespaceCreateAndAssign");
    String nsName = tablePrefix+"_foo";
    final TableName tableName = TableName.valueOf(nsName, tablePrefix + "_testCreateAndAssign");
    GroupInfo appInfo = addGroup(groupAdmin, "appInfo", 1);
    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(GroupInfo.NAMESPACEDESC_PROP_GROUP, "appInfo").build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
    ServerName targetServer = ServerName.parseServerName(appInfo.getServers().iterator().next());
    AdminProtos.AdminService.BlockingInterface rs = admin.getConnection().getAdmin(targetServer);
    //verify it was assigned to the right group
    assertEquals(1, ProtobufUtil.getOnlineRegions(rs).size());
    //verify prop was not stored as part of the schema
    assertNull(admin.getTableDescriptor(tableName).getValue(GroupInfo.TABLEDESC_PROP_GROUP));
  }

  @Test
  public void testDefaultNamespaceCreateAndAssign() throws Exception {
    LOG.info("testDefaultNamespaceCreateAndAssign");
    final byte[] tableName = Bytes.toBytes(tablePrefix + "_testCreateAndAssign");
    admin.modifyNamespace(NamespaceDescriptor.create("default")
        .addConfiguration(GroupInfo.NAMESPACEDESC_PROP_GROUP, "default").build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
  }

  @Test
  public void testNamespaceConstraint() throws Exception {
    String nsName = tablePrefix+"_foo";
    String groupName = tablePrefix+"_foo";
    LOG.info("testNamespaceConstraint");
    //verify we can't create a namespace without a group assigned
    try {
      admin.createNamespace(NamespaceDescriptor.create(nsName).build());
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    groupAdmin.addGroup(groupName);
    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(GroupInfo.NAMESPACEDESC_PROP_GROUP, groupName)
        .build());
    //test removing a referenced group
    try {
      groupAdmin.removeGroup(groupName);
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    //test modify group
    //changing with the same name is fine
    admin.modifyNamespace(
        NamespaceDescriptor.create(nsName)
          .addConfiguration(GroupInfo.NAMESPACEDESC_PROP_GROUP, groupName)
          .build());
    String anotherGroup = tablePrefix+"_anotherGroup";
    groupAdmin.addGroup(anotherGroup);
    //changing to a new group is not
    try {
      admin.modifyNamespace(
          NamespaceDescriptor.create(nsName)
            .addConfiguration(GroupInfo.NAMESPACEDESC_PROP_GROUP, anotherGroup)
            .build());
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    //test add non-existent group
    admin.deleteNamespace(nsName);
    groupAdmin.removeGroup(groupName);
    try {
      admin.createNamespace(NamespaceDescriptor.create(nsName)
          .addConfiguration(GroupInfo.NAMESPACEDESC_PROP_GROUP, "foo")
          .build());
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
  }

  @Test
  public void testGroupInfoMultiAccessing() throws Exception {
    GroupInfoManager manager = ((GroupAdminImpl) groupEndpoint.getGroupAdmin())
        .getGroupInfoManager();
    final GroupInfo defaultGroup = manager.getGroup("default");
    // getGroup updates default group's server list
    // this process must not affect other threads iterating the list
    Iterator<String> it = defaultGroup.getServers().iterator();
    manager.getGroup("default");
    it.next();
  }

  @Test
  public void testTracker() throws IOException, InterruptedException {
    LOG.info("testTracker");
    ZooKeeperWatcher watcher =
        new ZooKeeperWatcher(TEST_UTIL.getConfiguration(), "testTracker", null);
    GroupTracker tracker = new GroupTracker(watcher, null);
    try {
      final Map<String, GroupInfo> groupMap = new ConcurrentHashMap<String, GroupInfo>();
      final AtomicBoolean stateChanged = new AtomicBoolean(false);
      GroupTracker.Listener listener = new GroupTracker.Listener() {

        @Override
        public void groupMapChanged(Map<String, GroupInfo> map) {
          groupMap.clear();
          groupMap.putAll(map);
          stateChanged.set(true);
        }
      };
      tracker.addListener(listener);
      tracker.start();

      //wait for tracker to retrieve initial info
      tracker.blockUntilReady(0);
      int tries = 60000/100;
      while(groupMap.size() < 1 && tries > 0) {
        Thread.sleep(100);
        tries--;
      }
      assertNotSame(0, tries);
      assertNotNull(groupAdmin.getGroupInfo("default"));

      stateChanged.set(false);
      groupAdmin.addGroup("foo");
      while(!stateChanged.get()) {
        Thread.sleep(100);
      }
      stateChanged.set(false);
      assertEquals(3, groupMap.size());
      assertNotNull(tracker.getGroup("foo"));
      assertEquals(0, tracker.getGroup("foo").getServers().size());

      addGroup(groupAdmin, "bar", 1);
      while(!stateChanged.get()) {
        Thread.sleep(100);
      }
      stateChanged.set(false);
      assertEquals(4, groupMap.size());
      assertNotNull(tracker.getGroup("bar"));
      assertEquals(1, tracker.getGroup("bar").getServers().size());
    } finally {
      if(tracker != null) {
        tracker.stop();
      }
    }
  }
}
