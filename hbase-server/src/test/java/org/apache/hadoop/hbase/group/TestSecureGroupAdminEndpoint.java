/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.hbase.group;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
* Performs authorization checks for common operations, according to different
* levels of authorized users.
*/
@Category(MediumTests.class)
@SuppressWarnings("rawtypes")
public class TestSecureGroupAdminEndpoint {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  // user with all permissions
  private static User SUPERUSER;
  // user granted with all global permission
  private static User USER_ADMIN;
  // user with rw permissions
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  // user with create table permissions alone
  private static User USER_CREATE;
  // user with no permissions
  private static User USER_NONE;

  private static AccessController ACCESS_CONTROLLER;
  private static GroupAdminEndpoint GROUP_ENDPOINT;
  private static GroupMasterObserver GROUP_OBSERVER;

  private static GroupAdmin spy;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
		TEST_UTIL.getConfiguration().set(
				HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
				GroupBasedLoadBalancer.class.getName());
    conf.set("hbase.coprocessor.master.classes",
        conf.get("hbase.coprocessor.master.classes")+","+
        GroupAdminEndpoint.class.getName()+","+
        GroupMasterObserver.class.getName());

    TEST_UTIL.startMiniCluster(1,2);
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster().getMaster()
        .getMasterCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());

    GROUP_ENDPOINT = ((GroupAdminEndpoint)
        TEST_UTIL.getMiniHBaseCluster().getMaster()
           .getMasterCoprocessorHost().findCoprocessor(GroupAdminEndpoint.class.getName()));
    spy = spy(GROUP_ENDPOINT.getGroupAdmin());
    GROUP_ENDPOINT.setGroupAdmin(spy);

    GROUP_OBSERVER =
        (GroupMasterObserver) cpHost.findCoprocessor(GroupMasterObserver.class.getName());

    // Wait for the ACL table to become available
    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME.getName(), 60000);
    //wait for balancer to come online
    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return !master.isInitialized() ||
          !((GroupBasedLoadBalancer)master.getLoadBalancer()).isOnline();
      }
    });



    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[]{"supergroup"});
    USER_ADMIN = User.createUserForTesting(conf, "admin2", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);

    // initialize access control
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControlProtos.AccessControlService.BlockingInterface protocol =
      AccessControlProtos.AccessControlService
          .newBlockingStub(meta.coprocessorService(HConstants.EMPTY_START_ROW));

    AccessControlProtos.GrantRequest req =
      AccessControlProtos.GrantRequest.newBuilder().setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFrom(Bytes.toBytes(USER_ADMIN.getShortName())))
              .setPermission(AccessControlProtos.Permission.newBuilder()
                  .setType(AccessControlProtos.Permission.Type.Global)
                  .setGlobalPermission(AccessControlProtos.GlobalPermission.newBuilder()
                      .addAction(AccessControlProtos.Permission.Action.ADMIN)
                      .addAction(AccessControlProtos.Permission.Action.CREATE)
                      .addAction(AccessControlProtos.Permission.Action.READ)
                      .addAction(AccessControlProtos.Permission.Action.WRITE)))
      ).build();
    protocol.grant(null, req);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void verifyAllowed(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      }
    }
  }

  public void verifyAllowed(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyAllowed(user, action);
    }
  }

  public void verifyDenied(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
        fail("Expected AccessDeniedException for user '" + user.getShortName() + "'");
      } catch (RetriesExhaustedWithDetailsException e) {
        // in case of batch operations, and put, the client assembles a
        // RetriesExhaustedWithDetailsException instead of throwing an
        // AccessDeniedException
        boolean isAccessDeniedException = false;
        for (Throwable ex : e.getCauses()) {
          if (ex instanceof AccessDeniedException) {
            isAccessDeniedException = true;
            break;
          }
        }
        if (!isAccessDeniedException) {
          fail("Not receiving AccessDeniedException for user '" + user.getShortName() + "'");
        }
      } catch (AccessDeniedException ade) {
        // expected result
      }
    }
  }

  public void verifyDenied(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyDenied(user, action);
    }
  }

  @Test
  public void testGetAddRemove() throws Exception {
    final AtomicLong counter = new AtomicLong(0);

    PrivilegedExceptionAction getGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        new GroupAdminClient(conf).getGroupInfo("default");
        return null;
      }
    };
    verifyAllowed(getGroup, SUPERUSER, USER_ADMIN, USER_NONE);

    PrivilegedExceptionAction addGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        //new to create a new client everytime so the correct user is propagated to the server
        new GroupAdminClient(conf).addGroup("testGetAddRemove" + counter.incrementAndGet());
        return null;
      }
    };
    verifyDenied(addGroup, USER_NONE);
    verifyAllowed(addGroup, SUPERUSER, USER_ADMIN);

    PrivilegedExceptionAction removeGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        new GroupAdminClient(conf).removeGroup("testGetAddRemove" + counter.getAndDecrement());
        return null;
      }
    };
    verifyAllowed(removeGroup, SUPERUSER, USER_ADMIN);
    verifyDenied(removeGroup, USER_NONE);
  }

  @Test
  public void testMoveServer() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        String hostPort = "foo:123";
        Set<String> set = new TreeSet<String>();
        set.add(hostPort);
        doNothing().when(spy).moveServers(any(Set.class),anyString());
        new GroupAdminClient(conf).moveServers(set, "testMoveServer");
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_NONE);
  }

  @Test
  public void testMoveTable() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        doNothing().when(spy).moveTables(any(Set.class),anyString());
        new GroupAdminClient(conf).moveTables(Sets.newHashSet(TableName.valueOf("testMoveTable")), "default");
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_NONE);
  }

  @Test
  public void testListGroups() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        new GroupAdminClient(conf).listGroups();
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN,USER_NONE);
  }

  @Test
  public void testCreateAndAssign() throws Exception {
    final String nsName = "ns_testCreateAndAssign";
    final AtomicInteger counter = new AtomicInteger(0);
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(GroupInfo.NAMESPACEDESC_PROP_GROUP, "default").build());

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        final TableName tableName =
            TableName.valueOf(nsName, "testCreateAndAssign_"+counter.incrementAndGet());
        final HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor("f"));
        GROUP_OBSERVER.preCreateTable(null, desc, null);
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_NONE);

    action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        final TableName tableName =
            TableName.valueOf(nsName, "testCreateAndAssign_"+counter.incrementAndGet());
        final HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor("f"));
        desc.setValue(GroupInfo.TABLEDESC_PROP_GROUP, "default");
        GROUP_OBSERVER.preCreateTable(null, desc, null);
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_NONE);
  }

  private static void waitForCondition(PrivilegedExceptionAction<Boolean> action) throws Exception {
    long sleepInterval = 100;
    long timeout = 2*60000;
    long tries = timeout/sleepInterval;
    while(action.run()) {
      Thread.sleep(sleepInterval);
      if(tries-- < 0) {
        fail("Timeout");
      }
    }
  }
}

