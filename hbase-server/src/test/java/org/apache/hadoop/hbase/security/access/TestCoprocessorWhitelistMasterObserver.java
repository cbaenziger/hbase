/**
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
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import java.util.Optional;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.Level;

/**
 * Performs coprocessor loads for various paths and malformed strings
 */
@Category({SecurityTests.class, LargeTests.class})
public class TestCoprocessorWhitelistMasterObserver extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorWhitelistMasterObserver.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCoprocessorWhitelistMasterObserver.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final TableName TEST_TABLE = TableName.valueOf("testTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("fam1");

  @Before
  public void setUp() {
    org.apache.log4j.Logger.getLogger(CoprocessorWhitelistMasterObserver.class).setLevel(Level.TRACE);
    org.apache.log4j.Logger.getLogger("org.apache.hbase.server").setLevel(Level.TRACE);
  }

  public static void uploadCoprocessorWhitelistMasterObserver() {
    try {
      UTIL.getTestFileSystem().mkdirs(new Path("permitted"));
      UTIL.getTestFileSystem().copyFromLocalFile(false, true,
        new Path(new Path(new URI(Version.url).getPath().toString()),
        "hbase-server/target/test-classes/org/apache/hadoop/hbase/coprocessor/SimpleRegionObserver.class"),
        new Path(UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class"));
      UTIL.getTestFileSystem().copyFromLocalFile(false, true,
        new Path(new Path(new URI(Version.url).getPath().toString()),
        "hbase-server/target/test-classes/org/apache/hadoop/hbase/coprocessor/SimpleRegionObserver.class"),
        new Path(UTIL.getTestFileSystem().getUri().toString() + "/permitted2/SimpleRegionObserver.class"));
    } catch (IOException|URISyntaxException ex) {
      fail("Setup failed with " + ex.toString());
    }
  }

  @After
  public void tearDownTestCoprocessorWhitelistMasterObserver() throws Exception {
    Admin admin = UTIL.getAdmin();
    try {
      try {
        admin.disableTable(TEST_TABLE);
      } catch (TableNotEnabledException ex) {
        // Table was left disabled by test
        LOG.info("Table was left disabled by test");
      }
      admin.deleteTable(TEST_TABLE);
    } catch (TableNotFoundException ex) {
      // Table was not created for some reason?
      LOG.info("Table was not created for some reason");
    }
    UTIL.shutdownMiniDFSCluster();
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is not whitelisted gets caught
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   * @param whitelistedPaths A String array of paths to add in
   *         for the whitelisting configuration
   * @param coprocessorPaths A String array of coprocessors to use
   *         as the paths for mock coprocessors
   */
  private static void positiveTestCase(String[] whitelistedPaths,
      String[] coprocessorPaths) throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    conf.setStrings(
        CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        whitelistedPaths);
    // set retries low to raise exception quickly
    conf.setInt("hbase.client.retries.number", 5);
    UTIL.startMiniCluster();
    uploadCoprocessorWhitelistMasterObserver();
    UTIL.createTable(TEST_TABLE, new byte[][] { TEST_FAMILY });
    UTIL.waitUntilAllRegionsAssigned(TEST_TABLE);
    Connection connection = ConnectionFactory.createConnection(conf);
    Table t = connection.getTable(TEST_TABLE);
    HTableDescriptor htd = new HTableDescriptor(t.getTableDescriptor());
    for (int i=0; i<coprocessorPaths.length; i++) {
      // Make the index part of the coprocessor name
      htd.addCoprocessor("net.clayb.coprocessor.notWhitelisted.pathIndex" + i,
        new Path(coprocessorPaths[i]),
        Coprocessor.PRIORITY_USER, null);
    }
    LOG.info("Modifying Table");
    try {
      connection.getAdmin().modifyTable(TEST_TABLE, htd);
      fail("Expected coprocessor to raise IOException");
    } catch (IOException e) {
      // swallow exception from coprocessor
    }
    LOG.info("Done Modifying Table");
    assertEquals(0, t.getTableDescriptor().getCoprocessors().size());
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted is permitted
   * @result The coprocessor should be added to the table
   *         descriptor successfully
   * @param whitelistedPaths A String array of paths to add in
   *         for the whitelisting configuration
   * @param coprocessorPaths A String array of coprocessors to use
   *         as the paths for mock coprocessors
   */
  private static void negativeTestCase(String[] whitelistedPaths,
      String[] coprocessorPaths) throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.client.retries.number", 5);
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    // set retries low to raise exception quickly
    // set a coprocessor whitelist path for test
    conf.setStrings(
        CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        whitelistedPaths);
    conf.setInt("dfs.replication", 3);
    conf.setInt("dfs.namenode.replication.min", 3);
    UTIL.startMiniDFSCluster(null);
    UTIL.createRootDir();
    UTIL.startMiniCluster();
    uploadCoprocessorWhitelistMasterObserver();
    UTIL.createTable(TEST_TABLE, new byte[][] { TEST_FAMILY });
    UTIL.waitUntilAllRegionsAssigned(TEST_TABLE);
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    // disable table so we do not actually try loading non-existant
    // coprocessor file
    admin.disableTable(TEST_TABLE);
    Table t = connection.getTable(TEST_TABLE);
    HTableDescriptor htd = new HTableDescriptor(t.getTableDescriptor());
    for (int i=0; i<coprocessorPaths.length; i++) {
      // Make the index part of the coprocessor name
      htd.addCoprocessor("net.clayb.coprocessor.whitelisted.pathIndex" + i,
        new Path(coprocessorPaths[i]),
        Coprocessor.PRIORITY_USER, null);
    }
    LOG.info("Modifying Table");
    admin.modifyTable(TEST_TABLE, htd);
    assertEquals(coprocessorPaths.length, t.getTableDescriptor().getCoprocessors().size());
    LOG.info("Done Modifying Table");
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is not whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   */
  @Test
  public void testSubstringNonWhitelisted() throws Exception {
    positiveTestCase(new String[]{"/permitted/*"},
        new String[]{"file:///notpermitted/couldnotpossiblyexist.jar"});
  }

  /**
   * Test a table creation including a coprocessor path
   * which is not whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   */
  @Test
  public void testDifferentFileSystemNonWhitelisted() throws Exception {
    positiveTestCase(new String[]{"hdfs://foo/bar"},
        new String[]{"file:///notpermitted/couldnotpossiblyexist.jar"});
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  public void testSchemeAndDirectorywhitelisted() throws Exception {
    String permittedPath = new Path(new Path(new URI(Version.url).getPath().toString()),
      "hbase-server/target/test-classes/org/apache/hadoop/hbase/coprocessor").toString();
    negativeTestCase(new String[]{"/tmp","file:///" + permittedPath + "/*"},
        new String[]{"file://"+ permittedPath + "/SimpleRegionObserver.class"});
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  public void testSchemeWhitelisted() throws Exception {
    String permittedPath = new Path(new Path(new URI(Version.url).getPath().toString()),
      "hbase-server/target/test-classes/org/apache/hadoop/hbase/coprocessor/SimpleRegionObserver.class").toString();
    negativeTestCase(new String[]{"file:///"},
        new String[]{"file:///" + permittedPath});
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  public void testDFSNameWhitelistedWorks() throws Exception {
    negativeTestCase(new String[]{UTIL.getTestFileSystem().getUri().toString()},
        new String[]{UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class"});
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   */
  @Test
  public void testDFSNameNotWhitelistedFails() throws Exception {
    positiveTestCase(new String[]{UTIL.getTestFileSystem().getUri().toString()},
        new String[]{UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class"});
  }

  /**
   * Test a table modification adding a coprocessor path
   * which is whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  public void testBlanketWhitelist() throws Exception {
    negativeTestCase(new String[]{"*"},
        new String[]{UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class"});
  }

  /**
   * Test a table modification adding coprocessors with paths one which
   * is whitelisted first, then a second coprocessor which is not whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   */
  @Test
  public void testMultiplePathsFirstAllowedSecondDisallowed() throws Exception {
    positiveTestCase(new String[]{UTIL.getTestFileSystem().getUri().toString() + "/permitted/*"},
        new String[]{UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class",
                     UTIL.getTestFileSystem().getUri().toString() + "/notPermitted/couldnotpossiblyexist.jar"});
  }

  /**
   * Test a table modification adding coprocessors with paths one which
   * is not whitelisted first, then a second coprocessor which is whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   */
  @Test
  public void testMultiplePathsSecondAllowedFirstDisallowed() throws Exception {
    positiveTestCase(new String[]{UTIL.getTestFileSystem().getUri().toString()},
        new String[]{"hdfs://Your-FileSystem/notPermitted/couldnotpossiblyexist.jar",
                     UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class"});
  }

  /**
   * Test a table modification adding coprocessor paths with two coprocessor
   * paths, both of which are whitelisted
   * @result Coprocessor should be added to table descriptor
   *         Table is disabled to avoid an IOException due to
   *         the added coprocessor not actually existing on disk
   */
  @Test
  public void testTwoPathsAllAllowed() throws Exception {
    negativeTestCase(new String[]{UTIL.getTestFileSystem().getUri().toString()},
        new String[]{UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class",
                     UTIL.getTestFileSystem().getUri().toString() + "/permitted2/SimpleRegionObserver.class"});
  }

  /**
   * Test a table modification adding coprocessor paths with two coprocessor
   * paths, both of which are not whitelisted
   * @result An IOException should be thrown and caught
   *         to show coprocessor is working as desired
   */
  @Test
  public void testTwoPathsAllDisallowed() throws Exception {
    positiveTestCase(new String[]{"hdfs://Your-FileSystem"},
        new String[]{UTIL.getTestFileSystem().getUri().toString() + "/permitted/SimpleRegionObserver.class",
                     UTIL.getTestFileSystem().getUri().toString() + "/permitted2/SimpleRegionObserver2.class"});
  }

  /**
   * Test a table creation including a coprocessor path
   * which is not whitelisted
   * @result Table will not be created due to the offending coprocessor
   */
  @Test
  public void testCreationNonWhitelistedCoprocessorPath() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    conf.setStrings(CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        new String[]{});
    // set retries low to raise exception quickly
    conf.setInt("hbase.client.retries.number", 5);
    conf.setInt("dfs.replication", 3);
    conf.setInt("dfs.namenode.replication.min", 3);
    UTIL.startMiniDFSCluster(null);
    UTIL.createRootDir();
    UTIL.startMiniCluster();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    htd.addFamily(hcd);
    htd.addCoprocessor("net.clayb.hbase.coprocessor.NotWhitelisted",
      new Path("file:///notpermitted/couldnotpossiblyexist.jar"),
      Coprocessor.PRIORITY_USER, null);
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    LOG.info("Creating Table");
    try {
      admin.createTable(htd);
      fail("Expected coprocessor to raise IOException");
    } catch (IOException e) {
      // swallow exception from coprocessor
    }
    LOG.info("Done Creating Table");
    // ensure table was not created
    assertEquals(new HTableDescriptor[0],
      admin.listTables("^" + TEST_TABLE.getNameAsString() + "$"));
  }

  public static class TestRegionObserver implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

  }

  /**
   * Test a table creation including a coprocessor path
   * which is on the classpath
   * @result Table will be created with the coprocessor
   */
  @Test
  public void testCreationClasspathCoprocessor() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // load coprocessor under test
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CoprocessorWhitelistMasterObserver.class.getName());
    conf.setStrings(CoprocessorWhitelistMasterObserver.CP_COPROCESSOR_WHITELIST_PATHS_KEY,
        new String[]{});
    // set retries low to raise exception quickly
    conf.setInt("hbase.client.retries.number", 5);
    UTIL.startMiniCluster();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    htd.addFamily(hcd);
    htd.addCoprocessor(TestRegionObserver.class.getName());
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    LOG.info("Creating Table");
    admin.createTable(htd);
    // ensure table was created and coprocessor is added to table
    LOG.info("Done Creating Table");
    Table t = connection.getTable(TEST_TABLE);
    assertEquals(1, t.getTableDescriptor().getCoprocessors().size());
  }
}
