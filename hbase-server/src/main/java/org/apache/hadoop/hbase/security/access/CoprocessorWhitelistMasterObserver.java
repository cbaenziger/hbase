/*
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

import java.io.IOException;
import java.net.URI;
import java.nio.file.PathMatcher;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Master observer for restricting coprocessor assignments.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class CoprocessorWhitelistMasterObserver extends BaseMasterObserver {

  public static final String CP_COPROCESSOR_WHITELIST_PATHS_KEY =
      "hbase.coprocessor.region.whitelist.paths";


  private static final Log LOG = LogFactory
      .getLog(CoprocessorWhitelistMasterObserver.class);
  static {
    Logger.getLogger(CoprocessorWhitelistMasterObserver.class).setLevel(Level.TRACE);
    Logger.getLogger("org.apache.hbase.server").setLevel(Level.TRACE);
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
    verifyCoprocessors(ctx, htd);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor htd, HRegionInfo[] regions) throws IOException {
    verifyCoprocessors(ctx, htd);
  }

  /**
   * Validates a single whitelist path against the coprocessor path
   * @param  coproc_path the path to the coprocessor including scheme
   * @param  wl_path     can be:
   *                      1) a "*" to wildcard all coprocessor paths
   *                      2) a specific filesystem (e.g. hdfs://my-cluster/)
   *                      3) a wildcard path to be evaluated by
   *                         {@link FilenameUtils.wildcardMatch}
   *                         path can specify scheme or not (e.g.
   *                         "file:///usr/hbase/coprocessors" or for all
   *                         filesystems "/usr/hbase/coprocessors")
   * @return             if the path was found under the wl_path
   * @throws IOException if a failure occurs in getting the path file system
   */
  private static boolean validatePath(Path coproc_path, Path wl_path,
      Configuration conf) throws IOException {
    // verify if all are allowed
    if (wl_path.toString().equals("*")) {
      return(true);
    }

    // verify we are on the same filesystem if wl_path has a scheme
    if (!wl_path.isAbsoluteAndSchemeAuthorityNull()) {
      String wl_path_scheme = wl_path.toUri().getScheme();
      String coproc_path_scheme = coproc_path.toUri().getScheme();
      String wl_path_host = wl_path.toUri().getHost();
      String coproc_path_host = coproc_path.toUri().getHost();
      if (wl_path_scheme != null) {
        wl_path_scheme = wl_path_scheme.toString().toLowerCase();
      } else {
        wl_path_scheme = "";
      }
      if (wl_path_host != null) {
        wl_path_host = wl_path_host.toString().toLowerCase();
      } else {
        wl_path_host = "";
      }
      if (coproc_path_scheme != null) {
        coproc_path_scheme = coproc_path_scheme.toString().toLowerCase();
      } else {
        coproc_path_scheme = "";
      }
      if (coproc_path_host != null) {
        coproc_path_host = coproc_path_host.toString().toLowerCase();
      } else {
        coproc_path_host = "";
      }
      if (!wl_path_scheme.equals(coproc_path_scheme) || !wl_path_host.equals(coproc_path_host)) {
        return(false);
      }
    }

    // allow any on this file-system (file systems were verified to be the same above)
    if (wl_path.isRoot()) {
      return(true);
    }

    // allow "loose" matches stripping scheme
    if (FilenameUtils.wildcardMatch(
        Path.getPathWithoutSchemeAndAuthority(coproc_path).toString(),
        Path.getPathWithoutSchemeAndAuthority(wl_path).toString())) {
      return(true);
    }
    return(false);
  }

  /**
   * Perform the validation checks for a coprocessor to determine if the path
   * is white listed or not.
   * @throws IOException if path is not included in whitelist or a failure
   *                     occurs in processing
   * @param  ctx         as passed in from the coprocessor
   * @param  htd         as passed in from the coprocessor
   */
  private void verifyCoprocessors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor htd) throws IOException {

    MasterServices services = ctx.getEnvironment().getMasterServices();
    Configuration conf = services.getConfiguration();

    Collection<String> paths =
        conf.getStringCollection(
            CP_COPROCESSOR_WHITELIST_PATHS_KEY);

    List<String> coprocs = htd.getCoprocessors();
    for (int i = 0; i < coprocs.size(); i++) {
      String coproc = coprocs.get(i);

      String coproc_spec = Bytes.toString(htd.getValue(
          Bytes.toBytes("coprocessor$" + (i + 1))));
      if (coproc_spec == null) {
        continue;
      }

      // File path is the 1st field of the coprocessor spec
      Matcher matcher =
          HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(coproc_spec);
      if (matcher == null || !matcher.matches()) {
        continue;
      }

      String coproc_path_str = matcher.group(1).trim();
      // Check if coprocessor is being loaded via the classpath (i.e. no file path)
      if (coproc_path_str == "") {
        break;
      }
      Path coproc_path = new Path(coproc_path_str);
      String coprocessor_class = matcher.group(2).trim();

      boolean foundPathMatch = false;
      for (String path_str : paths) {
        Path wl_path = new Path(path_str);
        try {
          foundPathMatch = validatePath(coproc_path, wl_path, conf);
          if (foundPathMatch == true) {
            LOG.debug(String.format("Coprocessor %s found in directory %s",
                coprocessor_class, path_str));
            break;
          }
        } catch (IOException e) {
          LOG.warn(String.format("Failed to validate white list path %s for coprocessor path %s",
              path_str, coproc_path_str));
        }
      }
      if (!foundPathMatch) {
        throw new IOException(String.format("Loading %s DENIED in %s",
            coprocessor_class, CP_COPROCESSOR_WHITELIST_PATHS_KEY));
      }
    }
  }
}
