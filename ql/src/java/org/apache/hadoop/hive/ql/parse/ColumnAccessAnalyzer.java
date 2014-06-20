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
package org.apache.hadoop.hive.ql.parse;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class ColumnAccessAnalyzer {
  private static final Log LOG = LogFactory.getLog(ColumnAccessAnalyzer.class.getName());
  private final ParseContext pGraphContext;

  public ColumnAccessAnalyzer() {
    pGraphContext = null;
  }

  public ColumnAccessAnalyzer(ParseContext pactx) {
    pGraphContext = pactx;
  }

  public ColumnAccessInfo analyzeColumnAccess() throws SemanticException {
    ColumnAccessInfo columnAccessInfo = new ColumnAccessInfo();
    Map<TableScanOperator, Table> topOps = pGraphContext.getTopToTable();
    for (TableScanOperator op : topOps.keySet()) {
      Table table = topOps.get(op);
      String tableName = table.getCompleteName();
      List<String> referenced = op.getReferencedColumns();
      for (String column : referenced) {
        columnAccessInfo.add(tableName, column);
      }
      if (table.isPartitioned()) {
        PrunedPartitionList parts;
        try {
          parts = pGraphContext.getPrunedPartitions(table.getTableName(), op);
        } catch (HiveException e) {
          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
          throw new SemanticException(e.getMessage(), e);
        }
        if (parts.getReferredPartCols() != null) {
          for (String partKey : parts.getReferredPartCols()) {
            columnAccessInfo.add(tableName, partKey);
          }
        }
      }
    }
    return columnAccessInfo;
  }
}
