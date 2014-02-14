package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptAbstractTable;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;

//Fix Me: use table meta data and stats util to get stats
public class RelOptHiveTable extends RelOptAbstractTable {
  private final Table                 m_hiveTblMetadata;
  private final RowSchema             m_hiveRowSchema;
  private final HiveConf              m_hiveConf;
  private double                      m_rowCount           = -1;

  final Map<List<String>, Statistics> m_schemaToStatsMap   = new HashMap<List<String>, Statistics>();
  final Map<String, Double>           m_columnIdxToSizeMap = new HashMap<String, Double>();

  Map<String, Integer>                m_bucketingColMap;
  Map<String, Integer>                m_bucketingSortColMap;

  // NOTE: name here is the table alias which may or may not be the real name in
  // metadata. Use
  // m_hiveTblMetadata.getTableName() for table name and
  // m_hiveTblMetadata.getDbName() for db name.
  public RelOptHiveTable(RelOptSchema schema, String name, RelDataType rowType,
      Table hiveTblMetadata, RowSchema hiveRowSchema, HiveConf hconf) {
    super(schema, name, rowType);
    m_hiveTblMetadata = hiveTblMetadata;
    m_hiveRowSchema = hiveRowSchema;
    m_hiveConf = hconf;

    m_rowCount = StatsUtils.getNumRows(m_hiveTblMetadata);
  }

  @Override
  public boolean isKey(BitSet arg0) {
    return false;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return new TableAccessRel(context.getCluster(), this);
  }

  @Override
  public <T> T unwrap(Class<T> arg0) {
    return arg0.isInstance(this) ? arg0.cast(this) : null;
  }

  @Override
  public double getRowCount() {
    return m_rowCount;
  }

  public Statistics getColumnStats(List<String> columnLst) {
    Statistics colStat = m_schemaToStatsMap.get(columnLst);

    if (colStat == null) {
      // TODO: Pass in partition list
      colStat = StatsUtils.collectStatistics(m_hiveConf, null, m_hiveTblMetadata,
          m_hiveRowSchema.getSignature(), columnLst);
      if (colStat == null || colStat.getBasicStatsState().equals(State.NONE)) {
        StringBuffer colSB = new StringBuffer("<");
        for (String col : columnLst) {
          colSB.append(" ");
          colSB.append(col);
          colSB.append(" ");
        }
        colSB.append(">");
        throw new RuntimeException("No statistics available for table: "
            + m_hiveTblMetadata.getCompleteName() + ", Columns: " + colSB.toString());
      }

      m_schemaToStatsMap.put(columnLst, colStat);
    }

    return colStat;
  }

  public Table getHiveTableMD() {
    return m_hiveTblMetadata;
  }
}
