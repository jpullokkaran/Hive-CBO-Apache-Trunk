package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptAbstractTable;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;

//Fix Me: use table meta data and stats util to get stats
public class RelOptHiveTable extends RelOptAbstractTable {
  private final Table m_hiveTblMetadata;
  private final RowSchema m_hiveRowSchema;
  private final HiveConf m_hiveConf;
  private double m_rowCount = -1;

  final Map<List<String>, Statistics> m_schemaToStatsMap = new HashMap<List<String>, Statistics>();
  final Map<String, Double> m_columnIdxToSizeMap = new HashMap<String, Double>();

  private boolean m_bucketTraitComputed;
  private int m_noOfBuckets;
  private double m_sizeOfLargestBucket;
  private List<String> m_bucketingCols;
  private List<String> m_bucketSortCols;
  Map<String, Integer> m_bucketingColMap;
  Map<String, Integer> m_bucketingSortColMap;
  private boolean m_sortedAsc;

  // NOTE: name here is the table alias which may or may not be the real name in metadata. Use
  // m_hiveTblMetadata.getTableName() for table name and m_hiveTblMetadata.getDbName() for db name.
  protected RelOptHiveTable(RelOptSchema schema, String name, RelDataType rowType,
      Table hiveTblMetadata, RowSchema hiveRowSchema, HiveConf hconf) {
    super(schema, name, rowType);
    m_hiveTblMetadata = hiveTblMetadata;
    m_hiveRowSchema = hiveRowSchema;
    m_hiveConf = hconf;
    String rowCount = System.getenv(name);
    if (rowCount != null) {
      try {
        m_rowCount = Double.parseDouble(rowCount);
      } catch (Exception e) {
      }
    }

    if (m_rowCount < 0) {
      m_rowCount = 100;
    }
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
  public double getRowCount()
  {
    return m_rowCount;
  }

  public Statistics getColumnStats(List<String> columnLst) {
      Statistics colStat = m_schemaToStatsMap.get(columnLst);
      
      if (colStat ==  null) {
          colStat = StatsUtils.collectStatistics(m_hiveConf, null, m_hiveTblMetadata, m_hiveRowSchema.getSignature(), columnLst);
          if (colStat == null || colStat.getBasicStatsState().equals(State.NONE))
          {
              StringBuffer colSB = new StringBuffer("<");
              for (String col : columnLst) {
              colSB.append(" ");                  
              colSB.append(col);
              colSB.append(" ");
              }
              colSB.append(">");
              throw new RuntimeException("No statistics available for table: " + m_hiveTblMetadata.getCompleteName() + ", Columns: " + colSB.toString());
          }
          
          m_schemaToStatsMap.put(columnLst, colStat);
      }
      
      return colStat;
 /*   
    List<String> unknownColumnNameLst = new LinkedList<String>();
    List<Double> unknownColumnSzFromMeta = null;
    Map<String, Double> nameToSzMap = new HashMap<String, Double>();

    for (String colName : columnLst) {
      cSz = m_columnIdxToSizeMap.get(colName);
      if (cSz == null) {
        unknownColumnNameLst.add(colName);
      } else {
        nameToSzMap.put(colName, cSz);
      }
    }

    unknownColumnSzFromMeta = getColumnSizes(unknownColumnNameLst);

    for (int i = 0; i < unknownColumnSzFromMeta.size(); i++) {
      cSz = unknownColumnSzFromMeta.get(i);
      m_columnIdxToSizeMap.put(unknownColumnNameLst.get(i), cSz);
      nameToSzMap.put(unknownColumnNameLst.get(i), cSz);
    }

    return nameToSzMap;
    */
  }


  public RelBucketing getBucketTrait(RelDataType rowtype) {
    String tmp;
    List<String> bucketingColNames = new LinkedList<String>();
    List<String> sortColNames = new LinkedList<String>();
    List<Integer> bucketingColPos = new LinkedList<Integer>();
    List<Integer> sortColPos = new LinkedList<Integer>();

    if (!m_bucketTraitComputed) {
      computeBucketTrait();
    }

    if (m_bucketingCols != null && !m_bucketingCols.isEmpty()) {
      for (int i = 0; i < rowtype.getFieldCount(); i++) {
        tmp = rowtype.getFieldList().get(i).getName();
        bucketingColNames.add(tmp);
        bucketingColPos.add(i);
        if (m_bucketingSortColMap.containsKey(tmp)) {
          sortColNames.add(tmp);
          sortColPos.add(i);
        }
      }

      if (m_bucketingCols.equals(bucketingColNames)) {
        RelCollation collation = null;
        if (m_bucketSortCols != null && m_bucketSortCols.equals(sortColNames)) {
          List<RelFieldCollation> collationLst = new LinkedList<RelFieldCollation>();
          Direction orderingDirection = (m_sortedAsc ? Direction.Ascending : Direction.Descending);
          for (Integer sortCol : sortColPos) {
            collationLst.add(new RelFieldCollation(sortCol, orderingDirection));
          }
          collation = RelCollationImpl.of(collationLst);
        }
        return RelBucketingTraitImpl.of(bucketingColPos, collation, m_noOfBuckets,
            m_sizeOfLargestBucket);
      }
    }

    return null;
  }
/*
  public RelBucketing getBucketTrait(List<String> colNameLst, List<Integer> colOrdinalLst) {
    RelBucketing bucketTrait = null;

    if (!m_bucketTraitComputed) {
      computeBucketTrait();
    }

    if (m_bucketingCols != null && m_bucketingCols.size() > 0) {
      if (colNameLst.equals(m_bucketingCols)) {
        boolean ordered = false;
        List<Integer> sortColLst = null;
        if (m_bucketSortCols != null && colNameLst.equals(m_bucketSortCols)) {
          ordered = true;
          sortColLst = colOrdinalLst;
        }
        bucketTrait = RelBucketingTraitImpl.of(ordered, m_sortedAsc, m_noOfBuckets,
            m_sizeOfLargestBucket, colOrdinalLst, sortColLst);
      }
    }

    return bucketTrait;
  }
  */

  private void computeBucketTrait() {
    m_bucketTraitComputed = true;
    m_noOfBuckets = m_hiveTblMetadata.getNumBuckets();

    if (m_noOfBuckets > 0) {
      m_bucketingCols = m_hiveTblMetadata.getBucketCols();
      m_bucketingColMap = new HashMap<String, Integer>();
      for (int i = 0; i < m_bucketingCols.size(); i++) {
        m_bucketingColMap.put(m_bucketingCols.get(i), i);
      }

      List<Order> sortedColumnsFromMeta = m_hiveTblMetadata.getSortCols();
      if (sortedColumnsFromMeta != null && sortedColumnsFromMeta.size() > 0) {
        m_bucketSortCols = new LinkedList<String>();
        m_bucketingSortColMap = new HashMap<String, Integer>();
        String tmp;

        for (int i = 0; i < sortedColumnsFromMeta.size(); i++) {
          tmp = sortedColumnsFromMeta.get(i).getCol();
          m_bucketSortCols.add(tmp);
          m_bucketingSortColMap.put(tmp, i);
        }

        m_sortedAsc = (sortedColumnsFromMeta.get(0).getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) ? true
            : false;
      }

      // TODO: Once prashanth completes the largest bucket size, pull that in here
      m_sizeOfLargestBucket = 0.0;
    }
  }
}
