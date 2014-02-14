package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveOptiqStatsUtil;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

/**
 * Relational expression representing a scan of a HiveDB collection.
 * 
 * <p>
 * Additional operations might be applied, using the "find" or "aggregate"
 * methods.
 * </p>
 */
public class HiveTableScanRel extends TableAccessRelBase implements HiveRel {
  private final List<HiveColStat>    m_hiveColStat     = new LinkedList<HiveColStat>();
  private final List<String>         m_colNamesLst     = new LinkedList<String>();
  private final Map<Integer, String> m_projIdToNameMap = new HashMap<Integer, String>();

  /**
   * Creates a HiveTableScan.
   * 
   * @param cluster
   *          Cluster
   * @param traitSet
   *          Traits
   * @param table
   *          Table
   * @param table
   *          HiveDB table
   */
  public HiveTableScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptHiveTable table,
      RelDataType rowtype) {
    super(cluster, OptiqTraitsUtil.getTableScanTraitSet(cluster, traitSet, table, rowtype), table);
    assert getConvention() == HiveRel.CONVENTION;
    int i = 0;
    for (String colName : rowtype.getFieldNames()) {
      m_projIdToNameMap.put(i, colName);
      m_colNamesLst.add(colName);
    }

    m_hiveColStat.addAll(HiveOptiqStatsUtil.computeTableRelColStat(table, m_colNamesLst));
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public void register(RelOptPlanner planner) {

  }

  public void implement(Implementor implementor) {

  }

  @Override
  public double getRows() {
    return ((RelOptHiveTable) table).getRowCount();
  }

  public List<HiveColStat> getColStat(List<Integer> projIndxLst) {
    if (projIndxLst != null) {
      List<HiveColStat> hiveColStatLst = new LinkedList<HiveColStat>();
      for (Integer i : projIndxLst) {
        hiveColStatLst.add(m_hiveColStat.get(i));
      }
      return hiveColStatLst;
    } else {
      return m_hiveColStat;
    }
  }
}