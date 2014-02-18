package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
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
  private final List<ColStatistics> m_hiveColStat = new LinkedList<ColStatistics>();

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
    super(cluster, TraitsUtil.getTableScanTraitSet(cluster, traitSet, table, rowtype), table);
    assert getConvention() == HiveRel.CONVENTION;

    List<String> colNamesLst = new LinkedList<String>();
    for (String colName : rowtype.getFieldNames()) {
      colNamesLst.add(colName);
    }

    m_hiveColStat.addAll(table.getHiveStats().getColumnStats());
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

  public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
    if (projIndxLst != null) {
      List<ColStatistics> hiveColStatLst = new LinkedList<ColStatistics>();
      for (Integer i : projIndxLst) {
        hiveColStatLst.add(m_hiveColStat.get(i));
      }
      return hiveColStatLst;
    } else {
      return m_hiveColStat;
    }
  }

}