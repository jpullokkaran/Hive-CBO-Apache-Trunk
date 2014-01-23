package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelBucketing;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCostUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.HiveColStat;
import org.apache.hadoop.hive.ql.optimizer.optiq.stats.OptiqStatsUtil;
import org.eigenbase.rel.RelCollation;
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
    /*
     * TODO: 1. Support Projection pruning 2. Support Partition pruning (Filter
     * push down to TS)
     */
    private final List<HiveColStat> m_hiveColStat = new LinkedList<HiveColStat>();
    private final List<String> m_colNamesLst = new LinkedList<String>();
    private final Map<Integer, String> m_projIdToNameMap = new HashMap<Integer, String>();
    private final double m_avgTupleSize;

    /**
     * Creates a HiveTableScan.
     * 
     * @param cluster
     *            Cluster
     * @param traitSet
     *            Traits
     * @param table
     *            Table
     * @param table
     *            HiveDB table
     */
    public HiveTableScanRel(RelOptCluster cluster, RelTraitSet traitSet,
            RelOptHiveTable table, RelDataType rowtype) {
        super(cluster, OptiqTraitsUtil.getTableScanTraitSet(cluster, traitSet,
                table, rowtype), table);
        assert getConvention() == HiveRel.CONVENTION;
        int i = 0;
        for (String colName : rowtype.getFieldNames()) {
            m_projIdToNameMap.put(i, colName);
            m_colNamesLst.add(colName);
        }

        // NOTE: TableScanRel may not carry all of the columns in the original
        // table hence we assemble Col Stats before computing average tuple size
        m_hiveColStat.addAll(OptiqStatsUtil.computeTableRelColStat(table,
                m_colNamesLst));
        m_avgTupleSize = OptiqStatsUtil.computeAvgTupleSize(m_hiveColStat);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
    	return HiveCostUtil.computeCost(this);
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

    @Override
    public double getAvgTupleSize() {
        return m_avgTupleSize;
    }

    @Override
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

    @Override
    public HiveColStat getColStat(Integer projIndx) {
        return m_hiveColStat.get(projIndx);
    }

    /*
     * @Override public long getColumnAvgSize(List<Integer> projIndxLst) {
     * 
     * if (projIndxLst != null) { List<String> projLst = new
     * LinkedList<String>();
     * 
     * for (Integer projIdx : projIndxLst) {
     * projLst.add(m_projIdToNameMap.get(projIdx)); }
     * 
     * if (!m_colNamesLst.equals(projLst)) return ((RelOptHiveTable)
     * table).getColumnStats(projLst) .getAvgRowSize(); }
     * 
     * return m_avgSize; }
     */
    @Override
    public Double getEstimatedMemUsageInVertex() {
        return 0.0;
    }

    /*
     * @Override public Integer getDegreeOfParallelization() { return 0; }
     * 
     * @Override public Long getNDV(List<Integer> colOrderLst) { return
     * OptiqStatsUtil.computeNDV(this, colOrderLst); }
     */

    @Override
    public boolean propagateBucketingTraitUpwardsViaTransformation(
            List<Integer> bucketingCols, List<Integer> bucketSortCols) {
        return false;
    }

    @Override
    public boolean propagateSortingTraitUpwardsViaTransformation(
            List<Integer> sortingCols) {
        return false;
    }

    @Override
    public boolean shouldPropagateTraitFromChildViaTransformation(
            RelBucketing bucketTraitFromChild) {
        return false;
    }

    @Override
    public boolean shouldPropagateTraitFromChildViaTransformation(
            RelCollation sortTraitFromChild) {
        return false;
    }
}

// End HiveTableScan.java
