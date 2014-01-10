package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqTraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqUtil;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

public class HiveIRBroadCastRel extends HiveIRRel {

    public static HiveIRBroadCastRel constructHiveIRBroadCastRel(RelOptCluster cluster, RelTraitSet traitSet, HiveRel child) {
        List<RexNode> exps = OptiqUtil.constructChildInputRefs(child);
        RelDataType rowType = RexUtil.createStructType(
                cluster.getTypeFactory(), exps, child.getRowType().getFieldNames());
        return new HiveIRBroadCastRel(cluster, traitSet, child, exps, rowType);
    }

  public HiveIRBroadCastRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
    super(cluster, OptiqTraitsUtil.getIRBroadcastApplicableTraitSet(cluster, traitSet, child), child, exps, rowType, 0);
  }

  @Override
  public Double getEstimatedMemUsageInVertex() {
    return ((HiveRel)getChild()).getRows() * ((HiveRel)getChild()).getAvgTupleSize();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(HiveRel.CONVENTION);
    return new HiveIRBroadCastRel(
        getCluster(), traitSet,
        sole(inputs),
        exps,
        rowType);
  }
}
