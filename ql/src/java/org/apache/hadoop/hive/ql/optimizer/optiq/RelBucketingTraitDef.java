package org.apache.hadoop.hive.ql.optimizer.optiq;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveIRShuffleRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.relopt.RelTraitSet;

public class RelBucketingTraitDef extends RelTraitDef<RelBucketing> {
  public static final RelBucketingTraitDef INSTANCE =
      new RelBucketingTraitDef();

  private RelBucketingTraitDef() {
  }

  @Override
  public Class<RelBucketing> getTraitClass() {
    return RelBucketing.class;
  }

  @Override
  public String getSimpleName() {
    return "bucketing";
  }

  @Override
  public RelNode convert(RelOptPlanner planner, RelNode rel, RelBucketing toTrait,
      boolean allowInfiniteCostConverters) {
    if (toTrait.getNumberOfBuckets() != null || toTrait.getSizeOfLargestBucket() != null) {
      return null;
    }
    // Create a logical shuffle, then ask the planner to convert its remaining
    // traits

    final HiveIRShuffleRel shuffle =
            HiveIRShuffleRel.constructHiveIRShuffleRel(
            rel.getCluster(),
            rel.getCluster().traitSetOf(HiveRel.CONVENTION, toTrait),
            toTrait.getPartitionCols().asList().get(0),
            (HiveRel) rel);
    RelNode newRel = shuffle;
    final RelTraitSet newTraitSet = rel.getTraitSet().replace(toTrait);
    if (!newRel.getTraitSet().equals(newTraitSet)) {
      newRel = planner.changeTraits(newRel, newTraitSet);
    }
    return newRel;
  }

  @Override
  public boolean canConvert(RelOptPlanner planner, RelBucketing fromTrait, RelBucketing toTrait) {
    if (fromTrait.getNumberOfBuckets() != null || fromTrait.getSizeOfLargestBucket() != null
        || toTrait.getNumberOfBuckets() != null || toTrait.getSizeOfLargestBucket() != null) {
      return false;
    }

    return true;
  }

  @Override
  public RelBucketing getDefault() {
    return RelBucketingTraitImpl.EMPTY;
  }
}
