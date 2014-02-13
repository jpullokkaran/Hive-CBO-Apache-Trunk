package org.apache.hadoop.hive.ql.optimizer.optiq;

import com.google.common.collect.ImmutableList;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMdColumnOrigins;
import org.eigenbase.rel.metadata.RelMdColumnUniqueness;
import org.eigenbase.rel.metadata.RelMdPercentageOriginalRows;
import org.eigenbase.rel.metadata.RelMdPopulationSize;
import org.eigenbase.rel.metadata.RelMdRowCount;

public class HiveDefaultRelMetadataProvider extends ChainedRelMetadataProvider {
  public HiveDefaultRelMetadataProvider() {
    super(
        ImmutableList.of(
            RelMdPercentageOriginalRows.SOURCE,
            RelMdColumnOrigins.SOURCE,
            RelMdRowCount.SOURCE,
            RelMdColumnUniqueness.SOURCE,
            RelMdPopulationSize.SOURCE,
            HiveRelMdDistinctRowCount.SOURCE,
            HiveRelMdSelectivity.SOURCE));
  }
}
