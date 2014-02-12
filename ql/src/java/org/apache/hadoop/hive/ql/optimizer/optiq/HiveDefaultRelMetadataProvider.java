package org.apache.hadoop.hive.ql.optimizer.optiq;

import org.eigenbase.rel.metadata.DefaultRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMdColumnOrigins;
import org.eigenbase.rel.metadata.RelMdColumnUniqueness;
import org.eigenbase.rel.metadata.RelMdPercentageOriginalRows;
import org.eigenbase.rel.metadata.RelMdPopulationSize;
import org.eigenbase.rel.metadata.RelMdRowCount;

public class HiveDefaultRelMetadataProvider extends DefaultRelMetadataProvider {
  public HiveDefaultRelMetadataProvider() {
    addProvider(new RelMdPercentageOriginalRows());

    addProvider(new RelMdColumnOrigins());

    addProvider(new RelMdRowCount());

    addProvider(new RelMdColumnUniqueness());

    addProvider(new RelMdPopulationSize());

    addProvider(new HiveRelMdDistinctRowCount());

    addProvider(new HiveRelMdSelectivity());
  }
}
