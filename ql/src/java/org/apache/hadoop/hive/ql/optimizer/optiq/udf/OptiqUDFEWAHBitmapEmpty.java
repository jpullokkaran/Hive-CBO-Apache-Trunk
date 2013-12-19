package org.apache.hadoop.hive.ql.optimizer.optiq.udf;

import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.SqlTypeStrategies;

public class OptiqUDFEWAHBitmapEmpty extends SqlFunction {

  public OptiqUDFEWAHBitmapEmpty() {
    super("ewah_bitmap_empty", SqlKind.OTHER_FUNCTION, SqlTypeStrategies.rtiFirstArgType, null,
        SqlTypeStrategies.otcSameX2, SqlFunctionCategory.UserDefinedFunction);
  }
}
