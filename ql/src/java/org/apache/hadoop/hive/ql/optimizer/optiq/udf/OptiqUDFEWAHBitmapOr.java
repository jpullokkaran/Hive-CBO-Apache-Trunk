package org.apache.hadoop.hive.ql.optimizer.optiq.udf;

import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.OperandTypes;
import org.eigenbase.sql.type.ReturnTypes;

public class OptiqUDFEWAHBitmapOr extends SqlFunction {

  public OptiqUDFEWAHBitmapOr() {
    super("ewah_bitmap_or", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0, null,
        OperandTypes.SAME_SAME, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }
}
