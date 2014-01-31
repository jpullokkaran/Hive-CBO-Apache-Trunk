package org.apache.hadoop.hive.ql.optimizer.optiq.udf;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.*;

public class OptiqHiveUDF extends SqlFunction {
  public OptiqHiveUDF(SqlIdentifier sqlIdentifier, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
      RelDataType[] paramTypes, SqlFunctionCategory funcType) {
    super("OptiqHi", SqlKind.OTHER_FUNCTION, returnTypeInference, operandTypeInference,
        operandTypeChecker, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }
}
