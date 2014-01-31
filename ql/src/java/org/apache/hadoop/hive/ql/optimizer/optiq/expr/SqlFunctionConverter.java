package org.apache.hadoop.hive.ql.optimizer.optiq.expr;

import java.util.*;

import com.google.common.collect.*;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.*;

public class SqlFunctionConverter {
	static final Map<String, SqlOperator> operatorMap;
	static final Map<String, SqlOperator> hiveToOptiq;

	static {
    Builder builder = new Builder();
    operatorMap = ImmutableMap.copyOf(builder.operatorMap);
    hiveToOptiq = ImmutableMap.copyOf(builder.hiveToOptiq);
  }

  public static SqlOperator getOptiqOperator(GenericUDF hiveUDF) {
    return hiveToOptiq.get(hiveUDF.getClass().getName());
  }

  private static class Builder {
    final Map<String, SqlOperator> operatorMap = Maps.newHashMap();
    final Map<String, SqlOperator> hiveToOptiq = Maps.newHashMap();

    Builder() {
      registerFunction("concat", SqlStdOperatorTable.CONCAT);
      registerFunction("substr", SqlStdOperatorTable.SUBSTRING);
      registerFunction("substring", SqlStdOperatorTable.SUBSTRING);
      stringFunction("space");
      stringFunction("repeat");
      numericFunction("ascii");
      stringFunction("repeat");

      numericFunction("size");

      numericFunction("round");
      registerFunction("floor", SqlStdOperatorTable.FLOOR);
      registerFunction("sqrt", SqlStdOperatorTable.SQRT);
      registerFunction("ceil", SqlStdOperatorTable.CEIL);
      registerFunction("ceiling", SqlStdOperatorTable.CEIL);
      numericFunction("rand");
      operatorMap.put("abs", SqlStdOperatorTable.ABS);
      numericFunction("pmod");

      numericFunction("ln");
      numericFunction("log2");
      numericFunction("sin");
      numericFunction("asin");
      numericFunction("cos");
      numericFunction("acos");
      registerFunction("log10", SqlStdOperatorTable.LOG10);
      numericFunction("log");
      numericFunction("exp");
      numericFunction("power");
      numericFunction("pow");
      numericFunction("sign");
      numericFunction("pi");
      numericFunction("degrees");
      numericFunction("atan");
      numericFunction("tan");
      numericFunction("e");


      registerFunction("upper", SqlStdOperatorTable.UPPER);
      registerFunction("lower", SqlStdOperatorTable.LOWER);
      registerFunction("ucase", SqlStdOperatorTable.UPPER);
      registerFunction("lcase", SqlStdOperatorTable.LOWER);
      operatorMap.put("trim", SqlStdOperatorTable.TRIM);
      stringFunction("ltrim");
      stringFunction("rtrim");
      numericFunction("length");

      stringFunction("like");
      stringFunction("rlike");
      stringFunction("regexp");
      stringFunction("regexp_replace");

      stringFunction("regexp_extract");
      stringFunction("parse_url");

      numericFunction("day");
      numericFunction("dayofmonth");
      numericFunction("month");
      numericFunction("year");
      numericFunction("hour");
      numericFunction("minute");
      numericFunction("second");

      numericFunction("+");
      numericFunction("-");
      numericFunction("*");
      numericFunction("/");
      numericFunction("%");
      numericFunction("div");

      numericFunction("isnull");
      numericFunction("isnotnull");

      numericFunction("if");
      numericFunction("in");
      numericFunction("and");
      numericFunction("or");
      registerFunction("=", SqlStdOperatorTable.EQUALS);
      numericFunction("==");
      numericFunction("<=>");
      numericFunction("!=");

      numericFunction("<>");
      registerFunction("<", SqlStdOperatorTable.LESS_THAN);
      registerFunction("<=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
      registerFunction(">", SqlStdOperatorTable.GREATER_THAN);
      registerFunction(">=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
      numericFunction("not");
      registerFunction("!", SqlStdOperatorTable.NOT);
      numericFunction("between");
    }

    private void stringFunction(String name) {
      registerFunction(name, SqlFunctionCategory.STRING,
          ReturnTypes.explicit(SqlTypeName.VARCHAR));
    }

    private void numericFunction(String name) {
      registerFunction(name, SqlFunctionCategory.NUMERIC,
          ReturnTypes.explicit(SqlTypeName.DECIMAL));
    }

    private void registerFunction(String name, SqlFunctionCategory cat,
        SqlReturnTypeInference rti) {
      SqlOperator optiqFn = new SqlFunction(name.toUpperCase(),
          SqlKind.OTHER_FUNCTION, rti, null, null, cat);
      registerFunction(name, optiqFn);
    }

    private void registerFunction(String name, SqlOperator optiqFn) {
      FunctionInfo hFn = FunctionRegistry.getFunctionInfo(name);
      operatorMap.put(name, optiqFn);
      hiveToOptiq.put(hFn.getGenericUDF().getClass().getName(), optiqFn);
    }
  }
}
