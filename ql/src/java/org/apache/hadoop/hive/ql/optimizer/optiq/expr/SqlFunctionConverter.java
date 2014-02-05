package org.apache.hadoop.hive.ql.optimizer.optiq.expr;

import java.util.*;

import com.google.common.collect.*;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
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
	static final Map<SqlOperator, HiveToken> optiqToHiveToken;

	static {
    Builder builder = new Builder();
    operatorMap = ImmutableMap.copyOf(builder.operatorMap);
    hiveToOptiq = ImmutableMap.copyOf(builder.hiveToOptiq);
    optiqToHiveToken = ImmutableMap.copyOf(builder.optiqToHiveToken);
  }

  public static SqlOperator getOptiqOperator(GenericUDF hiveUDF) {
    return hiveToOptiq.get(hiveUDF.getClass().getName());
  }
  
  public static ASTNode buildAST(SqlOperator op, List<ASTNode> children) {
	  HiveToken hToken = optiqToHiveToken.get(op);
	  ASTNode node;
	  if ( hToken != null ) {
		 node = (ASTNode) ParseDriver.adaptor.create(hToken.type, hToken.text);
	  } else {
		  node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
		  node.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, op.getName()));
	  }
	  
	  for(ASTNode c : children) {
		  ParseDriver.adaptor.addChild(node, c);
	  }
	  return node;
  }

  private static class Builder {
    final Map<String, SqlOperator> operatorMap = Maps.newHashMap();
    final Map<String, SqlOperator> hiveToOptiq = Maps.newHashMap();
    final Map<SqlOperator, HiveToken> optiqToHiveToken = Maps.newHashMap();

    Builder() {
      registerFunction("concat", SqlStdOperatorTable.CONCAT, null);
      registerFunction("substr", SqlStdOperatorTable.SUBSTRING, null);
      registerFunction("substring", SqlStdOperatorTable.SUBSTRING, null);
      stringFunction("space");
      stringFunction("repeat");
      numericFunction("ascii");
      stringFunction("repeat");

      numericFunction("size");

      numericFunction("round");
      registerFunction("floor", SqlStdOperatorTable.FLOOR, null);
      registerFunction("sqrt", SqlStdOperatorTable.SQRT, null);
      registerFunction("ceil", SqlStdOperatorTable.CEIL, null);
      registerFunction("ceiling", SqlStdOperatorTable.CEIL, null);
      numericFunction("rand");
      operatorMap.put("abs", SqlStdOperatorTable.ABS);
      numericFunction("pmod");

      numericFunction("ln");
      numericFunction("log2");
      numericFunction("sin");
      numericFunction("asin");
      numericFunction("cos");
      numericFunction("acos");
      registerFunction("log10", SqlStdOperatorTable.LOG10, null);
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


      registerFunction("upper", SqlStdOperatorTable.UPPER, null);
      registerFunction("lower", SqlStdOperatorTable.LOWER, null);
      registerFunction("ucase", SqlStdOperatorTable.UPPER, null);
      registerFunction("lcase", SqlStdOperatorTable.LOWER, null);
      registerFunction("trim", SqlStdOperatorTable.TRIM, null);
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

      registerFunction("+", SqlStdOperatorTable.PLUS, hToken(HiveParser.PLUS, "+"));
      registerFunction("-", SqlStdOperatorTable.MINUS, hToken(HiveParser.MINUS, "-"));
      registerFunction("*", SqlStdOperatorTable.MULTIPLY, hToken(HiveParser.STAR, "*"));
      registerFunction("/", SqlStdOperatorTable.DIVIDE, hToken(HiveParser.STAR, "/"));
      registerFunction("%", SqlStdOperatorTable.MOD, hToken(HiveParser.STAR, "%"));
      numericFunction("div");

      numericFunction("isnull");
      numericFunction("isnotnull");

      numericFunction("if");
      numericFunction("in");
      registerFunction("and", SqlStdOperatorTable.AND, hToken(HiveParser.KW_AND, "and"));
      registerFunction("or", SqlStdOperatorTable.OR, hToken(HiveParser.KW_OR, "or"));
      registerFunction("=", SqlStdOperatorTable.EQUALS, hToken(HiveParser.EQUAL, "="));
      numericFunction("==");
      numericFunction("<=>");
      numericFunction("!=");

      numericFunction("<>");
      registerFunction("<", SqlStdOperatorTable.LESS_THAN, hToken(HiveParser.LESSTHAN, "<"));
      registerFunction("<=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL, hToken(HiveParser.LESSTHANOREQUALTO, "<="));
      registerFunction(">", SqlStdOperatorTable.GREATER_THAN, hToken(HiveParser.GREATERTHAN, ">"));
      registerFunction(">=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, hToken(HiveParser.GREATERTHANOREQUALTO, ">="));
      numericFunction("not");
      registerFunction("!", SqlStdOperatorTable.NOT, hToken(HiveParser.KW_NOT, "not"));
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
      registerFunction(name, optiqFn, null);
    }

    private void registerFunction(String name, SqlOperator optiqFn, HiveToken hiveToken) {
      FunctionInfo hFn = FunctionRegistry.getFunctionInfo(name);
      operatorMap.put(name, optiqFn);
      hiveToOptiq.put(hFn.getGenericUDF().getClass().getName(), optiqFn);
      if ( hiveToken != null ) {
    	  optiqToHiveToken.put(optiqFn, hiveToken);
      }
    }
  }
  
  private static HiveToken hToken(int type, String text) {
	  return new HiveToken(type, text);
  }
  
  static class HiveToken {
	  int type;
	  String text;
	  
	  HiveToken(int type, String text) {
		  this.type = type;
		  this.text = text;
	  }
  }
}
