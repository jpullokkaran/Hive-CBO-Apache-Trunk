package org.apache.hadoop.hive.ql.optimizer.optiq.expr;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.optiq.schema.TypeConverter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.sql.type.ExplicitReturnTypeInference;
import org.eigenbase.sql.type.SqlReturnTypeInference;
import org.eigenbase.sql.type.SqlTypeName;

public class SqlFunctionConverter {

	static Map<String, SqlOperator> operatorMap = new HashMap<String, SqlOperator>();
	static Map<String, SqlOperator> hiveToOptiq = new HashMap<String, SqlOperator>();
	
	
	static {
		registerFunction("concat", SqlStdOperatorTable.concatOperator);
		registerFunction("substr", SqlStdOperatorTable.substringFunc);
		registerFunction("substring", SqlStdOperatorTable.substringFunc);
		stringFunction("space");
		stringFunction("repeat");
		numericFunction("ascii");
		stringFunction("repeat");
		
		numericFunction("size");
		
		numericFunction("round");
		registerFunction("floor", SqlStdOperatorTable.floorFunc);
		registerFunction("sqrt", SqlStdOperatorTable.sqrtFunc);
		registerFunction("ceil", SqlStdOperatorTable.ceilFunc);
		registerFunction("ceiling", SqlStdOperatorTable.ceilFunc);
		numericFunction("rand");
		operatorMap.put("abs", SqlStdOperatorTable.absFunc);
		numericFunction("pmod");
		
		numericFunction("ln");
		numericFunction("log2");
		numericFunction("sin");
		numericFunction("asin");
		numericFunction("cos");
		numericFunction("acos");
		registerFunction("log10", SqlStdOperatorTable.log10Func);
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
		
		
		registerFunction("upper", SqlStdOperatorTable.upperFunc);
		registerFunction("lower", SqlStdOperatorTable.lowerFunc);
		registerFunction("ucase", SqlStdOperatorTable.upperFunc);
		registerFunction("lcase", SqlStdOperatorTable.lowerFunc);
		operatorMap.put("trim", SqlStdOperatorTable.trimFunc);
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
		registerFunction("=", SqlStdOperatorTable.equalsOperator);
		numericFunction("==");
		numericFunction("<=>");
		numericFunction("!=");
		
		numericFunction("<>");
		registerFunction("<", SqlStdOperatorTable.lessThanOperator);
		registerFunction("<=", SqlStdOperatorTable.lessThanOrEqualOperator);
		registerFunction(">", SqlStdOperatorTable.greaterThanOperator);
		registerFunction(">=", SqlStdOperatorTable.greaterThanOrEqualOperator);
		numericFunction("not");
		registerFunction("!", SqlStdOperatorTable.notOperator);
		numericFunction("between");
		
	}
	
	public static SqlOperator getOptiqOperator(GenericUDF hiveUDF) {
		return hiveToOptiq.get(hiveUDF.getClass().getName());
	}
	
	private static void stringFunction(String name) {
		registerFunction( name, SqlFunctionCategory.String, ExplicitReturnTypeInference.of(new BasicSqlType(SqlTypeName.VARCHAR)));
	}
	
	private static void numericFunction(String name) {
		registerFunction( name, SqlFunctionCategory.Numeric, ExplicitReturnTypeInference.of(new BasicSqlType(SqlTypeName.DECIMAL)));
	}
	
	private static void registerFunction(String name, SqlFunctionCategory cat, SqlReturnTypeInference rti) {
		SqlOperator optiqFn = new SqlFunction(
	            name.toUpperCase(),
	            SqlKind.OTHER_FUNCTION,
	            rti,
	            null,
	            null,
	            cat);
		registerFunction(name, optiqFn);
	}
	
	private static void registerFunction(String name, SqlOperator optiqFn) {
		FunctionInfo hFn = FunctionRegistry.getFunctionInfo(name);
		operatorMap.put(name, optiqFn);
		hiveToOptiq.put(hFn.getGenericUDF().getClass().getName(), optiqFn);
	}
	
}
