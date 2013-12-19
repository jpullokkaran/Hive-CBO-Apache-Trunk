package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.type.SqlTypeName;

public class HiveToOptiqTypeConverter {

  public static RelDataType getType(RelOptCluster cluster, Operator hiveOp) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
    RowSchema rs = hiveOp.getSchema();
    List<RelDataType> fieldTypeInOptiq = new LinkedList<RelDataType>();
    List<String> fieldNames = new LinkedList<String>();
    String colName;

    for (ColumnInfo ci : rs.getSignature()) {
      if (!ci.isHiddenVirtualCol()) {
        fieldTypeInOptiq.add(convert(ci.getType(), dtFactory));
        // TODO: 1) Should this be field alias or not?
        // 2) Should this be adding table alias
        colName = (ci.getAlias() != null) ? ci.getAlias() : ci.getInternalName();
        fieldNames.add(colName);
      }
    }
    return dtFactory.createStructType(fieldTypeInOptiq, fieldNames);
  }

  public static RelDataType convert(TypeInfo type, RelDataTypeFactory dtFactory) {
    RelDataType convertedType = null;

    switch (type.getCategory()) {
    case PRIMITIVE:
      convertedType = convertPrimitiveType((PrimitiveTypeInfo) type, dtFactory);
      break;
    case LIST:
      convertedType = convert((ListTypeInfo) type, dtFactory);
      break;
    case MAP:
      convertedType = convert((MapTypeInfo) type, dtFactory);
      break;
    case STRUCT:
      convertedType = convert((StructTypeInfo) type, dtFactory);
      break;
    case UNION:
      convertedType = convert((UnionTypeInfo) type, dtFactory);
      break;
    default:
      break;
    }
    return convertedType;
  }

  private static RelDataType convertPrimitiveType(PrimitiveTypeInfo type,
      RelDataTypeFactory dtFactory) {
    RelDataType convertedType = null;

    switch (type.getPrimitiveCategory()) {
    case VOID:
      break;
    case BOOLEAN:
      convertedType = dtFactory.createSqlType(SqlTypeName.BOOLEAN);
      break;
    case BYTE:
      convertedType = dtFactory.createSqlType(SqlTypeName.TINYINT);
      break;
    case SHORT:
      convertedType = dtFactory.createSqlType(SqlTypeName.SMALLINT);
      break;
    case INT:
      convertedType = dtFactory.createSqlType(SqlTypeName.INTEGER);
      break;
    case LONG:
      convertedType = dtFactory.createSqlType(SqlTypeName.BIGINT);
      break;
    case FLOAT:
      convertedType = dtFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    case DOUBLE:
      convertedType = dtFactory.createSqlType(SqlTypeName.DOUBLE);
      break;
    case STRING:
      convertedType = dtFactory.createSqlType(SqlTypeName.VARCHAR, 1);
      break;
    case DATE:
      convertedType = dtFactory.createSqlType(SqlTypeName.DATE);
      break;
    case TIMESTAMP:
      convertedType = dtFactory.createSqlType(SqlTypeName.TIMESTAMP);
      break;
    case BINARY:
      convertedType = dtFactory.createSqlType(SqlTypeName.BINARY);
      break;
    case DECIMAL:
      convertedType = dtFactory.createSqlType(SqlTypeName.DECIMAL);
      break;
    case UNKNOWN:
      break;
    }

    return convertedType;
  }

  static RelDataType convert(ListTypeInfo lstType, RelDataTypeFactory dtFactory) {
    return null;
  }

  static RelDataType convert(MapTypeInfo mapType, RelDataTypeFactory dtFactory) {
    return null;
  }

  static RelDataType convert(PrimitiveTypeInfo primitiveType, RelDataTypeFactory dtFactory) {
    return null;
  }

  static RelDataType convert(StructTypeInfo structType, RelDataTypeFactory dtFactory) {
    return null;
  }

  static RelDataType convert(UnionTypeInfo unionType, RelDataTypeFactory dtFactory) {
    return null;
  }
}
