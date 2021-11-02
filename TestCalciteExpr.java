package com.linkedin.beam.sql;

import java.util.Collections;
import java.util.Properties;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Test;


// NOT Working :(
public class TestCalciteExpr {

  @Test
  public void testSql1() throws Exception {
    String expr = "tmp.a";
    SqlParser sqlParser = SqlParser.create(expr, SqlParser.configBuilder()
        .setLex(Lex.JAVA)
        .setConformance(SqlConformanceEnum.LENIENT)
        .setCaseSensitive(true) // Make Udfs case insensitive
        .build());
    SqlNode sqlParseTree = sqlParser.parseExpression();

    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder().build();
    PlannerImpl planner = new PlannerImpl(frameworkConfig);

    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true, true);

    rootSchema.add("tmp", new ReflectiveSchema(new PC()));

    Properties calciteConnectionConfigProperties = new Properties();
    CalciteConnectionConfigImpl calciteConnectionConfigImpl = new CalciteConnectionConfigImpl(calciteConnectionConfigProperties);
    SqlTypeFactoryImpl sqlTypeFactoryImpl = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    CalciteCatalogReader calciteCatelogReader = new CalciteCatalogReader(rootSchema, Collections.emptyList(), sqlTypeFactoryImpl, calciteConnectionConfigImpl);
    SqlValidator defaultValidator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatelogReader, sqlTypeFactoryImpl,
        SqlValidator.Config.DEFAULT.withSqlConformance(SqlConformanceEnum.LENIENT));

    RelOptCluster relExpressionOptimizationCluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(sqlTypeFactoryImpl));

    SqlToRelConverter.Config sqlToRelConfig = SqlToRelConverter.configBuilder().build();

    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(planner, defaultValidator, calciteCatelogReader, relExpressionOptimizationCluster, StandardConvertletTable.INSTANCE, sqlToRelConfig);

    sqlToRelConverter.convertExpression(sqlParseTree);
  }

  static final class PC {
    public int a;

    public int getA() {
      return a;
    }

    public void setA(int a) {
      this.a = a;
    }
  }

}
