/**
 *  Big Data Systems (Fall, 2021)
 *  Hands-on 1
 *  University of New Brunswick, Fredericton
 */

package f2021.handson1;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.apache.calcite.adapter.csv.CsvSchemaFactory; 
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableMap;

public class BDS_handson1 {
	
	 private final boolean verbose= true;
	 private final String SCHEMA = "world";
	 private final String DATA_MODEL = "worldmodel"; 
	  
	 Connection calConn = null;
	 
	 public static void main(String[] args) { 
		 new BDS_handson1().runAll();
	 }


	 
	//--------------------------------------------------------------------------------------
	 
	 /**
	  * Using relational algebra expression implement the query:
	  * Query 1: Show the details (model, MPG, and country) of the top 5 most fuel efficient cars. 
	  * Note: fuel efficiency is in MPG and higher the better.
	  * 
	  */
	 private void runQuery1(RelBuilder builder) { 
		 System.out.println("\nRunning Q1: Show the details (model, MPG, and country of origin) of the top 5 most fuel efficient cars.");

//		  * SELECT Model, MPG, Origin 
//		  * FROM cars
//		  * SORTED DES BY MPG
		 
		 // write your relational algebra expression here
		 builder
		 .scan("cars")
		 .sort( builder.desc( builder.field("MPG")) )
		 .limit(0, 5)
		 .project(builder.field("Model"), builder.field("MPG"), builder.field("Origin"));
			 
		 //keep the following code template to build, show and execute the relational algebra expression
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
			    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
				 while (rs.next()) {
					 System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
				 }
				 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
		
	 }
	 
	//--------------------------------------------------------------------------------------
	 
	 /**
	  * Using relational algebra expression implement the query:
	  * Query 2: For each country of origin, show the average fuel economy (MPG) of the cars, where the average fuel economy is greater than 25
	  */
	 private void runQuery2(RelBuilder builder) { 
		 System.out.println("\nRunning Q2: For each country of origin, show the average fuel economy (MPG) of the cars, where the average fuel economy is greater than 25 ");
		
//		  * SELECT Origin, Average(MPG) AS C, 
//		  * FROM cars
//		  * GROUP BY Origin
//		  * HAVING C > 25
		  
		// write your relational algebra expression here
		 builder
		 .scan("cars")
		 .aggregate(builder.groupKey("Origin"),
				    builder.avg(false, "C", builder.field("MPG") )
		            )
		 .filter( builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"), builder.literal(25)));
			 
		 //keep the following code template to build, show and execute the relational algebra expression
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
			    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
				 while (rs.next()) {
					 System.out.println(rs.getString(1) + " " + rs.getString(2));
				 }
				 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
		 
	 }
		 
	//--------------------------------------------------------------------------------------
	 
	 /**
	  *  Using relational algebra expression implement the query:
	  *  Query 3: For each country, show the name of the country, currency, and the number of cars originated in that country. 
	  */
	 private void runQuery3(RelBuilder builder) { 
		 System.out.println("\nRunning Q3: For each country, show the name of the country, currency, and the number of cars originated in that country.");
							 

//		  * SELECT Country, Currency, Count(Origin) AS TotalCars, 
//		  * FROM  cars INNER JOIN countries
//		  * GROUP BY Origin
//		  * Aggregate count(Origin)
		  
		// write your relational algebra expression here
		 builder
		 .scan("countries").as("co")
		 .scan("cars")
		 .aggregate(builder.groupKey("Origin"),
				    builder.count(false, "TotalCars"))
		 .join(JoinRelType.INNER)
		 .filter(builder.equals(builder.field("co", "Country"), builder.field("Origin")))
		 .project(builder.field("co", "Country"), builder.field("co", "Currency"), builder.field("TotalCars"));
		 
		 
//		 builder
//		 .scan("countries").as("co")
//		 .project(builder.field("Country"), builder.field("Currency"))
//		 .scan("cars").as("ca")
//		 .join(JoinRelType.INNER)
//		 .aggregate(builder.groupKey("Country", "Currency", "Origin") )
//		 .filter( builder.equals(builder.field("Country"), builder.field("Origin")));
		 
//		 builder
//		 .scan("cars").as("ca")
//		 .scan("countries").as("co")
//		 .join(JoinRelType.INNER)
//
//		 .aggregate(builder.groupKey("Country", "Currency"),
//				    builder.count(false, "O", builder.field("Origin"))
//		            )
//		 .filter( builder.equals(builder.field("co", "Country"), builder.field("ca", "Origin")));
		 
		 
		 //keep the following code template to build, show and execute the relational algebra expression
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
			    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
			 int i = 0;
				 while (rs.next()) {
					 i++;
					 System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " => " + i);
//					 System.out.println(rs.getString(1) + " " + rs.getString(2) + " => " + i);
					 
				 }
				 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
		 
	 }
	 
	//--------------------------------------------------------------------------------------
	 
	 /**
	  * Example query: Show all the Models from the cars table 
	  */
	 private void runQuery0(RelBuilder builder) { 
		 System.out.println("\nRunning Q0: Show all the Models from the cars table ");
			 
		 // write your relational algebra expression here
		 builder
		 .scan("cars")
		 .project(builder.field("Model"));
			 
		 //keep the following code template to build, show and execute the relational algebra expression
		 final RelNode node = builder.build();
		 if (verbose) {
			 System.out.println(RelOptUtil.toString(node));
		 }
			    
		 // execute the query plan
		 try  {
			 final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			 ResultSet rs =  preparedStatement.executeQuery();
				 while (rs.next()) {
					 String model = rs.getString(1);
					// System.out.println(model);
				 }
				 rs.close();	 
		 } catch (SQLException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
	 }


	//--------------------------------------------------------------------------------------
	
        // setting all up
	
	//---------------------------------------------------------------------------------------
	public void runAll() {
		// Create a builder. The config contains a schema mapped
		final FrameworkConfig config = buildConfig();  
		final RelBuilder builder = RelBuilder.create(config);
			  
		for (int i = 0; i <= 3; i++) {
			runQueries(builder, i);
		}
	}

		 
	// Running the examples
	private void runQueries(RelBuilder builder, int i) {
		switch (i) {
		case 0:
			runQuery0(builder);
			break;
				 
		case 1:
			runQuery1(builder);
			break;
				 
		case 2:
			runQuery2(builder);
			break;
				 
		case 3:
			runQuery3(builder);
			break;
		}
	}
		 
	private String jsonPath(String model) {
		return resourcePath(model + ".json");
	}

	private String resourcePath(String path) {
		final URL url = BDS_handson1.class.getResource("/resources/" + path);
		 
		String s = url.toString();
		if (s.startsWith("file:")) {
			 s = s.substring("file:".length());
		 }
		 return s;
	}
		  
	private FrameworkConfig  buildConfig() {
		 FrameworkConfig calciteFrameworkConfig= null;
			  
		 Connection connection = null;
		 Statement statement = null;
		 try {
			 Properties info = new Properties();
			 info.put("model", jsonPath(DATA_MODEL));
			 connection = DriverManager.getConnection("jdbc:calcite:", info);
			 
			 final CalciteConnection calciteConnection = connection.unwrap(
					 CalciteConnection.class);

			 calConn = calciteConnection;
			 SchemaPlus rootSchemaPlus = calciteConnection.getRootSchema();
			      
			 final Schema schema =
					 CsvSchemaFactory.INSTANCE
					 .create(rootSchemaPlus, null,
							 ImmutableMap.<String, Object>of("directory",
									 resourcePath(SCHEMA), "flavor", "scannable"));
			      

			 SchemaPlus dbSchema = rootSchemaPlus.getSubSchema(SCHEMA);
			    		  
			 // Set<String> tables= schema.getTableNames();
			 // for (String t: tables)
			 //	  System.out.println(t);
			      
			 System.out.println("Available tables in the database:");
			 Set<String>  tables=rootSchemaPlus.getSubSchema(SCHEMA).getTableNames();
			 for (String t: tables)
				 System.out.println(t);
			       
			      //final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
			     
			      final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

			      traitDefs.add(ConventionTraitDef.INSTANCE);
			      traitDefs.add(RelCollationTraitDef.INSTANCE);

			      calciteFrameworkConfig = Frameworks.newConfigBuilder()
			          .parserConfig(SqlParser.configBuilder()
			        	// Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
			            // case when they are read, and whether identifiers are matched case-sensitively.
			          .setLex(Lex.MYSQL)
			          .build())
			          // Sets the schema to use by the planner
			          .defaultSchema(dbSchema) 
			          .traitDefs(traitDefs)
			          // Context provides a way to store data within the planner session that can be accessed in planner rules.
			          .context(Contexts.EMPTY_CONTEXT)
			          // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
			          .ruleSets(RuleSets.ofList())
			          // Custom cost factory to use during optimization
			          .costFactory(null)
			          .typeSystem(RelDataTypeSystem.DEFAULT)
			          .build();
			     
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
		 return calciteFrameworkConfig;
	}
	
}
