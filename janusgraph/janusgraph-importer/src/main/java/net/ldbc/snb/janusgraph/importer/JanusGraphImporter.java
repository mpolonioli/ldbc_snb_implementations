package net.ldbc.snb.janusgraph.importer;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;


import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JanusGraphImporter {
	private static final Logger logger =
			Logger.getLogger(JanusGraphImporter.class.getName());

	private static final long TX_MAX_RETRIES = 1000;

	private static long startLoadingVerticiesMills;

	private static long startMills;

	private static long startLoadingPropertiesMills;

	private static long startLoadingEdgesMills;

	private static long endMills;

	public static void loadVertices(JanusGraph graph, Path filePath, 
			boolean printLoadingDots, int batchSize, long progReportPeriod, int threadCount) 
					throws IOException, java.text.ParseException, InterruptedException {

		String fileNameParts[] = filePath.getFileName().toString().split("_");
		String entityName = fileNameParts[0];
		
		File file = filePath.toFile();
		Scanner fileScanner = new Scanner(file);

		final String[] colNames = fileScanner.nextLine().split("\\|");
				
		long lineCount = 0;

		// For progress reporting
		long startTime = System.currentTimeMillis();
		long nextProgReportTime = startTime + progReportPeriod*1000;
		long lastLineCount = 0;

		final String idLabel = (entityName.equals("post") || entityName.equals("comment"))?"messageId":(entityName + "Id");
			
		while(fileScanner.hasNextLine())
		{
			int batchIndex = 0;
			List<String> batchLines = new ArrayList<>();
			while(batchIndex < batchSize && fileScanner.hasNextLine())
			{
				batchLines.add(fileScanner.nextLine());
				batchIndex++;
			}
			List<Thread> threads = new ArrayList<>();
			for(int t = 0; t < threadCount; t++)
			{
				int threadStartIndex = ((batchSize / threadCount) * t);
				if (threadStartIndex >= batchLines.size())
				{
					break;
				}
				final List<String> threadLines = batchLines.subList(
						threadStartIndex, 
						Math.min(threadStartIndex + (batchSize / threadCount), batchLines.size())
						);
				
				Thread thread = new LoadVerticiesThread(
						graph,
						colNames,
						idLabel,
						entityName,
						TX_MAX_RETRIES,
						threadLines.toArray(new String[0]),
						lineCount
						);
				
				thread.setName("t" + t);
				threads.add(thread);
				thread.start();
			}
			for(Thread thread : threads)
			{
				thread.join();
			}

			lineCount += batchSize;

			if (printLoadingDots && 
					(System.currentTimeMillis() > nextProgReportTime)) {
				long timeElapsed = System.currentTimeMillis() - startTime;
				long linesLoaded = lineCount - lastLineCount;
				System.out.println(String.format(
						"Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
						(timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
				nextProgReportTime += progReportPeriod*1000;
				lastLineCount = lineCount;
			}
		}
		fileScanner.close();
	}

	public static void loadProperties(JanusGraph graph, Path filePath, 
			boolean printLoadingDots, int batchSize, long progReportPeriod, int threadCount) 
					throws IOException, InterruptedException {
		String fileNameParts[] = filePath.getFileName().toString().split("_");
		String entityName = fileNameParts[0];

		File file = filePath.toFile();
		Scanner fileScanner = new Scanner(file);

		final String[] colNames = fileScanner.nextLine().split("\\|");
				
		long lineCount = 0;

		// For progress reporting
		long startTime = System.currentTimeMillis();
		long nextProgReportTime = startTime + progReportPeriod*1000;
		long lastLineCount = 0;

		final String idLabel = (entityName.equals("post") || entityName.equals("comment"))?"messageId":(entityName + "Id");
			
		while(fileScanner.hasNextLine())
		{
			int batchIndex = 0;
			List<String> batchLines = new ArrayList<>();
			while(batchIndex < batchSize && fileScanner.hasNextLine())
			{
				batchLines.add(fileScanner.nextLine());
				batchIndex++;
			}
			List<Thread> threads = new ArrayList<>();
			for(int t = 0; t < threadCount; t++)
			{
				int threadStartIndex = ((batchSize / threadCount) * t);
				if (threadStartIndex >= batchLines.size())
				{
					break;
				}
				final List<String> threadLines = batchLines.subList(
						threadStartIndex, 
						Math.min(threadStartIndex + (batchSize / threadCount), batchLines.size())
						);
		
				Thread thread = new LoadPropertiesThread(
						graph,
						colNames,
						idLabel,
						TX_MAX_RETRIES,
						threadLines.toArray(new String[0]),
						lastLineCount
						);

				thread.setName("t" + t);
				threads.add(thread);
				thread.start();				
			}
			for(Thread thread : threads)
			{
				thread.join();
			}

			lineCount += batchSize;

			if (printLoadingDots && 
					(System.currentTimeMillis() > nextProgReportTime)) {
				long timeElapsed = System.currentTimeMillis() - startTime;
				long linesLoaded = lineCount - lastLineCount;
				System.out.println(String.format(
						"Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
						(timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
				nextProgReportTime += progReportPeriod*1000;
				lastLineCount = lineCount;
			}
		}
		fileScanner.close();
	}

	public static void loadEdges(JanusGraph graph, Path filePath, boolean undirected,
			boolean printLoadingDots, int batchSize, long progReportPeriod, int threadCount) 
					throws IOException,  java.text.ParseException, InterruptedException {

		String fileNameParts[] = filePath.getFileName().toString().split("_");
		String v1EntityName = fileNameParts[0];
		String edgeLabel = fileNameParts[1];
		String v2EntityName = fileNameParts[2];

		File file = filePath.toFile();
		Scanner fileScanner = new Scanner(file);

		final String[] colNames = fileScanner.nextLine().split("\\|");

		long lineCount = 0;

		// For progress reporting
		long startTime = System.currentTimeMillis();
		long nextProgReportTime = startTime + progReportPeriod*1000;
		long lastLineCount = 0;

		final String idLabelV1 = (v1EntityName.equals("post") || v1EntityName.equals("comment"))?"messageId":(v1EntityName + "Id");
		final String idLabelV2 = (v2EntityName.equals("post") || v2EntityName.equals("comment"))?"messageId":(v2EntityName + "Id");

		while(fileScanner.hasNextLine())
		{
			int batchIndex = 0;
			List<String> batchLines = new ArrayList<>();
			while(batchIndex < batchSize && fileScanner.hasNextLine())
			{
				batchLines.add(fileScanner.nextLine());
				batchIndex++;
			}
			List<Thread> threads = new ArrayList<>();
			for(int t = 0; t < threadCount; t++)
			{
				int threadStartIndex = ((batchSize / threadCount) * t);
				if (threadStartIndex >= batchLines.size())
				{
					break;
				}
				final List<String> threadLines = batchLines.subList(
						threadStartIndex, 
						Math.min(threadStartIndex + (batchSize / threadCount), batchLines.size())
						);

				Thread thread = new LoadEdgesThread(
						graph, 
						colNames, 
						idLabelV1, 
						idLabelV2, 
						edgeLabel, 
						undirected, 
						TX_MAX_RETRIES, 
						threadLines.toArray(new String[0]), 
						lastLineCount);

				thread.setName("t" + t);
				threads.add(thread);
				thread.start();
			}
			for(Thread thread : threads)
			{
				thread.join();
			}

			lineCount += batchSize;

			if (printLoadingDots && 
					(System.currentTimeMillis() > nextProgReportTime)) {
				long timeElapsed = System.currentTimeMillis() - startTime;
				long linesLoaded = lineCount - lastLineCount;
				System.out.println(String.format(
						"Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
						(timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
				nextProgReportTime += progReportPeriod*1000;
				lastLineCount = lineCount;
			}
		}
		fileScanner.close();
	}

	public static void main(String[] args) throws IOException {

		startMills = System.currentTimeMillis();

		// Get the required parameters from configuration file
		GetProperties propertiesGetter = new GetProperties();
		List<String> propValues = propertiesGetter.getPropValues();
		String hbaseConfFile = propValues.get(0);
		String inputBaseDir = propValues.get(1);
		int batchSize = Integer.parseInt(propValues.get(2));
		long progReportPeriod = Long.parseLong(propValues.get(3));
		int threadCount = Integer.parseInt(propValues.get(4));

		// Create the JanusGraph graph client instance using configuration file
		JanusGraph graph = JanusGraphFactory.open(hbaseConfFile);

		// Clear the existing graph
		graph.close();
		org.janusgraph.core.util.JanusGraphCleanup.clear(graph);
		graph = JanusGraphFactory.open(hbaseConfFile);

		String vertexLabels[] = {  
				"person",
				"comment",
				"forum",
				"organisation",
				"place",
				"post",
				"tag",
				"tagclass" 
		};

		String edgeLabels[] = {  
				"containerOf",
				"hasCreator",
				"hasInterest",
				"hasMember",
				"hasModerator",
				"hasTag",
				"hasType",
				"isLocatedIn",
				"isPartOf",
				"isSubclassOf",
				"knows",
				"likes",
				"replyOf",
				"studyAt",
				"workAt"
		};

		// All property keys with Cardinality.SINGLE of type String
		String singleCardPropKeysString[] = {
				//"birthday", // person
				"browserUsed", // comment person post
				//"classYear", // studyAt
				"content", // comment post
				//"creationDate", // comment forum person post knows likes
				"firstName", // person
				"gender", // person
				"imageFile", // post
				//"joinDate", // hasMember
				//"language", // post
				"lastName", // person
				//"length", // comment post
				"locationIP", // comment person post
				"name", // organisation place tag tagclass
				"title", // forum
				"type", // organisation place
				"url", // organisation place tag tagclass
				//"workFrom", // workAt
		};

		// All property keys with Cardinality.SINGLE of type Long
		String singleCardPropKeysLong[] = {
				"birthday", // person
				"creationDate", // comment forum person post knows likes
				"joinDate", // hasMember
		};

		// All property keys with Cardinality.SINGLE of type Integer
		String singleCardPropKeysInteger[] = {
				"classYear", // studyAt
				"workFrom", // workAt
				"length", // comment post
		};

		// All property keys with Cardinality.LIST
		String listCardPropKeys[] = {
				"email", // person
				"language" // person, post
		};

		// All property keys representing the SNB ids of the Vertex
		String idLabels[] = {  
				"personId",
				"forumId",
				"organisationId",
				"placeId",
				"tagId",
				"tagclassId",
				//"commentId",
				//"postId",
				"messageId"
		};

		/*
		 * Explicitly define the graph schema.
		 */
		try {
			System.out.println("Explicitly define the graph schema");
			JanusGraphManagement mgmt;

			System.out.println("Declaring all vertex labels");
			// Declare all vertex labels.
			for( String vLabel : vertexLabels ) {
				System.out.print(vLabel + " ");
				mgmt = graph.openManagement();
				mgmt.makeVertexLabel(vLabel).make();
				mgmt.commit();
			}

			System.out.println("\nDeclaring all edge labels");
			// Declare all edge labels.
			for( String eLabel : edgeLabels ) {
				System.out.print(eLabel + " ");
				mgmt = graph.openManagement();
				mgmt.makeEdgeLabel(eLabel).multiplicity(Multiplicity.SIMPLE).make();
				mgmt.commit();
			}

			System.out.println("\nDeclaring all properties with Cardinality.SINGLE of type String");
			// Delcare all properties with Cardinality.SINGLE of type String
			for ( String propKey : singleCardPropKeysString ) {
				System.out.print(propKey + " ");
				mgmt = graph.openManagement();
				mgmt.makePropertyKey(propKey).dataType(String.class)
				.cardinality(Cardinality.SINGLE).make();     
				mgmt.commit();
			}

			System.out.println("\nDeclaring all properties with Cardinality.SINGLE of type Long");
			// Delcare all properties with Cardinality.SINGLE of type Long
			for ( String propKey : singleCardPropKeysLong ) {
				System.out.print(propKey + " ");
				mgmt = graph.openManagement();
				mgmt.makePropertyKey(propKey).dataType(Long.class)
				.cardinality(Cardinality.SINGLE).make();     
				mgmt.commit();
			}

			System.out.println("\nDeclaring all properties with Cardinality.SINGLE of type Integer");
			// Delcare all properties with Cardinality.SINGLE of type Integer
			for ( String propKey : singleCardPropKeysInteger ) {
				System.out.print(propKey + " ");
				mgmt = graph.openManagement();
				mgmt.makePropertyKey(propKey).dataType(Integer.class)
				.cardinality(Cardinality.SINGLE).make();     
				mgmt.commit();
			}

			System.out.println("\nDeclaring all properties with Cardinality.LIST");
			// Delcare all properties with Cardinality.LIST
			for ( String propKey : listCardPropKeys ) {
				System.out.print(propKey + " ");
				mgmt = graph.openManagement();
				mgmt.makePropertyKey(propKey).dataType(String.class)
				.cardinality(Cardinality.LIST).make();     
				mgmt.commit();
			}

			/* 
			 * Create a special ID property where we will store the IDs of
			 * vertices in the SNB dataset, and a corresponding index, for each vertex. 
			 * This is necessary because JanusGraphDB generates its own IDs for graph
			 * vertices, but the benchmark references vertices by the ID they
			 * were originally assigned during dataset generation.
			 */
			System.out.println("\nDeclaring all id properties and relatives index");
			for( String idLabel : idLabels ) {
				mgmt = graph.openManagement();
				System.out.print(idLabel + "|");
				PropertyKey id = mgmt.makePropertyKey(idLabel).dataType(Long.class)
						.cardinality(Cardinality.SINGLE).make(); 
				String indexLabel = "by" + idLabel.substring(0, 1).toUpperCase() + idLabel.substring(1);
				System.out.print(indexLabel + " ");
				mgmt.buildIndex(indexLabel, Vertex.class).addKey(id).buildCompositeIndex();;
				mgmt.commit();
			}
			System.out.println("\nGraph schema explicitly defined");
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.toString());
			return;
		}

		// TODO: Make file list generation programmatic. This method of loading,
		// however, will be far too slow for anything other than the very 
		// smallest of SNB graphs, and is therefore quite transient. This will
		// do for now.
		String nodeFiles[] = {  
				"person_0_0.csv",
				"comment_0_0.csv",
				"forum_0_0.csv",
				"organisation_0_0.csv",
				"place_0_0.csv",
				"post_0_0.csv",
				"tag_0_0.csv",
				"tagclass_0_0.csv" 
		};

		String propertiesFiles[] = {    
				"person_email_emailaddress_0_0.csv",
				"person_speaks_language_0_0.csv"
		};

		String edgeFiles[] = {  
				"comment_hasCreator_person_0_0.csv",
				"comment_hasTag_tag_0_0.csv",
				"comment_isLocatedIn_place_0_0.csv",
				"comment_replyOf_comment_0_0.csv",
				"comment_replyOf_post_0_0.csv",
				"forum_containerOf_post_0_0.csv",
				"forum_hasMember_person_0_0.csv",
				"forum_hasModerator_person_0_0.csv",
				"forum_hasTag_tag_0_0.csv",
				"organisation_isLocatedIn_place_0_0.csv",
				"person_hasInterest_tag_0_0.csv",
				"person_isLocatedIn_place_0_0.csv",
				"person_knows_person_0_0.csv",
				"person_likes_comment_0_0.csv",
				"person_likes_post_0_0.csv",
				"person_studyAt_organisation_0_0.csv",
				"person_workAt_organisation_0_0.csv",
				"place_isPartOf_place_0_0.csv",
				"post_hasCreator_person_0_0.csv",
				"post_hasTag_tag_0_0.csv",
				"post_isLocatedIn_place_0_0.csv",
				"tag_hasType_tagclass_0_0.csv",
				"tagclass_isSubclassOf_tagclass_0_0.csv"
		};

		System.out.println("Start loading data");
		try {
			startLoadingVerticiesMills = System.currentTimeMillis();
			System.out.println("Loading verticies");
			for (String fileName : nodeFiles) {
				System.out.println("Loading node file " + fileName);
				try {
					loadVertices(graph, Paths.get(inputBaseDir + File.separator + fileName), 
							true, batchSize, progReportPeriod, threadCount);
					System.out.println("Finished");
				} catch (NoSuchFileException e) {
					System.out.println(" File not found.");
				}
			}			
			startLoadingPropertiesMills = System.currentTimeMillis();
			for (String fileName : propertiesFiles) {
				System.out.println("Loading properties file " + fileName);
				try {
					loadProperties(graph, Paths.get(inputBaseDir + File.separator + fileName), 
							true, batchSize, progReportPeriod, threadCount);
					System.out.println("Finished");
				} catch (NoSuchFileException e) {
					System.out.println(" File not found.");
				}
			}
			startLoadingEdgesMills = System.currentTimeMillis();
			for (String fileName : edgeFiles) {
				System.out.println("Loading edge file " + fileName);
				try {
					if (fileName.contains("person_knows_person")) {
						loadEdges(graph, Paths.get(inputBaseDir + File.separator + fileName), true, 
								true, batchSize, progReportPeriod, threadCount);
					} else {
						loadEdges(graph, Paths.get(inputBaseDir + File.separator + fileName), false, 
								true, batchSize, progReportPeriod, threadCount);
					}
					System.out.println("Finished");
				} catch (NoSuchFileException e) {
					System.out.println(" File not found.");
				}
			}
			System.out.println("Finished loading data");
			endMills = System.currentTimeMillis();
		} catch (Exception e) {
			System.out.println("Exception: " + e);
			e.printStackTrace();
		} finally {
			graph.close();
		}
		
		System.out.println();
		System.out.println("Time needed for loading schema in milliseconds: " + (startLoadingVerticiesMills - startMills));
		System.out.println("Time needed for loading verticies in milliseconds: " + (startLoadingPropertiesMills - startLoadingVerticiesMills));
		System.out.println("Time needed for loading properties in milliseconds: " + (startLoadingEdgesMills - startLoadingPropertiesMills));
		System.out.println("Time needed for loading edges in milliseconds: " + (endMills - startLoadingEdgesMills));
		System.out.println("Total duration in milliseconds: " + (endMills - startMills) + "\n");

		System.out.println(String.format(
				"Time Elapsed for loading schema: %03dh.%02dm.%02ds",
				((((startLoadingVerticiesMills - startMills) / 1000) / 60) / 60), ((((startLoadingVerticiesMills - startMills) / 1000) / 60) % 60), (((startLoadingVerticiesMills - startMills) / 1000) % 60)));

		System.out.println(String.format(
				"Time Elapsed for loading verticies: %03dh.%02dm.%02ds",
				((((startLoadingPropertiesMills - startLoadingVerticiesMills) / 1000) / 60) / 60), ((((startLoadingPropertiesMills - startLoadingVerticiesMills) / 1000) / 60) % 60), (((startLoadingPropertiesMills - startLoadingVerticiesMills) / 1000) % 60)));
		
		System.out.println(String.format(
				"Time Elapsed for loading properties: %03dh.%02dm.%02ds",
				((((startLoadingEdgesMills - startLoadingPropertiesMills) / 1000) / 60) / 60), ((((startLoadingEdgesMills - startLoadingPropertiesMills) / 1000) / 60) % 60), (((startLoadingEdgesMills - startLoadingPropertiesMills) / 1000) % 60)));

		System.out.println(String.format(
				"Time Elapsed for loading edges: %03dh.%02dm.%02ds",
				((((endMills - startLoadingEdgesMills) / 1000) / 60) / 60), ((((endMills - startLoadingEdgesMills) / 1000) / 60) % 60), (((endMills - startLoadingEdgesMills) / 1000) % 60)));

		System.out.println(String.format(
				"Total duration: %03dh.%02dm.%02ds",
				((((endMills - startMills) / 1000) / 60) / 60), ((((endMills - startMills) / 1000) / 60) % 60), (((endMills - startMills) / 1000) % 60)));
	}
}
