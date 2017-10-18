package net.ldbc.snb.janusgraph.importer;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class LoadPropertiesThread extends Thread {


	private JanusGraph graph;
	private String[] colNames;
	private String idLabel;
	private long txMaxRetries;
	private String[] threadLines;
	private long lineCount;
	
	public LoadPropertiesThread(
			JanusGraph graph,
			String[] colNames,
			String idLabel,
			long txMaxRetries,
			String[] threadLines,
			long lineCount
			) {
		this.graph = graph;
		this.colNames = colNames;
		this.idLabel = idLabel;
		this.threadLines = threadLines;
		this.lineCount = lineCount;
	}

	@Override
	public void run() {

		SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		SimpleDateFormat creationDateDateFormat = 
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		boolean txSucceeded = false;
		int txFailCount = 0;
		do {
			JanusGraphTransaction tx = graph.newTransaction();
			for (int i = 0; i < threadLines.length; i++) {
				String line = threadLines[i];

				String[] colVals = line.split("\\|");

				GraphTraversalSource g = tx.traversal();
				Vertex vertex = 
						g.V().has(idLabel, Long.parseLong(colVals[0])).next();

				for (int j = 1; j < colVals.length; ++j) {
					vertex.property(VertexProperty.Cardinality.list, colNames[j],
							colVals[j]);
				}
			}

			try {
				tx.commit();
				txSucceeded = true;
			} catch (Exception e) {
				txFailCount++;
			}

			if (txFailCount > txMaxRetries) {
				throw new RuntimeException(String.format(
						"ERROR: Transaction failed %d times, (file lines [%d,%d])" +  
								"aborting...", txFailCount, lineCount + 2, (lineCount + 2) + (threadLines.length - 1)));
			}
		} while (!txSucceeded);

	}
}
