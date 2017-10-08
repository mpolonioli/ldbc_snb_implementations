mvn clean compile assembly:single
java -cp ./target/janusgraph-importer-0.0.1-SNAPSHOT-jar-with-dependencies.jar:./resources net.ldbc.snb.janusgraph.janusgraph_importer.JanusGraphImporter