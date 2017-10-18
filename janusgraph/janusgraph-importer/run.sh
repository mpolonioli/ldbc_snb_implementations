mvn clean compile assembly:single
java -Xmx4g -cp ./target/janusgraph-importer-0.0.1-SNAPSHOT-jar-with-dependencies.jar:./resources net.ldbc.snb.janusgraph.importer.JanusGraphImporter