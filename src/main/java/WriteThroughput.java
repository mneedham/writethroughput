import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.util.StringLogger;

public class WriteThroughput
{
    private static final int MAX_ITERATIONS = 1000;
    public static final int BATCH_SIZE = 1;
    private int batchSize;
    private Path dbPath;

    public WriteThroughput( int batchSize ) throws IOException
    {
        this.batchSize = batchSize;
        this.dbPath = Files.createTempDirectory( "" );
    }

    public void go()
    {
        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath.toString() );
        ExecutionEngine engine = new ExecutionEngine( db, StringLogger.DEV_NULL );

        long rootNodeId = createRootNode( db );

        long startTime = System.currentTimeMillis();

        for ( int i = 0; i < MAX_ITERATIONS; i++ )
        {
            Transaction tx = db.beginTx();
            try
            {

                for ( int batch = 0; batch < BATCH_SIZE; i++, batch++ )
                {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put( "name", "name" + i );
                    final ExecutionResult executionResult = engine.execute(
                            "START root = node(" + rootNodeId + ") " +
                            "CREATE (n {name: {name}}) -[:LIKES]->(root) return n as theNode",
                            properties );

                    indexIt( db, executionResult );

                    tx.success();
                }
            }
            finally
            {
                tx.finish();
            }
        }

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println( "Batch Size: " + batchSize + ", Throughput: " + MAX_ITERATIONS * 1000 / elapsedTime );
    }

    public static void main(String[] args) throws Exception
    {
        new WriteThroughput(1).go();
        new WriteThroughput(10).go();
        new WriteThroughput(100).go();
    }

    private static void indexIt( GraphDatabaseService db, ExecutionResult executionResult )
    {
        EthernetAddress nic = EthernetAddress.fromInterface();
        // or bogus which would be gotten with: EthernetAddress.constructMulticastAddress()
        TimeBasedGenerator uuidGenerator = Generators.timeBasedGenerator( nic );
        // also: we don't specify synchronizer, getting an intra-JVM syncer; there is
        // also external file-locking-based synchronizer if multiple JVMs run JUG
        UUID name = uuidGenerator.generate();

        Node theNode = (Node) executionResult.iterator().next().get( "theNode" );

        Index<Node> whatever = db.index().forNodes( "Whatever" );
        whatever.remove( theNode, "name", name );
        whatever.add( theNode, "name", name );
    }

    private static long createRootNode( GraphDatabaseService database )
    {
        long result = -1;
        Transaction transaction = database.beginTx();
        try
        {
            result = database.createNode().getId();
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }

        return result;
    }
}
