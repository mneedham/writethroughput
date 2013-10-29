import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.UniqueFactory;

public class WriteThroughput
{
    private static final int MAX_ITERATIONS = 1000;
    public static final int BATCH_SIZE = 1;
    private static String INDEX_NAME = "Whatever";
    private DynamicRelationshipType LIKES = DynamicRelationshipType.withName( "LIKES" );

    private int batchSize;
    private Path dbPath;
    private Indexing indexingStrategy;

    enum Indexing {
        UNIQUE
                {
                    @Override
                    Node createNodeAndIndex( TimeBasedGenerator generator, GraphDatabaseService db )
                    {
                        final String name = generateName( generator ).toString();

                        UniqueFactory.UniqueNodeFactory factory = new UniqueFactory.UniqueNodeFactory(db, INDEX_NAME) {
                            protected void initialize(Node node, Map<String, Object> _) {
                                node.setProperty( "name", name );
                            }
                        };

                        return factory.getOrCreate( "name", name );
                    }
                },
        NONE
                {
                    @Override
                    Node createNodeAndIndex( TimeBasedGenerator generator, GraphDatabaseService db )
                    {
                        Node node = db.createNode();
                        final String name = generateName( generator ).toString();
                        node.setProperty( "name", name );

                        return node;
                    }
                };

        abstract Node createNodeAndIndex( final TimeBasedGenerator generator, GraphDatabaseService db );
    }

    public WriteThroughput( int batchSize, Indexing indexingStrategy ) throws IOException
    {
        this.batchSize = batchSize;
        this.indexingStrategy = indexingStrategy;
        this.dbPath = Files.createTempDirectory( "" );
    }

    public void go()
    {
        EthernetAddress nic = EthernetAddress.fromInterface();
        TimeBasedGenerator generator = Generators.timeBasedGenerator( nic );

        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath.toString() );

        long rootNodeId = createRootNode( db );

        long startTime = System.currentTimeMillis();

        for ( int i = 0; i < MAX_ITERATIONS; i++ )
        {
            Transaction tx = db.beginTx();
            try
            {

                for ( int batch = 0; batch < BATCH_SIZE; i++, batch++ )
                {
                    Node node = indexingStrategy.createNodeAndIndex( generator, db );
                    node.createRelationshipTo( db.getNodeById( rootNodeId ), LIKES );

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
        long throughput = MAX_ITERATIONS * 1000 / elapsedTime;

        System.out.println( "Batch Size: " + batchSize + ", Indexing: " + indexingStrategy.name()  + ", Throughput: " + throughput );
    }

    public static void main(String[] args) throws Exception
    {
        new WriteThroughput(1, Indexing.NONE ).go();
        new WriteThroughput(1, Indexing.UNIQUE ).go();

        new WriteThroughput(10, Indexing.NONE ).go();
        new WriteThroughput(10, Indexing.UNIQUE ).go();
    }

    private static UUID generateName( TimeBasedGenerator generator )
    {
        return generator.generate();
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
