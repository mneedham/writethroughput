import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.UniqueFactory;

public class WriteThroughput
{
    private static final int NODES_TO_CREATE = 1000;
    private DynamicRelationshipType LIKES = DynamicRelationshipType.withName( "LIKES" );
    private int batchSize;

    private Path dbPath;
    private Indexing indexingStrategy;
    private final ExecutorService executor;
    private int numberOfThreads;

    enum Indexing {
        UNIQUE
                {
                    @Override
                    Node createNodeAndIndex( TimeBasedGenerator generator, GraphDatabaseService db )
                    {
                        final String name = generateName( generator ).toString();

                        UniqueFactory.UniqueNodeFactory factory = new UniqueFactory.UniqueNodeFactory(db, "Whatever" ) {
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

    public WriteThroughput( int batchSize, Indexing indexingStrategy, int numberOfThreads ) throws IOException
    {
        this.batchSize = batchSize;
        this.indexingStrategy = indexingStrategy;
        this.dbPath = Files.createTempDirectory( "" );
        this.numberOfThreads = numberOfThreads;
        this.executor = Executors.newFixedThreadPool( numberOfThreads );
    }

    public void go() throws ExecutionException, InterruptedException
    {
        EthernetAddress nic = EthernetAddress.fromInterface();
        final TimeBasedGenerator generator = Generators.timeBasedGenerator( nic );

        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath.toString() );

        final long rootNodeId = createRootNode( db );

        long startTime = System.currentTimeMillis();

        int numberOfIterations = NODES_TO_CREATE / batchSize;

        List<Callable<Object>> jobs = new ArrayList<>(numberOfIterations);

        for ( int i = 0; i < numberOfIterations; i++ )
        {
            jobs.add( new CreateNode( db, generator, rootNodeId, batchSize ) );
        }

        List<Future<Object>> futures = executor.invokeAll( jobs );
        for ( Future<Object> future : futures )
        {
            future.get();
        }

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        long throughput = NODES_TO_CREATE * 1000 / elapsedTime;

        System.out.println( "Batch Size: " + batchSize + ", Indexing: " + indexingStrategy.name() + ", Threads: " + numberOfThreads  + ", Throughput: " + throughput );
//        System.out.println( "Batch Size: " + batchSize + ", Indexing: " + indexingStrategy.name()  + ", Throughput: " + throughput + ", nodeCount: " + nodeCount( db ) + ", relCount: " + relCount( db ) );
        executor.shutdown();
    }

    private Object nodeCount( GraphDatabaseService db )
    {
        return new ExecutionEngine( db ).execute( "START n=node(*) RETURN COUNT(n) as nodeCount" )
                    .iterator().next().get( "nodeCount" );
    }

    private Object relCount( GraphDatabaseService db )
    {
        return new ExecutionEngine( db ).execute( "START r=rel(*) RETURN COUNT(r) as relCount" )
                .iterator().next().get( "relCount" );
    }

    class CreateNode implements  Callable
    {

        private GraphDatabaseService db;
        private TimeBasedGenerator generator;
        private long rootNodeId;
        private int batchSize;

        public CreateNode( GraphDatabaseService db, TimeBasedGenerator generator, long rootNodeId, int batchSize )
        {
            this.db = db;
            this.generator = generator;
            this.rootNodeId = rootNodeId;
            this.batchSize = batchSize;
        }

        @Override
        public Object call() throws Exception
        {
            Transaction tx = db.beginTx();
            try
            {

                for ( int i = 0; i < batchSize; i++ )
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
            return "done";
        }
    }

    public static void main(String[] args) throws Exception
    {
        new WriteThroughput(1, Indexing.UNIQUE, 1 ).go();
        new WriteThroughput(1, Indexing.UNIQUE, 4 ).go();
        new WriteThroughput(1, Indexing.UNIQUE, 16 ).go();
        new WriteThroughput(1, Indexing.UNIQUE, 32 ).go();
        new WriteThroughput(1, Indexing.UNIQUE, 100 ).go();

        System.out.println("===============================================================");

        new WriteThroughput(10, Indexing.UNIQUE, 1 ).go();
        new WriteThroughput(10, Indexing.UNIQUE, 4 ).go();
        new WriteThroughput(10, Indexing.UNIQUE, 16 ).go();
        new WriteThroughput(10, Indexing.UNIQUE, 32 ).go();
        new WriteThroughput(10, Indexing.UNIQUE, 100 ).go();
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
