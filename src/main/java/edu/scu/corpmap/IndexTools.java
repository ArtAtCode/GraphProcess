package edu.scu.corpmap;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import edu.scu.corpmap.utils.IKAnalyzer5x;

import java.io.File;

public class IndexTools {
    private static IndexTools indexTools;


    public static final String DB_PATH = "C:\\Neo4j\\data\\databases\\new3graph.db";
    GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
    GraphDatabaseService db= dbFactory.newEmbeddedDatabase(new File(DB_PATH));

    private IndexTools(){
        registerShutdownHook(db);
    }
    public static IndexTools getInstance(){
        if(indexTools!=null) return indexTools;
        return new IndexTools();
    }

    public void createFullTextIndex(){

        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> corpIterator = db.findNodes(MyNodeLabel.企业);
            ResourceIterator<Node> indBusiIterator = db.findNodes(MyNodeLabel.个体工商户);
            IndexManager index = db.index();
            Index<Node> corpNodeFullTextIndex =
                    index.forNodes( "NodeFullTextIndex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "analyzer",  IKAnalyzer5x.class.getName()));
            while (corpIterator.hasNext()) {
                Node node = corpIterator.next();
                //对企业节点，name字段新建全文索引
                Object corp = node.getProperty( "name", null);
                corpNodeFullTextIndex.add(node, "name", corp);
            }
            while (indBusiIterator.hasNext()){
                Node node = indBusiIterator.next();
                //对个体工商户节点，name字段新建全文索引
                Object corp = node.getProperty( "name", null);
                corpNodeFullTextIndex.add(node, "name", corp);
            }

            tx.success();
        }
     //   testFullTextIndex("公司");

//        shutDown();
    }
    public  void createSchemaIndex(){

            try (Transaction tx = db.beginTx()) {
                ResourceIterator<Node> corpIterator = db.findNodes(MyNodeLabel.企业);
                ResourceIterator<Node> indBusiIterator = db.findNodes(MyNodeLabel.个体工商户);
                IndexManager indexManager = db.index();
                Index<Node> schemaIndex = indexManager.forNodes("corpId");
                trvNodeCreateSchemaIndex(corpIterator,schemaIndex);
                trvNodeCreateSchemaIndex(indBusiIterator,schemaIndex);
                tx.success();
            }
            shutDown();

        }
    private void trvNodeCreateSchemaIndex( ResourceIterator<Node>  iterator,Index<Node> schemaIndex){
        while (iterator.hasNext()){
            Node node = iterator.next();
            //对个体工商户节点，id字段新建全文索引
            Object corpId = node.getProperty( "id", null);
            if(corpId==null) continue;
            schemaIndex.add(node, "id", corpId.toString());
        }
    }


    private  void registerShutdownHook( final GraphDatabaseService graphDb )    {
        Runtime.getRuntime().addShutdownHook( new Thread(){
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }

    void shutDown()    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        db.shutdown();
        // END SNIPPET: shutdownServer
    }
    private void testFullTextIndex(String fuzzyName){
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> corpIterator = db.findNodes(MyNodeLabel.企业);
            ResourceIterator<Node> indBusiIterator = db.findNodes(MyNodeLabel.个体工商户);
            IndexManager index = db.index();
            Index<Node> corpNodeFullTextIndex =
                    index.forNodes( "NodeFullTextIndex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "analyzer",  IKAnalyzer5x.class.getName()));
            Term term = new Term("name",fuzzyName);
            Query query = new TermQuery(term);

            IndexHits<Node> foundNodes = corpNodeFullTextIndex.query(query);
            for(int i=0;i<5&&foundNodes.hasNext();i++){
                System.out.println(foundNodes.next().getProperty("name"));
            }
            tx.success();
        }
    }

}
