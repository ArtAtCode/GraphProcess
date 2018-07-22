package edu.scu.corpmap;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;

public class CorpController {
 //   public static final String DB_PATH = "/var/lib/neo4j/data/databases/corpmap.db";
    public static final String DB_PATH ="C:\\Neo4j\\data\\databases\\corpmap.db";
    GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
    GraphDatabaseService db= dbFactory.newEmbeddedDatabase(new File(DB_PATH));
    private static CorpController corpController;
    private  CorpController(){
        registerShutdownHook(db);
    }
    public static CorpController getCorpController(){
        if(corpController==null) return new CorpController();
        else  return corpController;
    }

    public void calGraphController(){
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> corpIterator = db.findNodes(MyNodeLabel.企业);
            ResourceIterator<Node> indvIterator = db.findNodes(MyNodeLabel.个体工商户);
            while(indvIterator.hasNext()){
                Node node = indvIterator.next();
                node.setProperty("corpController",node.getProperty("owner","非公示项"));
              //  System.out.println(node.getProperty("owner","非公示项"));

            }

            while(corpIterator.hasNext()){
                Node node = corpIterator.next();
                Iterable<Relationship> staffRel = node.getRelationships(Direction.INCOMING, MyRelationship.任职);//不存在关系则会返回空
                boolean isSet = false;
                for(Relationship r:staffRel){
                    if(r.getProperty("position").toString().contains("董事长")||r.getProperty("position").toString().contains("负责人")){
                        node.setProperty("corpController",r.getStartNode().getProperty("name").toString());
                        isSet = true;
                        break;
                    }
                }
                if(isSet==true) continue;
                Iterable<Relationship> investRel = node.getRelationships(Direction.INCOMING,MyRelationship.股东);

                if(!setCtrlFromInvOrPartner(node,investRel)){
                    isSet = setCtrlFromInvOrPartner(node,node.getRelationships(Direction.INCOMING,MyRelationship.合伙人));
                }else{
                    continue;
                }
                if(isSet==false)
                    node.setProperty("corpController",node.getProperty("legal_person","非公示项"));
            }
            tx.success();
        }
        db.shutdown();
    }

    public boolean setCtrlFromInvOrPartner(Node node,Iterable<Relationship> investRel){
        String maxInvName="";
        double maxInv=0f;
        for(Relationship r: investRel){
            double rInv = 0f;
            double lastInv = 0f;
            try{
                rInv = Double.parseDouble(r.getProperty("subscription",new Double(0)).toString());
                lastInv = rInv;
            }catch (Exception e){
                rInv = lastInv;
            }

            if(rInv==0f){
                try{
                    rInv = Double.parseDouble(r.getProperty("actual_subscription",new Double(0)).toString());
                    lastInv = rInv;
                }catch (Exception e){
                   rInv = lastInv;
                }
            }



            if(rInv>maxInv){
                maxInv=rInv;
                maxInvName=r.getStartNode().getProperty("name","非公示项").toString();
            }
        }
        node.setProperty("corpController",maxInvName);
        if(maxInvName.equals("")) return false;
        return true;
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

}
