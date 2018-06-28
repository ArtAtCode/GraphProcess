package edu.scu.corpmap;

import org.neo4j.graphdb.RelationshipType;

public enum MyRelationship implements RelationshipType {
    股东,任职,合伙人,经营异常;
}
