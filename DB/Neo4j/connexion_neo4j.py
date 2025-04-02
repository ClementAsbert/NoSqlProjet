from neo4j import GraphDatabase, TrustAll
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

def get_neo4j_driver():
    return GraphDatabase.driver("neo4j+s://2adfda03.databases.neo4j.io:7687", auth=(NEO4J_USER, NEO4J_PASSWORD))