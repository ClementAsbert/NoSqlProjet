import DB.Neo4j.connexion_neo4j as db
import csv

driver = db.get_neo4j_driver()

def run_query(query, params=None):
    with driver.session() as session:
        return session.run(query, params)
    
#----------- Creation de la base de donnees neo4j -----------
    
def createMovies():
    with open("DB/Neo4j/csv/movies.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            query = """
            MERGE (m:Movie {id: $id})
            SET m.title = $title, 
                m.year = toInteger($year), 
                m.votes = toInteger($votes),
                m.revenue = toFloat($revenue),
                m.rating = toFloat($rating);
            """
            params = {
                "id": row["id"],
                "title": row["title"],
                "year": row["year"],
                "votes": row["votes"],
                "revenue": row["revenue"],
                "rating": row["rating"]
            }
            run_query(query, params)

