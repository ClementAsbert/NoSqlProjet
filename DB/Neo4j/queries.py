import DB.Neo4j.connexion_neo4j as db
import csv

driver = db.get_neo4j_driver()
#driver.verify_connectivity()

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

def createActors():
    with open("DB/Neo4j/csv/actors.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            query = """
            MERGE (a:Actor {name: $name});
            """
            params = {"name": row["name"]}
            run_query(query, params)

def createDirectors():
    with open("DB/Neo4j/csv/directors.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            query = """
            MERGE (d:Director {name: $name});
            """
            params = {"name": row["name"]}
            run_query(query, params)

def createMovieActorRelationships():
    with open("/DB/Neo4j/csv/relation_movies_actors.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            query = """
            MATCH (m:Movie {id: $movie_id}), (a:Actor {name: $actor})
            MERGE (a)-[:A_jouer]->(m);
            """
            params = {"movie_id": row["movie_id"], "actor": row["actor"]}
            run_query(query, params)

def createMovieDirectorRelationships():
    with open("DB/Neo4j/csv/movies_directors.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            query = """
            MATCH (m:Movie {id: $movie_id}), (d:Director {name: $director})
            MERGE (d)-[:DIRECTED]->(m);
            """
            params = {"movie_id": row["movie_id"], "director": row["director"]}
            run_query(query, params)

def createGenre():
    with open("DB/Neo4j/csv/genres.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            query = """
            MERGE (g:Genre {name: $name});
            """
            params = {"name": row["name"]}
            run_query(query, params)

def createMovieGenreRelationships():
    with open("DB/Neo4j/csv/movies_genres.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            query = """
            MATCH (m:Movie {id: $movie_id}), (g:Genre {name: $genre})
            MERGE (m)-[:BELONGS_TO]->(g);
            """
            params = {"movie_id": row["movie_id"], "genre": row["genre"]}
            run_query(query, params)

def addMeToPassengerActor():
    query = """
    MERGE (a:Actor {name: "Clément ASBERT"})
    MATCH (m:Movie {title: "Passengers"})
    MERGE (a)-[:ACTED_IN]->(m)
    """
    run_query(query)


#------------ Requete --------------
def actorPlaylotsOfMovies():
    query = """
    MATCH (a:Actor)-[:A_jouer]->(m:Movie)
    RETURN a.name AS acteur, COUNT(m) AS nombre_de_films
    ORDER BY nombre_de_films DESC
    LIMIT 1;
    """
    result = run_query(query)
    return [(record["acteur"], record["nombre_de_films"]) for record in result]

def actorPlayWithAnneHathaway():
    query = """
    MATCH (anne:Actor {name: "Anne Hathaway"})-[:A_jouer]->(m:Movie)<-[:A_jouer]-(a:Actor)
    WHERE a.name <> "Anne Hathaway"
    RETURN DISTINCT a.name;
    """
    result = run_query(query)
    return [record["a.name"] for record in result]

def actorPlayMostRevenueFilm():
    query = """
    MATCH (a:Actor)-[:A_jouer]->(m:Movie)
    WITH a, SUM(m.revenue) AS total_revenue
    RETURN a.name AS acteur, total_revenue
    ORDER BY total_revenue DESC
    LIMIT 1;
    """
    result = run_query(query)
    return [(record["acteur"], record["total_revenue"]) for record in result]

def avgVotes():
    query = """
    MATCH (m:Movie)
    RETURN AVG(m.votes) AS moyenne_votes;
    """
    result = run_query(query)
    return result.single()["moyenne_votes"]

def genreMostRepresented():
    query = """
    MATCH (m:Movie)-[:BELONGS_TO]->(g:Genre)
    RETURN g.name AS genre, COUNT(m) AS nombre_de_films
    ORDER BY nombre_de_films DESC
    LIMIT 1;   
    """
    result = run_query(query)
    return result.single()["genre"]

def filmWithActorPlayWithMe():
    query = """
    MATCH (me:Actor {name: "Clément ASBERT"})-[:A_jouer]->(m1:Movie)<-[:A_jouer]-(a:Actor),
      (a)-[:A_jouer]->(m2:Movie)
    WHERE NOT (me)-[:A_jouer]->(m2)
    RETURN DISTINCT m2.title AS film;
    """
    result = run_query(query)
    return [record["film"] for record in result]

def DirectorPlayWithMostDistinctActors():
    query = """
    MATCH (d:Director)-[:DIRECTED]->(m:Movie)<-[:A_jouer]-(a:Actor)
    RETURN d.name AS réalisateur, COUNT(DISTINCT a) AS nombre_acteurs
    ORDER BY nombre_acteurs DESC
    LIMIT 1;
    """
    result = run_query(query)
    return result.single()["réalisateur"]

def filmWithMostConnected():
    query = """
    MATCH (m1:Movie)<-[:A_jouer]-(a:Actor)-[:A_jouer]->(m2:Movie)
    WHERE m1 <> m2
    RETURN m1.title AS film, COUNT(DISTINCT a) AS nombre_acteurs_connectés
    ORDER BY nombre_acteurs_connectés DESC
    LIMIT 3;
    """
    result = run_query(query)
    return [(record["film"], record["nombre_acteurs_connectés"]) for record in result]


def fiveActorsPlayWithMostDistinctDirector():
    query = """
    MATCH (a:Actor)-[:A_jouer]->(m:Movie)<-[:DIRECTED]-(d:Director)
    RETURN a.name AS acteur, COUNT(DISTINCT d) AS nombre_réalisateurs
    ORDER BY nombre_réalisateurs DESC
    LIMIT 5;
    """
    result = run_query(query)
    return [(record["acteur"] for record in result]

def RecommendedFilmsForActor():
    query = """
    MATCH (a:Actor {name: "Anne Hathaway"})-[:A_jouer]->(m:Movie)-[:BELONGS_TO]->(g:Genre),
      (rec:Movie)-[:BELONGS_TO]->(g)
    WHERE NOT (a)-[:A_jouer]->(rec)
    RETURN DISTINCT rec.title AS film_recommandé
    ORDER BY rec.rating DESC
    LIMIT 1;
    """
    result = run_query(query)
    return result.single()["film_recommandé"]

def createRelationInfluence():
    query = """
    MATCH (d1:Director)-[:DIRECTED]->(m1:Movie)-[:BELONGS_TO]->(g:Genre),
      (d2:Director)-[:DIRECTED]->(m2:Movie)-[:BELONGS_TO]->(g)
    WHERE d1 <> d2
    MERGE (d1)-[:INFLUENCE_PAR]->(d2);
    """
    run_query(query)

def shortestPathBetweenTwoActors(actor1, actor2):
    query = """
    MATCH (a1:Actor {name: $actor1}), (a2:Actor {name: $actor2}),
      p = shortestPath((a1)-[:A_jouer*]-(a2))
    RETURN p;
    """
    params = {"actor1": actor1, "actor2": actor2}
    result = run_query(query, params)
    return result.single()

def actorCommunity():
    query = """
    CALL gds.graph.project(
        'actorGraph',
        ['Actor', 'Movie'],
        {A_jouer: {orientation: 'UNDIRECTED'}}
    );

    CALL gds.louvain.stream('actorGraph')
    YIELD nodeId, communityId
    RETURN gds.util.asNode(nodeId).name AS acteur, communityId
    ORDER BY communityId, acteur;
    """
    result = run_query(query)
    return [(record["acteur"], record["communityId"]) for record in result]
    

