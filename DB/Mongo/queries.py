import DB.Mongo.connexion_mongo as db
import matplotlib.pyplot as plt
import numpy as np
import json

#Question 1
def filmsByYear():
    collection = db.get_dbCollection()
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}}, 
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = collection.aggregate(pipeline)
    most_common_year = list(result)
    if most_common_year:
        return most_common_year[0]["_id"]
    return None

#Question 2
def filmsAfter1999():
    collection = db.get_dbCollection()
    return collection.count_documents({"year": {"$gt": 1999}})

#Question 3
def averageVotes2007():
    collection = db.get_dbCollection()
    pipeline = [
        {"$match": {"year": 2007}}, 
        {"$group": {"_id": None, "avgVotes": {"$avg": "$Votes"}}}
    ]
    result = list(collection.aggregate(pipeline))
    if result:
        return result[0]["avgVotes"]
    return None

#Question 4
# retoune un histogramme
def nbFilmsPerYearsHistogram():
    collection = db.get_dbCollection()
    pipeline = [
        {"$match": {"year": {"$ne": None}}}, 
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}  
    ]
    result = list(collection.aggregate(pipeline))
    years = [int(x["_id"]) for x in result]
    counts = [x["count"] for x in result]
    # creation histo 
    fig, ax = plt.subplots()
    ax.bar(years, counts, color="skyblue")
    ax.set_xlabel("Année")
    ax.set_ylabel("Nombre de films")
    ax.set_title("Nombre de films par année")
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    return fig

#Question 5 
def genreAvailable():
    collection = db.get_dbCollection()
    pipeline = [
        {"$project": {
            "genres": {"$split": ["$genre", ","]}
        }},
        {"$unwind": "$genres"}, #permet de séparer les genres
        {"$group": {
            "_id": "$genres"
        }}
    ]
    result = list(collection.aggregate(pipeline))
    return [x["_id"] for x in result]

#Question 6
def mostRevenueFilm():
    collection = db.get_dbCollection()
    result = collection.find({"Revenue (Millions)": {"$ne": ""}}).sort("Revenue (Millions)", -1).limit(1)
    return list(result)[0]["title"]

#Question 7 (aucun resultat pour 5 films)
def directorMoreThan5Films():
    collection = db.get_dbCollection()
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": 5}}}
    ]
    result = list(collection.aggregate(pipeline))
    if not result:
        return "Aucun réalisateur avec plus de 5 films"
    return [x["_id"] for x in result]

#Question 8
def genreWithHighestRevenue():
    collection = db.get_dbCollection()
    pipeline = [
        {"$unwind": "$genre"},  
        {"$group": {"_id": "$genre", "avgRevenue": {"$avg": "$Revenue (Millions)"}}},
        {"$sort": {"avgRevenue": -1}}
    ]
    result = list(collection.aggregate(pipeline))
    return result[0]["_id"]

#Question 9
def topRatedFilmsPerDecennie():
    colection = db.get_dbCollection()
    decennies = [1990, 2000, 2010]
    top_3_films = {}

    for decennie in decennies:
        pipeline = [
            {"$match": {"year": {"$gte": decennie, "$lt": decennie + 9}}},
            {"$sort": {"Rating": -1}},
            {"$limit": 3}
        ]
        result = list(colection.aggregate(pipeline))
        top_3_films[decennie] = [film["title"] for film in result]
    return top_3_films

#Question 10
def longestFilmByGenre():
    collection = db.get_dbCollection()
    pipeline = [
        {"$project": {"title": 1, "genre": 1, "runtime": "$Runtime (Minutes)"}},
        {"$unwind": "$genre"}, 
        {"$group": {
            "_id": "$genre",
            "longestFilm": {"$max": "$runtime"},
            "title": {"$first": "$title"} 
        }},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return [{"genre": x["_id"], "title": x["title"]} for x in result]

#question 11 
def hightScoreRevenue():
    collection = db.get_dbCollection()
    result = collection.find({"Metascore": {"$gt":80}, "Revenue (Millions)": {"$gt": 50}}, {"title":1, "_id":0})
    return list(result)

#Question 12
# retourne un grpahique de point pour voir l'évolution la durer des films et des revenues
def correlationRuntimeRevenue():
    collection = db.get_dbCollection()
    films = list(collection.find({"Runtime (Minutes)": {"$exists": True, "$ne":""}, "Revenue (Millions)": {"$exists": True, "$ne":""}}))
    runtimes = [film["Runtime (Minutes)"] for film in films]
    revenues = [film["Revenue (Millions)"] for film in films]

    fig, ax = plt.subplots()
    ax.scatter(runtimes, revenues, alpha=0.5, color="blue")
    ax.set_xlabel("Durée du film (minutes)")
    ax.set_ylabel("Revenu (millions)")
    ax.set_title("Relation entre la durée du film et le revenu généré")
    ax.grid(True)
    return fig

#Question 13
def avgRuntimePerDecenies():
    collection = db.get_dbCollection()
    annees = [1990, 2000, 2010]
    avg_durations = {}
    for annee in annees:
        pipeline = [
            {"$match": {"year": {"$gte": annee, "$lte": annee + 9}}}, 
            {"$group": {"_id": None, "avgRuntime": {"$avg": "$Runtime (Minutes)"}}} 
        ]
        result = list(collection.aggregate(pipeline))
        if result:
            avg_durations[annee] = result[0]["avgRuntime"] 
        #retoune 0 si aucun film dans cette decenies
        else:
            avg_durations[annee] = 0 

    return avg_durations



#----------------------------------------- exportation des données pour neo4j -----------------------------------------

# creation d'un json contenant tous les films seulement avec les champs utile
def extraction():
    collection = db.get_dbCollection()
    films = collection.find({}, {"_id": 1, "title": 1, "Director": 1, "Actors": 1, "genre": 1, "year": 1, "Votes": 1, "rating": 1, "Revenue (Millions)": 1})
    # j'enregistre dans un json qui me serviras pour construire les csv pour neo4j
    with open("movies.json", "w", encoding="utf-8") as f:
        json.dump(list(films), f, indent=4)





    





