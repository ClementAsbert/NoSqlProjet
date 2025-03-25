import DB.Mongo.connexion_mongo as db
import matplotlib.pyplot as plt

#Question 1
def films_by_year():
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
def films_after_1999():
    collection = db.get_dbCollection()
    return collection.count_documents({"year": {"$gt": 1999}})

#Question 3
def average_votes_2007():
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
def nb_films_per_years_histogram():
    collection = db.get_dbCollection()
    pipeline = [
        {"$match": {"year": {"$ne": None}}}, 
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}  
    ]
    result = list(collection.aggregate(pipeline))
    years = [int(x["_id"]) for x in result]
    counts = [x["count"] for x in result]
    fig, ax = plt.subplots()
    ax.bar(years, counts, color="skyblue")
    ax.set_xlabel("Année")
    ax.set_ylabel("Nombre de films")
    ax.set_title("Nombre de films par année")
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    return fig

#Question 5 
def genre_available():
    collection = db.get_dbCollection()
    pipeline = [
        {"$project": {
            "genres": {"$split": ["$genre", ","]}
        }},
        {"$unwind": "$genres"},
        {"$group": {
            "_id": "$genres"
        }}
    ]
    result = list(collection.aggregate(pipeline))
    return [x["_id"] for x in result]

#Question 6
def most_revenue_film():
    collection = db.get_dbCollection()
    result = collection.find({"Revenue (Millions)": {"$ne": ""}}).sort("Revenue (Millions)", -1).limit(1)
    return list(result)[0]["title"]

    





