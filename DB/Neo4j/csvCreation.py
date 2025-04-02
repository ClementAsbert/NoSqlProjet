import pandas as pd
import json

#on charge les donnees ectraite de mongo dans data
with open("movies.json", encoding="utf-8") as f:
    data = json.load(f)


def extractFilms():
    #on prend que les champs demander 
    movies = [
        {
            "id": film["_id"],
            "title": film.get("title", ""),
            "year": film.get("year", 0),
            "votes": film.get("Votes", 0),
            "revenue": film.get("Revenue (Millions)", 0),
            "rating": film.get("rating", 0),
            "director": film.get("Director", ""),
        }
        for film in data
    ]

    df_movies = pd.DataFrame(movies)
    df_movies.to_csv("movies.csv", index=False)


def extractActors():
    actors = set() #set pour eviter les doublons
    for film in data:
        actor = film.get("Actors", "")
        if actor:
            for actor_name in actor.split(","):
                actors.add(actor_name.strip())
    df_actors = pd.DataFrame(list(actors), columns=["actor"])
    df_actors.to_csv("actors.csv", index=False)

def relationMoviesActors():
    relation = []
    for film in data:
        actor = film.get("Actors", "")
        if actor:
            for actor_name in actor.split(","):
                relation.append({
                    "movie_id": film.get("_id", ""),
                    "actor": actor_name.strip()
                })
    df_relation = pd.DataFrame(relation)
    df_relation.to_csv("relation_movies_actors.csv", index=False)

def extractDirectors():
    directors = {film.get("Director", "") for film in data}

    df_directors = pd.DataFrame(list(directors), columns=["director"])
    df_directors.to_csv("directors.csv", index=False)

def relationMoviesDirectors():
    relations_directors = [
        {"movie_id": film.get("_id", ""), "director": film.get("Director", "")}
        for film in data
    ]

    df_relations_directors = pd.DataFrame(relations_directors)
    df_relations_directors.to_csv("movies_directors.csv", index=False)

def extractGenres():
    genres = set()
    for film in data:
        genre = film.get("genre", "")
        for genre_name in genre.split(","):
            genres.add(genre_name.strip())
    df_genres = pd.DataFrame(list(genres), columns=["genre"])
    df_genres.to_csv("genres.csv", index=False)

def relationMoviesGenres():
    relations_genres = []
    for film in data:
        genre = film.get("genre", "")
        for genre_name in genre.split(","):
            relations_genres.append({
                "movie_id": film["_id"],
                "genre": genre_name.strip()
            })
    df_relations_genres = pd.DataFrame(relations_genres)
    df_relations_genres.to_csv("movies_genres.csv", index=False)







