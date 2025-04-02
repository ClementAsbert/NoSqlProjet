import streamlit as st
import DB.Mongo.queries as queries
import DB.Neo4j.queries as neo4j_queries
import DB.Neo4j.csvCreation as csvCreation

st.title("MongoDB & Neo4j avec Streamlit")

st.subheader("Question 1 : ")
st.write("Année la plus fréquente dans la collection 'films' : ", queries.filmsByYear())

st.subheader("Question 2 : ")
st.write("Nombre de films sortis après 1999 : ", queries.filmsAfter1999())

st.subheader("Question 3 : ")
st.write("Nombre moyen de votes pour les films sortis en 2007 : ", queries.averageVotes2007())

st.subheader("Question 4 : ")
st.pyplot(queries.nbFilmsPerYearsHistogram())

st.subheader("Question 5 : ")
st.write("Genres de films disponibles : ", queries.genreAvailable())

st.subheader("Question 6 : ")
st.write("le film qui a généré le plus de revenus : ", queries.mostRevenueFilm())

st.subheader("Question 7 : ")
st.write("Les réalisateur à avoir fait plus de 5 films : ", queries.directorMoreThan5Films())

st.subheader("Question 8 : ")
st.write("le genre qui rapporte le plus de revenus : ", queries.genreWithHighestRevenue())

st.subheader("Question 9 : ")
st.write("Les 3 films les mieux notés par decennies : ", queries.topRatedFilmsPerDecennie())

st.subheader("Question 10 : ")
st.write("les films les plus long par genre : ", queries.longestFilmByGenre())

st.subheader("Question 11 : ")
st.write("les films qui sont noté plus de 80 et ont généré plus de 50 millions : ", queries.hightScoreRevenue())

st.subheader("Question 12 : ")
st.write("correlation : ", queries.correlationRuntimeRevenue())

st.subheader("Question 13 : ")
st.write("Evolution de la dureer moyenne des films par decenies : ", queries.avgRuntimePerDecenies())


#--------- extraction de donnees dans un fichier json
queries.extraction()

#--------- creation des csv

""""
csvCreation.extractFilms()
csvCreation.extractActors()
csvCreation.relationMoviesActors() 
csvCreation.extractDirectors()
csvCreation.relationMoviesDirectors()
csvCreation.extractGenres()
csvCreation.relationMoviesGenres()"
"""

#--------- creation de la base de donnees neo4j

neo4j_queries.createMovies()
neo4j_queries.createActors()
neo4j_queries.createDirectors()
neo4j_queries.createGenre()
neo4j_queries.createMovieActorRelationships()
neo4j_queries.createMovieDirectorRelationships()
neo4j_queries.createMovieGenreRelationships()


#--------- requetes

st.subheader("Question 14 : ")
res14 = neo4j_queries.actorPlaylotsOfMovies()
for actor, count in res14:
    st.write(f"Acteur : {actor}, Nombre de films : {count}")

st.subheader("Question 15 : ")
res15 = neo4j_queries.actorPlayWithAnneHathaway()
for actor in res15:
    st.write(f"Acteur : {actor}")

st.subheader("Question 16 : ")
res16 = neo4j_queries.actorPlayMostRevenueFilm()
for actor, revenue in res16:
    st.write(f"Acteur : {actor}, Revenu : {revenue}")

st.subheader("Question 17 : ")
st.write("Moyenne des votes : ", neo4j_queries.avgVotes())

st.subheader("Question 18 : ")
st.write("Genre le plus representé : ", neo4j_queries.genreMostRepresented())")

st.subheader("Question 19 : ")
res19= neo4j_queries.filmWithActorPlayWithMe()
for film in res19:
    st.write(f"Film ou les acteur qui on jouer avec moi on jouer aussi: {film}")

st.subheader("Question 20 : ")
st.write("le réalisateur qui a jouer avec le plus d'acteur disctinct : ",neo4j_queries.DirectorPlayWithMostDistinctActors()))

st.subheader("Question 21 : ")
res21 = neo4j_queries.filmWithMostConnected()
for film, count in res21:
    st.write(f"Film : {film}, Nombre d'acteurs : {count}")

st.subheader("Question 22 : ")
res22 = neo4j_queries.fiveActorsPlayWithMostDistinctDirector()
for actor in res22:
    st.write(f"Acteur : {actor}")

st.subheader("Question 23 : ")
st.write("film recommander pour Anne Hathaway : ", neo4j_queries.RecommendedFilmsForActor())

st.subheader("Question 24 : ")
st.write("creation de relation")
neo4j_queries.createRelationInfluence()

st.subheader("Question 25 : ")
st.write("chemine le plus court entre deux acteur")
#st.write(neo4j_queries.shortestPathBetweenActors("Anne Hathaway", "Tom Hanks"))

st.subheader("Question 26 : ")
res26 = neo4j_queries.actorCommunity()
for actor, community in res26:
    st.write(f"Acteur : {actor}, Communauté : {community}")




