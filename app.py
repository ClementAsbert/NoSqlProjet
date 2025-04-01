import streamlit as st
import DB.Mongo.queries as queries

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
