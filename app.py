import streamlit as st
import DB.Mongo.queries as queries

st.title("MongoDB & Neo4j avec Streamlit")

st.subheader("Question 1 : ")
st.write("Année la plus fréquente dans la collection 'films' : ", queries.films_by_year())

st.subheader("Question 2 : ")
st.write("Nombre de films sortis après 1999 : ", queries.films_after_1999())

st.subheader("Question 3 : ")
st.write("Nombre moyen de votes pour les films sortis en 2007 : ", queries.average_votes_2007())

st.subheader("Question 4 : ")
st.pyplot(queries.nb_films_per_years_histogram())

st.subheader("Question 5 : ")
st.write("Genres de films disponibles : ", queries.genre_available())

st.subheader("Question 6 : ")
st.write("le film qui a généré le plus de revenus : ", queries.most_revenue_film())

st.subheader("Question 7 : ")
