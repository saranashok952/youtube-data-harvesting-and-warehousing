Problem Statement:
The goal is to create a Streamlit application that will let users examine data from different YouTube channels. To view features including channel information, video details, and user interaction analytics, users must enter their YouTube channel ID. The software ought to enable users to collect data from up to ten distinct channels and store it in a MongoDB database. It should also have the ability to move specific channel data from MongoDB to a SQL database for further examination. With sophisticated features like table joins for an extensive view of channel information, the application should enable users to search and get data from the SQL database.

Utilized Technologies: 
MySQL
Python
MongoDB
Google api client library

Method:
1. Begin by configuring a Streamlit application with the "streamlit" Python library. This library has a user-friendly interface that allows users to browse channel details, enter a YouTube channel ID, and choose which channels to migrate.
2. Use the Google API client library for Python to connect to the YouTube API V3, which enables me to retrieve channel and video data.
3. As MongoDB is an appropriate option for managing unstructured and semi-structured data, store the recovered data in a MongoDB data lake. To accomplish this, write a function to obtain the previous api request that was made and then save the identical data in the database in three distinct collections.
4. Moving the information gathered from various channels—that is, the channels, videos, and comments—to a SQL data warehouse by using a SQL database, such as MySQL or PostgreSQL.
5. Use SQL queries to get particular channel data based on user input and join tables in the SQL data warehouse. For that reason, the foreign and primary keys for the previously created SQL table must be appropriately set.
6. The retrieved data is shown in the Streamlit application, which makes advantage of Streamlit's data visualization features to produce graphs and charts that let users examine the data.
