#!/user/bin/python3

from queryOnMySQL import QuestionerMySQL
from queryOnMongoDB import QuestionerMongoDB
import mysql.connector
from pymongo import MongoClient

database_MySQL = None
try:
    database_MySQL = mysql.connector.connect(
        host="your_host",
        user="your_user",
        password="your_password",
        database="your_database"
    )
except:
    exit
try:
    MongoDBConnection = MongoClient("your_mongodb_connection")
except:
    exit

my_questioner_MySQL = QuestionerMySQL(database_MySQL)
my_questioner_MongoDB = QuestionerMongoDB(MongoDBConnection)

my_questioner_MongoDB.start()
my_questioner_MySQL.start()
