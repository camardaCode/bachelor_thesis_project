#!/user/bin/python3
from threading import Thread
import re


class QuestionerMongoDB(Thread):
    def __init__(self, connection):
        super().__init__()
        self.connection = connection
        self.database = connection.tesi2022
        self.my_file = open("MongoDBStatistics_Index.txt", "a", encoding="utf-8")

    def run(self):
        self.execute_query("Cerca il libro con id pari a --cCEAAAQBAJ" + "\n", {'_id': '--cCEAAAQBAJ'})
        self.execute_query("Cerca tutti i libri di lingua inglese" + "\n", {'language': 'en'})
        self.execute_query("Cerca tutti i libri dell'autore con full_name pari a Younkyoo Kim" + "\n",
                           {'authors': "Younkyoo Kim"})
        self.execute_query("Cerca tutti i libri appartenenti alla categoria con name pari a Self-Help" + "\n",
                           {'categories': "Self-Help"})
        self.execute_query("Cerca tutti i libri appartenenti alla categoria con name pari a Self-Help appartenenti "
                           "all'autore con Full_name pari a Clarissa Pinkola Estés" + "\n",
                           {'categories': "Self-Help", 'authors': "Clarissa Pinkola Estés"})
        self.execute_query("Cerca tutti i libri dell'autore con nome pari a Clarissa Pinkola Estés che hanno un numero "
                           "di categorie > 0" + "\n",
                           {'$and': [{'$and': [{'categories': {'$exists': 'true'}},
                                               {"$expr": {'$gt': [{'$size': "$categories"}, 0]}}]},
                                     {'authors': 'Clarissa Pinkola Estés'}]})
        self.connection.close()
        self.my_file.close()

    def execute_query(self, title, query):
        self.my_file.write(title)
        query_results = self.database.book.find(query).explain()['executionStats']

        matched_strings = re.findall(r"('executionTimeMillis').\s(\d+)", str(query_results))
        self.my_file.write(str(matched_strings) + "\n")
        self.my_file.write("\n")
