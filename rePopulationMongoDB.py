from pymongo import MongoClient
import random
import string


def add_books(connection):
    database = connection.tesi2022
    all_my_books = database.book.find().limit(3000000)
    books_list = list(all_my_books)
    for book in books_list:
        exists = True
        random_id = 0
        while exists:
            random_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
            print(random_id)
            check_existed_id = database.book.find_one({'_id': random_id})
            if check_existed_id is None:
                exists = False
        book['_id'] = random_id
        result = database.book.insert_one(book)

try:
    MongoDBConnection = MongoClient("your_mongodb_connection")
except:
    exit

add_books(MongoDBConnection)
