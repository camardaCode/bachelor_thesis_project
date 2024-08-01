#!/user/bin/python3

import requests
import mysql.connector
from pymongo import MongoClient
from Book import Book


def range_char(start, stop):
    return (chr(n) for n in range(ord(start), ord(stop) + 1))


def insert_into_MongoDB(book: Book, connection):
    database = connection.tesi2022
    my_book = {
        '_id': book.id
    }
    if book.title != "":
        my_book['title'] = book.title
    if book.subtitle != "":
        my_book['subtitle'] = book.subtitle
    if len(book.authors) != 0:
        my_book['authors'] = book.authors
    if book.publisher != "":
        my_book['publisher'] = book.publisher
    if book.description != "":
        my_book['description'] = book.description
    if len(book.categories) != 0:
        my_book['categories'] = book.categories
    if book.language != "":
        my_book['language'] = book.language
    if book.price != "":
        my_book['price'] = book.price
    if book.currency != "":
        my_book['currency'] = book.currency
    result = database.book.insert_one(my_book)


def insert_into_MySQL(book: Book, databaseMySQL):
    try:
        my_cursor = databaseMySQL.cursor(buffered=True)
        check_existed_book = "SELECT * FROM book where id = %s"
        check_existed_book_values = [book.id]
        my_cursor.execute(check_existed_book, check_existed_book_values)
        records = my_cursor.fetchall()
        if records is None or len(records) == 0:
            insert_book = "INSERT INTO book (id, title, subtitle, publisher, description, language, price, currency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            insert_book_values = [(book.id, book.title, book.subtitle, book.publisher, book.description, book.language,
                                   book.price, book.currency)]
            for values in insert_book_values:
                my_cursor.execute(insert_book, values)
        for a in book.authors:
            id_author = None
            check_existed_author = "SELECT id FROM author where full_name = %s"
            check_existed_author_values = [a]
            my_cursor.execute(check_existed_author, check_existed_author_values)
            records = my_cursor.fetchall()
            if records is None or len(records) == 0:
                insert_author = "INSERT INTO author (full_name) VALUES (%s)"
                insert_author_value = [a]
                my_cursor.execute(insert_author, insert_author_value)
                id_author = my_cursor.lastrowid
            else:
                for row in records:
                    id_author = str(row[0])
            insert_book_author_couple = "INSERT INTO has_written (id_book, id_author) VALUES (%s, %s)"
            insert_book_author_couple_values = [(book.id, int(id_author))]
            for values in insert_book_author_couple_values:
                my_cursor.execute(insert_book_author_couple, values)
        for c in book.categories:
            id_category = None
            check_existed_category = "SELECT id FROM category where name = %s"
            check_existed_category_values = [c]
            my_cursor.execute(check_existed_category, check_existed_category_values)
            records = my_cursor.fetchall()
            if records is None or len(records) == 0:
                insert_category = "INSERT INTO category (name) VALUES (%s)"
                insert_category_value = [c]
                my_cursor.execute(insert_category, insert_category_value)
                id_category = my_cursor.lastrowid
            else:
                for row in records:
                    id_category = str(row[0])
            insert_book_category_couple = "INSERT INTO has_category (id_book, id_category) VALUES (%s, %s)"
            insert_book_category_couple_values = [(book.id, id_category)]
            for values in insert_book_category_couple_values:
                my_cursor.execute(insert_book_category_couple, values)
        databaseMySQL.commit()
    except Exception as e:
        print(e)


databaseMySQL = None
try:
    databaseMySQL = mysql.connector.connect(
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
urlAPI = "https://www.googleapis.com/books/v1/volumes"
books = []
for character in range_char("a", "z"):
    startIndex = 0
    parameters = {'q': character, 'maxResults': '40', 'startIndex': startIndex}
    nextValue = True
    while nextValue:
        results = requests.get(urlAPI, params=parameters)
        results = results.json()
        if "items" not in results:
            nextValue = False
        else:
            for item in results["items"]:
                id = item["id"]
                title = ""
                if "title" in item["volumeInfo"]:
                    title = item["volumeInfo"]["title"]
                else:
                    continue
                subtitle = ""
                if "subtitle" in item["volumeInfo"]:
                    subtitle = item["volumeInfo"]["subtitle"]
                authors = []
                if "authors" in item["volumeInfo"]:
                    authors = item["volumeInfo"]["authors"]
                publisher = ""
                if "publisher" in item["volumeInfo"]:
                    publisher = item["volumeInfo"]["publisher"]
                description = ""
                if "description" in item["volumeInfo"]:
                    description = item["volumeInfo"]["description"]
                categories = []
                if "categories" in item["volumeInfo"]:
                    categories = item["volumeInfo"]["categories"]
                language = ""
                if "language" in item["volumeInfo"]:
                    language = item["volumeInfo"]["language"]
                price = ""
                currency = ""
                if "listPrice" in item["saleInfo"]:
                    price = item["saleInfo"]["listPrice"]["amount"]
                    currency = item["saleInfo"]["listPrice"]["currencyCode"]
                book = Book(id, title, subtitle, authors, publisher, description, categories, language, price, currency)
                if id not in books:
                    books.append(id)
                    insert_into_MongoDB(book, MongoDBConnection)
                    insert_into_MySQL(book, databaseMySQL)
            if len(results['items']) < 40:
                nextValue = False
            else:
                startIndex += 40
                parameters['startIndex'] = startIndex

databaseMySQL.close()
MongoDBConnection.close()
