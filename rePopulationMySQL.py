import random
import string
import mysql.connector


def new_id():
    my_search_cursor = databaseMySQL.cursor(buffered=True)
    exists = True
    random_id = 0
    while exists:
        random_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
        check_existed_id = "SELECT id FROM book where id = %s"
        check_existed_id_values = [random_id]
        my_search_cursor.execute(check_existed_id, check_existed_id_values)
        records = my_search_cursor.fetchall()
        if records is None or len(records) == 0:
            exists = False
    return random_id


def add_books():
    try:
        my_cursor = databaseMySQL.cursor(buffered=True)
        existed_books = "SELECT * FROM book limit 3000000"
        my_cursor.execute(existed_books)
        records = my_cursor.fetchall()
        for row in records:
            my_new_id = new_id()
            insert_book = "INSERT INTO book (id, title, subtitle, publisher, description, language, price, currency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            insert_book_values = [(my_new_id, row[1], row[2], row[3], row[4], row[5], row[6], row[7])]
            for values in insert_book_values:
                my_cursor.execute(insert_book, values)
            add_authors(row[0], my_cursor, my_new_id)
            # add_categories(row[0], my_cursor, my_new_id)
            add_random_has_categories(row[0], my_cursor, my_new_id)
        # databaseMySQL.commit()
    except Exception as e:
        print(e)


def add_authors(book_id, my_cursor, my_new_id):
    exists_author = "SELECT full_name FROM has_written, author WHERE id_book = %s and id_author = author.id"
    exists_author_values = [book_id]
    my_cursor.execute(exists_author, exists_author_values)
    authors_records = my_cursor.fetchall()
    authors = []
    for author in authors_records:
        insert_author = "INSERT INTO author (full_name) VALUES (%s)"
        insert_author_value = [author[0]]
        my_cursor.execute(insert_author, insert_author_value)
        authors.append(my_cursor.lastrowid)
    for author_id in authors:
        add_has_written(my_new_id, author_id, my_cursor)


def add_categories(book_id, my_cursor, my_new_id):
    exists_category = "SELECT name FROM has_category, category WHERE id_book = %s and id_category = category.id"
    exists_category_values = [book_id]
    my_cursor.execute(exists_category, exists_category_values)
    categories_records = my_cursor.fetchall()
    categories = []
    for category in categories_records:
        insert_category = "INSERT INTO category (name) VALUES (%s)"
        insert_category_values = [category[0]]
        my_cursor.execute(insert_category, insert_category_values)
        categories.append(my_cursor.lastrowid)
    for category_id in categories:
        add_has_categories(my_new_id, category_id, my_cursor)


def add_has_written(book_id, author_id, my_cursor):
    insert_book_author_couple = "INSERT INTO has_written (id_book, id_author) VALUES (%s, %s)"
    insert_book_author_couple_values = [(book_id, int(author_id))]
    for values in insert_book_author_couple_values:
        my_cursor.execute(insert_book_author_couple, values)


def add_has_categories(book_id, category_id, my_cursor):
    insert_book_category_couple = "INSERT INTO has_category (id_book, id_category) VALUES (%s, %s)"
    insert_book_category_couple_values = [(book_id, category_id)]
    for values in insert_book_category_couple_values:
        my_cursor.execute(insert_book_category_couple, values)


def add_random_has_categories(book_id, my_cursor, my_new_id):
    exists_category = "SELECT name FROM has_category, category WHERE id_book = %s and id_category = category.id"
    exists_category_values = [book_id]
    my_cursor.execute(exists_category, exists_category_values)
    categories_records = my_cursor.fetchall()
    for category in categories_records:
        select_categories = "SELECT id FROM category WHERE name = %s"
        select_categories_values = [category[0]]
        my_cursor.execute(select_categories, select_categories_values)
        associated_categories = my_cursor.fetchall()
        add_has_categories(my_new_id, associated_categories[random.randint(0, len(associated_categories) - 1)][0],
                           my_cursor)


databaseMySQL = None
try:
    databaseMySQL = mysql.connector.connect(
        host="your_host",
        user="your_user",
        password="your_password",
        database="your_database"
    )

    databaseMySQL.autocommit = True
except:
    exit

add_books()
