#!/user/bin/python3
from threading import Thread
import time


class QuestionerMySQL(Thread):
    def __init__(self, connection):
        super().__init__()
        self.my_connection = connection
        self.my_cursor = self.my_connection.cursor(buffered=True)
        self.my_file = open("MySQLStatistics_Index.txt", "a")

    def run(self):
        self.execute_select("Cerca il libro con id pari a --cCEAAAQBAJ" + "\n", "SELECT * FROM book where id = %s",
                            ['--cCEAAAQBAJ'])
        self.execute_select("Cerca tutti i libri di lingua inglese" + "\n", "SELECT * FROM book WHERE language = %s",
                            ["en"])
        self.execute_select("Cerca il libro con il costo minore in euro" + "\n", "SELECT * FROM book WHERE price != ' '"
                                                                                 "and price = (SELECT min(price) FROM book WHERE currency = 'EUR' and price > %s)",
                            ["0"])
        self.execute_select("Cerca tutti i libri dell'autore con full_name pari a Younkyoo Kim" + "\n",
                            "SELECT book.id, book.title,"
                            " book.subtitle, book.publisher, book.description, book.language, book.price, book.currency "
                            "FROM book, author, has_written WHERE book.id = has_written.id_book and "
                            "author.id = has_written.id_author and full_name = %s", ["Younkyoo Kim"])
        self.execute_select("Cerca tutti i libri appartenenti alla categoria con name pari a Self-Help" + "\n",
                            "SELECT book.id, book.title, book.subtitle, book.publisher, book.description, book.language,"
                            "book.price, book.currency FROM book, has_category, category WHERE book.id = has_category.id_book "
                            "and has_category.id_category = category.id and name = %s", ["Self-Help"])
        self.execute_select(
            "Cerca tutti i libri appartenenti alla categoria con name pari a Self-Help appartenenti all'autore con "
            "full_name pari a Clarissa Pinkola Estés" + "\n",
            "SELECT book.id, book.title, book.subtitle, book.publisher, "
            "book.description, book.language, book.price, book.currency FROM book, has_written, has_category, category, author "
            "WHERE book.id = has_written.id_book and has_category.id_book = book.id and has_category.id_category = category.id"
            " and category.name = %s and has_written.id_author = author.id and author.full_name = %s", ["Self-Help", "Clarissa Pinkola Estés"])
        self.execute_select(
            "Cerca il libro con il costo minore dell'autore con full_name pari a Centro Fede e Cultura Alberto Hurtado" + "\n",
            "SELECT book.id, book.title, book.subtitle, book.publisher, book.description, book.language, book.price, "
            "book.currency FROM book WHERE book.price != '' and book.currency = 'EUR' and book.price = (SELECT min(price) FROM "
            "book as b, has_written, author WHERE b.id = has_written.id_book and has_written.id_author = author.id and "
            "author.full_name = %s)", ["Centro Fede e Cultura “Alberto Hurtado”"])
        self.execute_select(
            "Cerca tutti i libri dell'autore con nome pari a Clarissa Pinkola Estés che hanno un numero di categorie > 0" + "\n",
            "SELECT book.id, book.title, book.subtitle, book.publisher, book.description, book.language, book.price, book.currency "
            "FROM book, has_written, author WHERE book.id = has_written.id_book and has_written.id_author = author.id and "
            "author.full_name = %s and 0 < (SELECT count(*) FROM has_category WHERE has_category.id_book = book.id)",
            ["Clarissa Pinkola Estés"])
        self.execute_select(
            "Cerca tutti gli autori del libro con nome pari a A Study of Income and Expenditures in Sixty Colleges. Year 1953-54" + "\n",
            "SELECT author.id, author.full_name FROM author, book, has_written WHERE book.title = %s and book.id = has_written.id_book "
            "and has_written.id_author = author.id", ["A Study of Income and Expenditures in Sixty Colleges. Year 1953-54"])
        self.my_cursor.close()
        self.my_connection.close()
        self.my_file.close()

    def execute_select(self, title, query, value):
        self.my_file.write(title)
        start_time = time.time()
        self.my_cursor.execute(query, value)
        total_time = time.time() - start_time

        self.my_file.write("Tempo impiegato per eseguire la query: " + str(total_time) + "\n")
        self.my_file.write("\n")

