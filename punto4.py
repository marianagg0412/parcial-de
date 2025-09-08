# --- Esquema estrella ---

import psycopg2
import os
from dotenv import load_dotenv

# --- Cargar variables de entorno ---
load_dotenv()

BASE_URL = os.getenv("BASE_URL")

# --- Conexión a PostgreSQL DB transaccional (OLTP - normalizado) ---
src_conn = psycopg2.connect(
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASS"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT")
)
src_cur = src_conn.cursor()

# --- Conexión a PostgreSQL DB analítica ---
dw_conn = psycopg2.connect(
    dbname=os.getenv("PG_DB_OLAP"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASS"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT")
)
dw_cur = dw_conn.cursor()

# --- Crear tablas si no existen ---
dw_cur.execute("""
CREATE TABLE IF NOT EXISTS dim_book (
    book_key SERIAL PRIMARY KEY,
    book_id INT UNIQUE,
    upc VARCHAR(255),
    title TEXT,
    description TEXT
);
""")

dw_cur.execute("""
CREATE TABLE IF NOT EXISTS dim_category (
    category_key SERIAL PRIMARY KEY,
    category_id INT UNIQUE,
    name VARCHAR(255),
    url TEXT
);
""")

dw_cur.execute("""
CREATE TABLE IF NOT EXISTS fact_book (
    fact_id SERIAL PRIMARY KEY,
    book_key INT REFERENCES dim_book(book_key),
    category_key INT REFERENCES dim_category(category_key),
    price_no_tax NUMERIC,
    tax NUMERIC,
    stock INT,
    number_of_reviews INT,
    rating INT
);
""")
dw_conn.commit()

# --- Cargar datos desde la DB transaccional ---

src_cur.execute("SELECT category_id, name, url FROM category;")
categories = src_cur.fetchall()

for cat in categories:
    dw_cur.execute("""
        INSERT INTO dim_category (category_id, name, url)
        VALUES (%s, %s, %s)
        ON CONFLICT (category_id) DO NOTHING;
    """, cat)

src_cur.execute("SELECT book_id, upc, title, description FROM book;")
books = src_cur.fetchall()
for book in books:
    dw_cur.execute("""
        INSERT INTO dim_book (book_id, upc, title, description)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (book_id) DO NOTHING;
    """, book)

src_cur.execute("""
    SELECT bc.book_id, bc.category_id, b.price_no_tax, b.tax, b.stock, b.number_of_reviews, b.rating
    FROM bookxcategory bc
    JOIN book b ON b.book_id = bc.book_id;
""")
facts = src_cur.fetchall()

for fact in facts:
    book_id, category_id, price, tax, stock, reviews, rating = fact

    dw_cur.execute("""
        INSERT INTO fact_book (book_key, category_key, price_no_tax, tax, stock, number_of_reviews, rating)
        SELECT bdim.book_key, cdim.category_key, %s, %s, %s, %s, %s
        FROM dim_book bdim
        JOIN dim_category cdim ON cdim.category_id = %s
        WHERE bdim.book_id = %s;
    """, (price, tax, stock, reviews, rating, category_id, book_id))

dw_conn.commit()

# --- Cerrar conexiones ---

src_cur.close()
dw_cur.close()
src_conn.close()
dw_conn.close()

