# --- Responder preguntas con el modelo estrella ---

import psycopg2
import os
from dotenv import load_dotenv

# --- Cargar variables de entorno ---
load_dotenv()

BASE_URL = os.getenv("BASE_URL")

# --- Conexión a PostgreSQL DB analítica ---
dw_conn = psycopg2.connect(
    dbname=os.getenv("PG_DB_OLAP"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASS"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT")
)
dw_cur = dw_conn.cursor()

# --- Pregunta 1: ¿Cuántas categorías de libros se tienen? ---

dw_cur.execute("SELECT COUNT(*) FROM dim_category;")
num_categories = dw_cur.fetchone()[0]
print(f"Número de categorías de libros: {num_categories}")

# fetchone() retrieves the next row of a query result set, returning a single sequence,
# it was used here to get the count result from the executed query, which returns a single row with one column.

# --- Pregunta 2: ¿Cuántos libros hay por categoría hay? ---

dw_cur.execute("""
SELECT dc.name, COUNT(fb.book_key) AS num_books
FROM dim_category dc
LEFT JOIN fact_book fb ON dc.category_key = fb.category_key
GROUP BY dc.name
ORDER BY num_books DESC;
""")
books_per_category = dw_cur.fetchall()
print("Número de libros por categoría:")
for category, count in books_per_category:
    print(f"{category}: {count}")

# A LEFT JOIN was used here to ensure that all categories are included in the result,
# even those that may not have any associated books. This way, categories with zero books are also counted.

# --- Pregunta 3: ¿Cuál es el libro más caro? ---

dw_cur.execute("""
SELECT db.title, MAX(fb.price_no_tax + fb.tax) AS max_price
FROM dim_book db
JOIN fact_book fb ON db.book_key = fb.book_key
GROUP BY db.title
ORDER BY max_price DESC
LIMIT 1;
""")
most_expensive_book = dw_cur.fetchone()
print(f"Libro más caro: {most_expensive_book[0]} con precio £{most_expensive_book[1]}")

# A JOIN was used here instead of a LEFT JOIN because we are only interested in books that
# have associated pricing information in the fact_book table. Using a JOIN ensures that only
# books with corresponding entries in fact_book are considered, which is necessary to determine the most expensive book.

# --- Pregunta 4: ¿Hay algún libro que esté en dos categorías? ---

dw_cur.execute("""
SELECT db.title, COUNT(DISTINCT fb.category_key) AS category_count
FROM dim_book db
JOIN fact_book fb ON db.book_key = fb.book_key
GROUP BY db.title
HAVING COUNT(DISTINCT fb.category_key) > 1;
""")
books_in_multiple_categories = dw_cur.fetchall()
if books_in_multiple_categories:
    print("Libros en más de una categoría:")
    for title, count in books_in_multiple_categories:
        print(f"{title}: {count} categorías")
else:
    print("No hay libros en más de una categoría.")

# --- Pregunta 5: ¿Cuál es el libro más barato por categoría? Si es más de uno, se deben mostrar. ---

dw_cur.execute("""
SELECT dc.name AS category_name, db.title AS book_title, (fb.price_no_tax + fb.tax) AS total_price
FROM dim_category dc
JOIN fact_book fb ON dc.category_key = fb.category_key
JOIN dim_book db ON fb.book_key = db.book_key
WHERE (dc.category_key, (fb.price_no_tax + fb.tax)) IN (
    SELECT dc2.category_key, MIN(fb2.price_no_tax + fb2.tax)
    FROM dim_category dc2
    JOIN fact_book fb2 ON dc2.category_key = fb2.category_key
    GROUP BY dc2.category_key
)
ORDER BY dc.name, total_price;
""")
cheapest_books_per_category = dw_cur.fetchall()
print("Libro(s) más barato(s) por categoría:")
for category, title, price in cheapest_books_per_category:
    print(f"{category}: {title} con precio £{price}")

# Two JOINs were used here to combine data from three tables: dim_category, fact_book, and dim_book.
# The first JOIN connects categories to their corresponding fact records, and the second JOIN connects those fact records to the actual book details.
# This allows us to retrieve the category name, book title, and price information in a single query.

# --- Pregunta 6: ¿Cuánto más caro o barato es cada libro respecto al promedio de su categoría? ---
dw_cur.execute("""
SELECT dc.name AS category_name, db.title AS book_title,
         (fb.price_no_tax + fb.tax) AS book_price,
            AVG(fb.price_no_tax + fb.tax) OVER (PARTITION BY dc.category_key) AS avg_category_price,
            ((fb.price_no_tax + fb.tax) - AVG(fb.price_no_tax + fb.tax) OVER (PARTITION BY dc.category_key)) AS price_difference
FROM dim_category dc
JOIN fact_book fb ON dc.category_key = fb.category_key
JOIN dim_book db ON fb.book_key = db.book_key
ORDER BY dc.name, db.title;
""")

price_comparison = dw_cur.fetchall()
print("Comparación de precio de cada libro respecto al promedio de su categoría:")
for category, title, book_price, avg_price, price_diff in price_comparison:
    print(f"{category} - {title}: Precio del libro = £{book_price}, Precio promedio de la categoría = £{avg_price}, Diferencia = £{price_diff}")

# A window function was used here to calculate the average price of books within each category without collapsing
# the result set. This allows us to retain the individual book records while still being able to compare each book's price to the average price of its category.

# --- Pregunta 7: Asumiendo que se venden todos los libros que están en stock en este momento ¿Cuál es el libro que daría más ingresos por categoría? ---

dw_cur.execute("""
SELECT dc.name AS category_name, db.title AS book_title,
         (fb.price_no_tax + fb.tax) * fb.stock AS potential_revenue
FROM dim_category dc
JOIN fact_book fb ON dc.category_key = fb.category_key
JOIN dim_book db ON fb.book_key = db.book_key
WHERE (dc.category_key, (fb.price_no_tax + fb.tax) * fb.stock) IN (
    SELECT dc2.category_key, MAX((fb2.price_no_tax + fb2.tax) * fb2.stock)
    FROM dim_category dc2
    JOIN fact_book fb2 ON dc2.category_key = fb2.category_key
    GROUP BY dc2.category_key
)
ORDER BY dc.name, potential_revenue DESC;
""")

top_revenue_books = dw_cur.fetchall()
print("Libro que daría más ingresos por categoría:")
for category, title, revenue in top_revenue_books:
    print(f"{category}: {title} con ingresos posibles de £{revenue}")

# A subquery was used here to first calculate the maximum potential revenue for each category.
# This allows us to filter the main query to only include books that match this maximum revenue,
# effectively identifying the book that would generate the most income per category.
