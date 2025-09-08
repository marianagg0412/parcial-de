import requests
from bs4 import BeautifulSoup
import psycopg2
import re
import os
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

# --- Cargar variables de entorno ---
load_dotenv()

BASE_URL = os.getenv("BASE_URL")

# --- Conexión a PostgreSQL ---
conn = psycopg2.connect(
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASS"),
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT")
)
cur = conn.cursor()

# --- Crear tablas si no existen ---
cur.execute("""
CREATE TABLE IF NOT EXISTS category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE,
    url TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS book (
    book_id SERIAL PRIMARY KEY,
    upc VARCHAR(255) UNIQUE,
    title TEXT,
    description TEXT,
    price_no_tax NUMERIC,
    tax NUMERIC,
    availability TEXT,
    stock INT,
    number_of_reviews INT,
    rating INT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS bookxcategory (
    book_id INT REFERENCES book(book_id),
    category_id INT REFERENCES category(category_id),
    PRIMARY KEY (book_id, category_id)
);
""")
conn.commit()

# --- Sesión con reintentos ---
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

# --- Funciones auxiliares ---
def fetch(url: str):
    """Hace la petición HTTP con fallback a http:// y corrige encoding."""
    try:
        res = session.get(url, timeout=10)
        res.encoding = "utf-8"
        return res
    except requests.exceptions.SSLError:
        # fallback a http
        if url.startswith("https://"):
            url = url.replace("https://", "http://", 1)
            res = session.get(url, timeout=10)
            res.encoding = "utf-8"
            return res
        raise


def get_rating(rating_str: str) -> int:
    mapping = {"One":1, "Two":2, "Three":3, "Four":4, "Five":5}
    return mapping.get(rating_str, None)

def get_stock(avail_text: str) -> int:
    # Ej: "In stock (20 available)"
    match = re.search(r"\((\d+) available\)", avail_text)
    return int(match.group(1)) if match else 0

def parse_price(text: str) -> float:
    # Elimina cualquier símbolo extraño (£, Â, etc.)
    clean = re.sub(r"[^\d\.]", "", text)
    return float(clean) if clean else 0.0

# --- Scraping de categorías ---
def scrape_categories():
    res = fetch(BASE_URL)
    soup = BeautifulSoup(res.text, "html.parser")
    cats = soup.select("div.side_categories ul li ul li a")

    categories = []
    for c in cats:
        name = c.get_text(strip=True)
        url = BASE_URL + c["href"]
        categories.append((name, url))
        cur.execute(
            "INSERT INTO category (name, url) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING category_id",
            (name, url)
        )
        cat_id = cur.fetchone()
        if cat_id:
            conn.commit()
    return categories

# --- Scraping de libros por categoría ---
def scrape_books_in_category(category_name, category_url):
    page_url = category_url
    while True:
        res = fetch(page_url)
        soup = BeautifulSoup(res.text, "html.parser")

        books = soup.select("article.product_pod h3 a")
        for b in books:
            book_url = BASE_URL + "catalogue/" + b["href"].replace("../../../", "")
            scrape_book_detail(book_url, category_name)

        # Siguiente página
        next_page = soup.select_one("li.next a")
        if next_page:
            page_url = category_url.replace("index.html", "") + next_page["href"]
        else:
            break

# --- Scraping detalle de cada libro ---
def scrape_book_detail(book_url, category_name):
    res = fetch(book_url)
    soup = BeautifulSoup(res.text, "html.parser")

    title = soup.h1.get_text(strip=True)
    description_tag = soup.select_one("#product_description ~ p")
    description = description_tag.get_text(strip=True) if description_tag else None

    rating_str = soup.select_one("p.star-rating")["class"][1]
    rating = get_rating(rating_str)

    table = {row.th.get_text(strip=True): row.td.get_text(strip=True)
             for row in soup.select("table.table.table-striped tr")}

    upc = table["UPC"]
    price_excl_tax = parse_price(table["Price (excl. tax)"])
    tax = parse_price(table["Tax"])
    availability = table["Availability"]
    stock = get_stock(availability)
    reviews = int(table["Number of reviews"])

    # Insertar libro
    cur.execute("""
        INSERT INTO book (upc, title, description, price_no_tax, tax, availability, stock, number_of_reviews, rating)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (upc) DO NOTHING RETURNING book_id
    """, (upc, title, description, price_excl_tax, tax, availability, stock, reviews, rating))

    book_id = cur.fetchone()
    if book_id:
        book_id = book_id[0]
    else:
        cur.execute("SELECT book_id FROM book WHERE upc = %s", (upc,))
        book_id = cur.fetchone()[0]

    # Insertar relación libro-categoría
    cur.execute("SELECT category_id FROM category WHERE name = %s", (category_name,))
    category_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO bookxcategory (book_id, category_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (book_id, category_id))

    conn.commit()

# --- MAIN ---
if __name__ == "__main__":
    categories = scrape_categories()
    for name, url in categories:
        print(f"Scraping categoría: {name}")
        scrape_books_in_category(name, url)

cur.close()
conn.close()