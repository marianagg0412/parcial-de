# Data Engineering - Book Analytics Project

This project demonstrates a complete data engineering pipeline for extracting, transforming, and analyzing book data from [Books to Scrape](https://books.toscrape.com/index.html) using PostgreSQL. The workflow covers web scraping, data normalization, star schema design, ETL, and analytical queries.

## Project Structure

- `punto2.py`: Web scraping and ETL script for loading normalized data into a PostgreSQL OLTP (transactional) database.
- `punto4.py`: ETL script to transform and load data from the normalized OLTP database into a star schema (OLAP/analytical database).
- `punto5.py`: Analytical queries using the star schema to answer business questions about books and categories.
- `Parcial - estrella.pdf`: PDF with the star schema model.
- `Parcial - normalizado.pdf`: PDF with the normalized relational model.

## Workflow

### 1. Data Model Design
- **Normalized Model**: Designed to store all book and category information from the website in a relational format. See `Parcial - normalizado.pdf`.
- **Star Schema**: Designed for analytical queries, with dimension and fact tables. See `Parcial - estrella.pdf`.

### 2. Web Scraping & Data Loading (`punto2.py`)
- Scrapes categories and book details from the website.
- Loads data into the normalized PostgreSQL database (`category`, `book`, `bookxcategory` tables).
- Handles retries, encoding, and data cleaning.

### 3. Star Schema ETL (`punto4.py`)
- Reads data from the normalized OLTP database.
- Populates the star schema in the OLAP database (`dim_book`, `dim_category`, `fact_book` tables).
- Ensures referential integrity and avoids duplicates.

### 4. Analytical Queries (`punto5.py`)
- Connects to the OLAP database.
- Answers business questions such as:
  - Number of categories
  - Number of books per category
  - Most expensive/cheapest books
  - Books in multiple categories
  - Price comparison to category average
  - Potential revenue by book and category
- Outputs results to the console.

## Setup

1. **Environment Variables**: Create a `.env` file with the following variables:
   ```env
   BASE_URL=https://books.toscrape.com/
   PG_DB=your_oltp_db
   PG_DB_OLAP=your_olap_db
   PG_USER=your_user
   PG_PASS=your_password
   PG_HOST=localhost
   PG_PORT=5432
   ```
2. **Install Dependencies**:
   Required packages: `psycopg2`, `requests`, `beautifulsoup4`, `python-dotenv`.

3. **Run Scripts**:
   - Scrape and load normalized data:
     ```bash
     python punto2.py
     ```
   - Transform and load star schema:
     ```bash
     python punto4.py
     ```
   - Run analytical queries:
     ```bash
     python punto5.py
     ```

## Notes
- Ensure PostgreSQL is running and accessible with the provided credentials.
- The scripts are idempotent and can be run multiple times without duplicating data.
- The project is structured for educational purposes and can be extended for more complex analytics.

---

**Author:** Mariana González
