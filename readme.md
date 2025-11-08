# MovieLens ETL Pipeline with OMDb API Integration

## 📋 Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that enriches the MovieLens dataset with additional metadata from the OMDb API and loads the processed data into a PostgreSQL database for analytical querying.

### Pipeline Flow

1. **Extract**: Read movie and ratings data from MovieLens CSV files and fetch additional metadata (director, plot, box office) from the OMDb API
2. **Transform**: Clean data types, derive new attributes (decade from release year), and normalize genres from pipe-separated strings to individual rows
3. **Load**: Insert enriched data into a normalized PostgreSQL schema with idempotent operations

### Key Features

- ✅ **Intelligent API Matching**: 3-tier matching strategy (exact match → title-only → fuzzy search)
- ✅ **API Quota Management**: Built-in caching and daily request limiting (1000 requests/day)
- ✅ **Resume Capability**: Automatic checkpoint system to resume from last processed movie
- ✅ **Fuzzy Match Logging**: Logs potential matches for manual review instead of automatic incorrect enrichment
- ✅ **Idempotent Loading**: Safe to rerun without creating duplicate records

---

## 🚀 Setup Instructions

### Prerequisites

- Python 3.10 or higher
- PostgreSQL 12 or higher
- OMDb API key (free tier: [http://www.omdbapi.com/apikey.aspx](http://www.omdbapi.com/apikey.aspx))

### Step 1: Clone the Repository

```bash
git clone <REPOSITORY_URL>
cd <REPOSITORY_FOLDER>
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

**Required packages:**
- pandas
- sqlalchemy
- requests
- requests-cache
- python-dotenv
- psycopg2-binary

### Step 3: Download MovieLens Dataset

1. Download the MovieLens dataset (small version): [https://grouplens.org/datasets/movielens/](https://grouplens.org/datasets/movielens/)
2. Extract the ZIP file
3. Place `movies.csv` and `ratings.csv` in the same directory as `ETL.py`

### Step 4: Configure Environment Variables

Create a `.env` file in the project root:

```bash
OMDB_API_KEY=your_omdb_api_key_here
PGUSER=postgres
PGPASSWORD=your_password
PGHOST=localhost
PGPORT=5432
PGDATABASE=moviesdb
```

⚠️ **Replace placeholder values with your actual credentials**

### Step 5: Set Up PostgreSQL Database

1. Create the database:
```sql
CREATE DATABASE moviesdb;
```

2. Run the schema file to create tables:
```bash
psql -U postgres -d moviesdb -f schema.sql
```

Or using PgAdmin, open and execute `schema.sql`.

### Step 6: Run the ETL Pipeline

```bash
python ETL.py
```

**What happens during execution:**
- Reads movies and ratings from CSV files
- Calls OMDb API to enrich movie metadata
- Saves progress to `progress.json` for resumption
- Logs fuzzy matches to `fuzzy_matches.json` for review
- Loads cleaned data into PostgreSQL tables

**Output files:**
- `progress.json` - Tracks last processed movie index
- `omdb_cache.sqlite` - Local API response cache
- `fuzzy_matches.json` - Potential title matches for manual review

### Step 7: Run Analytics Queries

Execute the provided SQL queries:

```bash
psql -U postgres -d moviesdb -f queries.sql
```

Or run individual queries from `queries.sql` in your PostgreSQL client.

### Step 8: Reset ETL (Optional)

To restart the pipeline from scratch:

1. Delete temporary files:
```bash
rm progress.json omdb_cache.sqlite fuzzy_matches.json
```

2. Truncate database tables:
```sql
TRUNCATE TABLE rating, movie_genre, movie, user_t, genre RESTART IDENTITY CASCADE;
```

3. Re-run `python ETL.py`

---

## 🏗️ Design Decisions & Architecture

### Database Schema Design

The database follows a **normalized relational design** to eliminate redundancy and maintain data integrity:

**Tables:**
- `user_t`: User information (user_id, username)
- `movie`: Core movie details (movie_id, title, release_year, director, plot, box_office, decade)
- `genre`: Genre lookup table (genre_id, genre_name)
- `movie_genre`: Many-to-many relationship between movies and genres
- `rating`: User ratings with timestamps

**Normalization:**
- Achieves BCNF (Boyce-Codd Normal Form)
- Genres separated into junction table to avoid multi-valued attributes
- Foreign key constraints ensure referential integrity
- Composite primary keys on junction tables

### API Optimization Strategy

**1. Three-Tier Matching Logic**

| Priority | Type | Description | Example |
|----------|------|-------------|---------|
| 1️⃣ | Exact Match | Title + Year | "Toy Story (1995)" |
| 2️⃣ | Title-Only | Title with cleaned comparison | "Toy Story" |
| 3️⃣ | Fuzzy Search | Logged only, not auto-applied | "Jumanjii" → "Jumanji" (score: 0.92) |

Fuzzy matches are **logged but not automatically applied** to prevent incorrect enrichment. These are saved to `fuzzy_matches.json` for manual verification.

**2. Request Management**

- **Persistent caching** (`requests_cache`): Eliminates duplicate API calls
- **Retry mechanism**: Exponential backoff for failed requests (5 retries)
- **Daily quota tracking**: Hard limit of 1000 requests/day (OMDb free tier)
- **Progress checkpointing**: Resume from last processed movie

**3. Session Optimization**

```python
session = requests.Session()
retry_config = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
```

Reuses TCP connections and handles transient failures automatically.

### Idempotent Loading

All database inserts use `ON CONFLICT DO NOTHING` to ensure:
- Safe reruns without duplicate data
- No manual cleanup required between runs
- Supports incremental loading patterns

### Assumptions Made

1. **MovieLens Format**: Titles follow the format "Movie Title (Year)"
2. **API Availability**: OMDb API is accessible and responses follow documented schema
3. **Genre Separator**: Genres in CSV are pipe-separated (`Action|Adventure|Sci-Fi`)
4. **Rating Range**: All ratings are between 0 and 5
5. **Unique Ratings**: A user cannot rate the same movie twice at the exact same timestamp
6. **Character Encoding**: All text data is UTF-8 compatible

---

## 🔧 Challenges Faced & Solutions

### Challenge 1: Slow API Response Times

**Problem:**
- Initial implementation made multiple sequential API calls per movie
- Each movie required 1-3 API requests (exact → title → fuzzy)
- Total runtime exceeded practical limits for large datasets

**Solution:**
```python
# Implemented persistent HTTP session
session = requests.Session()
retry_config = Retry(total=5, backoff_factor=1, ...)
session.mount("http://", HTTPAdapter(max_retries=retry_config))
```

**Impact:**
- Reduced connection overhead by reusing TCP connections
- Automatic retry with exponential backoff improved reliability
- Processing time significantly decreased

### Challenge 2: OMDb API Daily Quota (1000 requests/day)

**Problem:**
- Free tier limited to 1000 requests per day
- Script restarted from beginning on interruption, wasting quota
- Large dataset required multiple days to complete

**Solution Implemented:**

**a) Progress Checkpointing:**
```python
# Save progress after each movie
Path(PROGRESS_FILE).write_text(json.dumps({"last_index": index}))

# Resume from checkpoint
if Path(PROGRESS_FILE).exists():
    start_index = json.loads(Path(PROGRESS_FILE).read_text()).get("last_index", 0)
```

**b) Request Caching:**
```python
requests_cache.install_cache("omdb_cache", backend="sqlite", expire_after=None)
```

**Results:**

| Before Optimization | After Optimization |
|---------------------|-------------------|
| Multiple redundant API calls | Only unique calls made |
| Restart from beginning on failure | Automatic resume |
| Quota exhausted in single run | Multi-day processing supported |
| No local cache | SQLite cache prevents duplicates |

**Impact:**
- Zero API calls for previously fetched movies
- Ability to process dataset over multiple days
- Script can be interrupted and resumed without penalty

### Challenge 3: Movie Title Matching Inconsistencies

**Problem:**
- MovieLens titles don't always match OMDb exactly
- Punctuation differences: "Batman: The Movie" vs "Batman The Movie"
- Alternate titles or typos in source data
- Wrong matches could corrupt enrichment data

**Solution:**
1. Implemented title normalization (remove punctuation, lowercase)
2. Created fuzzy matching with SequenceMatcher scoring
3. **Crucially**: Fuzzy matches are logged to `fuzzy_matches.json` but NOT automatically applied
4. Manual review process prevents incorrect enrichment

```python
def clean(t):
    return (t.lower()
            .replace(",", "").replace("'", "")
            .replace(":", "").replace("-", "")
            .replace(".", "").strip())
```

**Impact:**
- Reduced false matches
- Created audit trail for data quality
- Maintained data integrity through manual verification

---

## 📊 Sample Analytics Queries

The project includes pre-built queries in `queries.sql`:

1. **Highest Rated Movie** - Movie with the best average rating
2. **Top 5 Genres** - Genres with highest average ratings
3. **Most Prolific Director** - Director with the most movies
4. **Year-wise Trends** - Average ratings by release year

---

## 📁 Project Structure

```
.
├── ETL.py                  # Main ETL pipeline script
├── schema.sql              # PostgreSQL database schema
├── queries.sql             # Analytics queries
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (not in repo)
├── movies.csv              # MovieLens data (not in repo)
├── ratings.csv             # MovieLens data (not in repo)
├── progress.json           # ETL checkpoint (generated)
├── omdb_cache.sqlite       # API response cache (generated)
└── fuzzy_matches.json      # Fuzzy match log (generated)
```