# ------------------ IMPORTS ------------------
import os
import json
import time
from pathlib import Path
import pandas as pd
import requests
import requests_cache
from difflib import SequenceMatcher
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import quote_plus
from dotenv import load_dotenv


# ------------------ LOAD ENVIRONMENT VARIABLES ------------------
load_dotenv()

OMDB_API_KEY  = os.getenv("OMDB_API_KEY")
POSTGRES_USER = os.getenv("PGUSER")
POSTGRES_PASS = os.getenv("PGPASSWORD")
POSTGRES_HOST = os.getenv("PGHOST", "localhost")
POSTGRES_PORT = os.getenv("PGPORT", "5432")
POSTGRES_DB   = os.getenv("PGDATABASE")

if not all([OMDB_API_KEY, POSTGRES_USER, POSTGRES_PASS, POSTGRES_DB]):
    raise ValueError(" Missing values in .env (OMDB_API_KEY / PGUSER / PGPASSWORD / PGDATABASE)")


OMDB_URL = "http://www.omdbapi.com/"

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{quote_plus(POSTGRES_PASS)}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


# ------------------ CONSTANTS ------------------
MAX_REQUESTS_PER_DAY = 1000
PROGRESS_FILE = "progress.json"


# ------------------ HTTP SESSION + CACHE ------------------
requests_cache.install_cache("omdb_cache", backend="sqlite", expire_after=None)

session = requests.Session()
retry_config = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
session.mount("http://", HTTPAdapter(max_retries=retry_config))
session.mount("https://", HTTPAdapter(max_retries=retry_config))


# ------------------ DB CONNECTION ------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# ------------------ HELPER FUNCTION ------------------
def extract_title_year(full_title: str):
    """Extract Title and Year from 'Movie (1995)'."""
    if "(" in full_title and ")" in full_title[-5:]:
        return full_title.rsplit("(", 1)[0].strip(), full_title[-5:-1]
    return full_title, None


# ------------------ EXTRACT ------------------
print("\n=== EXTRACT PHASE ===")

movies = pd.read_csv("movies.csv")
ratings = pd.read_csv("ratings.csv")

movies["Director"] = None
movies["Plot"] = None
movies["BoxOffice"] = None

# resume progress
start_index = 0
if Path(PROGRESS_FILE).exists():
    start_index = json.loads(Path(PROGRESS_FILE).read_text()).get("last_index", 0)

print(f"Resuming from movie index {start_index}")

requests_used = 0
fuzzy_log = []


for index in range(start_index, len(movies)):

    if requests_used >= MAX_REQUESTS_PER_DAY:
        print("\nDAILY LIMIT REACHED (1000 requests). Come back tomorrow.")
        break

    title, year = extract_title_year(movies.at[index, "title"])

    # Exact match (title + year)
    r = session.get(OMDB_URL, params={"apikey": OMDB_API_KEY, "t": title, "y": year}, timeout=6)

    if not getattr(r, "from_cache", False):
        requests_used += 1

    data = r.json()
    
    if data.get("Response") == "True":
        print(f"Exact: {title} ({year})")
        movies.at[index, "Director"]   = data.get("Director")
        movies.at[index, "Plot"]       = data.get("Plot")
        movies.at[index, "BoxOffice"]  = data.get("BoxOffice")
        continue

    else:
        # Title-only fallback
        r = session.get(OMDB_URL, params={"apikey": OMDB_API_KEY, "t": title}, timeout=6)
        if not getattr(r, "from_cache", False):
         requests_used += 1

        data = r.json()

        if data.get("Response") == "True":

            returned_title = data.get("Title", "")

            def clean(t):
                return (
                    t.lower()
                     .replace(",", "")
                     .replace("'", "")
                     .replace(":", "")
                     .replace("-", "")
                     .replace(".", "")
                     .strip()
                )

            if clean(returned_title) == clean(title):
                print(f"Title-only exact match: {title} == {returned_title}")
                movies.at[index, "Director"]  = data.get("Director")
                movies.at[index, "Plot"]      = data.get("Plot")
                movies.at[index, "BoxOffice"] = data.get("BoxOffice")
                continue  # do not go to fuzzy mode

            else:
                print(f"Title mismatch logged: CSV:`{title}` → API:`{returned_title}`")
                fuzzy_log.append({
                    "CSV Title": title,
                    "Suggested Match": returned_title,
                    "Year": data.get("Year"),
                    "Score": SequenceMatcher(None, title.lower(), returned_title.lower()).ratio()
                })

        else:
            #Fuzzy search
            r = session.get(OMDB_URL, params={"apikey": OMDB_API_KEY, "s": title}, timeout=6)
            if not getattr(r, "from_cache", False):
             requests_used += 1
            data = r.json()

            if data.get("Response") == "True":
                best = max(data["Search"], key=lambda m: SequenceMatcher(None, title, m["Title"]).ratio())
                score = SequenceMatcher(None, title, best["Title"]).ratio()

                print(f"Fuzzy match: {title} → {best['Title']} (score={score:.2f})")
                fuzzy_log.append({
                    "CSV Title": title,
                    "Suggested Match": best["Title"],
                    "Year": best["Year"],
                    "Score": score
                })

    Path(PROGRESS_FILE).write_text(json.dumps({"last_index": index}))
    time.sleep(0.05)

    # save fuzzy log after each batch update
if fuzzy_log:
    with open("fuzzy_matches.json", "w", encoding="utf-8") as f:
        json.dump(fuzzy_log, f, indent=4, ensure_ascii=False)

    print(f"\nSaved fuzzy match suggestions → fuzzy_matches.json ({len(fuzzy_log)} entries)")
else:
    print("\nNo fuzzy matches to log.")



# ------------------ TRANSFORM ------------------
print("\n=== TRANSFORM PHASE ===")

ratings["timestamp"] = pd.to_datetime(ratings["timestamp"], unit="s")

movies["release_year"] = movies["title"].str.extract(r"\((\d{4})\)").astype("Int64")
movies["decade"] = movies["release_year"].apply(
    lambda y: f"{int(y)//10*10}s" if pd.notna(y) else None
)

movie_genres = movies[["movieId", "genres"]].copy()
movie_genres.loc[:, "genre"] = movie_genres["genres"].str.split("|")
movie_genres = movie_genres.explode("genre")
movie_genres["genre"] = movie_genres["genre"].str.strip()

users = ratings["userId"].unique().tolist()
genres = pd.unique(movie_genres["genre"])


# ------------------ LOAD ------------------
print("\n=== LOAD PHASE ===")

with engine.begin() as conn:

    conn.execute(text("""
        INSERT INTO user_t (user_id, username)
        VALUES (:uid, :uname)
        ON CONFLICT (user_id) DO NOTHING;
    """), [{"uid": int(uid), "uname": f"User_{uid}"} for uid in users])

    conn.execute(text("""
        INSERT INTO genre (genre_name)
        VALUES (:g)
        ON CONFLICT (genre_name) DO NOTHING;
    """), [{"g": g} for g in genres])

    conn.execute(text("""
        INSERT INTO movie (movie_id, title, release_year, director, plot, box_office, decade)
        VALUES (:mid, :title, :release_year, :director, :plot, :box_office, :decade)
        ON CONFLICT (movie_id) DO NOTHING;
    """), movies.rename(columns={
        "movieId": "mid",
        "Director": "director",
        "Plot": "plot",
        "BoxOffice": "box_office"
    })[["mid", "title", "release_year", "director", "plot", "box_office", "decade"]]
    .to_dict(orient="records"))

    for _, row in movie_genres.iterrows():
        conn.execute(text("""
            INSERT INTO movie_genre (movie_id, genre_id)
            VALUES (
                :mid,
                (SELECT genre_id FROM genre WHERE genre_name = :g)
            )
            ON CONFLICT (movie_id, genre_id) DO NOTHING;
        """), {"mid": int(row.movieId), "g": row.genre})

    conn.execute(text("""
        INSERT INTO rating (user_id, movie_id, rating, rating_ts)
        VALUES (:uid, :mid, :rating, :ts)
        ON CONFLICT DO NOTHING;
    """), ratings.rename(columns={"userId": "uid", "movieId": "mid", "timestamp": "ts"}).to_dict(orient="records"))

print("\nETL COMPLETE — Loaded today's batch into PostgreSQL.")
print(" Come back tomorrow, script resumes automatically.")
