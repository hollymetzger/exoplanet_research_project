import arxiv
import pandas as pd
import ads
import sqlite3
import api_keys
import string

#############################################################
########################## set up ###########################
#############################################################

MAX_RESULTS = 999
DATABASE = "exoplanet_papers.db"

main_papers_df = pd.DataFrame() # stores the main list of papers
# paper_id, title, authors, year, doi, url

main_authors_df = pd.DataFrame()
# author_id, author_name

#############################################################
################## data scraping methods ####################
#############################################################

# returns a pandas dataframe of paper metadata from arxiv
def queryArxiv(*queries, MAX_RESULTS=1):

    papers = []

    for query in queries:
        search = arxiv.Search(
            query=query,
            max_results=MAX_RESULTS,
            sort_by=arxiv.SortCriterion.Relevance
        )
        for result in search.results():
            print(result)
            papers.append({
                "paper_id": len(papers),
                "title": result.title,
                "authors": [a.name for a in result.authors],
                "year": result.published.year,
                "doi": result.doi or None,
                "url": result.entry_id,
                "abstract": result.summary
            })

    return pd.DataFrame(papers)

def queryADS(*queries, max_results=MAX_RESULTS):
    papers = []

    for query in queries:
        results = ads.SearchQuery(
            q=query,
            fl=["title", "author", "year", "doi", "bibcode", "abstract"],
            rows=max_results,
            sort="score desc"
        )

        for i, result in enumerate(results):
            papers.append({
                "title": result.title[0] if result.title else None,
                "authors": result.author if result.author else [],
                "year": int(result.year) if result.year else None,
                "doi": result.doi[0] if result.doi else None,
                "url": f"https://ui.adsabs.harvard.edu/abs/{result.bibcode}" if result.bibcode else None,
                "bibcode": result.bibcode,
                "abstract": result.abstract
            })

    return pd.DataFrame(papers)

# accepts a list of bibcodes and returns a dataframe of them as papers
def fetch_ads_metadata(bibcodes, chunk_size=20):
    papers = []

    for i in range(0, len(bibcodes), chunk_size):
        chunk = bibcodes[i:i+chunk_size]
        query_string = "(" + " OR ".join(chunk) + ")"

        results = list(ads.SearchQuery(
            bibcode=query_string,
            fl=["title", "year", "author", "doi", "bibcode", "abstract"]
        ))

        for r in results:
            papers.append({
                "title": r.title[0] if r.title else None,
                "authors": r.author if r.author else [],
                "year": r.year,
                "doi": r.doi[0] if r.doi else None,
                "url": f"https://ui.adsabs.harvard.edu/abs/{r.bibcode}" if r.bibcode else None,
                "bibcode": r.bibcode,
                "abstract": r.abstract
            })

    return pd.DataFrame(papers)




#############################################################
################## data handling methods ####################
#############################################################

# accepts df with authors column and returns an authors dataframe
def createAuthorsDF(df):
    authors_set = set()
    for authors in df["authors"]:
        for a in authors:
            authors_set.add(a)
    authors_df = pd.DataFrame({
        "author_name": list(authors_set)
    })
    authors_df["author_id"] = range(len(authors_df))

    return authors_df

# accepts two dfs of papers and authors and returns a joint df with paper ids and author ids associated with that paper id
def createPaperAuthorsDF(papers_df, authors_df):
    paper_authors = []

    for _, row in papers_df.iterrows():
        paper_id = row["paper_id"]
        
        for author in row["authors"]:
            author_id = authors_df.loc[
                authors_df["author_name"] == author, "author_id"
            ].values[0]
            
            paper_authors.append({
                "paper_id": paper_id,
                "author_id": author_id
            })

    return pd.DataFrame(paper_authors)

# returns a list of a references from a paper with the given doi
def get_references(doi):
    papers = list(ads.SearchQuery(
        doi=doi,
        fl=["title", "reference"]
    ))
    
    if papers:
        return papers[0].reference
    return []

# returns a list of bibcodes of citations from a paper with the given doi
def get_citations(doi):
    papers = list(ads.SearchQuery(
        doi=doi,
        fl=["citation"]
    ))
    
    if papers and hasattr(papers[0], "citation"):
        return papers[0].citation
    return []

# accepts a df of papers with a "doi" col and returns a df of edges
def createEdgesDF(papers_df):
    edges = []
    for index, paper in papers_df.iterrows():
        doi = paper["doi"]
        
        if pd.notna(doi):
            refs = get_references(doi)
            
            for ref in refs:
                edges.append({
                    "source_doi": doi,
                    "target_bibcode": ref
                })
    return pd.DataFrame(edges)


###########################################################
##################### sqlite methods ######################
###########################################################

def insert_into_db(papers_df, conn):
    cursor = conn.cursor()

    #################################################
    # 1. Create tables (if they don't exist)
    #################################################

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS papers (
        paper_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT UNIQUE,
        year INTEGER,
        doi TEXT,
        url TEXT,
        bibcode TEXT,
        abstract TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS authors (
        author_id INTEGER PRIMARY KEY AUTOINCREMENT,
        author_name TEXT UNIQUE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS paper_authors (
        paper_id INTEGER,
        author_id INTEGER,
        UNIQUE(paper_id, author_id),
        FOREIGN KEY(paper_id) REFERENCES papers(paper_id),
        FOREIGN KEY(author_id) REFERENCES authors(author_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS citations (
        source_paper_id INTEGER,
        target_paper_id INTEGER,
        UNIQUE(source_paper_id, target_paper_id),
        FOREIGN KEY(source_paper_id) REFERENCES papers(paper_id),
        FOREIGN KEY(target_paper_id) REFERENCES papers(paper_id)
    )
    """)

    conn.commit()

    #################################################
    # 2. Insert papers
    #################################################

    for _, row in papers_df.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO papers (title, year, doi, url, bibcode, abstract)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (cleanString(row["title"]), row["year"], row["doi"], row["url"], row["bibcode"], row["abstract"]))
            if cursor.rowcount == 1:
                print(f"Inserted '{row['title']}' into {DATABASE}")
        except Exception as e:
            print(f"Error inserting paper: {row['title']}")
            print(e)

    conn.commit()

    #################################################
    # 3. Insert authors + relationships
    #################################################

    for _, row in papers_df.iterrows():
        doi = row["doi"]

        # get paper_id from DB
        cursor.execute("SELECT paper_id FROM papers WHERE doi IS ?", (doi,))
        result = cursor.fetchone()

        if not result:
            continue

        paper_id = result[0]

        for author in row["authors"]:
            try:
                # insert author if new
                cursor.execute("""
                    INSERT OR IGNORE INTO authors (author_name)
                    VALUES (?)
                """, (author,))

                # get author_id
                cursor.execute("""
                    SELECT author_id FROM authors WHERE author_name = ?
                """, (author,))
                author_id = cursor.fetchone()[0]

                # link paper ↔ author
                cursor.execute("""
                    INSERT OR IGNORE INTO paper_authors (paper_id, author_id)
                    VALUES (?, ?)
                """, (paper_id, author_id))

            except Exception as e:
                print(f"Error linking author {author} to paper {doi}")
                print(e)

    conn.commit()

    print("Database updated successfully.")

def get_paper_id(cursor, doi, title):
    if doi:
        cursor.execute("SELECT paper_id FROM papers WHERE doi = ?", (doi,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # fallback to title
    cursor.execute("SELECT paper_id FROM papers WHERE title = ?", (title,))
    result = cursor.fetchone()
    if result:
        return result[0]

    return None

def add_references_for_paper(doi, conn):
    cursor = conn.cursor()

    # get source paper (the one doing the citing)
    cursor.execute("SELECT paper_id, title FROM papers WHERE doi = ?", (doi,))
    source = cursor.fetchone()

    if not source:
        print(f"Paper with DOI {doi} not found in DB.")
        return

    source_paper_id, source_title = source

    print(f"\nFetching references for:\n{source_title}\n")

    bibcodes = get_references(doi)

    if not bibcodes:
        print("No references found.")
        return

    ref_df = fetch_ads_metadata(bibcodes)

    # insert referenced papers
    insert_into_db(ref_df, conn)

    # create edges
    for _, row in ref_df.iterrows():
        target_id = get_paper_id(cursor, row["doi"], row["title"])

        if target_id:
            try:
                # source → target
                cursor.execute("""
                    INSERT OR IGNORE INTO citations (source_paper_id, target_paper_id)
                    VALUES (?, ?)
                """, (source_paper_id, target_id))

            except Exception as e:
                print("Error inserting reference edge:", e)

    conn.commit()

    print(f"Added {len(ref_df)} reference edges.\n")

def add_citations_for_paper(doi, conn):
    cursor = conn.cursor()

    # target paper (being cited)
    cursor.execute("SELECT paper_id, title FROM papers WHERE doi = ?", (doi,))
    target = cursor.fetchone()

    if not target:
        print(f"Paper with DOI {doi} not found in DB.")
        return

    target_paper_id, target_title = target

    print(f"\nFetching forward citations for:\n{target_title}\n")

    bibcodes = get_citations(doi)

    if not bibcodes:
        print("No citations found.")
        return

    citing_df = fetch_ads_metadata(bibcodes)

    # insert citing papers
    insert_into_db(citing_df, conn)

    # create edges
    for _, row in citing_df.iterrows():
        source_id = get_paper_id(cursor, row["doi"], row["title"])

        if source_id:
            try:
                # source → target
                cursor.execute("""
                    INSERT OR IGNORE INTO citations (source_paper_id, target_paper_id)
                    VALUES (?, ?)
                """, (source_id, target_paper_id))

            except Exception as e:
                print("Error inserting citation edge:", e)

    conn.commit()

    print(f"Added {len(citing_df)} citation edges.\n")

def normalize_titles_and_deduplicate(db_path=DATABASE):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\nNormalizing titles and removing duplicates...\n")

    #################################################
    # 1. Normalize titles to uppercase
    #################################################

    cursor.execute("""
        UPDATE papers
        SET title = UPPER(title)
    """)

    conn.commit()

    #################################################
    # 2. Find duplicates (same title after normalization)
    #################################################

    cursor.execute("""
        SELECT title, GROUP_CONCAT(paper_id)
        FROM papers
        GROUP BY title
        HAVING COUNT(*) > 1
    """)

    duplicates = cursor.fetchall()

    print(f"Found {len(duplicates)} duplicate title groups.\n")

    #################################################
    # 3. Merge duplicates
    #################################################

    for title, id_list_str in duplicates:
        ids = list(map(int, id_list_str.split(",")))

        # choose one to keep
        keep_id = ids[0]
        remove_ids = ids[1:]

        print(f"Merging duplicates for title:\n{title}")
        print(f"Keeping ID {keep_id}, removing {remove_ids}\n")

        for rid in remove_ids:

            # --- Update paper_authors ---
            cursor.execute("""
                UPDATE OR IGNORE paper_authors
                SET paper_id = ?
                WHERE paper_id = ?
            """, (keep_id, rid))

            # --- Update citations (source side) ---
            cursor.execute("""
                UPDATE OR IGNORE citations
                SET source_paper_id = ?
                WHERE source_paper_id = ?
            """, (keep_id, rid))

            # --- Update citations (target side) ---
            cursor.execute("""
                UPDATE OR IGNORE citations
                SET target_paper_id = ?
                WHERE target_paper_id = ?
            """, (keep_id, rid))

            # --- Delete duplicate paper ---
            cursor.execute("""
                DELETE FROM papers
                WHERE paper_id = ?
            """, (rid,))

    conn.commit()

    print("✅ Normalization and deduplication complete.\n")

def deduplicate_by_doi(db_path=DATABASE):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\nremoving duplicates...\n")

    #################################################
    # 1. Find duplicates
    #################################################

    cursor.execute("""
        SELECT doi, GROUP_CONCAT(paper_id)
        FROM papers
        GROUP BY doi
        HAVING COUNT(*) > 1
    """)

    duplicates = cursor.fetchall()

    print(f"Found {len(duplicates)} duplicate doi groups.\n")

    if (len(duplicates) == 0):
        return

    duplicates.pop(0)


    for doi, id_list_str in duplicates:
        ids = list(map(int, id_list_str.split(",")))
        print(doi)
        print(len(ids))

    #################################################
    # 3. Merge duplicates
    #################################################

    for doi, id_list_str in duplicates:
        ids = list(map(int, id_list_str.split(",")))

        # choose one to keep
        keep_id = ids[0]
        remove_ids = ids[1:]

        print(f"Merging duplicates for title:\n{doi}")
        print(f"Keeping ID {keep_id}, removing {remove_ids}\n")

        for rid in remove_ids:

            # --- Update paper_authors ---
            cursor.execute("""
                UPDATE OR IGNORE paper_authors
                SET paper_id = ?
                WHERE paper_id = ?
            """, (keep_id, rid))

            # --- Update citations (source side) ---
            cursor.execute("""
                UPDATE OR IGNORE citations
                SET source_paper_id = ?
                WHERE source_paper_id = ?
            """, (keep_id, rid))

            # --- Update citations (target side) ---
            cursor.execute("""
                UPDATE OR IGNORE citations
                SET target_paper_id = ?
                WHERE target_paper_id = ?
            """, (keep_id, rid))

            # --- Delete duplicate paper ---
            cursor.execute("""
                DELETE FROM papers
                WHERE paper_id = ?
            """, (rid,))

    conn.commit()
    conn.close()

    print("✅ Normalization and deduplication complete.\n")

def expand_all_citations(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT doi FROM papers WHERE doi IS NOT NULL")
    dois = [row[0] for row in cursor.fetchall()]

    for doi in dois:
        add_citations_for_paper(doi, conn)

def getPaperIDsByTitleKeyword(conn, *keywords):
    cursor = conn.cursor()
    ids = []
    for kw in keywords:
        cursor.execute(f"""
                        SELECT paper_id from papers where
                        title like '%{kw}%';
                        """)
        ids = ids + cursor.fetchall()

    # deduplicate the list if a title had more than one of the keywords in it
    return list(set(ids))

def initSQLiteConnection(db_path=DATABASE):
    return sqlite3.connect(db_path)

def cleanString(text):
    return text.translate(str.maketrans('', '', string.punctuation)).upper()

def getDOIs(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT doi FROM papers;
    """)

    return cursor.fetchall()

def main1():
    x = queryADS('hycean AND analysis OR "water world" AND analysis OR "ocean planet" AND analysis', max_results=999)
    conn = initSQLiteConnection()
    insert_into_db(x, conn)
    conn.close()

def main():
    conn = initSQLiteConnection()

    # do initial search for key papers
    x = queryADS('hycean AND analysis OR "water world" AND analysis OR "ocean planet" AND analysis', max_results=10)
    insert_into_db(x, conn)
    normalize_titles_and_deduplicate()
    deduplicate_by_doi()

    return

    # get citations for each of the papers
    dois = getDOIs(conn)

    for doi in dois:
        add_citations_for_paper(doi, conn)
    

main()