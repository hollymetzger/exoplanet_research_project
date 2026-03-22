import sys
from arxivsearcher import queryArxiv, insert_into_db

def print_papers(df):
    print("\n" + "="*60)
    print("PAPERS FOUND")
    print("="*60)

    for i, row in df.iterrows():
        print(f"\n[{i+1}] {row['title']}")
        
        authors = ", ".join(row["authors"])
        print(f"    Authors: {authors}")
        print(f"    Year: {row['year']}")
        
        if row["doi"]:
            print(f"    DOI: {row['doi']}")
        
        print(f"    URL: {row['url']}")

    print("\n" + "="*60)
    print(f"Total papers found: {len(df)}")
    print("="*60 + "\n")


def main():
    # get queries from command line
    queries = sys.argv[1:]

    if not queries:
        print("Usage: python run_scraper.py \"query1\" \"query2\" ...")
        sys.exit(1)

    print("\nSearching arXiv for:")
    for q in queries:
        print(f"  - {q}")

    # run query
    df = queryArxiv(*queries)

    if df.empty:
        print("\nNo papers found.")
        return

    # print results nicely
    print_papers(df)

    # save to database
    insert_into_db(df)

    print("Done.\n")


if __name__ == "__main__":
    main()
