import arxiv
import pandas as pd
import ads

#############################################################
########################## set up ###########################
#############################################################

MAX_RESULTS = 3

ads.config.token = "B1zNcCGbdLDSYXEXWEmJkB6AFa1tpTFWGstwwSZZ"

main_papers_df = pd.DataFrame() # stores the main list of papers
# paper_id, title, authors, year, doi, url

main_authors_df = pd.DataFrame()
# author_id, author_name


#############################################################
################## data scraping methods ####################
#############################################################

# returns a pandas dataframe of paper metadata from arxiv
def queryArxiv(*queries):

    papers = []

    for query in queries:
        search = arxiv.Search(
            query=query,
            max_results=MAX_RESULTS,
            sort_by=arxiv.SortCriterion.Relevance
        )
        for result in search.results():
            papers.append({
                "paper_id": len(papers),
                "title": result.title,
                "authors": [a.name for a in result.authors],
                "year": result.published.year,
                "doi": result.doi or None,
                "url": result.entry_id
            })

    return pd.DataFrame(papers)

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


##### testing

main_papers_df = queryArxiv('"ocean world"')
main_authors_df = createAuthorsDF(main_papers_df)
paper_authors = createPaperAuthorsDF(main_papers_df, main_authors_df)
edges_df = createEdgesDF(main_papers_df)

print("****************** Paper authors head")
print(paper_authors.head())
print("****************** Paper authors cols")
print(paper_authors.columns)
print("****************** Paper authors dtypes")
print(paper_authors.dtypes)




