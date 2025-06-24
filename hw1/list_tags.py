import requests
import time

# GraphQL endpoint URL
GRAPHQL_URL = "http://localhost:9003/api/v1/scrapping/graph/query"
# Keyword service endpoint
KEYWORD_SERVICE_URL = "http://localhost:5000/keywords"

# GraphQL query
GRAPHQL_QUERY = """
query GetArticles($page: Int!, $pageSize: Int!) {
  articles(page: $page, pageSize: $pageSize) {
    items {
      id
      name
      text
      complexity
      readingTime
      tags
      likes
      likedByUser
    }
    pageInfo {
      page
      pageSize
      hasNextPage
      hasPreviousPage
    }
  }
}
"""

def extract_keywords(text):
    response = requests.post(
        KEYWORD_SERVICE_URL,
        json={"text": text},
        headers={"Content-Type": "application/json"},
        timeout=20
    )
    return response.json().get("keywords", [])

def fetch_all_articles():
    page = 1
    page_size = 10
    
    while True:
        variables = {"page": page, "pageSize": page_size}
        
        try:
            response = requests.post(
                GRAPHQL_URL,
                json={"query": GRAPHQL_QUERY, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=15
            )
            
            if response.status_code != 200:
                print(f"GraphQL request failed: {response.status_code}")
                print(response.text)
                break
                
            data = response.json()
            articles = data["data"]["articles"]["items"]
            page_info = data["data"]["articles"]["pageInfo"]
            for article in articles:
                keywords = extract_keywords(article["text"])
                print(article["id"], article['name'])
                print(', '.join(keywords))
                print()

            
            if not page_info["hasNextPage"]:
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Network error: {str(e)}")
            break
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            break

if __name__ == "__main__":
    fetch_all_articles()