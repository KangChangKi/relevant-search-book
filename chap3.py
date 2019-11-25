from helpers import *

# you'll need to have an API key for TMDB
# to run these examples,
# run export TMDB_API_KEY=<YourAPIKey>
tmdb_api_key = os.environ["TMDB_API_KEY"]

# Setup tmdb as its own session, caching requests
# (we only want to cache tmdb, not elasticsearch)
# Get your TMDB API key from
#  https://www.themoviedb.org/documentation/api
# then in shell do export TMDB_API_KEY=<Your Key>
tmdb_api = requests.Session()
tmdb_api.params={'api_key': tmdb_api_key}

# Optional, enable client-side caching for TMDB
# Requires: https://httpcache.readthedocs.org/en/latest/
#from httpcache import CachingHTTPAdapter
#tmdb_api.mount('https://', CachingHTTPAdapter())
#tmdb_api.mount('http://', CachingHTTPAdapter())

## 3.2.2 Indexing TMDB Movies

movieIds = [];
numMoviesToGrab = 10000
numPages = numMoviesToGrab // 20

if os.path.isfile('movieIds.dat'):
    with open('movieIds.dat', 'rb') as file:
        movieIds = pickle.load(file)
else:
    for page in range(1, numPages + 1):
        httpResp = tmdb_api.get('https://api.themoviedb.org/3/movie/top_rated', params={'page': page})  #(1)
        jsonResponse = json.loads(httpResp.text) #(2)
        if 'results' not in jsonResponse:
            continue
        movies = jsonResponse['results']
        for movie in movies:
            if (movie['id'] not in [9549]):
                movieIds.append(movie['id'])

print(len(movieIds))

movieDict = {}

if os.path.isfile('movieDict.dat'):
    with open('movieDict.dat', 'rb') as file:
        movieDict = pickle.load(file)
else:
    for i, movieId in enumerate(movieIds):
        httpResp = tmdb_api.get(f"https://api.themoviedb.org/3/movie/{movieId}")
        movie = json.loads(httpResp.text)
        movieDict[movieId] = movie
        print(f"{i}/{len(movieIds)}")

print(len(movieDict))

# Leave local data files for saving time to build the data from scratch

if not os.path.isfile('movieIds.dat'):
    with open('movieIds.dat', 'wb') as file:
        pickle.dump(movieIds, file)

if not os.path.isfile('movieDict.dat'):
    with open('movieDict.dat', 'wb') as file:
        pickle.dump(movieDict, file)

# Destroy any existing index (equiv to SQL "drop table")

delete_index('tmdb')
    
# Create the index with explicit settings
# We need to explicitely set number of shards to 1 to eliminate the impact of 
# distributed IDF on our small collection
# See also "Relavance is Broken!"
# http://www.elastic.co/guide/en/elasticsearch/guide/current/relevance-is-broken.html

settings = {
    "settings": {"number_of_shards": 1}
}

create_index('tmdb', settings)

# Bulk index title & overview to the movie endpoint

print(f"Indexing {len(movieDict)} movies")

bulkMovies = ""
for k, movie in movieDict.items():
    if 'id' not in movie:
        continue
    addCmd = {"index": {"_index": "tmdb", "_id": movie["id"]}}
    esDoc  = {"title": movie['title'], 'overview': movie['overview'], 'tagline': movie['tagline']}
    bulkMovies += json.dumps(addCmd) + "\n" + json.dumps(esDoc) + "\n"

fill_index_bulk(bulkMovies)

## 3.2.3 Basic Searching

usersSearch = 'basketball with cartoon aliens'
query = {
    'query': {
        'multi_match': { 
            'query': usersSearch,  #User's query
            'fields': ['title^10', 'overview'],
        }
    },
    'size': '100',
    'explain': True
}

search(query)

## 2.3.1 Query Validation API

query = {
   'query': {
        'multi_match': { 
            'query': usersSearch,  #User's query
            'fields': ['title^10', 'overview']
        }
    }
}

explain(query)

#     {u'valid': True, u'explanations': [{u'index': u'tmdb', u'explanation': u'filtered((((title:basketball title:with title:cartoon title:aliens)^10.0) | (overview:basketball overview:with overview:cartoon overview:aliens)))->cache(_type:movie)', u'valid': True}], u'_shards': {u'successful': 1, u'failed': 0, u'total': 1}}
# 

## 2.3.3 Debugging Analysis

# Inner Layer of the Onion -- Why did the search engine consider these movies matches? Two sides to this
# (1) What tokens are placed in the search engine?
# (2) What did the search engine attempt to match exactly?

# Explain of what's happening when we construct these terms
# https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-analyze.html

query = {"field":"title", "text": "Fire with Fire"}

analyze('tmdb', query)

## 2.3.5 -- Solving The Matching Problem

# DELETE AND RECREATE THE INDEX WITH ENGLISH ANALYZERS

delete_index('tmdb')

settings = {
    'settings': {
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
    'mappings': {
        'properties': {
            'title': {
                'type': 'text',
                'analyzer': 'english'
            },
            'overview': {
                'type': 'text',
                'analyzer': 'english'
            }
        }
    }
}

create_index('tmdb', settings)
sleep(1)

# Inspecting the mappings

mappings('tmdb')

# Reanalyze the string

query = {"field":"title", "text": "Fire with Fire"}
analyze('tmdb', query)

# Reindex

fill_index_bulk(bulkMovies)

resp = requests.get('http://localhost:9200/tmdb/_refresh')
print(resp.text)
sleep(1)

# Search again

usersSearch = 'Fire with Fire'
query = {
    'query': {
        'multi_match': { 
            'query': usersSearch,  #User's query
            'fields': ['title^10', 'overview'],
        }
    },
    'size': '100',
    'explain': True
}

search(query)

## 2.4.1	Decomposing Relevance Score With Lucene’s Explain

query['explain'] = True
httpResp = search(query, print=False)
jsonResp = json.loads(httpResp.text)
print(json.dumps(jsonResp['hits']['hits'][0]['_explanation'], indent=True))
print("Explain for %s" % jsonResp['hits']['hits'][0]['_source']['title'])
print(simplerExplain(jsonResp['hits']['hits'][0]['_explanation']))
print("Explain for %s" % jsonResp['hits']['hits'][1]['_source']['title'])
print(simplerExplain(jsonResp['hits']['hits'][1]['_explanation']))
print("Explain for %s" % jsonResp['hits']['hits'][2]['_source']['title'])
print(simplerExplain(jsonResp['hits']['hits'][2]['_explanation']))
print("Explain for %s" % jsonResp['hits']['hits'][3]['_source']['title'])
print(simplerExplain(jsonResp['hits']['hits'][3]['_explanation']))
print("Explain for %s" % jsonResp['hits']['hits'][10]['_source']['title'])
print(simplerExplain(jsonResp['hits']['hits'][10]['_explanation']))

## 2.4.4	Fixing Space Jam vs Alien Ranking

# Search with saner boosts

query = {
    'query': {
        'multi_match': { 
            'query': usersSearch,  #User's query
            'fields': ['title^0.1', 'overview'],
        }
    },
    'size': '100',
    'explain': True
}

search(query)
