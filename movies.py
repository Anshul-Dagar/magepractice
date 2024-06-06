import dlt
from dlt.sources.helpers import requests

pages=1
base_url = "https://api.themoviedb.org/3/trending/all/day?language=en-US"

headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZDc2ZTIxZDdmZjZjZDNmMzA3NmUzMzE2YTE4NDBjNiIsInN1YiI6IjVmZTQ4YmQ5MTYwZTczMDAzZGE5N2U3YiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.REKwLi3q5TDV8QXgvfQHywOt__CYv-Xb9e4sjNjOvlk"
}
# Make a request and check if it was successful
data =[]
while pages<=500:
    response = requests.get(base_url +"&page=" +str(pages), headers=headers)
    response.raise_for_status()
    data.append(response.json())
    length =len(data)
    pages = pages+1
    print("Page " +str(pages) + " loaded")

movies_data =[]    
for x in range(0,500):
    movies_data.append(data[x]['results'])
    

pipeline = dlt.pipeline(
    pipeline_name="movies_pipeline",
    destination=dlt.destinations.postgres("postgres://postgres.dpmlropwzmetnnkpbgmh:CMTFX51oHWWPseCS@aws-0-us-east-1.pooler.supabase.com:5432/postgres"),
    dataset_name="movies",
)

# The response contains a list of issues
load_info = pipeline.run(movies_data, table_name="movies", write_disposition="replace")

print(load_info)