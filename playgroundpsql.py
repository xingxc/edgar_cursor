#%%
import requests

url = 'https://www.sec.gov/Archives/edgar/data/0001756262/000095017025067004/tmdx-20250331.htm'
file_path = "tmdx-20250331.html"

# Fetch the content from the URL
response = requests.get(url, headers={"User-agent": "email@email.com"})

# Save the raw HTML content directly
with open(file_path, "wb") as file:
    file.write(response.content)


# %%
