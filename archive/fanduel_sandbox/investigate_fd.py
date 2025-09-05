import requests

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Let's check what the working endpoint actually returns
url = "https://sportsbook.fanduel.com/cache/psmg/UK/67388.json"
headers = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

r = requests.get(url, headers=headers)
print(f"Status: {r.status_code}")
print(f"Content-Type: {r.headers.get('content-type')}")
print(f"Size: {len(r.content)} bytes")
print(f"First 500 chars:\n{r.text[:500]}")

# If it's not JSON, let's see what it is
if r.status_code == 200:
    # Check if it's actually JSON but with a different structure
    try:
        data = r.json()
        print("\nSuccessfully parsed as JSON!")
        print(f"Keys: {list(data.keys()) if isinstance(data, dict) else 'List/Array'}")
    except:
        print("\nNot valid JSON, checking content...")
        if "odds" in r.text.lower() or "price" in r.text.lower():
            print("Contains odds/price keywords!")

# Let's try to find patterns in FanDuel's actual site
print("\n\nChecking main FanDuel page for API patterns...")
main_page = requests.get("https://sportsbook.fanduel.com/", headers=headers)
print(f"Main page status: {main_page.status_code}")

# Look for API URLs in the main page
import re

api_patterns = re.findall(
    r'(https?://[^"\']+(?:api|cache|data|feed)[^"\']+)', main_page.text
)
unique_apis = list(set(api_patterns))[:10]
print(f"\nFound {len(unique_apis)} potential API URLs:")
for api in unique_apis:
    print(f"  - {api}")
