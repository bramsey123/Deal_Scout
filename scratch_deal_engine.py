import cloudscraper, bs4, pprint, textwrap

url = "https://dealstream.com/texas-businesses-for-sale"
scraper = cloudscraper.create_scraper()
html = scraper.get(url).text

# Dump the first 500 chars to see if we still get Cloudflare
print(textwrap.shorten(html, 500))

soup = bs4.BeautifulSoup(html, "html.parser")
titles = [a.get_text(strip=True)
          for a in soup.select('a[href*="/listing/"]')]
pprint.pprint(titles[:10])
print(f"Parsed {len(titles)} titles.")