import os
import re
import requests
from pathlib import Path

def check_links_in_md(directory_path):
    broken_links = []
    pattern = re.compile(r'\[(.*?)\]\((http.*?)\)')

    for md_file in Path(directory_path).glob('*.md'):
        with open(md_file, 'r', encoding='utf-8') as file:
            content = file.read()
            links = pattern.findall(content)

            print(f"\nChecking links in: {md_file.name}")
            for text, url in links:
                try:
                    response = requests.head(url, allow_redirects=True, timeout=5)
                    if response.status_code >= 400:
                        print(f"❌ Kırık: {url} ({response.status_code})")
                        broken_links.append((md_file.name, text, url, response.status_code))
                    else:
                        print(f"✅ OK: {url}")
                except Exception as e:
                    print(f"❌ Error: {url} ({str(e)})")
                    broken_links.append((md_file.name, text, url, str(e)))

    if broken_links:
        print("\n--- Broken Links Report ---")
        for file, text, url, err in broken_links:
            print(f"In '{file}': [{text}]({url}) → {err}")
    else:
        print("\nNo broken links found!")


if __name__ == "__main__":
    check_links_in_md(".")