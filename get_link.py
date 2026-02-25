import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup

TURKISH_MONTHS = {
    'Ocak': 1, 'Şubat': 2, 'Mart': 3, 'Nisan': 4, 'Mayıs': 5, 'Haziran': 6,
    'Temmuz': 7, 'Ağustos': 8, 'Eylül': 9, 'Ekim': 10, 'Kasım': 11, 'Aralık': 12,
}

CATEGORY_MAP = {
    'egitim': 'Eğitim',
    'ogrenci-faaliyetleri-ve-odulleri': 'Eğitim',
    'ar-ge': 'Arge',
    'saglik': 'Sağlık',
    'sağlık': 'Sağlık',
    'oduller': 'Sağlık',
    'sosyal-sorumluluk': 'Sosyal Sorumluluk',
    'sosyal_sorumluluk': 'Sosyal Sorumluluk',
}

CATEGORY_FILENAME = {
    'Eğitim': 'education_activities',
    'Arge': 'research_activities',
    'Sağlık': 'health_activities',
    'Sosyal Sorumluluk': 'social_responsibility_activities',
}

def parse_date(tarih):
    if not tarih or not str(tarih).strip():
        return pd.NaT
    tarih = str(tarih).strip()
    try:
        if '.' in tarih:
            return pd.to_datetime(tarih, dayfirst=True)
        match = re.match(r'^(\d{1,2})\s+(\w+)\s+(\d{4})$', tarih)
        if match:
            day, month_name, year = match.groups()
            month = TURKISH_MONTHS.get(month_name)
            if month is not None:
                return pd.Timestamp(year=int(year), month=month, day=int(day))
        if '-' in tarih and tarih.count('-') == 2:
            parts = tarih.split('-')
            if len(parts[0]) == 4 and parts[0].isdigit():
                return pd.to_datetime(tarih)
            return pd.to_datetime(parts[1], dayfirst=True)
        if re.match(r'^\d{4}$', tarih):
            return pd.to_datetime(f'{tarih}-01-01')
        if re.match(r'^\d{4}(-\d{4})+$', tarih):
            year = tarih.split('-')[-1]
            return pd.to_datetime(f'{year}-01-01')
        return pd.NaT
    except Exception:
        return pd.NaT

def format_date_dd_mm_yyyy(ts, fallback):
    if pd.isna(ts):
        return fallback
    try:
        return ts.strftime('%d.%m.%Y')
    except Exception:
        return fallback

def _fetch_page(session, url, prefix, category_hint):
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
    except Exception as e:
        return None, [], url, str(e)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        return None, [], url, "no table"
    rows = table.find_all("tr")[1:]
    category = CATEGORY_MAP.get(category_hint.lower(), 'Diğer')
    if category not in ('Eğitim', 'Arge', 'Sağlık', 'Sosyal Sorumluluk'):
        return None, [], url, "unknown category"
    items = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        link_tag = cols[0].find("a")
        title_text = link_tag.get_text(strip=True) if link_tag else cols[0].get_text(strip=True)
        full_title = f"{prefix.strip()} {title_text}".strip()
        activity = f"[{full_title}]({link_tag['href']})" if link_tag else full_title
        date = cols[1].get_text(strip=True)
        items.append((activity, date))
    return category, items, url, None

def extract_multiple_tables(urls, prefixes, categories):
    all_data = {'Eğitim': [], 'Arge': [], 'Sağlık': [], 'Sosyal Sorumluluk': []}
    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.Session() as session:
        session.headers.update(headers)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(_fetch_page, session, u, p, c): (u, p, c)
                for u, p, c in zip(urls, prefixes, categories)
            }
            for future in as_completed(futures):
                category, items, url, err = future.result()
                if err:
                    print(f"Failed to fetch URL {url}: {err}")
                elif category:
                    all_data[category].extend(items)

    combined_all = [
        (cat, act, date)
        for cat, rows in all_data.items()
        for act, date in rows
    ]
    df_all = pd.DataFrame(combined_all, columns=['Kategori', 'Faaliyet', 'Tarih'])
    df_all['Tarih_dt'] = df_all['Tarih'].apply(parse_date)
    df_all = df_all.sort_values('Tarih_dt', ascending=False)

    for category in df_all['Kategori'].unique():
        df_cat = df_all[df_all['Kategori'] == category]
        lines = ['| Activity | Date |', '|----------|------|']
        for row in df_cat.itertuples(index=False):
            date_str = format_date_dd_mm_yyyy(row.Tarih_dt, row.Tarih)
            lines.append(f'| {row.Faaliyet} | {date_str} |')
        filename = f'{CATEGORY_FILENAME.get(category, category.lower().replace(" ", "_"))}.md'
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        print(f"Markdown file saved: {filename}")

if __name__ == "__main__":
    urls = [
        "https://end-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim/eğitim",
        "https://end-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim/sosyal_sorumluluk",
        "https://end-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim/sağlık",
        "https://end-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim/ar-ge",

        "https://bil-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/egitim",
        "https://bil-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/sosyal-sorumluluk",
        "https://bil-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/saglik",
        "https://bil-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/ar-ge",

        "https://cev-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/ar-ge",
        "https://cev-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/egitim",
        "https://cev-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/saglik",
        "https://cev-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/sosyal-sorumluluk",

        "https://eem-muhendislik.omu.edu.tr/tr/toplumsal_yasama_katki/birim-faaliyetleri/saglik",
        "https://eem-muhendislik.omu.edu.tr/tr/toplumsal_yasama_katki/birim-faaliyetleri/egitim",
        "https://eem-muhendislik.omu.edu.tr/tr/toplumsal_yasama_katki/birim-faaliyetleri/ar-ge",
        "https://eem-muhendislik.omu.edu.tr/tr/toplumsal_yasama_katki/birim-faaliyetleri/sosyal-sorumluluk",

        "https://hrt-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/ar-ge",
        "https://hrt-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/saglik",
        "https://hrt-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/egitim",
        "https://hrt-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/sosyal-sorumluluk",

        "https://ins-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/ar-ge",
        "https://ins-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/sosyal-sorumluluk",
        "https://ins-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/oduller",
        "https://ins-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/ogrenci-faaliyetleri-ve-odulleri",

        "https://mlz-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/ar-ge",
        "https://mlz-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/egitim",
        "https://mlz-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/saglik",
        "https://mlz-muhendislik.omu.edu.tr/tr/toplumsal-katki/birim-faaliyetleri/sosyal-sorumluluk",
        
        "https://gida-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/education",
        "https://gida-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/sa%C4%9Fl%C4%B1k",
        "https://gida-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/ar-ge",
        "https://gida-muhendislik.omu.edu.tr/tr/toplumsal-yasama-katki/birim-faaliyetleri/sosyal-sorumluluk"
        
    ]

    prefixes = [
        "Fakültemiz Endüstri Mühendisliği bölümünde ",
        "Fakültemiz Endüstri Mühendisliği bölümünde ",
        "Fakültemiz Endüstri Mühendisliği bölümünde ",
        "Fakültemiz Endüstri Mühendisliği bölümünde ",

        "Fakültemiz Bilgisayar Mühendisliği bölümünde ",
        "Fakültemiz Bilgisayar Mühendisliği bölümünde ",
        "Fakültemiz Bilgisayar Mühendisliği bölümünde ",
        "Fakültemiz Bilgisayar Mühendisliği bölümünde ",

        "Fakültemiz Çevre Mühendisliği bölümünde ",
        "Fakültemiz Çevre Mühendisliği bölümünde ",
        "Fakültemiz Çevre Mühendisliği bölümünde ",
        "Fakültemiz Çevre Mühendisliği bölümünde ",

        "Fakültemiz Elektrik ve Elektronik Mühendisliği bölümünde ",
        "Fakültemiz Elektrik ve Elektronik Mühendisliği bölümünde ",
        "Fakültemiz Elektrik ve Elektronik Mühendisliği bölümünde ",
        "Fakültemiz Elektrik ve Elektronik Mühendisliği bölümünde ",

        "Fakültemiz Harita Mühendisliği bölümünde ",
        "Fakültemiz Harita Mühendisliği bölümünde ",
        "Fakültemiz Harita Mühendisliği bölümünde ",
        "Fakültemiz Harita Mühendisliği bölümünde ",

        "Fakültemiz İnşaat Mühendisliği bölümünde ",
        "Fakültemiz İnşaat Mühendisliği bölümünde ",
        "Fakültemiz İnşaat Mühendisliği bölümünde ",
        "Fakültemiz İnşaat Mühendisliği bölümünde ",

        "Fakültemiz Metalurji ve Malzeme Mühendisliği bölümünde ",
        "Fakültemiz Metalurji ve Malzeme Mühendisliği bölümünde ",
        "Fakültemiz Metalurji ve Malzeme Mühendisliği bölümünde ",
        "Fakültemiz Metalurji ve Malzeme Mühendisliği bölümünde ",
        
        "Fakültemiz Gıda Mühendisliği bölümünde ",
        "Fakültemiz Gıda Mühendisliği bölümünde ",
        "Fakültemiz Gıda Mühendisliği bölümünde ",
        "Fakültemiz Gıda Mühendisliği bölümünde ",
    ]

    categories = [
        'egitim', 'sosyal_sorumluluk', 'sağlık', 'ar-ge',
        'egitim', 'sosyal-sorumluluk', 'saglik', 'ar-ge',
        'ar-ge', 'egitim', 'saglik', 'sosyal-sorumluluk',
        'saglik', 'egitim', 'ar-ge', 'sosyal-sorumluluk',
        'ar-ge', 'saglik', 'egitim', 'sosyal-sorumluluk',
        'ar-ge', 'sosyal-sorumluluk', 'oduller', 'ogrenci-faaliyetleri-ve-odulleri',
        'ar-ge', 'egitim', 'saglik', 'sosyal-sorumluluk',
        'bap-projeleri', 'tubitak-projeleri', 'egitim' , 'saglik'
    ]

    if not (len(urls) == len(prefixes) == len(categories)):
        print("Error: URLs, prefixes and categories must have the same length.")
    else:
        extract_multiple_tables(urls, prefixes, categories)
