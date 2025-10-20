import requests
from bs4 import BeautifulSoup, Tag, ResultSet
import pandas as pd
from datetime import datetime, timedelta
import re
from typing import List, Dict, Optional, Any, Union, cast
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================================================================
# КОНСТАНТИ ТА ПАРАМЕТРИ
# ==============================================================================
URL = 'https://www.rbc.ua/'
BASE_URL = 'https://www.rbc.ua'
CSV_FILENAME = 'news.csv' # Назва файлу в якому будуть зберігатись дані
MAX_DAYS_TO_FILTER = 7 # Фільтрація новин за останні 7 днів
MAX_ARTICLES_TO_PARSE = 15 # Кількість статей для парсингу


# ==============================================================================
# ФУНКЦІЯ: get_page(url)
# ==============================================================================

def get_page(url: str) -> Optional[BeautifulSoup]:
    """ Завантажує HTML-код сторінки та повертає BeautifulSoup-об'єкт. """
    print(f"Завантаження сторінки з інтернету: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = response.apparent_encoding
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.exceptions.RequestException as ex:
        print(f"Помилка завантаження сторінки {url}: {ex}")
        return None


# ==============================================================================
# ФУНКЦІЯ: parse_news(soup)
# ==============================================================================

def parse_news(soup: BeautifulSoup) -> List[Dict[str, Optional[Union[str, Any]]]]:
    """ Витягує всі новини у вигляді списку словників із ключами title, link, date, summary. """
    if not soup:
        return []

    news_data: List[Dict[str, Optional[Union[str, Any]]]] = []
    print("Початок парсингу списку новин ....")

    try:
        news_items: ResultSet[Tag] = soup.find_all('div', class_=re.compile(r'(news-card|item)'))

        if not news_items:
            print("ПОМИЛКА: Не знайдено жодного контейнера новин. Парсинг неможливий.")
            return []

        for item in news_items:
            link_tag: Optional[Tag] = item.find('a', href=re.compile(r'(rus|ukr)/news/'))
            time_tag: Optional[Tag] = item.find(class_=re.compile(r'(pub-date|time|news-card__time)'))

            # Пошук анотації (якщо вона є)
            summary: str = ""
            if link_tag:
                next_el: Optional[Tag] = link_tag.find_next_sibling(['p', 'span', 'div'])
                if next_el:
                    summary = next_el.get_text(strip=True)

            # Вилучення та валідація даних
            title: str = link_tag.get_text(strip=True) if link_tag else 'Загаловок відсутній'

            # Явна перевірка типу для href, щоб уникнути помилок AttributeValueList
            # В Beautiful Soup, атрибут може бути str, list (AttributeValueList) або None
            link: str = 'Посилання відсутнє'
            if link_tag:
                href_attr: Any = link_tag.get('href') # Приймаємо будь-який тип, але перевіряємо його

                if isinstance(href_attr, str):
                    link = href_attr
                elif isinstance(href_attr, list):
                    link = str(href_attr[0]) # Якщо це список, беремо перший елемент

                # Форматування посилання, якщо воно не абсолютне
                if link != 'посилання відсутнє' and not link.startswith('http'):
                    link = BASE_URL + link

            date_time_raw: str = time_tag.get_text(strip=True) if time_tag else 'Дата/час відсутній'

            # Додаємо дані, якщо є заголовок та валідне посилання на новину
            if title != 'Заголовок відсутній' and '/news/' in link:
                news_data.append({
                    'title': title,
                    'link': link,
                    'date_raw': date_time_raw,
                    'summary': summary,
                    'author': None,
                    'full_text': None
                })

        print(f"Зібрано {len(news_data)} унікальних посилань.")
        return news_data

    except Exception as ex:
        print(f"Виникла помилка під час парсингу списку: {ex}")
        return news_data


# ==============================================================================
# ФУНКЦІЯ: parse_article_content
# ==============================================================================

def parse_article_content(article_url: str) -> tuple[str, str]:
    """ Парсинг повного тексту та автора (для збагачення даних). """
    article_soup = get_page(article_url)
    if not article_soup:
        return 'Автор не знайдений', 'Текст не завантажено'

    author_tag: Optional[Tag] = article_soup.find('div', class_='publication-wrapper-author')
    author = 'Автор не вказаний'

    if author_tag:
        author_link: Optional[Tag] = author_tag.find('a')
        if author_link:
            author = author_link.get_text(strip=True)

    text_container: Optional[Tag] = article_soup.find('div', class_='txt')
    full_text = 'Повний текст не знайдено'

    if text_container:
        content_parts: List[str] = []
        for element in text_container.find_all(['p', 'ul', 'h2'], recursive=False):
            if isinstance(element, Tag):
                text = element.get_text(separator=' ', strip=True)

                if element.name == 'h2':
                    text = f"\n\n## {text}\n"
                elif element.name == 'ul':
                    items = [li.get_text(strip=True) for li in element.find_all('li')]
                    text = "\n* " + "\n* ".join(items) + "\n"
                else:
                    text = f"{text}\n"
                content_parts.append(text)

        full_text = ''.join(content_parts).strip() if content_parts else text_container.get_text(separator='\n', strip=True)

    return author, full_text


# ==============================================================================
# ФУНКЦІЯ: save_to_csv(data)
# ==============================================================================

def save_to_csv(data: List[Dict[str, Any]], filename: str = CSV_FILENAME):
    """ Приймає список новин та зберігає його в CSV-файлі news.csv """
    if not data:
        print("Немає даних для збереження.")
        return

    try:
        df = pd.DataFrame(data)
        df = df.drop(columns=['data_raw'], errors='ignore')

        required_cols = ['title', 'link', 'datetime', 'summary', 'author', 'full_text']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''

        df = df[required_cols]
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nДані успішно збережені у файлі: {filename}")
    except Exception as ex:
        print(f"Помилка при збереженні у CSV: {ex}")


# ==============================================================================
# ДОПОМІЖНІ ФУНКЦІЇ
# ==============================================================================

def normalize_news_data(df: pd.DataFrame) -> pd.DataFrame:
    """ Обробляє сиру дату 'ЧЧ:ММ' і створює повний 'datetime'. """
    current_date_str = datetime.now().strftime('%Y-%m-%d')

    def normalize_time(date_str: str) -> Any:
        if 'відсутній' in date_str:
            return pd.NaT
        try:
            time_obj = datetime.strptime(date_str.strip(), '%H:%M')
            return datetime.strptime(current_date_str, '%Y-%m-%d').replace(
                hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0
            )
        except ValueError:
            return pd.NaT

    df['datetime'] = df['date_raw'].apply(normalize_time)
    return df


def filter_by_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    """ Фільтрує новини за останні 'days' """
    if 'datetime' not in df.columns or df['datetime'].empty:
        return df.copy()

    threshold_date = datetime.now() - timedelta(days=days)
    df_valid_dates = df.dropna(subset=['datetime'])
    filtered_df = df_valid_dates[cast(pd.Series, df_valid_dates['datetime']) >= threshold_date].copy()

    print(f"\nФільтрація: Залишено {len(filtered_df)} статей за останні {days} днів.")
    return filtered_df


def print_analysis(df: pd.DataFrame):
    """ Виводить короткий статистичний звіт. """
    if df.empty:
        print("\nСтатистичний звіт")
        print("Не знайдено новин для аналізу після фільтрації.")
        return

    df = df.sort_values(by='datetime', ascending=False)
   # daily_count = df['datetime'].dt.date.value_counts().sort_index(ascending=False)

    print("\nСтатистичний звіт:")
    print(f"Всього у фінальному списку новин: {len(df)}")
    print("-" *30)

    print(f"Найновіша стаття: {df.iloc[0]['title']} ({df.iloc[0]['datetime'].strftime('%H:%M')})")


# ==============================================================================
# ГОЛОВНА ФУНКЦІЯ
# ==============================================================================
def main():
    """Керуюча функція."""

    # Завантаження сторінки
    soup = get_page(URL)
    if not soup:
        return

    # Парсинг списку новин
    news_data = parse_news(soup)
    if not news_data:
        return

    # Збагачення даних (повний текст і автор для перших статей)
    articles_to_process = news_data[:MAX_ARTICLES_TO_PARSE]
    print(f"\nПочаток парсингу даних для для {len(articles_to_process)} статей...")

    updated_articles_data: List[Dict[str, Any]] = []

    # Використовуємо ThreadPoolExecutor для багатопотоковості
    with ThreadPoolExecutor(max_workers=10) as executor:

        # Словник для зберігання майбутніх результатів (Future) та відповідного словника статті
        future_to_article = {
            executor.submit(parse_article_content, article['link']): article
            for article in articles_to_process if article['link'] != 'Посилання відсутнє'
        }

        # Обробка результатів по мірі їх находження
        for future in as_completed(future_to_article):
            article = future_to_article[future]
            link = cast(str, article['link'])

            try:
                author, full_text = future.result()
                article['author'] = author
                article['full_text'] = full_text
                print(f"Успішно опрацьовано: {link}")
                updated_articles_data.append(article)

            except Exception as exc:
                print(f"Помилка парсингу контента для {link}: {exc}")
                # Додаємо статтю без контенту, щоб не втратити її
                updated_articles_data.append(article)
    # Замінюємо початкові дані лише оновленими статтями
    news_data = updated_articles_data + news_data[MAX_ARTICLES_TO_PARSE:]

    # Форматування дати
    df = pd.DataFrame(news_data)
    df = normalize_news_data(df)

    # Фільтрація за датою
    df_filtered = filter_by_days(df, MAX_DAYS_TO_FILTER)

    # Звіт та збереження
    print_analysis(df_filtered)
    save_to_csv(cast(List[Dict[str, Any]], df_filtered.to_dict('records')), CSV_FILENAME)

if __name__ == '__main__':
    main()
