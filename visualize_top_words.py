import string
import requests
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

def get_text(url):
    """Завантажує текст із заданої URL-адреси."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Помилка завантаження: {e}")
        return None

def remove_punctuation(text):
    """Видаляє знаки пунктуації з тексту."""
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word):
    """Функція відображення (Map), яка повертає пару (слово, 1)."""
    return word.lower(), 1

def shuffle_function(mapped_values):
    """Групує слова з однаковими ключами."""
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    """Функція зменшення (Reduce), яка підраховує загальну кількість появ слова."""
    key, values = key_values
    return key, sum(values)

def map_reduce(text):
    """Виконує парадигму MapReduce для аналізу частоти слів."""
    text = remove_punctuation(text)
    words = text.split()

    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    shuffled_values = shuffle_function(mapped_values)

    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

def visualize_top_words(word_counts, top_n=10):
    """Візуалізує топ-слова за частотою використання."""
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    words, counts = zip(*sorted_words)

    plt.figure(figsize=(10, 6))
    plt.barh(words, counts)
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.gca().invert_yaxis()
    plt.show()

if __name__ == "__main__":
    url = "https://www.gutenberg.org/files/1342/1342-0.txt"
    text = get_text(url)

    if text:
        word_counts = map_reduce(text)
        visualize_top_words(word_counts)
    else:
        print("Не вдалося отримати текст.")