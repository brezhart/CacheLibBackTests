from lru_cache import LRUCache
from batch_refresh_cache import BatchRefreshCache
import time
import datetime

from read_real_bullets import GetRealBullets


def test_cache(cache, bullets):

    already_presented_bullets = set()

    for i, bullet in enumerate(bullets):
        # Преобразуем timestamp в datetime объект
        dt = datetime.datetime.fromtimestamp(bullet['timestamp'])
        first_time_bullets = []
        for key in (bullet['keys']):
            if (not (key in already_presented_bullets)):
                first_time_bullets.append(key)

        already_presented_bullets.update(first_time_bullets)



        cache._add_keys_to_cache(first_time_bullets, dt)
        #  Insert key for time, as it already was there, to simulate contigious testing, not no a segment

        cache.get(bullet['keys'], dt)
        
        # if (i + 1) % 50000 == 0:
        #     print(f"  Кэш 1: Обработано {i + 1:,} патронов")

def test_cache_comparison(bullets, cache1, cache2):
    """Сравнение двух типов кэша на одинаковых данных"""
    print("СРАВНЕНИЕ ДВУХ ТИПОВ КЭШЕЙ")
    print("="*60)
    
    print(f"Тестируем с {len(bullets):,} патронами")
    
    print("\nТестирование первого кэша...")

    test_cache(cache1, bullets)
    print("\nТестирование второго кэша...")
    test_cache(cache2, bullets)
    
    # Выводим сравнительную статистику
    print(f"\nРЕЗУЛЬТАТЫ СРАВНЕНИЯ")
    print("="*60)
    
    print(f"Первый кэш")
    cache1_stats = cache1.get_stats()
    print(f"  Hit rate: {cache1_stats['hit_rate_percent']:.2f}%")
    print(f"  Hits: {cache1_stats['hits']:,}")
    print(f"  Misses (ключи): {cache1_stats['misses']:,}")
    print(f"  Bulk misses (запросы): {cache1_stats['bulk_misses']:,}")
    print(f"  Протухания: {cache1_stats['expirations']:,}")
    print(f"  Вытеснения: {cache1_stats['evictions']:,}")
    print(f"  Всего запросов ключей: {cache1_stats['total_requests']:,}")
    if 'batch_updates' in cache1_stats:
        print(f"  Batch обновления: {cache1_stats['batch_updates']:,}")
    
    print(f"\nВторой кэш")
    cache2_stats = cache2.get_stats()
    print(f"  Hit rate: {cache2_stats['hit_rate_percent']:.2f}%")
    print(f"  Hits: {cache2_stats['hits']:,}")
    print(f"  Misses (ключи): {cache2_stats['misses']:,}")
    print(f"  Bulk misses (запросы): {cache2_stats['bulk_misses']:,}")
    print(f"  Протухания: {cache2_stats['expirations']:,}")
    print(f"  Вытеснения: {cache2_stats['evictions']:,}")
    print(f"  Всего запросов ключей: {cache2_stats['total_requests']:,}")
    if 'batch_updates' in cache2_stats:
        print(f"  Batch обновления: {cache2_stats['batch_updates']:,}")
    
    # Анализ эффективности
    print(f"\nАНАЛИЗ ЭФФЕКТИВНОСТИ")
    print("-" * 40)
    
    hit_rate_diff = cache2_stats['hit_rate_percent'] - cache1_stats['hit_rate_percent']
    expiration_diff = cache1_stats['expirations'] - cache2_stats['expirations']
    
    print(f"Разница в hit rate: {hit_rate_diff:+.2f}%")
    print(f"Разница в протуханиях: {expiration_diff:+,}")

def main():
    bullets = GetRealBullets(100000000)

    print(f"Прочитано {len(bullets):,} патронов")
    
    # Создаём два типа кэша с одинаковыми параметрами
    print("\nСоздаём кэши...")
    lru_cache = LRUCache(
        max_size=300000, 
        ttl=datetime.timedelta(minutes=360)
    )
    
    batch_cache = BatchRefreshCache(
        max_size=300000, 
        ttl=datetime.timedelta(minutes=360),
        half_ttl=0.5,
        max_request_size=100
    )
    
    print("LRU кэш создан")
    print("Batch Refresh кэш создан")
    
    # Запускаем сравнение
    print("\n" + "="*60)
    test_cache_comparison(bullets, lru_cache, batch_cache)

if __name__ == "__main__":
    main()
