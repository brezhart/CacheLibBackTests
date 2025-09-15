import datetime
from typing import Dict, Any, Optional, List, Set
from collections import OrderedDict

class LRUCache:
    """
    LRU кэш с поддержкой времени протухания ключей и bulk операций
    """
    
    def __init__(self, max_size: int = 20000, ttl: datetime.timedelta = datetime.timedelta(hours=3)):
        """
        Инициализация LRU кэша
        
        Args:
            max_size: максимальный размер кэша
            ttl: время жизни ключа (timedelta объект)
        """
        self.max_size = max_size
        self.ttl = ttl
        self.ttl_seconds = ttl.total_seconds()
        
        # OrderedDict для LRU логики
        self.cache = OrderedDict()
        
        # Статистика
        self.hits = 0
        self.misses = 0  # количество отдельных ключей с промахами
        self.bulk_misses = 0  # количество bulk запросов с хотя бы одним промахом
        self.evictions = 0  # количество вытеснений
        self.expirations = 0  # количество протуханий
        
        print(f"Инициализирован LRU кэш: размер={max_size}, TTL={ttl}")


    def get(self, keys: List[Any], request_time: datetime.datetime = None) -> List[int]:
        if not keys:
            return []
        
        if request_time is None:
            request_time = datetime.datetime.now()
        
        results = []
        missing_keys = []
        expired_keys = []
        
        # Проверяем каждый ключ
        for key in keys:
            if key in self.cache:
                _, timestamp = self.cache[key]

                if self._is_expired_at_time(timestamp, request_time):
                    expired_keys.append(key)
                    results.append(0)
                else:
                    self.cache.move_to_end(key)
                    self.hits += 1
                    results.append(0)
            else:
                # Cache miss
                missing_keys.append(key)
                results.append(0)
    
        all_missing_keys = missing_keys + expired_keys
        
        if all_missing_keys:
            for key in expired_keys:
                self.cache[key] = (0, request_time)
                self.cache.move_to_end(key)
                self.expirations += 1

            if missing_keys:
                self._add_keys_to_cache(missing_keys, request_time)
            
            self.misses += len(all_missing_keys)
            self.bulk_misses += 1
        
        return results
    
    def get_single(self, key: Any, request_time: datetime.datetime = None) -> int:
        return self.get([key], request_time)[0]
    
    def _add_keys_to_cache(self, keys: List[Any], timestamp: datetime.datetime) -> None:
        """Добавляет множество ключей в кэш"""
        for key in keys:
            # Если кэш полон, удаляем самый старый элемент (LRU)
            if len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
                self.evictions += 1
            
            # Добавляем новый ключ с переданным timestamp
            self.cache[key] = (0, timestamp)  # value всегда 0
    
    def _add_to_cache(self, key: Any, timestamp: datetime.datetime = None) -> None:
        """Добавляет один ключ в кэш (для совместимости)"""
        if timestamp is None:
            timestamp = datetime.datetime.now()
        self._add_keys_to_cache([key], timestamp)
    
    def _is_expired_at_time(self, cache_timestamp: datetime.datetime, request_time: datetime.datetime) -> bool:
        """Проверяет, протух ли ключ относительно времени запроса"""
        return (request_time - cache_timestamp).total_seconds() > self.ttl_seconds
    
    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику кэша"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'hits': self.hits,
            'misses': self.misses,
            'bulk_misses': self.bulk_misses,
            'total_requests': total_requests,
            'hit_rate_percent': hit_rate,
            'evictions': self.evictions,
            'expirations': self.expirations,
            'current_size': len(self.cache),
            'max_size': self.max_size,
            'ttl': self.ttl
        }
    
    def print_stats(self) -> None:
        """Выводит статистику кэша"""
        stats = self.get_stats()
        
        print("\n" + "="*50)
        print("СТАТИСТИКА LRU КЭША")
        print("="*50)
        print(f"Общие запросы ключей: {stats['total_requests']}")
        print(f"Cache hits: {stats['hits']}")
        print(f"Cache misses (ключи): {stats['misses']}")
        print(f"Bulk misses (запросы): {stats['bulk_misses']}")
        print(f"Hit rate: {stats['hit_rate_percent']:.2f}%")
        print(f"Вытеснения (evictions): {stats['evictions']}")
        print(f"Протухания (expirations): {stats['expirations']}")
        print(f"Текущий размер: {stats['current_size']}")
        print(f"Максимальный размер: {stats['max_size']}")
        print(f"TTL: {stats['ttl']}")
        print("="*50)
    
    def clear(self) -> None:
        """Очищает кэш"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        self.bulk_misses = 0
        self.evictions = 0
        self.expirations = 0
    
    def size(self) -> int:
        """Возвращает текущий размер кэша"""
        return len(self.cache)


def test_lru_cache():
    """Тестирование LRU кэша"""
    print("ТЕСТИРОВАНИЕ LRU КЭША")
    print("="*50)
    
    # Создаём кэш
    cache = LRUCache(max_size=5, ttl=datetime.timedelta(hours=1))  # Маленький размер для тестирования
    
    print("\n1. Тестирование базовой функциональности (bulk):")
    
    # Добавляем ключи bulk запросом
    keys_to_test = [1, 2, 3, 4, 5]
    results = cache.get(keys_to_test)
    print(f"   get({keys_to_test}) = {results} (bulk miss)")
    
    cache.print_stats()
    
    print("\n2. Тестирование cache hits (bulk):")
    
    # Повторно запрашиваем те же ключи
    results = cache.get(keys_to_test)
    print(f"   get({keys_to_test}) = {results} (bulk hit)")
    
    cache.print_stats()
    
    print("\n3. Тестирование LRU вытеснения (bulk):")
    
    # Добавляем новые ключи, которые должны вытеснить старые
    new_keys = [6, 7, 8]
    results = cache.get(new_keys)
    print(f"   get({new_keys}) = {results} (bulk miss, eviction)")
    
    cache.print_stats()
    
    print("\n4. Тестирование смешанного запроса (hits + misses):")
    
    # Проверяем смешанный запрос
    mixed_keys = [3, 4, 5, 9, 10]  # 3,4,5 должны быть в кэше, 9,10 - нет
    results = cache.get(mixed_keys)
    print(f"   get({mixed_keys}) = {results} (mixed: hits + misses)")
    
    cache.print_stats()
    
    print("\n5. Тестирование одиночных запросов:")
    
    # Тестируем get_single для совместимости
    result = cache.get_single(1)
    print(f"   get_single(1) = {result}")
    
    result = cache.get_single(100)
    print(f"   get_single(100) = {result}")
    
    cache.print_stats()


def simulate_cache_with_bullets():
    """Симуляция работы кэша с патронами"""
    from generate_cache_bullets import generate_cache_bullets, generate_partners_data
    
    print("\nСИМУЛЯЦИЯ РАБОТЫ КЭША С ПАТРОНАМИ")
    print("="*60)
    
    # Генерируем данные
    print("Генерируем данные партнёров...")
    partners_data = generate_partners_data(num_partners=100, min_places=10, max_places=20)  # Меньше для теста
    
    print("Генерируем патроны...")
    bullets = generate_cache_bullets(
        partners_data=partners_data,
        min_requests_per_day=10,
        max_requests_per_day=20
    )
    
    # Создаём кэш
    cache = LRUCache(max_size=1000, ttl=datetime.timedelta(hours=3))
    
    print(f"\nОбрабатываем {len(bullets)} патронов...")
    
    # Обрабатываем патроны используя bulk API с timestamp
    for i, bullet in enumerate(bullets):
        # Для каждого патрона обрабатываем все его keys одним bulk запросом
        # Передаём timestamp из патрона
        cache.get(bullet['keys'], bullet['timestamp'])
        
        # Прогресс каждые 100 патронов
        if (i + 1) % 100 == 0:
            print(f"Обработано {i + 1} патронов...")
    
    # Выводим финальную статистику
    cache.print_stats()
    
    return cache


def main():
    """Основная функция"""
    # Базовое тестирование
    test_lru_cache()
    
    # Симуляция с патронами
    simulate_cache_with_bullets()


if __name__ == "__main__":
    main()
