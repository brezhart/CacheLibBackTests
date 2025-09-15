import datetime
from typing import Dict, Any, Optional, List, Set
from collections import OrderedDict
from lru_cache import LRUCache

class BatchRefreshCache(LRUCache):
    """
    LRU кэш с batch обновлением ключей, которые скоро протухнут
    """
    
    def __init__(self, max_size: int = 20000, 
                 ttl: datetime.timedelta = datetime.timedelta(hours=3),
                 half_ttl: float = 0.5,
                 max_request_size: int = 100):
        """
        Инициализация Batch Refresh кэша
        
        Args:
            max_size: максимальный размер кэша
            ttl: время жизни ключа (timedelta объект)
            half_ttl: доля от TTL, после которой ключ считается "скоро протухнет" (0.5 = половина TTL)
            max_request_size: размер batch для обновления ключей
        """
        super().__init__(max_size, ttl)
        
        self.half_ttl = half_ttl
        self.max_request_size = max_request_size
        self.half_ttl_seconds = ttl.total_seconds() * half_ttl
        
        # Batch для ключей, которые скоро протухнут
        self.refresh_batch = []
        
        # Дополнительная статистика
        self.batch_updates = 0  # количество batch обновлений
        
        print(f"Инициализирован Batch Refresh кэш: размер={max_size}, TTL={ttl}, half_TTL={half_ttl}, batch_size={max_request_size}")
    
    def _is_near_expiration(self, cache_timestamp: datetime.datetime, request_time: datetime.datetime) -> bool:
        """Проверяет, скоро ли протухнет ключ (больше half_ttl от времени жизни)"""
        age = (request_time - cache_timestamp).total_seconds()
        return age > self.half_ttl_seconds
    
    def _process_refresh_batch(self, request_time: datetime.datetime) -> None:
        """Обрабатывает batch ключей для обновления"""

        
        if len(self.refresh_batch) >= self.max_request_size:
            # Обновляем все ключи в batch
            for key in self.refresh_batch:
                if key in self.cache:
                    # Обновляем timestamp (имитируя поход в сервис)
                    self.cache[key] = (0, request_time)
                    # Перемещаем в конец для LRU
                    self.cache.move_to_end(key)
            
            self.batch_updates += 1
            self.refresh_batch.clear()
    
    def get(self, keys: List[Any], request_time: datetime.datetime = None) -> List[int]:
        """
        Получает значения из кэша с batch refresh логикой
        """
        if not keys:
            return []
        
        if request_time is None:
            request_time = datetime.datetime.now()
        
        # Сначала собираем ключи, которые скоро протухнут
        near_expired_keys = []
        for key in keys:
            if key in self.cache:
                value, timestamp = self.cache[key]
                if not self._is_expired_at_time(timestamp, request_time):
                    if self._is_near_expiration(timestamp, request_time):
                        if key not in self.refresh_batch:
                            near_expired_keys.append(key)
        
        # Добавляем near_expired ключи в batch
        self.refresh_batch.extend(near_expired_keys)
        
        # Вызываем get от super() (LRU кэш)
        results = super().get(keys, request_time)
        
        # Если refresh_batch заполнен, обновляем эти ключи
        self._process_refresh_batch(request_time)
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику кэша с дополнительными метриками"""
        stats = super().get_stats()
        stats.update({
            'batch_updates': self.batch_updates,
            'refresh_batch_size': len(self.refresh_batch),
            'half_ttl': self.half_ttl,
            'max_request_size': self.max_request_size
        })
        return stats
    
    def print_stats(self) -> None:
        """Выводит статистику кэша"""
        stats = self.get_stats()
        
        print("\n" + "="*50)
        print("СТАТИСТИКА BATCH REFRESH КЭША")
        print("="*50)
        print(f"Общие запросы ключей: {stats['total_requests']}")
        print(f"Cache hits: {stats['hits']}")
        print(f"Cache misses (ключи): {stats['misses']}")
        print(f"Bulk misses (запросы): {stats['bulk_misses']}")
        print(f"Hit rate: {stats['hit_rate_percent']:.2f}%")
        print(f"Вытеснения (evictions): {stats['evictions']}")
        print(f"Протухания (expirations): {stats['expirations']}")
        print(f"Batch обновления: {stats['batch_updates']}")
        print(f"Текущий batch размер: {stats['refresh_batch_size']}")
        print(f"Текущий размер: {stats['current_size']}")
        print(f"Максимальный размер: {stats['max_size']}")
        print(f"TTL: {stats['ttl']}")
        print(f"Half TTL: {stats['half_ttl']}")
        print(f"Max batch size: {stats['max_request_size']}")
        print("="*50)
    
    def clear(self) -> None:
        """Очищает кэш"""
        super().clear()
        self.batch_updates = 0
        self.refresh_batch.clear()


def test_batch_refresh_cache():
    """Тестирование Batch Refresh кэша"""
    print("ТЕСТИРОВАНИЕ BATCH REFRESH КЭША")
    print("="*50)
    
    # Создаём кэш с коротким TTL для тестирования
    cache = BatchRefreshCache(
        max_size=10, 
        ttl=datetime.timedelta(seconds=10),
        half_ttl=0.5,  # 5 секунд
        max_request_size=3
    )
    
    print("\n1. Добавляем ключи в кэш:")
    base_time = datetime.datetime.now()
    keys_to_test = [1, 2, 3, 4, 5]
    results = cache.get(keys_to_test, base_time)
    print(f"   get({keys_to_test}) = {results}")
    cache.print_stats()
    
    print("\n2. Запрашиваем через 6 секунд (ключи скоро протухнут):")
    future_time = base_time + datetime.timedelta(seconds=6)
    results = cache.get([1, 2], future_time)
    print(f"   get([1, 2]) = {results} (должны попасть в refresh batch)")
    cache.print_stats()
    
    print("\n3. Добавляем ещё один ключ в batch:")
    results = cache.get([3], future_time)
    print(f"   get([3]) = {results} (batch должен обновиться)")
    cache.print_stats()
    
    print("\n4. Проверяем обновлённые ключи:")
    results = cache.get([1, 2, 3], future_time)
    print(f"   get([1, 2, 3]) = {results} (должны быть свежими)")
    cache.print_stats()


def main():
    """Основная функция"""
    test_batch_refresh_cache()


if __name__ == "__main__":
    main()
