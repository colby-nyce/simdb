from abc import abstractmethod

# Cache data structure with LRU eviction policy (Least Recently Used).
class LRUCache:
    def __init__(self, capacity=100):
        self.cache = {}
        self.access_keys = []
        self.capacity = capacity

    @abstractmethod
    def load(self, key):
        pass

    def get(self, key):
        if key in self.access_keys:
            self.access_keys.remove(key)

        self.access_keys.append(key)
        if len(self.access_keys) > self.capacity:
            k = self.access_keys[0]
            self.access_keys.pop(0)
            del self.cache[k]

        if key not in self.cache:
            self.cache[key] = self.load(key)

        return self.cache[key]
