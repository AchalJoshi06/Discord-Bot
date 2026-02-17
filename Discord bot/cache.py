"""API response caching with TTL to reduce API calls and avoid rate limits."""
import asyncio
import time
from typing import Dict, Optional, Any, Tuple
from datetime import datetime, timezone

from config import PLAYER_CACHE_TTL, CLAN_CACHE_TTL, WAR_CACHE_TTL


class APICache:
    """Thread-safe API cache with TTL support."""
    
    def __init__(self):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a specific cache key."""
        async def _ensure_lock():
            async with self._lock:
                if key not in self._locks:
                    self._locks[key] = asyncio.Lock()
                return self._locks[key]
        # For async context, we need to handle this differently
        # Since we can't await in __init__, we'll create locks on demand
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]
    
    async def get(self, key: str, ttl: float) -> Optional[Any]:
        """Get cached value if it exists and hasn't expired."""
        lock = self._get_lock(key)
        async with lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if time.time() - timestamp < ttl:
                    return value
                # Expired, remove it
                del self._cache[key]
        return None
    
    async def set(self, key: str, value: Any) -> None:
        """Store a value in cache with current timestamp."""
        lock = self._get_lock(key)
        async with lock:
            self._cache[key] = (value, time.time())
    
    async def invalidate(self, key: str) -> None:
        """Remove a key from cache."""
        lock = self._get_lock(key)
        async with lock:
            self._cache.pop(key, None)
    
    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._locks.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics (synchronous)."""
        # Return basic info without async (for status command)
        return {"total_keys": len(self._cache)}


# Global cache instance
api_cache = APICache()


class RequestDeduplicator:
    """Prevent duplicate concurrent requests for the same resource."""
    
    def __init__(self):
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
    
    async def get_or_create(self, key: str, factory) -> Any:
        """
        If a request for this key is already pending, wait for it.
        Otherwise, create a new request using the factory function.
        """
        async with self._lock:
            if key in self._pending:
                # Request already in progress, wait for it
                future = self._pending[key]
            else:
                # Create new request
                future = asyncio.create_task(factory())
                self._pending[key] = future
        
        try:
            result = await future
            return result
        finally:
            # Clean up
            async with self._lock:
                if key in self._pending and self._pending[key] == future:
                    del self._pending[key]

    async def clear(self) -> dict:
        """Safely clear completed pending requests and return stats.

        This method will not cancel running requests; it only removes
        entries that are already done to free memory. Returns a dict
        with counts for reporting.
        """
        async with self._lock:
            total = len(self._pending)
            removed = 0
            for k in list(self._pending.keys()):
                fut = self._pending.get(k)
                if fut is None:
                    del self._pending[k]
                    removed += 1
                elif fut.done():
                    del self._pending[k]
                    removed += 1
            return {"total": total, "removed_done": removed}

# Global deduplicator instance
request_deduplicator = RequestDeduplicator()

