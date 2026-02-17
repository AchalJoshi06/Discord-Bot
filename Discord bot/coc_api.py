"""Clash of Clans API client with caching and rate limiting."""
import asyncio
import urllib.parse
from typing import Optional, Dict, Any, List
import aiohttp

from config import (
    COC_API_KEYS, COC_API_BASE_URL, COC_CONCURRENCY, COC_TIMEOUT,
    PLAYER_CACHE_TTL, CLAN_CACHE_TTL, WAR_CACHE_TTL
)
from cache import api_cache, request_deduplicator


class COCAPI:
    """Clash of Clans API client with caching and request deduplication."""
    
    def __init__(self, http_session: aiohttp.ClientSession):
        self.session = http_session
        self.semaphore = asyncio.Semaphore(COC_CONCURRENCY)
    
    async def _make_request(self, path: str, cache_key: Optional[str] = None, 
                           ttl: float = 60.0) -> Optional[Dict[str, Any]]:
        """
        Make an API request with caching and deduplication.
        
        Args:
            path: API endpoint path (e.g., "/clans/#TAG")
            cache_key: Optional cache key (defaults to path)
            ttl: Cache TTL in seconds
        """
        if not COC_API_KEYS:
            return None
        
        cache_key = cache_key or path
        
        # Check cache first
        cached = await api_cache.get(cache_key, ttl)
        if cached is not None:
            return cached
        
        # Use deduplicator to prevent concurrent duplicate requests
        async def _fetch():
            url = f"{COC_API_BASE_URL}{path}"
            # pick a key to try first
            keys = list(COC_API_KEYS.values())
            if not keys:
                return None
            async with self.semaphore:
                # try keys in order, stop on first success
                for k in keys:
                    headers = {"Authorization": f"Bearer {k}"}
                    try:
                        async with self.session.get(url, headers=headers, timeout=COC_TIMEOUT) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                # Cache successful responses
                                await api_cache.set(cache_key, data)
                                return data
                            elif resp.status == 404:
                                # Cache 404s briefly to avoid repeated requests
                                await api_cache.set(cache_key, None)
                                return None
                            else:
                                # try next key
                                continue
                    except asyncio.TimeoutError:
                        continue
                    except aiohttp.ClientError:
                        continue
                # all keys failed
                return None
        
        return await request_deduplicator.get_or_create(cache_key, _fetch)
    
    async def get_clan(self, clan_tag: str) -> Optional[Dict[str, Any]]:
        """Get clan information."""
        path = f"/clans/{urllib.parse.quote(clan_tag)}"
        return await self._make_request(path, cache_key=f"clan:{clan_tag}", ttl=CLAN_CACHE_TTL)
    
    async def get_clan_members(self, clan_tag: str) -> List[Dict[str, Any]]:
        """Get clan member list."""
        clan = await self.get_clan(clan_tag)
        if not clan:
            return []
        return clan.get("memberList", [])
    
    async def get_player(self, tag: str) -> Optional[Dict[str, Any]]:
        """Get player information."""
        path = f"/players/{urllib.parse.quote(tag)}"
        return await self._make_request(path, cache_key=f"player:{tag}", ttl=PLAYER_CACHE_TTL)
    
    async def get_current_war(self, clan_tag: str) -> Optional[Dict[str, Any]]:
        """Get current war information."""
        path = f"/clans/{urllib.parse.quote(clan_tag)}/currentwar"
        return await self._make_request(path, cache_key=f"war:{clan_tag}", ttl=WAR_CACHE_TTL)
    
    async def get_capital_raid_season(self, clan_tag: str) -> Optional[Dict[str, Any]]:
        """Get capital raid season information."""
        path = f"/clans/{urllib.parse.quote(clan_tag)}/capitalraidseason"
        return await self._make_request(path, cache_key=f"raid:{clan_tag}", ttl=CLAN_CACHE_TTL)
    
    async def invalidate_cache(self, cache_key: str) -> None:
        """Invalidate a specific cache entry."""
        await api_cache.invalidate(cache_key)
    
    async def invalidate_clan_cache(self, clan_tag: str) -> None:
        """Invalidate all cache entries for a clan."""
        await api_cache.invalidate(f"clan:{clan_tag}")
        await api_cache.invalidate(f"war:{clan_tag}")
        await api_cache.invalidate(f"raid:{clan_tag}")


