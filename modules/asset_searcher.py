"""
Multi-Source Video Searcher
Queries Pexels, Pixabay, and Archive.org in parallel
"""

import aiohttp
import asyncio
import hashlib
import json
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import imagehash
from PIL import Image
import io

@dataclass
class SearchResult:
    source: str
    asset_id: str
    url: str
    preview_url: str
    duration: Optional[float]
    width: int
    height: int
    tags: List[str]
    description: str
    license: str
    perceptual_hash: Optional[str] = None

class PexelsClient:
    """Pexels API Client - 200 requests/hour limit"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.pexels.com/videos"
        self.rate_limit_delay = 18  # seconds between requests (200/hr = 18s)
        self.last_request = None

    async def search(
        self,
        query: str,
        per_page: int = 15,
        min_duration: Optional[int] = None
    ) -> List[SearchResult]:
        """Search Pexels videos"""
        await self._respect_rate_limit()

        headers = {"Authorization": self.api_key}
        params = {
            "query": query,
            "per_page": per_page,
            "orientation": "landscape"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/search",
                headers=headers,
                params=params
            ) as resp:
                if resp.status != 200:
                    print(f"Pexels API error: {resp.status}")
                    return []

                data = await resp.json()
                results = []

                for video in data.get("videos", []):
                    # Filter by duration if specified
                    if min_duration and video.get("duration", 0) < min_duration:
                        continue

                    # Get best quality video file
                    video_files = video.get("video_files", [])
                    best_file = self._get_best_file(video_files)

                    if not best_file:
                        continue

                    result = SearchResult(
                        source="pexels",
                        asset_id=str(video["id"]),
                        url=best_file["link"],
                        preview_url=video.get("image", ""),
                        duration=video.get("duration"),
                        width=best_file.get("width", 1920),
                        height=best_file.get("height", 1080),
                        tags=video.get("tags", []),
                        description=video.get("user", {}).get("name", ""),
                        license="pexels_free"
                    )
                    results.append(result)

                return results

    def _get_best_file(self, video_files: List[Dict]) -> Optional[Dict]:
        """Get highest quality file under 1080p (faster downloads)"""
        if not video_files:
            return None

        # Prefer 1080p or 720p
        for quality in [1920, 1280, 854]:
            for f in video_files:
                if f.get("width") == quality:
                    return f

        # Fallback to largest
        return max(video_files, key=lambda x: x.get("width", 0))

    async def _respect_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        if self.last_request:
            elapsed = (datetime.now() - self.last_request).total_seconds()
            if elapsed < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - elapsed)
        self.last_request = datetime.now()


class PixabayClient:
    """Pixabay API Client - 2000 requests/hour limit"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://pixabay.com/api/videos/"
        self.rate_limit_delay = 1.8  # seconds between requests
        self.last_request = None

    async def search(
        self,
        query: str,
        per_page: int = 20
    ) -> List[SearchResult]:
        """Search Pixabay videos"""
        await self._respect_rate_limit()

        params = {
            "key": self.api_key,
            "q": query,
            "per_page": per_page,
            "orientation": "horizontal",
            "safesearch": "true"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=params) as resp:
                if resp.status != 200:
                    print(f"Pixabay API error: {resp.status}")
                    return []

                data = await resp.json()
                results = []

                for hit in data.get("hits", []):
                    result = SearchResult(
                        source="pixabay",
                        asset_id=str(hit["id"]),
                        url=hit["videos"]["large"]["url"],
                        preview_url=hit["videos"]["medium"]["url"],
                        duration=hit.get("duration"),
                        width=hit["videos"]["large"].get("width", 1920),
                        height=hit["videos"]["large"].get("height", 1080),
                        tags=hit.get("tags", "").split(", "),
                        description=hit.get("user", ""),
                        license="pixabay_free"
                    )
                    results.append(result)

                return results

    async def _respect_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        if self.last_request:
            elapsed = (datetime.now() - self.last_request).total_seconds()
            if elapsed < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - elapsed)
        self.last_request = datetime.now()


class ArchiveOrgClient:
    """Internet Archive Client - No rate limits but be polite"""

    def __init__(self):
        self.base_url = "https://archive.org"
        self.rate_limit_delay = 6  # 10 requests/minute to be polite
        self.last_request = None

    async def search(
        self,
        query: str,
        collection: Optional[str] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: int = 20
    ) -> List[SearchResult]:
        """Search Internet Archive for videos"""
        await self._respect_rate_limit()

        # Build search query
        q = f"({query}) AND mediatype:movies"
        if collection:
            q += f" AND collection:{collection}"
        if year_from:
            q += f" AND year:[{year_from} TO {year_to if year_to else 2024}]"

        params = {
            "q": q,
            "fl[]": ["identifier", "title", "description", "year", "collection"],
            "rows": max_results,
            "page": 1,
            "output": "json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.base_url}/advancedsearch.php",
                params=params
            ) as resp:
                if resp.status != 200:
                    return []

                data = await resp.json()
                results = []

                for doc in data.get("response", {}).get("docs", []):
                    identifier = doc.get("identifier")
                    if not identifier:
                        continue

                    # Get metadata to find video files
                    video_files = await self._get_video_files(session, identifier)

                    for vf in video_files:
                        result = SearchResult(
                            source="archive_org",
                            asset_id=f"{identifier}_{vf['filename']}",
                            url=vf["url"],
                            preview_url=f"{self.base_url}/download/{identifier}/{identifier}.jpg",
                            duration=vf.get("duration"),
                            width=vf.get("width", 720),
                            height=vf.get("height", 480),
                            tags=doc.get("collection", []),
                            description=doc.get("title", "")[:200],
                            license="public_domain"
                        )
                        results.append(result)

                return results

    async def _get_video_files(
        self,
        session: aiohttp.ClientSession,
        identifier: str
    ) -> List[Dict]:
        """Get video files from item metadata"""
        await self._respect_rate_limit()

        url = f"{self.base_url}/metadata/{identifier}"

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return []

                data = await resp.json()
                files = data.get("files", [])
                videos = []

                for f in files:
                    name = f.get("name", "").lower()
                    if any(name.endswith(ext) for ext in [".mp4", ".webm", ".ogv"]):
                        # Prefer smaller files for B-roll (under 100MB)
                        size = f.get("size", 0)
                        if size > 0 and size < 100 * 1024 * 1024:
                            videos.append({
                                "filename": f["name"],
                                "url": f"{self.base_url}/download/{identifier}/{f['name']}",
                                "size": size,
                                "duration": f.get("length"),
                                "width": f.get("width"),
                                "height": f.get("height")
                            })

                return videos
        except Exception as e:
            print(f"Error getting archive files: {e}")
            return []

    async def _respect_rate_limit(self):
        """Be polite to Archive.org"""
        if self.last_request:
            elapsed = (datetime.now() - self.last_request).total_seconds()
            if elapsed < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - elapsed)
        self.last_request = datetime.now()


class MultiSourceSearcher:
    """Unified searcher across all sources with caching"""

    def __init__(
        self,
        pexels_key: Optional[str] = None,
        pixabay_key: Optional[str] = None,
        cache_db = None
    ):
        self.pexels = PexelsClient(pexels_key) if pexels_key else None
        self.pixabay = PixabayClient(pixabay_key) if pixabay_key else None
        self.archive = ArchiveOrgClient()
        self.cache = cache_db

    async def search(
        self,
        query: str,
        use_cache: bool = True,
        min_duration: Optional[int] = 5
    ) -> List[SearchResult]:
        """
        Search all available sources, merge and deduplicate results.
        Priority: Archive.org (unique content) > Pixabay (volume) > Pexels (quality)
        """
        # Check cache
        if use_cache and self.cache:
            query_hash = hashlib.md5(query.encode()).hexdigest()
            cached = await self._get_cached(query_hash)
            if cached:
                print(f"Cache hit for: {query}")
                return cached

        # Search all sources in parallel
        tasks = []

        if self.archive:
            tasks.append(self._search_archive(query, min_duration))
        if self.pixabay:
            tasks.append(self._search_pixabay(query))
        if self.pexels:
            tasks.append(self._search_pexels(query, min_duration))

        results_lists = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten and filter errors
        all_results = []
        for result in results_lists:
            if isinstance(result, list):
                all_results.extend(result)

        # Deduplicate by perceptual hash (if available) or URL
        seen_hashes = set()
        deduplicated = []

        for r in all_results:
            key = r.perceptual_hash or r.url
            if key not in seen_hashes:
                seen_hashes.add(key)
                deduplicated.append(r)

        # Sort by priority: archive > pixabay > pexels
        priority = {"archive_org": 0, "pixabay": 1, "pexels": 2}
        deduplicated.sort(key=lambda x: priority.get(x.source, 3))

        # Cache results
        if use_cache and self.cache:
            await self._cache_results(query_hash, deduplicated)

        return deduplicated

    async def _search_pexels(self, query: str, min_duration: Optional[int]) -> List[SearchResult]:
        try:
            return await self.pexels.search(query, min_duration=min_duration)
        except Exception as e:
            print(f"Pexels search error: {e}")
            return []

    async def _search_pixabay(self, query: str) -> List[SearchResult]:
        try:
            return await self.pixabay.search(query)
        except Exception as e:
            print(f"Pixabay search error: {e}")
            return []

    async def _search_archive(self, query: str, min_duration: Optional[int]) -> List[SearchResult]:
        try:
            # Archive.org works well for historical/mystery content
            # Search prelinger archives for ephemeral films
            return await self.archive.search(
                query,
                collection="prelinger",
                year_from=1940,
                year_to=1990
            )
        except Exception as e:
            print(f"Archive.org search error: {e}")
            return []

    async def _get_cached(self, query_hash: str) -> Optional[List[SearchResult]]:
        """Get cached search results"""
        if not self.cache:
            return None
        # Implementation depends on cache_db interface
        return None

    async def _cache_results(self, query_hash: str, results: List[SearchResult]):
        """Cache search results"""
        if not self.cache:
            return
        # Implementation depends on cache_db interface
        pass
