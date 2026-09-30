"""Fire concurrent like/unlike requests at the API.

python -m scripts.loadtest --requests 20000 --concurrency 200
"""
import argparse
import asyncio
import random
import statistics
import time
from collections import Counter

import httpx


async def main(args):
    rng = random.Random(args.seed)
    hot = list(range(1, args.hot_posts + 1))
    all_posts = list(range(1, args.posts + 1))

    def next_request():
        # Most traffic hits a few "viral" posts - that's the realistic worst case
        post = rng.choice(hot) if rng.random() < args.hot_ratio else rng.choice(all_posts)
        user = rng.randint(1, args.users)
        method = "DELETE" if rng.random() < args.unlike_ratio else "POST"
        return method, post, user

    latencies: list[float] = []
    statuses: Counter = Counter()
    remaining = args.requests

    limits = httpx.Limits(max_connections=args.concurrency,
                          max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(base_url=args.base_url, limits=limits, timeout=30) as client:

        async def worker():
            nonlocal remaining
            while remaining > 0:
                remaining -= 1
                method, post, user = next_request()
                t0 = time.perf_counter()
                try:
                    r = await client.request(method, f"/posts/{post}/like",
                                             headers={"X-User-Id": str(user)})
                    statuses[r.status_code] += 1
                except httpx.HTTPError as e:
                    statuses[type(e).__name__] += 1
                latencies.append((time.perf_counter() - t0) * 1000)

        start = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(args.concurrency)))
        elapsed = time.perf_counter() - start

    latencies.sort()
    q = statistics.quantiles(latencies, n=100)
    print(f"requests:   {len(latencies)} in {elapsed:.1f}s  ->  {len(latencies) / elapsed:,.0f} req/s")
    print(f"latency ms: p50={q[49]:.1f}  p95={q[94]:.1f}  p99={q[98]:.1f}  max={latencies[-1]:.1f}")
    print(f"statuses:   {dict(statuses)}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://127.0.0.1:8000")
    p.add_argument("--requests", type=int, default=20000)
    p.add_argument("--concurrency", type=int, default=200)
    p.add_argument("--users", type=int, default=5000)
    p.add_argument("--posts", type=int, default=100)
    p.add_argument("--hot-posts", type=int, default=3)
    p.add_argument("--hot-ratio", type=float, default=0.7)
    p.add_argument("--unlike-ratio", type=float, default=0.3)
    p.add_argument("--seed", type=int, default=None)
    asyncio.run(main(p.parse_args()))
