"""Pooling utilities and globals"""

import concurrent.futures

thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
