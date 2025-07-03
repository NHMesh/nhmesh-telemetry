import threading
import queue


class DeduplicatedQueue:
    """
    A queue that prevents duplicate items from being added based on a key function.
    Thread-safe and supports the same basic interface as queue.Queue.
    """
    
    def __init__(self, key_func=None):
        """
        Initialize the deduplicated queue.
        
        Args:
            key_func: Function to extract the key from queue items for deduplication.
                     If None, the item itself is used as the key.
        """
        self._queue = queue.Queue()
        self._queued_items = set()
        self._lock = threading.Lock()
        self._key_func = key_func or (lambda x: x)
    
    def put(self, item):
        """
        Put an item into the queue if it's not already queued.
        
        Args:
            item: The item to add to the queue
            
        Returns:
            bool: True if item was added, False if it was already queued
        """
        key = self._key_func(item)
        
        with self._lock:
            if key not in self._queued_items:
                self._queue.put(item)
                self._queued_items.add(key)
                return True
            else:
                return False
    
    def get(self, block=True, timeout=None):
        """
        Get an item from the queue and remove it from the deduplication set.
        
        Args:
            block: Whether to block if no item is available
            timeout: Timeout for blocking
            
        Returns:
            The next item from the queue
        """
        item = self._queue.get(block, timeout)
        key = self._key_func(item)
        
        with self._lock:
            self._queued_items.discard(key)
        
        return item
    
    def empty(self):
        """Check if the queue is empty."""
        return self._queue.empty()
    
    def qsize(self):
        """Return the approximate size of the queue."""
        return self._queue.qsize()
