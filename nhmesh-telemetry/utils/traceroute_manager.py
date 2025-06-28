import logging
import os
import time
import threading
from typing import Dict
from utils.deduplicated_queue import DeduplicatedQueue


class TracerouteManager:
    """
    Manages traceroute operations with exponential backoff and retry logic.
    
    Configuration via environment variables:
    - TRACEROUTE_INTERVAL: Interval between periodic traceroutes in seconds (default: 10800 = 3 hours)
    - TRACEROUTE_MAX_RETRIES: Maximum number of retry attempts for failed traceroutes (default: 5)
    - TRACEROUTE_MAX_BACKOFF: Maximum backoff time in seconds (default: 86400 = 24 hours)
    """
    
    def __init__(self, interface, node_cache, traceroute_cooldown=30, traceroute_interval=None, max_retries=None, max_backoff=None):
        """
        Initialize the TracerouteManager.
        
        Args:
            interface: The Meshtastic interface for sending traceroutes
            node_cache: The NodeCache instance for node information lookup
            traceroute_cooldown (int): Cooldown between traceroutes in seconds
            traceroute_interval (int): Interval between periodic traceroutes in seconds (default from env or 10800)
            max_retries (int): Maximum number of retry attempts (default from env or 5)
            max_backoff (int): Maximum backoff time in seconds (default from env or 86400)
        """
        self.interface = interface
        self.node_cache = node_cache
        
        # Traceroute tracking (no longer managing node cache)
        self._last_traceroute_time: Dict[str, float] = {}  # node_id -> timestamp when last traceroute was sent
        self._node_failure_counts: Dict[str, int] = {}  # node_id -> count of consecutive failures
        self._node_backoff_until: Dict[str, float] = {}  # node_id -> timestamp when node can be retried again
        
        # Configuration with environment variable defaults or passed parameters
        self._TRACEROUTE_INTERVAL = traceroute_interval if traceroute_interval is not None else int(os.getenv('TRACEROUTE_INTERVAL', 3 * 60 * 60))  # Default: 3 hours
        self._TRACEROUTE_COOLDOWN = traceroute_cooldown  # Configurable cooldown between any traceroutes
        self._last_global_traceroute_time = 0  # Global cooldown timestamp
        self._MAX_RETRIES = max_retries if max_retries is not None else int(os.getenv('TRACEROUTE_MAX_RETRIES', 5))  # Default: 5 retries
        self._MAX_BACKOFF = max_backoff if max_backoff is not None else int(os.getenv('TRACEROUTE_MAX_BACKOFF', 24 * 60 * 60))  # Default: 24 hours
        
        # Queue and threading
        self._traceroute_queue = DeduplicatedQueue(key_func=lambda x: x[0])
        self._traceroute_in_progress = threading.Lock()  # Ensure only one traceroute at a time
        
        # Start worker thread
        self._traceroute_worker_thread = threading.Thread(target=self._traceroute_worker, daemon=True)
        self._traceroute_worker_thread.start()
        logging.info(f"Traceroute worker thread started with single-threaded processing and {traceroute_cooldown}s cooldown.")
        logging.info(f"Traceroute configuration: interval={self._TRACEROUTE_INTERVAL}s, max_retries={self._MAX_RETRIES}, max_backoff={self._MAX_BACKOFF}s")

    def _calculate_backoff_time(self, failure_count):
        """
        Calculate exponential backoff time based on failure count.
        Uses the traceroute interval as the base backoff time.
        
        Args:
            failure_count (int): Number of consecutive failures
            
        Returns:
            int: Backoff time in seconds
        """
        if failure_count < 2:
            return 0  # No backoff for first failure
        
        # Exponential backoff: traceroute_interval * 2^(failures-2)
        backoff_multiplier = 2 ** (failure_count - 2)
        backoff_time = min(self._TRACEROUTE_INTERVAL * backoff_multiplier, self._MAX_BACKOFF)
        return backoff_time

    def _is_node_in_backoff(self, node_id):
        """
        Check if a node is currently in backoff period.
        
        Args:
            node_id (str): The node ID to check
            
        Returns:
            bool: True if node is in backoff, False otherwise
        """
        if node_id not in self._node_backoff_until:
            return False
        
        now = time.time()
        return now < self._node_backoff_until[node_id]

    def _record_traceroute_success(self, node_id):
        """
        Record a successful traceroute for a node, resetting failure count.
        
        Args:
            node_id (str): The node ID that succeeded
        """
        node_id = str(node_id)
        if node_id in self._node_failure_counts:
            failure_count = self._node_failure_counts[node_id]
            if failure_count > 0:
                logging.info(f"[Traceroute] Node {node_id} traceroute succeeded after {failure_count} failures, resetting backoff.")
            del self._node_failure_counts[node_id]
        
        if node_id in self._node_backoff_until:
            del self._node_backoff_until[node_id]

    def _record_traceroute_failure(self, node_id):
        """
        Record a failed traceroute for a node and calculate backoff.
        
        Args:
            node_id (str): The node ID that failed
            
        Returns:
            bool: True if node should be retried, False if max retries exceeded
        """
        node_id = str(node_id)
        failure_count = self._node_failure_counts.get(node_id, 0) + 1
        self._node_failure_counts[node_id] = failure_count
        
        if failure_count >= self._MAX_RETRIES:
            logging.warning(f"[Traceroute] Node {node_id} has failed {failure_count} times, giving up.")
            return False
        
        backoff_time = self._calculate_backoff_time(failure_count)
        if backoff_time > 0:
            self._node_backoff_until[node_id] = time.time() + backoff_time
            logging.info(f"[Traceroute] Node {node_id} failed {failure_count} times, backing off for {backoff_time/60:.1f} minutes.")
        else:
            logging.info(f"[Traceroute] Node {node_id} failed {failure_count} times, no backoff applied yet.")
        
        return True

    def _format_position(self, pos):
        """
        Format position tuple for display.
        
        Args:
            pos: Tuple of (lat, lon, alt) or None
            
        Returns:
            str: Formatted position string
        """
        if pos is None:
            return "UNKNOWN"
        lat, lon, alt = pos
        s = f"({lat:.7f}, {lon:.7f})"
        if alt is not None:
            s += f" {alt}m"
        return s

    def _run_traceroute(self, node_id):
        """
        Execute a traceroute for the given node.
        
        Args:
            node_id (str): The node ID to traceroute
            
        Returns:
            bool: True if successful, False otherwise
        """
        node_id = str(node_id)  # Ensure node_id is always a string
        entry = self.node_cache.get_node_info(node_id)
        long_name = entry.get("long_name")
        pos = entry.get("position")
        failure_count = self._node_failure_counts.get(node_id, 0)
        
        logging.info(f"[Traceroute] Running traceroute for Node {node_id} | Long name: {long_name if long_name else 'UNKNOWN'} | Position: {self._format_position(pos)} | Failures: {failure_count}")
        
        # Acquire lock to ensure only one traceroute at a time
        with self._traceroute_in_progress:
            try:
                # Log node info before traceroute
                try:
                    info = self.interface.getMyNodeInfo()
                    logging.info(f"[Traceroute] Node info before traceroute: {info}")
                except Exception as e:
                    logging.error(f"[Traceroute] Failed to get node info before traceroute: {e}")
                
                # Update global traceroute time before attempting
                self._last_global_traceroute_time = time.time()

                logging.debug(f"[Traceroute] About to send traceroute to {node_id} and setting last traceroute time.")
                
                try:
                    self.interface.sendTraceRoute(dest=node_id, hopLimit=10)
                    self._last_traceroute_time[node_id] = time.time()
                    logging.info(f"[Traceroute] Traceroute command sent for node {node_id}.")
                    
                    # Record success (reset failure count and backoff)
                    self._record_traceroute_success(node_id)
                    return True
                    
                except Exception as e:
                    logging.error(f"[Traceroute] Error sending traceroute to node {node_id}: {e}")
                    # Record failure and check if we should continue retrying
                    self._record_traceroute_failure(node_id)
                    return False
                    
            except Exception as e:
                logging.error(f"[Traceroute] Unexpected error sending traceroute to node {node_id}: {e}")
                # Record failure for unexpected errors too
                self._record_traceroute_failure(node_id)
                return False

    def _traceroute_worker(self):
        """
        Worker thread that processes traceroute jobs from the queue.
        """
        while True:
            try:
                # Get the next job from the queue (blocks until available)
                node_id, retries = self._traceroute_queue.get()
                logging.info(f"[Traceroute] Worker picked up job for node {node_id}, attempt {retries+1}.")
                logging.info(f"[Traceroute] Current queue depth: {self._traceroute_queue.qsize()}")
                
                # Check if this node is in backoff period
                if self._is_node_in_backoff(node_id):
                    backoff_remaining = self._node_backoff_until[node_id] - time.time()
                    logging.info(f"[Traceroute] Node {node_id} is in backoff for {backoff_remaining/60:.1f} more minutes, re-queueing for later.")
                    # Re-queue the job for later processing
                    self._traceroute_queue.put((node_id, retries))
                    # Sleep a bit to avoid tight loop
                    time.sleep(min(60, backoff_remaining))
                    continue
                
                # Check global cooldown before processing
                now = time.time()
                time_since_last = now - self._last_global_traceroute_time
                
                if time_since_last < self._TRACEROUTE_COOLDOWN:
                    wait_time = self._TRACEROUTE_COOLDOWN - time_since_last
                    logging.info(f"[Traceroute] Global cooldown active, sleeping {wait_time:.1f} seconds before processing node {node_id}")
                    
                    # Sleep in small increments to remain responsive
                    while wait_time > 0:
                        sleep_duration = min(wait_time, 1.0)  # Sleep max 1 second at a time
                        time.sleep(sleep_duration)
                        wait_time -= sleep_duration
                        
                        # Re-check if we still need to wait (in case another traceroute completed)
                        current_time = time.time()
                        remaining_cooldown = self._TRACEROUTE_COOLDOWN - (current_time - self._last_global_traceroute_time)
                        if remaining_cooldown <= 0:
                            break
                        wait_time = min(wait_time, remaining_cooldown)
                
                success = self._run_traceroute(node_id)
                failure_count = self._node_failure_counts.get(node_id, 0)
                
                if not success:
                    logging.error(f"[Traceroute] Failed to traceroute node {node_id} after {retries+1} attempts. Total failures: {failure_count}")
                    
                    # Check if we should retry this node (based on failure count and max retries)
                    if failure_count < self._MAX_RETRIES:
                        # Re-queue with incremented retry count if we haven't hit max retries
                        new_retries = retries + 1
                        logging.info(f"[Traceroute] Re-queueing node {node_id} for retry {new_retries+1}/{self._MAX_RETRIES}")
                        self._traceroute_queue.put((node_id, new_retries))
                    else:
                        logging.warning(f"[Traceroute] Node {node_id} has reached maximum retry limit ({self._MAX_RETRIES}), giving up.")
                else:
                    logging.info(f"[Traceroute] Traceroute for node {node_id} completed successfully.")
                    
            except Exception as e:
                logging.error(f"[Traceroute] Worker encountered error: {e}")

    def process_packet_for_traceroutes(self, node_id, is_new_node):
        """
        Process a node from a packet for traceroute queueing logic.
        This method only handles traceroute-specific logic.
        
        Args:
            node_id (str): The node ID from the packet
            is_new_node (bool): Whether this is a new node (from NodeCache)
        """
        node_id = str(node_id)  # Ensure node_id is always a string
        
        # Enqueue traceroute for new nodes
        if is_new_node:
            if self._is_node_in_backoff(node_id):
                backoff_remaining = self._node_backoff_until[node_id] - time.time()
                logging.debug(f"[Traceroute] New node {node_id} is in backoff for {backoff_remaining/60:.1f} more minutes, skipping.")
            elif self._traceroute_queue.put((node_id, 0)):  # 0 retries so far
                logging.info(f"[Traceroute] New node discovered: {node_id}, enqueued traceroute job.")
            else:
                logging.debug(f"[Traceroute] New node {node_id} already queued, skipping duplicate.")
            
        # Periodic re-traceroute
        now = time.time()
        last_time = self._last_traceroute_time.get(node_id, 0)
        if now - last_time > self._TRACEROUTE_INTERVAL:
            if self._is_node_in_backoff(node_id):
                backoff_remaining = self._node_backoff_until[node_id] - time.time()
                logging.debug(f"[Traceroute] Periodic traceroute for node {node_id} is in backoff for {backoff_remaining/60:.1f} more minutes, skipping.")
            elif self._traceroute_queue.put((node_id, 0)):  # 0 retries so far
                logging.info(f"[Traceroute] Periodic traceroute needed for node {node_id}, enqueued job.")
            else:
                logging.debug(f"[Traceroute] Periodic traceroute for node {node_id} already queued, skipping duplicate.")

    def queue_traceroute(self, node_id):
        """
        Manually queue a traceroute for a specific node.
        
        Args:
            node_id (str): The node ID to queue for traceroute
            
        Returns:
            bool: True if queued successfully, False if already queued or in backoff
        """
        node_id = str(node_id)
        
        if self._is_node_in_backoff(node_id):
            backoff_remaining = self._node_backoff_until[node_id] - time.time()
            logging.debug(f"[Traceroute] Manual traceroute for node {node_id} is in backoff for {backoff_remaining/60:.1f} more minutes, skipping.")
            return False
        
        if self._traceroute_queue.put((node_id, 0)):  # 0 retries so far
            logging.info(f"[Traceroute] Manual traceroute queued for node {node_id}.")
            return True
        else:
            logging.debug(f"[Traceroute] Manual traceroute for node {node_id} already queued, skipping duplicate.")
            return False
