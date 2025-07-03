import logging
import os
import time
import threading
import json
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from utils.deduplicated_queue import DeduplicatedQueue


class TracerouteManager:
    """
    Manages traceroute operations with exponential backoff and retry logic.
    
    Configuration via environment variables:
    - TRACEROUTE_INTERVAL: Interval between periodic traceroutes in seconds (default: 10800 = 3 hours)
    - TRACEROUTE_MAX_RETRIES: Maximum number of retry attempts for failed traceroutes (default: 5)
    - TRACEROUTE_MAX_BACKOFF: Maximum backoff time in seconds (default: 86400 = 24 hours)
    - TRACEROUTE_SEND_TIMEOUT: Timeout for individual traceroute send operations in seconds (default: 30)
    - TRACEROUTE_MAX_THREADS: Maximum number of concurrent traceroute threads (default: 2)
    - TRACEROUTE_PERSISTENCE_FILE: Path to file for persisting retry/backoff state (default: /tmp/traceroute_state.json)
    """
    
    def __init__(self, interface, node_cache, traceroute_cooldown=30, traceroute_interval=None, max_retries=None, max_backoff=None, traceroute_persistence_file='/tmp/traceroute_state.json'):
        """
        Initialize the TracerouteManager.
        
        Args:
            interface: The Meshtastic interface for sending traceroutes
            node_cache: The NodeCache instance for node information lookup
            traceroute_cooldown (int): Cooldown between traceroutes in seconds
            traceroute_interval (int): Interval between periodic traceroutes in seconds (default from env or 10800)
            max_retries (int): Maximum number of retry attempts (default from env or 5)
            max_backoff (int): Maximum backoff time in seconds (default from env or 86400)
            persistence_file (str): Path to file for persisting retry/backoff data (default from env or '/tmp/traceroute_state.json')
        """
        self.interface = interface
        self.node_cache = node_cache
        
        # Configuration with environment variable defaults or passed parameters
        self._TRACEROUTE_INTERVAL = traceroute_interval if traceroute_interval is not None else int(os.getenv('TRACEROUTE_INTERVAL', 3 * 60 * 60))  # Default: 3 hours
        self._TRACEROUTE_COOLDOWN = traceroute_cooldown  # Configurable cooldown between any traceroutes
        self._last_global_traceroute_time = 0  # Global cooldown timestamp
        self._MAX_RETRIES = max_retries if max_retries is not None else int(os.getenv('TRACEROUTE_MAX_RETRIES', 5))  # Default: 5 retries
        self._MAX_BACKOFF = max_backoff if max_backoff is not None else int(os.getenv('TRACEROUTE_MAX_BACKOFF', 24 * 60 * 60))  # Default: 24 hours
        
        # Persistence configuration
        self._persistence_file = traceroute_persistence_file
        self._persistence_lock = threading.Lock()  # Thread-safe file operations
        
        # Traceroute tracking (initialized from persistence or empty)
        self._last_traceroute_time: Dict[str, float] = {}  # node_id -> timestamp when last traceroute was sent
        self._node_failure_counts: Dict[str, int] = {}  # node_id -> count of consecutive failures
        self._node_backoff_until: Dict[str, float] = {}  # node_id -> timestamp when node can be retried again
        
        # Load persisted state
        self._load_state()
        
        # Queue and threading
        self._traceroute_queue = DeduplicatedQueue(key_func=lambda x: x[0])
        
        # Thread pool for non-blocking traceroute execution with limited concurrency
        max_traceroute_threads = int(os.getenv('TRACEROUTE_MAX_THREADS', 2))
        self._traceroute_executor = ThreadPoolExecutor(max_workers=max_traceroute_threads, thread_name_prefix="TracerouteExec")
        
        # Start worker thread
        self._traceroute_worker_thread = threading.Thread(target=self._traceroute_worker, daemon=True)
        self._traceroute_worker_thread.start()
        logging.info(f"Traceroute worker thread started with single-threaded processing and {traceroute_cooldown}s cooldown.")
        logging.info(f"Traceroute configuration: interval={self._TRACEROUTE_INTERVAL}s, max_retries={self._MAX_RETRIES}, max_backoff={self._MAX_BACKOFF}s, max_threads={max_traceroute_threads}")
        logging.info(f"Traceroute persistence: {self._persistence_file}")

    def _load_state(self):
        """
        Load persisted traceroute state from filesystem.
        """
        try:
            if os.path.exists(self._persistence_file):
                with open(self._persistence_file, 'r') as f:
                    data = json.load(f)
                    
                # Load data with validation
                self._last_traceroute_time = data.get('last_traceroute_time', {})
                self._node_failure_counts = data.get('node_failure_counts', {})
                self._node_backoff_until = data.get('node_backoff_until', {})
                
                # Clean up expired backoffs
                now = time.time()
                expired_nodes = []
                for node_id, backoff_until in self._node_backoff_until.items():
                    if backoff_until < now:
                        expired_nodes.append(node_id)
                        time_expired = now - backoff_until
                        logging.info(f"[Persistence] Node {node_id} backoff expired {time_expired/60:.1f} minutes ago, cleaning up")
                
                for node_id in expired_nodes:
                    del self._node_backoff_until[node_id]
                    # Also clear failure counts for expired backoffs
                    if node_id in self._node_failure_counts:
                        failure_count = self._node_failure_counts[node_id]
                        logging.info(f"[Persistence] Clearing {failure_count} failure count(s) for expired node {node_id}")
                        del self._node_failure_counts[node_id]
                
                # Log detailed state information
                total_nodes_with_state = len(set(self._last_traceroute_time.keys()) | 
                                            set(self._node_failure_counts.keys()) | 
                                            set(self._node_backoff_until.keys()))
                
                logging.info(f"[Persistence] Loaded state for {total_nodes_with_state} nodes total:")
                logging.info(f"[Persistence] - {len(self._last_traceroute_time)} nodes with traceroute history")
                logging.info(f"[Persistence] - {len(self._node_failure_counts)} nodes with active failures")
                logging.info(f"[Persistence] - {len(self._node_backoff_until)} nodes in backoff")
                
                if expired_nodes:
                    logging.info(f"[Persistence] Cleaned up {len(expired_nodes)} expired backoffs: {', '.join(expired_nodes)}")
                
                # Log nodes with active failures and backoffs for debugging
                if self._node_failure_counts:
                    failure_details = [f"{node_id}({count})" for node_id, count in self._node_failure_counts.items()]
                    logging.info(f"[Persistence] Nodes with active failures: {', '.join(failure_details)}")
                
                if self._node_backoff_until:
                    backoff_details = []
                    for node_id, backoff_until in self._node_backoff_until.items():
                        remaining_time = (backoff_until - now) / 60
                        backoff_details.append(f"{node_id}({remaining_time:.1f}m)")
                    logging.info(f"[Persistence] Nodes in backoff: {', '.join(backoff_details)}")
            else:
                logging.info(f"[Persistence] No existing state file found at {self._persistence_file}, starting fresh")
        except Exception as e:
            logging.error(f"[Persistence] Failed to load state from {self._persistence_file}: {e}, starting fresh")
            self._last_traceroute_time = {}
            self._node_failure_counts = {}
            self._node_backoff_until = {}

    def _save_state(self):
        """
        Save current traceroute state to filesystem.
        """
        try:
            with self._persistence_lock:
                # Prepare data to save
                data = {
                    'last_traceroute_time': self._last_traceroute_time,
                    'node_failure_counts': self._node_failure_counts,
                    'node_backoff_until': self._node_backoff_until,
                    'saved_at': time.time()
                }
                
                # Write to temporary file first, then rename (atomic operation)
                temp_file = f"{self._persistence_file}.tmp"
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Atomic rename
                os.rename(temp_file, self._persistence_file)
                logging.debug(f"[Persistence] State saved to {self._persistence_file}")
        except Exception as e:
            logging.error(f"[Persistence] Failed to save state to {self._persistence_file}: {e}")

    def cleanup(self):
        """
        Cleanup resources and save final state.
        """
        logging.info("[TracerouteManager] Cleaning up and saving final state...")
        
        # Shutdown the thread pool
        if hasattr(self, '_traceroute_executor'):
            logging.info("[TracerouteManager] Shutting down traceroute thread pool...")
            self._traceroute_executor.shutdown(wait=True)
            logging.info("[TracerouteManager] Traceroute thread pool shutdown complete.")
        
        self._save_state()

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

    def record_traceroute_success(self, node_id):
        """
        Record a successful traceroute for a node, resetting failure count.
        
        Args:
            node_id (str): The node ID that succeeded
        """
        node_id = str(node_id)
        
        self._last_traceroute_time[node_id] = time.time()
        
        if node_id in self._node_failure_counts:
            failure_count = self._node_failure_counts[node_id]
            logging.info(f"[Traceroute] Node {node_id} traceroute succeeded after {failure_count} failures, resetting backoff.")
            del self._node_failure_counts[node_id]
        else:
            # Log success for nodes without previous failures
            logging.info(f"[Traceroute] Node {node_id} traceroute succeeded.")
        
        if node_id in self._node_backoff_until:
            del self._node_backoff_until[node_id]
        
        self._save_state()

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
            # Save state after updating failure count
            self._save_state()
            return False
        
        backoff_time = self._calculate_backoff_time(failure_count)
        if backoff_time > 0:
            self._node_backoff_until[node_id] = time.time() + backoff_time
            logging.info(f"[Traceroute] Node {node_id} failed {failure_count} times, backing off for {backoff_time/60:.1f} minutes.")
        else:
            logging.info(f"[Traceroute] Node {node_id} failed {failure_count} times, no backoff applied yet.")
        
        # Save state after updating failure and backoff data
        self._save_state()
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
        
        try:
            # Log node info before traceroute
            try:
                info = self.interface.getMyNodeInfo()
                logging.debug(f"[Traceroute] Node info before traceroute: {info}")
            except Exception as e:
                logging.error(f"[Traceroute] Failed to get node info before traceroute: {e}")
            
            # Update global traceroute time before attempting
            self._last_global_traceroute_time = time.time()

            logging.debug(f"[Traceroute] About to send traceroute to {node_id} and setting last traceroute time.")
            
            try:
                # Use thread pool to prevent blocking indefinitely with limited concurrency
                timeout_seconds = int(os.getenv('TRACEROUTE_SEND_TIMEOUT', 30))
                
                # Submit traceroute to thread pool and wait with timeout
                future = self._traceroute_executor.submit(
                    self.interface.sendTraceRoute, 
                    dest=node_id, 
                    hopLimit=10
                )
                
                try:
                    # Wait for completion with timeout
                    future.result(timeout=timeout_seconds)
                    logging.info(f"[Traceroute] Traceroute command sent for node {node_id}.")
                    # Note: Success will be recorded when we receive the TRACEROUTE_APP response packet
                    return True
                    
                except FutureTimeoutError:
                    logging.error(f"[Traceroute] Traceroute to node {node_id} timed out after {timeout_seconds} seconds")
                    # Cancel the future to prevent resource leaks
                    future.cancel()
                    self._record_traceroute_failure(node_id)
                    return False
                    
                except Exception as e:
                    logging.error(f"[Traceroute] Error in traceroute execution for node {node_id}: {e}")
                    self._record_traceroute_failure(node_id)
                    return False
                
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
                    # Pull the next job from the queue
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
