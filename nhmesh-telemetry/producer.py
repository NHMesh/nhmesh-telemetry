#!/usr/bin/env python3
"""
Test script to verify that the traceroute persistence file argument is properly handled.
"""

import sys
import os
import argparse
from unittest.mock import patch, MagicMock

# Add the nhmesh-telemetry module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'nhmesh-telemetry'))

from utils.envdefault import EnvDefault

def test_persistence_file_arg():
    """Test that the persistence file argument is properly parsed and handled."""
    
    # Create the argument parser like in producer.py
    parser = argparse.ArgumentParser()
    parser.add_argument('--node-ip', default='127.0.0.1', action=EnvDefault, envvar="NODE_IP", help='IP address of the node to connect to')
    parser.add_argument('--traceroute-persistence-file', required=False, action=EnvDefault, envvar="TRACEROUTE_PERSISTENCE_FILE", help='Path to file for persisting traceroute retry/backoff state (default: /tmp/traceroute_state.json)')
    
    # Test 1: No argument provided (should be None)
    args1 = parser.parse_args(['--node-ip', '192.168.1.100'])
    print(f"Test 1 - No persistence file arg: {args1.traceroute_persistence_file}")
    
    # Test 2: Argument provided via command line
    args2 = parser.parse_args(['--node-ip', '192.168.1.100', '--traceroute-persistence-file', '/custom/path/state.json'])
    print(f"Test 2 - Custom persistence file: {args2.traceroute_persistence_file}")
    
    # Test 3: Argument provided via environment variable
    with patch.dict(os.environ, {'TRACEROUTE_PERSISTENCE_FILE': '/env/path/state.json'}):
        args3 = parser.parse_args(['--node-ip', '192.168.1.100'])
        print(f"Test 3 - Env var persistence file: {args3.traceroute_persistence_file}")
    
    # Test 4: Command line overrides environment variable
    with patch.dict(os.environ, {'TRACEROUTE_PERSISTENCE_FILE': '/env/path/state.json'}):
        args4 = parser.parse_args(['--node-ip', '192.168.1.100', '--traceroute-persistence-file', '/cmdline/path/state.json'])
        print(f"Test 4 - Cmdline overrides env: {args4.traceroute_persistence_file}")
    
    # Test getattr fallback behavior
    print(f"\nTesting getattr fallback:")
    print(f"args1 has attribute: {hasattr(args1, 'traceroute_persistence_file')}")
    print(f"getattr with None default: {getattr(args1, 'traceroute_persistence_file', None)}")
    print(f"getattr with custom default: {getattr(args1, 'traceroute_persistence_file', '/default/path.json')}")

if __name__ == '__main__':
    test_persistence_file_arg()
