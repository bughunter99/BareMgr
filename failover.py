#!/usr/bin/env python3
import zmq
import json
import threading
import time
from datetime import datetime

class FailoverNode:
    def __init__(self, config_file):
        self.config = json.load(open(config_file))
        self.node_id = self.config['node_id']
        self.weight = self.config['weight']
        self.peers = self.config['peers']  # List of peer addresses
        self.port = self.config.get('port', 5555)
        self.heartbeat_interval = self.config.get('heartbeat_interval', 2)
        
        self.isActive = False
        self.peers_state = {}
        self.ctx = zmq.Context()
        self.running = True
        
    def start(self):
        """Start heartbeat server and client threads"""
        threading.Thread(target=self._heartbeat_server, daemon=True).start()
        threading.Thread(target=self._heartbeat_sender, daemon=True).start()
        threading.Thread(target=self._leader_election, daemon=True).start()
        
        print(f"[{self.node_id}] Failover node started with weight={self.weight}")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def _heartbeat_server(self):
        """Receive heartbeats from peers"""
        sock = self.ctx.socket(zmq.REP)
        sock.bind(f"tcp://*:{self.port}")
        
        while self.running:
            try:
                data = sock.recv_json(flags=zmq.NOBLOCK)
                peer_id = data['node_id']
                self.peers_state[peer_id] = {
                    'weight': data['weight'],
                    'timestamp': time.time()
                }
                sock.send_json({'status': 'ok'})
            except zmq.Again:
                time.sleep(0.1)
    
    def _heartbeat_sender(self):
        """Send heartbeats to peers"""
        sock = self.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        
        while self.running:
            for peer_addr in self.peers:
                try:
                    sock.connect(f"tcp://{peer_addr}")
                    sock.send_json({'node_id': self.node_id, 'weight': self.weight})
                    sock.recv_json()
                    sock.disconnect(f"tcp://{peer_addr}")
                except:
                    pass
            time.sleep(self.heartbeat_interval)
    
    def _leader_election(self):
        """Check if this node should be active"""
        while self.running:
            # Remove stale peers (no heartbeat in 3x interval)
            now = time.time()
            for peer_id in list(self.peers_state.keys()):
                if now - self.peers_state[peer_id]['timestamp'] > self.heartbeat_interval * 3:
                    del self.peers_state[peer_id]
            
            # Check if we have highest weight
            max_weight = self.weight
            is_leader = True
            
            for peer_id, state in self.peers_state.items():
                if state['weight'] > max_weight:
                    max_weight = state['weight']
                    is_leader = False
                elif state['weight'] == max_weight and peer_id > self.node_id:
                    is_leader = False
            
            old_status = self.isActive
            self.isActive = is_leader
            
            if self.isActive != old_status:
                status = "ACTIVE" if self.isActive else "STANDBY"
                print(f"[{self.node_id}] Status changed to {status} (weight={self.weight})")
            
            time.sleep(self.heartbeat_interval)
    
    def stop(self):
        self.running = False
        self.ctx.term()
        print(f"[{self.node_id}] Stopped")

if __name__ == '__main__':
    node = FailoverNode('config.json')
    node.start()
