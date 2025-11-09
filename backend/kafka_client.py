"""
Kafka Producer Client for sending events
Handles access events and move events
"""

import json
from kafka import KafkaProducer as KafkaProducerClient
from typing import Dict, Any
import time
import logging

logger = logging.getLogger(__name__)

class KafkaProducer:
    """
    Kafka producer for sending storage events
    """
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Connect to Kafka with retry logic"""
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducerClient(
                    bootstrap_servers=self.bootstrap_servers.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3
                )
                logger.info("Connected to Kafka at %s", self.bootstrap_servers)
                return
            except Exception as e:
                logger.warning("Kafka connection attempt %s/%s failed: %s", attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka after %s attempts", max_retries)
                    raise
    
    def send_access_event(self, event: Dict[str, Any]):
        """
        Send an access event to Kafka
        
        Args:
            event: Dictionary containing access event data
        """
        try:
            if self.producer:
                future = self.producer.send('access_events', value=event, key=str(event.get('dataset_id')))
                # Don't block on send - let it happen asynchronously
                return future
        except Exception as e:
            logger.error("Failed to send access event: %s", e)
    
    def send_move_event(self, event: Dict[str, Any]):
        """
        Send a move event to Kafka
        
        Args:
            event: Dictionary containing move event data
        """
        try:
            if self.producer:
                future = self.producer.send('move_events', value=event, key=str(event.get('dataset_id')))
                # Block on this one to ensure move events are delivered
                future.get(timeout=10)
                logger.info(
                    "Move event sent: %s %s -> %s",
                    event.get('dataset_name'),
                    event.get('from_tier'),
                    event.get('to_tier')
                )
                return future
        except Exception as e:
            logger.error("Failed to send move event: %s", e)
    
    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
