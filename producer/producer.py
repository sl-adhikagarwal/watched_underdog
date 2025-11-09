"""
Data Access Event Producer
Simulates realistic data access patterns and sends events to Kafka
Also creates sample datasets in the backend
"""

import os
import time
import random
import json
from datetime import datetime
from kafka import KafkaProducer
import requests
import logging

logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "access_events")
PRODUCE_INTERVAL = int(os.getenv("PRODUCE_INTERVAL", "5"))
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")

# Sample datasets to create
SAMPLE_DATASETS = [
    {"name": "user_analytics_2024", "size_bytes": 5 * 1024**3, "initial_tier": "hot"},
    {"name": "customer_profiles", "size_bytes": 2 * 1024**3, "initial_tier": "hot"},
    {"name": "transaction_logs_q4", "size_bytes": 10 * 1024**3, "initial_tier": "warm"},
    {"name": "ml_training_data", "size_bytes": 50 * 1024**3, "initial_tier": "warm"},
    {"name": "website_images_2023", "size_bytes": 20 * 1024**3, "initial_tier": "warm"},
    {"name": "archived_logs_2022", "size_bytes": 100 * 1024**3, "initial_tier": "cold"},
    {"name": "backup_database_jan", "size_bytes": 80 * 1024**3, "initial_tier": "cold"},
    {"name": "legal_documents", "size_bytes": 15 * 1024**3, "initial_tier": "cold"},
    {"name": "product_catalog", "size_bytes": 1 * 1024**3, "initial_tier": "hot"},
    {"name": "email_archive_2021", "size_bytes": 45 * 1024**3, "initial_tier": "cold"},
]

def connect_kafka():
    """Connect to Kafka with retry logic"""
    max_retries = 10
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info("Producer connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except Exception as e:
            logger.warning("Kafka connection attempt %s/%s failed: %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

def wait_for_backend():
    """Wait for backend to be ready"""
    max_retries = 30
    retry_delay = 2
    
    logger.info("Waiting for backend to be ready")
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{BACKEND_URL}/health", timeout=5)
            if response.status_code == 200:
                logger.info("Backend is ready")
                return True
        except Exception as e:
            logger.warning("Backend not ready (attempt %s/%s): %s", attempt + 1, max_retries, e)
        
        time.sleep(retry_delay)
    
    logger.error("Backend did not become ready in time")
    return False

def create_sample_datasets():
    """Create sample datasets in the backend"""
    logger.info("Creating sample datasets")
    
    created_ids = []
    for dataset in SAMPLE_DATASETS:
        try:
            response = requests.post(
                f"{BACKEND_URL}/datasets",
                json=dataset,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                created_ids.append(data['id'])
                logger.info("Created dataset %s (ID %s)", dataset['name'], data['id'])
            elif response.status_code == 400:
                logger.debug("Dataset %s already exists", dataset['name'])
        except Exception as e:
            logger.warning("Failed to create dataset %s: %s", dataset['name'], e)
    
    return created_ids

def get_all_datasets():
    """Get all datasets from backend"""
    try:
        response = requests.get(f"{BACKEND_URL}/datasets", timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.warning("Failed to fetch datasets: %s", e)
    
    return []

def simulate_access_patterns(datasets):
    """
    Simulate realistic access patterns
    - Hot tier datasets: accessed frequently (80% chance)
    - Warm tier datasets: accessed occasionally (40% chance)
    - Cold tier datasets: accessed rarely (5% chance)
    """
    access_events = []
    
    for dataset in datasets:
        tier = dataset['current_tier']
        
        # Determine access probability based on tier
        if tier == "hot":
            access_prob = 0.80
            latency_range = (5, 50)
        elif tier == "warm":
            access_prob = 0.40
            latency_range = (50, 200)
        else:  # cold
            access_prob = 0.05
            latency_range = (200, 1000)
        
        # Randomly decide if this dataset is accessed
        if random.random() < access_prob:
            latency_ms = random.uniform(*latency_range)
            
            event = {
                "dataset_id": dataset['id'],
                "dataset_name": dataset['name'],
                "tier": tier,
                "latency_ms": round(latency_ms, 2),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            access_events.append(event)
    
    return access_events

def record_access_in_backend(dataset_id, latency_ms):
    """Record access event in backend to update metrics"""
    try:
        requests.post(
            f"{BACKEND_URL}/datasets/{dataset_id}/access",
            params={"latency_ms": latency_ms},
            timeout=5
        )
    except Exception as e:
        logger.warning("Failed to record access in backend: %s", e)

def trigger_ml_decisions(datasets):
    """
    Periodically trigger ML-based tier decisions
    Simulates automated tier optimization
    """
    # Select a random subset of datasets to evaluate
    num_to_evaluate = min(3, len(datasets))
    selected = random.sample(datasets, num_to_evaluate)
    
    for dataset in selected:
        try:
            response = requests.post(
                f"{BACKEND_URL}/decide",
                json={
                    "dataset_id": dataset['id'],
                    "auto_execute": True
                },
                timeout=10
            )
            
            if response.status_code == 200:
                decision = response.json()
                if decision['should_move']:
                    logger.info(
                        "ML decision triggered: %s %s -> %s (confidence %.2f%%)",
                        dataset['name'],
                        decision['current_tier'],
                        decision['recommended_tier'],
                        decision['confidence'] * 100
                    )
        except Exception as e:
            logger.warning("Failed to trigger ML decision: %s", e)

def main():
    """Main producer loop"""
    logger.info("Starting Data Access Producer")
    
    # Wait for backend
    if not wait_for_backend():
        return
    
    # Create sample datasets
    time.sleep(3)
    create_sample_datasets()
    
    # Connect to Kafka
    time.sleep(2)
    producer = connect_kafka()
    
    logger.info(
        "Producing access events every %s seconds on topic %s",
        PRODUCE_INTERVAL,
        KAFKA_TOPIC
    )
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            
            # Get current datasets
            datasets = get_all_datasets()
            
            if not datasets:
                logger.warning("No datasets found, waiting before next iteration")
                time.sleep(PRODUCE_INTERVAL)
                continue
            
            # Simulate access patterns
            access_events = simulate_access_patterns(datasets)
            
            # Send events to Kafka and backend
            for event in access_events:
                try:
                    producer.send(KAFKA_TOPIC, value=event, key=str(event['dataset_id']))
                    record_access_in_backend(event['dataset_id'], event['latency_ms'])
                    logger.debug(
                        "Access event: dataset=%s tier=%s latency=%.1fms",
                        event['dataset_name'],
                        event['tier'],
                        event['latency_ms']
                    )
                except Exception as e:
                    logger.error("Failed to send access event: %s", e)
            
            if not access_events:
                logger.debug("No access events generated this round")
            
            # Every 5 iterations, trigger ML decisions
            if iteration % 5 == 0:
                logger.info("Triggering scheduled ML tier optimization")
                trigger_ml_decisions(datasets)
            
            # Wait before next batch
            time.sleep(PRODUCE_INTERVAL)
    
    except KeyboardInterrupt:
        logger.info("Shutting down producer (keyboard interrupt)")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
