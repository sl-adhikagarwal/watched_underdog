"""
Machine Learning Engine for Tier Prediction
Uses scikit-learn to predict optimal storage tier based on access patterns
"""

import os
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from datetime import datetime
from typing import Tuple
import logging

logger = logging.getLogger(__name__)

class MLEngine:
    """
    ML Engine for predicting optimal storage tier
    Features: access_freq_7d, avg_latency_ms, size_bytes
    Output: hot (0), warm (1), cold (2)
    """
    
    def __init__(self, model_path: str = "models/tier_model.pkl"):
        self.model_path = model_path
        self.model = None
        self.scaler = StandardScaler()
        self.tier_mapping = {0: "hot", 1: "warm", 2: "cold"}
        self.reverse_tier_mapping = {"hot": 0, "warm": 1, "cold": 2}
        
        # Load existing model if available
        if os.path.exists(self.model_path):
            self.load_model()
    
    def train_model(self):
        """
        Train the tier prediction model
        Uses synthetic training data based on common access patterns
        """
        logger.info("Training ML model")
        
        # Create synthetic training data
        # Hot tier: high access frequency, low latency sensitivity, any size
        # Warm tier: medium access frequency, medium latency, medium size
        # Cold tier: low access frequency, high latency tolerance, large size
        
        X_train = []
        y_train = []
        
        # Hot tier examples (150 samples)
        for _ in range(150):
            access_freq = np.random.randint(50, 1000)  # High frequency
            latency = np.random.uniform(1, 50)  # Low latency
            size = np.random.randint(1024, 10 * 1024**3)  # Any size
            X_train.append([access_freq, latency, size])
            y_train.append(0)  # hot
        
        # Warm tier examples (150 samples)
        for _ in range(150):
            access_freq = np.random.randint(10, 60)  # Medium frequency
            latency = np.random.uniform(50, 200)  # Medium latency
            size = np.random.randint(100 * 1024**2, 50 * 1024**3)  # Medium size
            X_train.append([access_freq, latency, size])
            y_train.append(1)  # warm
        
        # Cold tier examples (150 samples)
        for _ in range(150):
            access_freq = np.random.randint(0, 15)  # Low frequency
            latency = np.random.uniform(200, 1000)  # High latency OK
            size = np.random.randint(1 * 1024**3, 100 * 1024**3)  # Large size
            X_train.append([access_freq, latency, size])
            y_train.append(2)  # cold
        
        X_train = np.array(X_train)
        y_train = np.array(y_train)
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        
        # Train Random Forest model
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'
        )
        self.model.fit(X_train_scaled, y_train)
        
        # Save model
        self.save_model()
        
        # Print training accuracy
        train_accuracy = self.model.score(X_train_scaled, y_train)
        logger.info("Model trained with accuracy %.2f%%", train_accuracy * 100)
        
        return train_accuracy
    
    def predict_tier(
        self,
        access_freq_7d: int,
        avg_latency_ms: float,
        size_bytes: int
    ) -> Tuple[str, float]:
        """
        Predict optimal tier for a dataset
        
        Args:
            access_freq_7d: Number of accesses in last 7 days
            avg_latency_ms: Average access latency in milliseconds
            size_bytes: Dataset size in bytes
        
        Returns:
            Tuple of (predicted_tier, confidence)
        """
        if self.model is None:
            # If no model exists, use simple rule-based logic
            return self._rule_based_prediction(access_freq_7d, avg_latency_ms, size_bytes)
        
        # Prepare features
        features = np.array([[access_freq_7d, avg_latency_ms, size_bytes]])
        features_scaled = self.scaler.transform(features)
        
        # Predict
        prediction = self.model.predict(features_scaled)[0]
        probabilities = self.model.predict_proba(features_scaled)[0]
        confidence = float(np.max(probabilities))
        
        tier = self.tier_mapping[prediction]
        
        return tier, confidence
    
    def _rule_based_prediction(
        self,
        access_freq_7d: int,
        avg_latency_ms: float,
        size_bytes: int
    ) -> Tuple[str, float]:
        """
        Fallback rule-based tier prediction
        Used when ML model is not available
        """
        # Simple rule-based logic
        if access_freq_7d > 50:
            return "hot", 0.85
        elif access_freq_7d > 10:
            return "warm", 0.75
        else:
            return "cold", 0.80
    
    def save_model(self):
        """Save model and scaler to disk"""
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'tier_mapping': self.tier_mapping,
            'trained_at': datetime.utcnow().isoformat()
        }
        
        joblib.dump(model_data, self.model_path)
        logger.info("Model saved to %s", self.model_path)
    
    def load_model(self):
        """Load model and scaler from disk"""
        try:
            model_data = joblib.load(self.model_path)
            self.model = model_data['model']
            self.scaler = model_data['scaler']
            self.tier_mapping = model_data['tier_mapping']
            logger.info("Model loaded from %s", self.model_path)
            logger.info("Model trained at %s", model_data.get('trained_at', 'unknown'))
        except Exception as e:
            logger.warning("Failed to load model: %s", e)
            self.model = None
    
    def get_model_info(self):
        """Get information about the current model"""
        if self.model is None:
            return {
                "status": "not_trained",
                "message": "No model available, using rule-based predictions"
            }
        
        return {
            "status": "ready",
            "model_type": type(self.model).__name__,
            "features": ["access_freq_7d", "avg_latency_ms", "size_bytes"],
            "tiers": list(self.tier_mapping.values()),
            "model_path": self.model_path
        }

