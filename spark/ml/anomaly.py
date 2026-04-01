from sklearn.ensemble import IsolationForest
import pandas as pd

class AnomalyDetector:

    def __init__(self):
        self.model = IsolationForest(
            n_estimators=50,
            contamination=0.05,
            random_state=42
        )

    def detect(self, pdf: pd.DataFrame) -> pd.DataFrame:
        features = pdf[["speed_kmh", "altitude", "vertical_rate"]].fillna(0)

        # fit_predict returns -1 (anomaly) or 1 (normal)
        raw_scores = self.model.fit_predict(features)

        # Store as float so Cassandra double column accepts it
        pdf = pdf.copy()
        pdf["anomaly_score"] = raw_scores.astype(float)

        anomalies = pdf[pdf["anomaly_score"] == -1.0]
        return anomalies