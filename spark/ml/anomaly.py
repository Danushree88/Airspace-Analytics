from sklearn.ensemble import IsolationForest

class AnomalyDetector:

    def __init__(self):
        self.model = IsolationForest(
            n_estimators=50,
            contamination=0.05,
            random_state=42
        )

    def detect(self, pdf):
        features = pdf[["speed_kmh", "altitude", "vertical_rate"]]

        pdf["anomaly_score"] = self.model.fit_predict(features)

        anomalies = pdf[pdf["anomaly_score"] == -1]

        return anomalies