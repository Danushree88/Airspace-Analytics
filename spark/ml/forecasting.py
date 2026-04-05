import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


class TrafficForecaster:
    """
    Predicts future aircraft count per region using
    a simple linear regression over time-bucketed counts.
    """

    def __init__(self):
        self.model = LinearRegression()

    def forecast(self, pdf: pd.DataFrame) -> dict:
        """
        Takes a pandas DataFrame with columns: region, timestamp (unix epoch).
        Returns a dict: { region -> predicted_count_next_window }
        """
        results = {}

        if pdf.empty or "region" not in pdf.columns:
            return results

        # Use current unix time as reference
        now = pd.Timestamp.utcnow().timestamp()

        for region, group in pdf.groupby("region"):
            if len(group) < 3:
                # Not enough data to fit — use raw count as prediction
                results[region] = len(group)
                continue

            # Bin into 30-second windows and count aircraft
            group = group.copy()
            group["time_bucket"] = (group["timestamp"] // 30).astype(int)
            counts = group.groupby("time_bucket").size().reset_index(name="count")
            counts = counts.sort_values("time_bucket")

            X = counts[["time_bucket"]].values
            y = counts["count"].values

            self.model.fit(X, y)

            # Predict next window (30 seconds ahead)
            next_bucket = np.array([[counts["time_bucket"].max() + 1]])
            predicted = max(0, round(float(self.model.predict(next_bucket)[0]), 1))
            results[region] = predicted

        return results