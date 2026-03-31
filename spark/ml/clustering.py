from sklearn.cluster import KMeans

class FlightClustering:

    def __init__(self):
        self.model = KMeans(n_clusters=3, random_state=42)

    def cluster(self, pdf):
        features = pdf[["speed_kmh", "altitude"]]

        pdf["cluster"] = self.model.fit_predict(features)

        return pdf