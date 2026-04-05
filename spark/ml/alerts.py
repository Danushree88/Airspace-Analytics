import pandas as pd
from datetime import datetime


class AlertEngine:
    """
    Step 10: Real-Time Decision Engine
    Generates actionable alerts:
    - Congestion alerts
    - Anomaly alerts
    - Traffic surge alerts
    """

    def __init__(self):
        self._prev_counts = {}   # region -> previous aircraft count

    def congestion_alerts(self, region_metrics_pdf: pd.DataFrame) -> list:
        """
        Triggers congestion alerts when ACI exceeds thresholds.
        """
        alerts = []
        for _, row in region_metrics_pdf.iterrows():
            aci    = row.get("aci", 0)
            region = row.get("region", "UNKNOWN")
            count  = row.get("aircraft_count", 0)

            if aci > 1.2:
                alerts.append({
                    "type":    "CONGESTION",
                    "level":   "HIGH",
                    "region":  region,
                    "message": f"HIGH CONGESTION in {region}: ACI={aci:.2f}, {count} aircraft",
                })
            elif aci > 0.8:
                alerts.append({
                    "type":    "CONGESTION",
                    "level":   "MEDIUM",
                    "region":  region,
                    "message": f"MEDIUM CONGESTION in {region}: ACI={aci:.2f}, {count} aircraft",
                })
        return alerts

    def anomaly_alerts(self, anomalies_pdf):
        alerts = []
        seen = set()
        for _, row in anomalies_pdf.iterrows():
            if row['icao24'] not in seen:
                seen.add(row['icao24'])
                alerts.append({
                    "type":    "ANOMALY",
                    "level":   "HIGH",
                    "region":  "N/A",
                    "message": f"ANOMALY: {row['icao24']} — speed={row['speed_kmh']:.1f} km/h, alt={row['altitude']:.0f}m, vrate={row['vertical_rate']:.1f} m/s"
                })
        return alerts

    def surge_alerts(self, region_counts: dict) -> list:
        """
        Detects sudden traffic surges by comparing current
        aircraft count to the previous batch's count.
        Triggers when count increases > 50% in one batch.
        """
        alerts = []
        for region, current_count in region_counts.items():
            prev = self._prev_counts.get(region, current_count)
            if prev > 0:
                change_pct = (current_count - prev) / prev * 100
                if change_pct > 50:
                    alerts.append({
                        "type":    "SURGE",
                        "level":   "MEDIUM",
                        "region":  region,
                        "message": (
                            f"TRAFFIC SURGE in {region}: "
                            f"{prev} → {current_count} aircraft "
                            f"(+{change_pct:.0f}%)"
                        ),
                    })
        # Update history
        self._prev_counts = dict(region_counts)
        return alerts

    def print_alerts(self, all_alerts: list):
        if not all_alerts:
            print("  ✅ No alerts this batch.")
            return
        print(f"  🚨 {len(all_alerts)} ALERT(S):")
        for a in all_alerts:
            icon = "🔴" if a["level"] == "HIGH" else "🟡"
            print(f"    {icon} [{a['type']}] {a['message']}")