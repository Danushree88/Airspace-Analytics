import pandas as pd


class OptimizationEngine:
    """
    Step 9: Optimization Layer
    - Congestion-based rerouting suggestions
    - Efficiency scoring per flight
    - Load balancing across regions
    """

    # Typical cruise speed and altitude for a commercial flight
    IDEAL_SPEED_KMH  = 850.0
    IDEAL_ALTITUDE_M = 10000.0

    def efficiency_score(self, pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Scores each flight 0-100 based on how close it is to
        ideal cruise speed and altitude.
        Higher = more efficient.
        """
        pdf = pdf.copy()

        speed_score    = 1 - abs(pdf["speed_kmh"]  - self.IDEAL_SPEED_KMH)  / self.IDEAL_SPEED_KMH
        altitude_score = 1 - abs(pdf["altitude"]   - self.IDEAL_ALTITUDE_M) / self.IDEAL_ALTITUDE_M

        # Clip to [0, 1] then scale to 0-100
        speed_score    = speed_score.clip(0, 1)
        altitude_score = altitude_score.clip(0, 1)

        pdf["efficiency_score"] = ((speed_score + altitude_score) / 2 * 100).round(1)
        return pdf

    def reroute_suggestions(self, region_aci: dict) -> list:
        """
        Given a dict of { region -> ACI }, suggests rerouting
        away from congested regions toward lighter ones.
        Returns a list of suggestion strings.
        """
        suggestions = []

        if not region_aci:
            return suggestions

        congested = {r: a for r, a in region_aci.items() if a > 0.8}
        light     = {r: a for r, a in region_aci.items() if a < 0.4}

        if not congested:
            return ["✅ No rerouting needed — all regions within capacity."]

        light_regions = sorted(light.keys(), key=lambda r: light[r])[:3]

        for region, aci in sorted(congested.items(), key=lambda x: -x[1]):
            level = "HIGH" if aci > 1.2 else "MEDIUM"
            if light_regions:
                alts = ", ".join(light_regions)
                suggestions.append(
                    f"[{level}] {region} (ACI={aci:.2f}) → reroute via: {alts}"
                )
            else:
                suggestions.append(
                    f"[{level}] {region} (ACI={aci:.2f}) → no light regions available"
                )

        return suggestions

    def load_balance(self, region_aci: dict) -> dict:
        """
        Calculates how overloaded or underloaded each region is
        relative to the average ACI.
        Returns { region -> load_status }
        """
        if not region_aci:
            return {}

        avg_aci = sum(region_aci.values()) / len(region_aci)
        result  = {}

        for region, aci in region_aci.items():
            if aci > avg_aci * 1.5:
                result[region] = "OVERLOADED"
            elif aci < avg_aci * 0.5:
                result[region] = "UNDERLOADED"
            else:
                result[region] = "BALANCED"

        return result