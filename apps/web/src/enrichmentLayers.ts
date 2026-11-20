export type EnrichmentLayer = "risk" | "weather";

export const enrichmentLayerLabels: Record<EnrichmentLayer, string> = {
  risk: "Fire risk",
  weather: "Weather context",
};

export const enrichmentLayerDescriptions: Record<EnrichmentLayer, string> = {
  risk: "Risk score (0-12) based on wind, temperature, dryness, and fire intensity",
  weather: "Temperature, precipitation, and wind conditions at detection time",
};
