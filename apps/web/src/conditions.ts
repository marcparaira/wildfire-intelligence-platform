/** Country, fire risk, and weather attached to detections and incidents by the stream processor. */

export type RiskLevel = "low" | "moderate" | "high" | "extreme";

export interface RiskSummary {
  score: number;
  level: RiskLevel;
}

export interface DetectionRisk extends RiskSummary {
  wind_points: number;
  temperature_points: number;
  dryness_points: number;
  intensity_points: number;
}

export interface DetectionWeather {
  valid_at: string;
  temperature_c: number | null;
  precipitation_mm: number | null;
  precipitation_24h_mm: number | null;
  wind_speed_kmh: number | null;
  wind_direction_degrees: number | null;
}

export const RISK_MAX_SCORE = 12;

export const riskLevels: Record<RiskLevel, { label: string; color: string }> = {
  low: { label: "Low", color: "#3f8f5a" },
  moderate: { label: "Moderate", color: "#b98a06" },
  high: { label: "High", color: "#d9600b" },
  extreme: { label: "Extreme", color: "#a32020" },
};

export function isRiskLevel(value: unknown): value is RiskLevel {
  return typeof value === "string" && Object.hasOwn(riskLevels, value);
}

/** "High · 7/12" */
export function formatRisk(risk: RiskSummary): string {
  return `${riskLevels[risk.level].label} · ${risk.score}/${RISK_MAX_SCORE}`;
}

const countryNames: Record<string, string> = {
  BRN: "Brunei",
  IDN: "Indonesia",
  KHM: "Cambodia",
  LAO: "Laos",
  MMR: "Myanmar",
  MYS: "Malaysia",
  PHL: "Philippines",
  PNG: "Papua New Guinea",
  SGP: "Singapore",
  THA: "Thailand",
  TLS: "Timor-Leste",
  VNM: "Vietnam",
};

/** Country name for an ISO 3166-1 alpha-3 code; unknown codes are shown as they are. */
export function countryName(code: string | null | undefined): string {
  if (!code) return "Outside the selected countries";
  return countryNames[code] ?? code;
}

const COMPASS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];

/** Compass point the wind blows from, for a meteorological direction in degrees. */
export function compassPoint(degrees: number): string {
  const normalized = ((degrees % 360) + 360) % 360;
  return COMPASS[Math.round(normalized / 45) % COMPASS.length];
}

export function formatNumber(value: number | null | undefined, unit: string, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${value.toFixed(digits)} ${unit}`;
}

/** "14 km/h from NE" */
export function formatWind(speed: number | null, direction: number | null): string {
  if (speed === null || !Number.isFinite(speed)) return "—";
  const from = direction === null || !Number.isFinite(direction) ? "" : ` from ${compassPoint(direction)}`;
  return `${Math.round(speed)} km/h${from}`;
}
