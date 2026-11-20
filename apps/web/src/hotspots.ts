import type { DetectionRisk, DetectionWeather } from "./conditions";

export interface Hotspot {
  /** PostGIS row ID; null for a detection that arrived live and is not stored yet. */
  id: number | null;
  event_id: string;
  source: string;
  instrument: string;
  satellite: string;
  observed_at: string;
  latitude: number;
  longitude: number;
  attributes: Record<string, string>;
  duplicate_of: string | null;
  /** ISO 3166-1 alpha-3 code; absent or null until the detection has been enriched. */
  country?: string | null;
  risk?: DetectionRisk | null;
  /** Forecast weather at the observation hour. */
  weather?: DetectionWeather | null;
}

export interface HotspotResults {
  items: Hotspot[];
  truncated: boolean;
}

export type Bounds = [number, number, number, number];

export const DEFAULT_VIEW_BOUNDS: Bounds = [92, -12, 156, 29];

export const regions = {
  mainland: { label: "Mainland Southeast Asia", bounds: [92, 8, 110, 29] },
  malayPeninsula: { label: "Malay Peninsula", bounds: [98, 0, 105, 15] },
  sumatra: { label: "Sumatra", bounds: [94, -7, 106, 6] },
  javaBali: { label: "Java and Bali", bounds: [105, -9.6, 116, -5.5] },
  borneo: { label: "Borneo", bounds: [108, -5, 120, 8] },
  sulawesi: { label: "Sulawesi", bounds: [118, -7, 126, 3] },
  lesserSunda: { label: "Lesser Sunda Islands", bounds: [114, -12, 128, -7] },
  maluku: { label: "Maluku Islands", bounds: [125, -10, 135, 3] },
  philippines: { label: "Philippine Islands", bounds: [116, 4, 127, 22] },
  newGuinea: { label: "New Guinea and nearby islands", bounds: [129, -12, 156, 1] },
} satisfies Record<string, { label: string; bounds: Bounds }>;

export type RegionId = keyof typeof regions;

export function isRegionId(value: string): value is RegionId {
  return Object.hasOwn(regions, value);
}

export function formatUtc(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "Unknown";
  return `${date.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}

export const HOTSPOT_LIMIT = 5000;

export interface DateRange {
  dateFrom: string;
  dateTo: string;
  /** The range ends today or later, so detections observed after it are still current. */
  openEnded: boolean;
}

export function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

export function inDateRange(hotspot: Hotspot, range: DateRange): boolean {
  const day = hotspot.observed_at.slice(0, 10);
  return day >= range.dateFrom && (range.openEnded || day <= range.dateTo);
}

/**
 * Merge live detections into a snapshot: one entry per event ID (the snapshot's
 * stored copy wins), newest first, at most `limit` entries.
 */
export function mergeHotspots(
  snapshot: Hotspot[],
  live: Hotspot[],
  limit = HOTSPOT_LIMIT,
): { items: Hotspot[]; trimmed: boolean } {
  if (live.length === 0) return { items: snapshot, trimmed: false };
  const known = new Set(snapshot.map((hotspot) => hotspot.event_id));
  const added = live.filter((hotspot) => {
    if (known.has(hotspot.event_id)) return false;
    known.add(hotspot.event_id);
    return true;
  });
  if (added.length === 0) return { items: snapshot, trimmed: false };
  const items = [...added, ...snapshot].sort(
    (a, b) => Date.parse(b.observed_at) - Date.parse(a.observed_at),
  );
  return { items: items.slice(0, limit), trimmed: items.length > limit };
}
