import { useEffect, useState } from "react";
import { formatUtc, HOTSPOT_LIMIT } from "./hotspots";
import type { Bounds, Hotspot, HotspotResults } from "./hotspots";
import { formatNumber, formatRisk, isRiskLevel } from "./conditions";
import type { RiskSummary } from "./conditions";

export type IncidentStatus = "new" | "growing" | "stable" | "inactive" | "merged";

export interface IncidentSummary {
  incident_id: string;
  revision: number;
  status: IncidentStatus;
  previous_status: IncidentStatus | null;
  merged_into: string | null;
  first_observed_at: string;
  last_observed_at: string;
  updated_at: string;
  detection_count: number;
  footprint_cells: number;
  footprint_km2: number;
  latitude: number;
  longitude: number;
  /** [west, south, east, north] of the incident's detections. */
  bbox: Bounds;
  max_frp_mw: number | null;
  total_frp_mw: number;
  products: string[];
  peak_risk: RiskSummary | null;
}

export interface IncidentList {
  items: IncidentSummary[];
  truncated: boolean;
}

export interface IncidentDay {
  day: string;
  detections: number;
  new_cells: number;
  max_frp_mw: number | null;
  /** Cumulative footprint at the end of the day. */
  footprint_cells: number;
  footprint_km2: number;
}

export interface IncidentWeatherHour {
  valid_at: string;
  temperature_c: number | null;
  precipitation_mm: number | null;
  wind_speed_kmh: number | null;
  wind_direction_degrees: number | null;
}

export interface IncidentWeather {
  cell: string;
  generated_at: string;
  hours: IncidentWeatherHour[];
}

export interface IncidentDetail extends IncidentSummary {
  daily: IncidentDay[];
  weather: IncidentWeather | null;
}

export const INCIDENT_REFRESH_MS = 60_000;

export const incidentStatuses: Record<IncidentStatus, { label: string; color: string }> = {
  new: { label: "New", color: "#d97706" },
  growing: { label: "Growing", color: "#b91c1c" },
  stable: { label: "Stable", color: "#466f8c" },
  inactive: { label: "Inactive", color: "#7b8781" },
  merged: { label: "Merged", color: "#9aa59f" },
};

export function formatArea(km2: number): string {
  return `${km2 < 10 ? km2.toFixed(1) : Math.round(km2).toLocaleString("en-US")} km²`;
}

/** Label and value of each key figure of an incident, shared by the sidebar and the report. */
export function incidentFields(shown: IncidentSummary): [string, string][] {
  const status = shown.previous_status && shown.previous_status !== shown.status
    ? `${incidentStatuses[shown.status].label} (was ${incidentStatuses[shown.previous_status].label.toLowerCase()})`
    : incidentStatuses[shown.status].label;
  return [
    ["Status", status],
    ["First observed", formatUtc(shown.first_observed_at)],
    ["Last observed", formatUtc(shown.last_observed_at)],
    ["Detections", shown.detection_count.toLocaleString("en-US")],
    ["Footprint", `${formatArea(shown.footprint_km2)} (${shown.footprint_cells.toLocaleString("en-US")} cells)`],
    ["Maximum FRP", formatNumber(shown.max_frp_mw, "MW")],
    ["Total FRP", formatNumber(shown.total_frp_mw, "MW")],
    ["Products", shown.products.join(", ") || "—"],
    ["Peak fire risk (24 h)", shown.peak_risk ? formatRisk(shown.peak_risk) : "Unknown"],
  ];
}

export const INCIDENT_METHOD_NOTE =
  "Detections clustered by space and time; the footprint counts ~5 km² grid cells with detections, not a mapped fire perimeter. " +
  "Peak risk is the highest risk among the incident's detections in its last 24 hours of activity.";

export function isIncidentStatus(value: unknown): value is IncidentStatus {
  return typeof value === "string" && Object.hasOwn(incidentStatuses, value);
}

const INCIDENT_MARKER_MIN_RADIUS = 7;
const INCIDENT_MARKER_MAX_RADIUS = 18;
/** Footprint of one resolution-7 H3 cell, in km². */
const CELL_AREA_KM2 = 5;

/** Map marker radius in pixels, growing logarithmically with the footprint. */
export function incidentMarkerRadius(footprintKm2: number): number {
  const radius = INCIDENT_MARKER_MIN_RADIUS + 3 * Math.log2(1 + Math.max(0, footprintKm2) / CELL_AREA_KM2);
  return Math.min(INCIDENT_MARKER_MAX_RADIUS, radius);
}

/** Most recently active first; ties keep a stable order by ID. */
export function sortIncidents(items: IncidentSummary[]): IncidentSummary[] {
  return [...items].sort(
    (a, b) =>
      Date.parse(b.last_observed_at) - Date.parse(a.last_observed_at) ||
      a.incident_id.localeCompare(b.incident_id),
  );
}

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

/** Checks the fields the dashboard relies on, so a malformed item cannot break rendering. */
export function isIncidentSummary(value: unknown): value is IncidentSummary {
  if (typeof value !== "object" || value === null) return false;
  const item = value as Record<string, unknown>;
  const risk = item.peak_risk as Record<string, unknown> | null | undefined;
  return (
    typeof item.incident_id === "string" &&
    isIncidentStatus(item.status) &&
    (item.previous_status === null || isIncidentStatus(item.previous_status)) &&
    typeof item.first_observed_at === "string" &&
    typeof item.last_observed_at === "string" &&
    isNumber(item.latitude) &&
    isNumber(item.longitude) &&
    Array.isArray(item.bbox) &&
    item.bbox.length === 4 &&
    item.bbox.every(isNumber) &&
    isNumber(item.detection_count) &&
    isNumber(item.footprint_km2) &&
    Array.isArray(item.products) &&
    (risk === null || (typeof risk === "object" && isNumber(risk.score) && isRiskLevel(risk.level)))
  );
}

function isIncidentList(value: unknown): value is IncidentList {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Partial<IncidentList>;
  return (
    Array.isArray(candidate.items) &&
    candidate.items.every(isIncidentSummary) &&
    typeof candidate.truncated === "boolean"
  );
}

function isIncidentDetail(value: unknown): value is IncidentDetail {
  if (!isIncidentSummary(value)) return false;
  const candidate = value as Partial<IncidentDetail>;
  return Array.isArray(candidate.daily) && (candidate.weather === null || typeof candidate.weather === "object");
}

export interface GrowthBar {
  day: string;
  detections: number;
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface GrowthChart {
  bars: GrowthBar[];
  /** SVG polyline points of the cumulative footprint (km²), one per day. */
  footprintLine: string;
  maxDetections: number;
  maxFootprintKm2: number;
}

/**
 * Lay out daily detections as bars and the cumulative footprint as a line in a
 * `width` × `height` plot area. Each series is scaled to its own maximum.
 */
export function growthChart(daily: IncidentDay[], width: number, height: number): GrowthChart {
  const maxDetections = Math.max(0, ...daily.map((day) => day.detections));
  const maxFootprintKm2 = Math.max(0, ...daily.map((day) => day.footprint_km2));
  const slot = daily.length > 0 ? width / daily.length : 0;
  const barWidth = slot * 0.7;
  const scale = (value: number, maximum: number) => (maximum > 0 ? (value / maximum) * height : 0);
  const round = (value: number) => Math.round(value * 10) / 10;
  const bars = daily.map((day, index) => {
    const barHeight = scale(day.detections, maxDetections);
    return {
      day: day.day,
      detections: day.detections,
      x: round(index * slot + (slot - barWidth) / 2),
      y: round(height - barHeight),
      width: round(barWidth),
      height: round(barHeight),
    };
  });
  const footprintLine = daily
    .map((day, index) => `${round(index * slot + slot / 2)},${round(height - scale(day.footprint_km2, maxFootprintKm2))}`)
    .join(" ");
  return { bars, footprintLine, maxDetections, maxFootprintKm2 };
}

export const OUTLOOK_STEP_HOURS = 3;
export const OUTLOOK_HOURS = 24;

/** The first forecast hour as current conditions, then one hour every 3 hours up to 24 h ahead. */
export function weatherOutlook(hours: IncidentWeatherHour[]): {
  current: IncidentWeatherHour | null;
  upcoming: IncidentWeatherHour[];
} {
  const [current, ...rest] = [...hours].sort((a, b) => Date.parse(a.valid_at) - Date.parse(b.valid_at));
  if (!current) return { current: null, upcoming: [] };
  const start = Date.parse(current.valid_at);
  const upcoming = rest.filter((hour) => {
    const offset = (Date.parse(hour.valid_at) - start) / 3_600_000;
    return offset > 0 && offset <= OUTLOOK_HOURS && offset % OUTLOOK_STEP_HOURS === 0;
  });
  return { current, upcoming };
}

export interface IncidentsState {
  items: IncidentSummary[];
  truncated: boolean;
  loading: boolean;
  error: string;
}

/**
 * Active incidents for an area query (`west`, `south`, … or empty), reloaded every
 * `intervalMs` and whenever `query` or `refreshKey` changes. A failed reload keeps
 * the previous items and reports the error.
 */
export function useIncidents(query: string, refreshKey = 0, intervalMs = INCIDENT_REFRESH_MS): IncidentsState {
  const [state, setState] = useState<IncidentsState>({ items: [], truncated: false, loading: true, error: "" });

  useEffect(() => {
    let controller: AbortController | null = null;
    setState({ items: [], truncated: false, loading: true, error: "" });

    function load() {
      controller?.abort();
      const current = new AbortController();
      controller = current;
      fetch(query ? `/api/incidents?${query}` : "/api/incidents", { signal: current.signal })
        .then((response) => {
          if (!response.ok) throw new Error(`Request failed (${response.status})`);
          return response.json() as Promise<unknown>;
        })
        .then((data) => {
          if (!isIncidentList(data)) throw new Error("Unexpected response");
          setState({ items: sortIncidents(data.items), truncated: data.truncated, loading: false, error: "" });
        })
        .catch((reason: unknown) => {
          if (current.signal.aborted) return;
          const message = reason instanceof Error ? reason.message : "Unable to load incidents";
          setState((previous) => ({ ...previous, loading: false, error: message }));
        });
    }

    load();
    const timer = setInterval(load, intervalMs);
    return () => {
      clearInterval(timer);
      controller?.abort();
    };
  }, [query, refreshKey, intervalMs]);

  return state;
}

export interface IncidentDetailState {
  incident: IncidentDetail | null;
  loading: boolean;
  error: string;
}

/** Load one incident; reloads when `revision` changes so the detail follows the list. */
export function useIncidentDetail(incidentId: string | null, revision: number | null): IncidentDetailState {
  const [state, setState] = useState<IncidentDetailState>({ incident: null, loading: false, error: "" });

  useEffect(() => {
    if (!incidentId) {
      setState({ incident: null, loading: false, error: "" });
      return;
    }
    const controller = new AbortController();
    setState((previous) => ({
      incident: previous.incident?.incident_id === incidentId ? previous.incident : null,
      loading: true,
      error: "",
    }));
    fetch(`/api/incidents/${encodeURIComponent(incidentId)}`, { signal: controller.signal })
      .then((response) => {
        if (response.status === 404) throw new Error("Incident not found");
        if (!response.ok) throw new Error(`Request failed (${response.status})`);
        return response.json() as Promise<unknown>;
      })
      .then((data) => {
        if (!isIncidentDetail(data)) throw new Error("Unexpected response");
        setState({ incident: data, loading: false, error: "" });
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted) return;
        const message = reason instanceof Error ? reason.message : "Unable to load incident";
        setState({ incident: null, loading: false, error: message });
      });
    return () => controller.abort();
  }, [incidentId, revision]);

  return state;
}

export interface IncidentDetectionsState {
  items: Hotspot[];
  truncated: boolean;
  loading: boolean;
  error: string;
}

/** Report links use the hash router: `#/incidents/<id>`. */
export function incidentReportHref(incidentId: string): string {
  return `#/incidents/${encodeURIComponent(incidentId)}`;
}

/** The incident ID of a report hash, or null for any other hash. */
export function reportIncidentId(hash: string): string | null {
  const match = /^#\/incidents\/([^/]+)$/.exec(hash);
  if (!match) return null;
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return null;
  }
}

/**
 * Detections inside the incident's bounding box during its days of activity (UTC),
 * including cross-sensor duplicates so each day shows every satellite's view.
 */
export function useIncidentDetections(incident: IncidentSummary | null): IncidentDetectionsState {
  const [state, setState] = useState<IncidentDetectionsState>({ items: [], truncated: false, loading: false, error: "" });
  const [west, south, east, north] = incident?.bbox ?? [0, 0, 0, 0];
  const query = incident
    ? new URLSearchParams({
        west: String(west),
        south: String(south),
        east: String(east),
        north: String(north),
        date_from: incident.first_observed_at.slice(0, 10),
        date_to: incident.last_observed_at.slice(0, 10),
        include_duplicates: "true",
        limit: String(HOTSPOT_LIMIT),
      }).toString()
    : "";

  useEffect(() => {
    if (!query) {
      setState({ items: [], truncated: false, loading: false, error: "" });
      return;
    }
    const controller = new AbortController();
    setState((previous) => ({ ...previous, loading: true, error: "" }));
    fetch(`/api/hotspots?${query}`, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`Request failed (${response.status})`);
        return response.json() as Promise<HotspotResults>;
      })
      .then((data) => {
        if (!Array.isArray(data.items)) throw new Error("Unexpected response");
        setState({ items: data.items, truncated: Boolean(data.truncated), loading: false, error: "" });
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted) return;
        const message = reason instanceof Error ? reason.message : "Unable to load detections";
        setState({ items: [], truncated: false, loading: false, error: message });
      });
    return () => controller.abort();
  }, [query]);

  return state;
}
