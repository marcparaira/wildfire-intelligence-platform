import { useEffect, useState } from "react";
import type { Bounds } from "./hotspots";

export type ImageryLayer =
  | "Sentinel-2_L2A_SWIR_B12-B8A-B04"
  | "VIIRS_NOAA20_CorrectedReflectance_TrueColor"
  | "VIIRS_NOAA20_CorrectedReflectance_BandsM11-I2-I1";

export const SENTINEL_SWIR: ImageryLayer = "Sentinel-2_L2A_SWIR_B12-B8A-B04";

/** In display order; the first available layer is shown by default. */
export const imageryLayers: Record<ImageryLayer, { label: string; description: string }> = {
  "Sentinel-2_L2A_SWIR_B12-B8A-B04": {
    label: "Sentinel-2 SWIR (20 m)",
    description:
      "Short-wave infrared (bands B12, B8A, B04) of a single Sentinel-2 pass: flames show bright red to yellow, fresh burn scars dark red-brown, vegetation green; sees through smoke but not cloud.",
  },
  "VIIRS_NOAA20_CorrectedReflectance_TrueColor": {
    label: "True color",
    description: "Natural color, as the eye would see it: smoke appears grey-brown, clouds white.",
  },
  "VIIRS_NOAA20_CorrectedReflectance_BandsM11-I2-I1": {
    label: "False color",
    description: "VIIRS bands M11, I2, I1: active fire appears orange-red, burn scars brown, vegetation green; sees through thin smoke.",
  },
};

export const IMAGERY_LAYER_ORDER = Object.keys(imageryLayers) as ImageryLayer[];

export function isImageryLayer(value: unknown): value is ImageryLayer {
  return typeof value === "string" && Object.hasOwn(imageryLayers, value);
}

export interface SatelliteScene {
  scene_id: string;
  provider: string;
  layer: ImageryLayer;
  instrument: string;
  platform: string;
  /** UTC day of the daily composite or the pass (YYYY-MM-DD). */
  acquisition_date: string;
  retrieved_at: string;
  /** [west, south, east, north] of the image. */
  bbox: Bounds;
  width: number;
  height: number;
  /** Sensing time of a single pass (Sentinel-2); null for daily composites. */
  acquired_at: string | null;
  /** Cloud cover of the source granules in percent (not of the tile alone). */
  cloud_cover: number | null;
}

export interface IncidentImagery {
  incident_id: string;
  first_day: string;
  last_day: string;
  scenes: SatelliteScene[];
}

export function sceneImageUrl(scene: Pick<SatelliteScene, "scene_id">): string {
  return `/api/imagery/${encodeURIComponent(scene.scene_id)}`;
}

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

/** Scenes from before the API returned the optional fields lack them. */
type SceneResponse = Omit<SatelliteScene, "acquired_at" | "cloud_cover"> & {
  acquired_at?: string | null;
  cloud_cover?: number | null;
};

function isScene(value: unknown): value is SceneResponse {
  if (typeof value !== "object" || value === null) return false;
  const scene = value as Record<string, unknown>;
  return (
    typeof scene.scene_id === "string" &&
    isImageryLayer(scene.layer) &&
    typeof scene.acquisition_date === "string" &&
    typeof scene.retrieved_at === "string" &&
    typeof scene.platform === "string" &&
    Array.isArray(scene.bbox) &&
    scene.bbox.length === 4 &&
    scene.bbox.every(isNumber) &&
    (scene.acquired_at == null || typeof scene.acquired_at === "string") &&
    (scene.cloud_cover == null || isNumber(scene.cloud_cover))
  );
}

/**
 * Accepts the response when its envelope is valid; scenes of layers this dashboard
 * does not know (added to the API later) are dropped rather than failing the report.
 */
export function parseIncidentImagery(value: unknown): IncidentImagery | null {
  if (typeof value !== "object" || value === null) return null;
  const candidate = value as Record<string, unknown>;
  if (
    typeof candidate.incident_id !== "string" ||
    typeof candidate.first_day !== "string" ||
    typeof candidate.last_day !== "string" ||
    !Array.isArray(candidate.scenes)
  ) {
    return null;
  }
  return {
    incident_id: candidate.incident_id,
    first_day: candidate.first_day,
    last_day: candidate.last_day,
    scenes: candidate.scenes
      .filter(isScene)
      .map((scene) => ({ ...scene, acquired_at: scene.acquired_at ?? null, cloud_cover: scene.cloud_cover ?? null })),
  };
}

export interface ImageryDay {
  day: string;
  scenes: SatelliteScene[];
}

/** One entry per acquisition date that has scenes of `layer`, oldest first. */
export function imageryDays(scenes: SatelliteScene[], layer: ImageryLayer): ImageryDay[] {
  const byDay = new Map<string, SatelliteScene[]>();
  for (const scene of scenes) {
    if (scene.layer !== layer) continue;
    byDay.set(scene.acquisition_date, [...(byDay.get(scene.acquisition_date) ?? []), scene]);
  }
  return [...byDay.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([day, items]) => ({ day, scenes: items }));
}

/** When an image was taken relative to the incident's days with detections. */
export type DayTiming = "active" | "before" | "after" | "between";

export function dayTiming(day: string, activeDays: readonly string[]): DayTiming {
  if (activeDays.includes(day)) return "active";
  const sorted = [...activeDays].sort();
  if (sorted.length === 0) return "between";
  if (day < sorted[0]) return "before";
  if (day > sorted[sorted.length - 1]) return "after";
  return "between";
}

/**
 * The day to show first, to confirm the fire: the latest image on a day with
 * detections, else the first one after them (the burn scar), else the latest.
 */
export function defaultImageryDay(days: ImageryDay[], activeDays: readonly string[]): ImageryDay | null {
  const active = days.filter((entry) => dayTiming(entry.day, activeDays) === "active");
  if (active.length > 0) return active[active.length - 1];
  return days.find((entry) => dayTiming(entry.day, activeDays) === "after") ?? days[days.length - 1] ?? null;
}

/** Layers that have at least one scene, in display order. */
export function availableLayers(scenes: SatelliteScene[]): ImageryLayer[] {
  const present = new Set(scenes.map((scene) => scene.layer));
  return IMAGERY_LAYER_ORDER.filter((layer) => present.has(layer));
}

export interface IncidentImageryState {
  imagery: IncidentImagery | null;
  loading: boolean;
  error: string;
}

/** Scenes over one incident; reloads when `refreshKey` changes. */
export function useIncidentImagery(incidentId: string, refreshKey = 0): IncidentImageryState {
  const [state, setState] = useState<IncidentImageryState>({ imagery: null, loading: true, error: "" });

  useEffect(() => {
    const controller = new AbortController();
    setState((previous) => ({
      imagery: previous.imagery?.incident_id === incidentId ? previous.imagery : null,
      loading: true,
      error: "",
    }));
    fetch(`/api/incidents/${encodeURIComponent(incidentId)}/imagery`, { signal: controller.signal })
      .then((response) => {
        if (response.status === 404) throw new Error("Incident not found");
        if (!response.ok) throw new Error(`Request failed (${response.status})`);
        return response.json() as Promise<unknown>;
      })
      .then((data) => {
        const imagery = parseIncidentImagery(data);
        if (!imagery) throw new Error("Unexpected response");
        setState({ imagery, loading: false, error: "" });
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted) return;
        const message = reason instanceof Error ? reason.message : "Unable to load imagery";
        setState({ imagery: null, loading: false, error: message });
      });
    return () => controller.abort();
  }, [incidentId, refreshKey]);

  return state;
}
