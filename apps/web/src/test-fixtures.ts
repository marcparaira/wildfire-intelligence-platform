import type { Hotspot } from "./hotspots";
import type { ImageryLayer, SatelliteScene } from "./imagery";
import type { IncidentDay, IncidentDetail, IncidentSummary, IncidentWeatherHour } from "./incidents";

/** Test data shaped like the API responses (docs: Phase 8 contracts). */
export function incidentSummary(changes: Partial<IncidentSummary> = {}): IncidentSummary {
  return {
    incident_id: "0b6f3a52-1c1e-5d55-9a7e-1f0c6b8f2a10",
    revision: 3,
    status: "growing",
    previous_status: "new",
    merged_into: null,
    first_observed_at: "2026-03-03T18:05:00Z",
    last_observed_at: "2026-03-05T06:20:00Z",
    updated_at: "2026-03-05T06:20:00Z",
    detection_count: 42,
    footprint_cells: 6,
    footprint_km2: 31.2,
    latitude: 0.52,
    longitude: 114.12,
    bbox: [114.1, 0.5, 114.15, 0.55],
    max_frp_mw: 85.5,
    total_frp_mw: 640.25,
    products: ["VIIRS_NOAA20_NRT", "MODIS_NRT"],
    peak_risk: { score: 8, level: "high" },
    ...changes,
  };
}

export const incidentDays: IncidentDay[] = [
  { day: "2026-03-03", detections: 4, new_cells: 2, max_frp_mw: 12, footprint_cells: 2, footprint_km2: 10.4 },
  { day: "2026-03-04", detections: 30, new_cells: 3, max_frp_mw: 85.5, footprint_cells: 5, footprint_km2: 26 },
  { day: "2026-03-05", detections: 8, new_cells: 1, max_frp_mw: null, footprint_cells: 6, footprint_km2: 31.2 },
];

function hour(time: string, temperature: number): IncidentWeatherHour {
  return { valid_at: time, temperature_c: temperature, precipitation_mm: 0, wind_speed_kmh: 14, wind_direction_degrees: 45 };
}

export function incidentDetail(changes: Partial<IncidentDetail> = {}): IncidentDetail {
  return {
    ...incidentSummary(),
    daily: incidentDays,
    weather: {
      cell: "8265a7fffffffff",
      generated_at: "2026-03-05T06:00:00Z",
      hours: Array.from({ length: 25 }, (_, index) =>
        hour(new Date(Date.UTC(2026, 2, 5, 7 + index)).toISOString(), 30 + (index % 4)),
      ),
    },
    ...changes,
  };
}

export function enrichedHotspot(changes: Partial<Hotspot> = {}): Hotspot {
  return {
    id: 7,
    event_id: "6a1f8f4e-7d0c-5a6e-9c1d-2b3e4f5a6b7c",
    source: "VIIRS_NOAA20_NRT",
    instrument: "VIIRS",
    satellite: "N20",
    observed_at: "2026-03-05T00:07:00Z",
    latitude: 0.5234,
    longitude: 114.1234,
    attributes: { confidence: "nominal", frp: "12.5", daynight: "N" },
    duplicate_of: null,
    country: "IDN",
    risk: { score: 7, level: "high", wind_points: 1, temperature_points: 2, dryness_points: 3, intensity_points: 1 },
    weather: {
      valid_at: "2026-03-05T00:00:00Z",
      temperature_c: 31.4,
      precipitation_mm: 0,
      precipitation_24h_mm: 0.1,
      wind_speed_kmh: 12.2,
      wind_direction_degrees: 90,
    },
    ...changes,
  };
}

export const SWIR: ImageryLayer = "Sentinel-2_L2A_SWIR_B12-B8A-B04";

export function satelliteScene(changes: Partial<SatelliteScene> = {}): SatelliteScene {
  const day = changes.acquisition_date ?? "2026-03-05";
  return {
    scene_id: "5d2c1e0a-0000-5000-8000-000000000001",
    provider: "planetary-computer",
    layer: SWIR,
    instrument: "MSI",
    platform: "Sentinel-2B",
    acquisition_date: day,
    retrieved_at: `${day}T13:02:00Z`,
    bbox: [114, 0.5, 114.5, 1],
    width: 2048,
    height: 2048,
    acquired_at: `${day}T02:13:49Z`,
    cloud_cover: 12.5,
    ...changes,
  };
}
