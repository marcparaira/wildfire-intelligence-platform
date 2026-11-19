export interface Hotspot {
  id: number;
  event_id: string;
  source: string;
  instrument: string;
  satellite: string;
  observed_at: string;
  latitude: number;
  longitude: number;
  attributes: Record<string, string>;
  duplicate_of: string | null;
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
