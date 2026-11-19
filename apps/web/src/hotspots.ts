export interface Hotspot {
  id: number;
  source: string;
  satellite: string;
  observed_at: string;
  latitude: number;
  longitude: number;
  attributes: Record<string, string>;
}

export interface HotspotResults {
  items: Hotspot[];
  truncated: boolean;
}

export type Bounds = [number, number, number, number];

export const regions: Record<string, { label: string; bounds: Bounds }> = {
  borneo: { label: "Borneo pilot", bounds: [108, -5, 120, 8] },
  sumatra: { label: "Sumatra", bounds: [94, -7, 106, 6] },
  mainland: { label: "Mainland Southeast Asia", bounds: [92, 8, 110, 29] },
  southeastAsia: { label: "Southeast Asia", bounds: [92, -12, 156, 29] },
};

export function formatUtc(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "Unknown";
  return `${date.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}
