export interface WeatherPoint {
  latitude: number;
  longitude: number;
  west: number;
  south: number;
  east: number;
  north: number;
  temperature_c: number | null;
  precipitation_mm: number | null;
  wind_speed_kmh: number | null;
  wind_direction_degrees: number | null;
}

export interface WeatherForecast {
  generated_at: string;
  valid_at: string;
  times: string[];
  items: WeatherPoint[];
}

export type WeatherMetric = "" | "temperature" | "precipitation" | "wind";

export const weatherMetrics: { value: WeatherMetric; label: string }[] = [
  { value: "", label: "Off" },
  { value: "temperature", label: "Temperature" },
  { value: "precipitation", label: "Precipitation" },
  { value: "wind", label: "Wind speed" },
];

export function isWeatherMetric(value: string): value is WeatherMetric {
  return weatherMetrics.some((metric) => metric.value === value);
}

const palettes: Record<Exclude<WeatherMetric, "">, [number, number, string[]]> = {
  temperature: [0, 40, ["#2563eb", "#fde047", "#dc2626"]],
  precipitation: [0, 10, ["#dbeafe", "#3b82f6", "#1e3a8a"]],
  wind: [0, 60, ["#34d399", "#facc15", "#b91c1c"]],
};

export function weatherValue(point: WeatherPoint, metric: WeatherMetric): number | null {
  switch (metric) {
    case "temperature":
      return point.temperature_c;
    case "precipitation":
      return point.precipitation_mm;
    case "wind":
      return point.wind_speed_kmh;
    default:
      return null;
  }
}

function parseColor(hex: string): [number, number, number] {
  return [
    Number.parseInt(hex.slice(1, 3), 16),
    Number.parseInt(hex.slice(3, 5), 16),
    Number.parseInt(hex.slice(5, 7), 16),
  ];
}

function interpolateColor(value: number, metric: Exclude<WeatherMetric, "">): string {
  const [minimum, maximum, colors] = palettes[metric];
  const position = Math.max(0, Math.min(1, (value - minimum) / (maximum - minimum)));
  const segment = Math.min(colors.length - 2, Math.floor(position * (colors.length - 1)));
  const fraction = position * (colors.length - 1) - segment;
  const start = parseColor(colors[segment]);
  const end = parseColor(colors[segment + 1]);
  const channels = start.map((channel, index) =>
    Math.round(channel + (end[index] - channel) * fraction),
  );
  return `rgba(${channels.join(",")},0.58)`;
}

export function weatherColor(value: number | null, metric: WeatherMetric): string {
  if (value === null || metric === "" || !Number.isFinite(value)) return "transparent";
  if (metric === "precipitation" && value <= 0) return "transparent";
  return interpolateColor(value, metric);
}

export function weatherLegend(metric: WeatherMetric): {
  gradient: string;
  label: string;
} | null {
  if (!metric) return null;
  const [minimum, maximum, colors] = palettes[metric];
  const units = metric === "temperature" ? "°C" : metric === "precipitation" ? "mm" : "km/h";
  return {
    gradient: `linear-gradient(to right, ${colors.join(", ")})`,
    label: `${minimum}${units}–${maximum}+${units}`,
  };
}

export function formatWeatherTime(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "Unknown";
  return `${date.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}
