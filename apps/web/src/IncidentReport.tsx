import { useMemo, useState } from "react";
import ImageryMap from "./ImageryMap";
import { GrowthChart, RiskBadge, StatusBadge, WeatherContext } from "./IncidentPanel";
import { formatUtc } from "./hotspots";
import {
  availableLayers,
  dayTiming,
  defaultImageryDay,
  imageryDays,
  imageryLayers,
  SENTINEL_SWIR,
  useIncidentImagery,
} from "./imagery";
import type { DayTiming, ImageryLayer, IncidentImagery, SatelliteScene } from "./imagery";
import { INCIDENT_METHOD_NOTE, incidentFields, useIncidentDetail, useIncidentDetections } from "./incidents";
import type { IncidentDetail } from "./incidents";
import type { Hotspot } from "./hotspots";

const REPORT_CHART_WIDTH = 640;
const REPORT_CHART_HEIGHT = 140;
const NO_DETECTIONS: Hotspot[] = [];

function formatCoordinate(value: number, positive: string, negative: string): string {
  return `${Math.abs(value).toFixed(3)}° ${value >= 0 ? positive : negative}`;
}

function incidentLocation(incident: IncidentDetail): string {
  return `${formatCoordinate(incident.latitude, "N", "S")}, ${formatCoordinate(incident.longitude, "E", "W")}`;
}

const DAY_TIMING_TITLES: Record<DayTiming, string> = {
  active: "Fire detections on this day",
  before: "Before the first detection",
  after: "After the last detection",
  between: "No detections on this day",
};

// Typical colours of the SWIR composite (B12, B8A, B04), for reading the image.
const SWIR_LEGEND: [string, string][] = [
  ["#ffd23c", "Active flames (bright red to yellow)"],
  ["#6e2a1e", "Fresh burn scar (dark red-brown)"],
  ["#5fd84a", "Vegetation"],
  ["#c79a6b", "Dry grass, bare soil"],
  ["#e8f6fb", "Cloud"],
  ["#10204a", "Water"],
];

function SwirLegend() {
  return (
    <ul className="swir-legend" aria-label="Colour key">
      {SWIR_LEGEND.map(([color, label]) => (
        <li key={label}><i style={{ background: color }} aria-hidden="true" />{label}</li>
      ))}
    </ul>
  );
}

function SceneCaption({ layer, day, scenes }: { layer: ImageryLayer; day: string; scenes: SatelliteScene[] }) {
  const first = scenes[0];
  const tiles = `${scenes.length} ${scenes.length === 1 ? "tile" : "tiles"}`;
  if (first.acquired_at === null) {
    return (
      <>
        <strong>{imageryLayers[layer].label}</strong> · {first.platform} {first.instrument} daily composite ·
        acquired <time dateTime={day}>{day}</time> (UTC day) · retrieved {formatUtc(first.retrieved_at)} · {tiles}
      </>
    );
  }
  const cloud = scenes.map((scene) => scene.cloud_cover).filter((value): value is number => value !== null);
  return (
    <>
      <strong>{imageryLayers[layer].label}</strong> · {first.platform} {first.instrument} pass at{" "}
      <time dateTime={first.acquired_at}>{formatUtc(first.acquired_at)}</time>
      {cloud.length > 0 && <> · granule cloud cover {Math.round(Math.min(...cloud))}–{Math.round(Math.max(...cloud))}%</>}
      {" "}· {tiles}
    </>
  );
}

interface SatelliteImageryProps {
  incident: IncidentDetail;
  imagery: IncidentImagery | null;
  loading: boolean;
  error: string;
  detections: Hotspot[];
}

function SatelliteImagery({ incident, imagery, loading, error, detections }: SatelliteImageryProps) {
  const scenes = useMemo(() => imagery?.scenes ?? [], [imagery]);
  const layers = useMemo(() => availableLayers(scenes), [scenes]);
  const [chosenLayer, setChosenLayer] = useState<ImageryLayer | null>(null);
  const layer = chosenLayer && layers.includes(chosenLayer) ? chosenLayer : layers[0] ?? null;
  const days = useMemo(() => (layer ? imageryDays(scenes, layer) : []), [scenes, layer]);
  const activeDays = useMemo(
    () => incident.daily.filter((entry) => entry.detections > 0).map((entry) => entry.day),
    [incident.daily],
  );
  const [chosenDay, setChosenDay] = useState<string | null>(null);
  const selected = days.find((entry) => entry.day === chosenDay) ?? defaultImageryDay(days, activeDays);
  const [showDetections, setShowDetections] = useState(true);
  const [opacity, setOpacity] = useState(1);
  const selectedDay = selected?.day ?? null;
  const dayDetections = useMemo(
    () => detections.filter((hotspot) => hotspot.observed_at.slice(0, 10) === selectedDay),
    [detections, selectedDay],
  );

  if (!imagery) {
    if (error) return <p className="detail-note" role="alert">Could not load satellite imagery: {error}.</p>;
    return <p className="detail-note" role="status">{loading ? "Loading satellite imagery…" : "Satellite imagery unavailable."}</p>;
  }
  if (!layer || !selected) {
    return (
      <p className="detail-note" role="status">
        No satellite imagery has been acquired for this incident yet. Sentinel-2 passes every 2–5 days
        and its images arrive 1–3 days later; NASA GIBS daily composites are fetched from about 12:00 UTC.
      </p>
    );
  }
  return (
    <div className="imagery">
      <div className="imagery-controls">
        <div className="segmented" role="radiogroup" aria-label="Imagery layer">
          {layers.map((item) => (
            <button key={item} type="button" role="radio" aria-checked={item === layer} title={imageryLayers[item].description} onClick={() => setChosenLayer(item)}>
              {imageryLayers[item].label}
            </button>
          ))}
        </div>
        <div className="imagery-days" role="radiogroup" aria-label="Acquisition date (UTC)">
          {days.map((entry) => {
            const timing = dayTiming(entry.day, activeDays);
            return (
              <button key={entry.day} type="button" role="radio" className={`day-${timing}`} title={DAY_TIMING_TITLES[timing]} aria-checked={entry.day === selected.day} onClick={() => setChosenDay(entry.day)}>
                {entry.day}
              </button>
            );
          })}
        </div>
      </div>
      <figure className="imagery-figure">
        <ImageryMap scenes={selected.scenes} incidentBounds={incident.bbox} detections={showDetections ? dayDetections : NO_DETECTIONS} opacity={opacity} />
        <figcaption>
          <SceneCaption layer={layer} day={selected.day} scenes={selected.scenes} />
          {" "}· <span className={`timing timing-${dayTiming(selected.day, activeDays)}`}>{DAY_TIMING_TITLES[dayTiming(selected.day, activeDays)].toLowerCase()}</span>
          <span className="detail-note">{imageryLayers[layer].description}</span>
          {layer === SENTINEL_SWIR && <SwirLegend />}
        </figcaption>
      </figure>
      <div className="imagery-options">
        <label className="layer-toggle">
          <input type="checkbox" checked={showDetections} onChange={(event) => setShowDetections(event.target.checked)} />
          Show detections from {selected.day} ({dayDetections.length.toLocaleString("en-US")})
        </label>
        <label className="opacity-control">
          Imagery opacity
          <input type="range" min={0} max={1} step={0.1} value={opacity} onChange={(event) => setOpacity(Number(event.target.value))} />
        </label>
      </div>
      <p className="detail-note">
        Dashed outline: the incident's detection bounding box. Days with fire detections are marked in orange.
        {layer === SENTINEL_SWIR
          ? " Sentinel-2 passes in the late morning, while VIIRS detects fires in the early afternoon and at night, so a fire may not be burning at the pass. A pass after the detections confirms the fire by its fresh burn scar; compare it with a pass from before. Cloud hides the ground."
          : " Clouds and smoke can hide the ground; the daily composite combines the day's passes, so its time of day is not exact."}
      </p>
    </div>
  );
}

export function IncidentReport({ incidentId }: { incidentId: string }) {
  const { incident, loading, error } = useIncidentDetail(incidentId, null);
  const imagery = useIncidentImagery(incidentId);
  const detections = useIncidentDetections(incident);

  return (
    <article className="report" aria-label="Incident report">
      <nav className="report-nav"><a href="#/">← Back to map</a></nav>
      {!incident ? (
        error
          ? <div className="map-message map-error" role="alert">Could not load incident: {error}.</div>
          : <div className="map-message" role="status">{loading ? "Loading incident…" : "Incident unavailable."}</div>
      ) : (
        <>
          <header className="report-header">
            <h2>Fire incident <code title={incident.incident_id}>{incident.incident_id.slice(0, 8)}</code></h2>
            <div className="report-badges"><StatusBadge status={incident.status} /><RiskBadge risk={incident.peak_risk} /></div>
            <p>
              Near {incidentLocation(incident)} · active {formatUtc(incident.first_observed_at)} to {formatUtc(incident.last_observed_at)} ·
              revision {incident.revision}, updated {formatUtc(incident.updated_at)}
            </p>
          </header>

          <section className="report-section" aria-labelledby="report-details">
            <h3 id="report-details">Incident details</h3>
            <dl className="report-fields">{incidentFields(incident).map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>
            {incident.merged_into && <p>Merged into incident <a href={`#/incidents/${encodeURIComponent(incident.merged_into)}`}>{incident.merged_into.slice(0, 8)}</a>.</p>}
          </section>

          <section className="report-section" aria-labelledby="report-evolution">
            <h3 id="report-evolution">Fire evolution</h3>
            <GrowthChart daily={incident.daily} width={REPORT_CHART_WIDTH} height={REPORT_CHART_HEIGHT} dataOpen />
          </section>

          <section className="report-section" aria-labelledby="report-weather">
            <h3 id="report-weather">Weather context</h3>
            <WeatherContext weather={incident.weather} />
          </section>

          <section className="report-section report-imagery" aria-labelledby="report-imagery">
            <h3 id="report-imagery">Satellite imagery</h3>
            <SatelliteImagery incident={incident} imagery={imagery.imagery} loading={imagery.loading} error={imagery.error} detections={detections.items} />
            {detections.error && <p className="detail-note" role="alert">Could not load detections: {detections.error}.</p>}
            {detections.truncated && <p className="detail-note">Only the most recent detections are drawn.</p>}
          </section>

          <footer className="report-footer">
            <p>{INCIDENT_METHOD_NOTE}</p>
            <p>
              Sources: NASA FIRMS active-fire detections (VIIRS &amp; MODIS NRT), Open-Meteo forecasts,
              NASA GIBS corrected-reflectance imagery, and Copernicus Sentinel-2 imagery via Microsoft
              Planetary Computer. Detections are not fire perimeters.
            </p>
          </footer>
        </>
      )}
    </article>
  );
}
