import { useEffect, useRef } from "react";
import Feature from "ol/Feature";
import Map from "ol/Map";
import View from "ol/View";
import Point from "ol/geom/Point";
import Polygon, { fromExtent } from "ol/geom/Polygon";
import TileLayer from "ol/layer/Tile";
import VectorLayer from "ol/layer/Vector";
import { fromLonLat, transformExtent } from "ol/proj";
import OSM from "ol/source/OSM";
import VectorSource from "ol/source/Vector";
import { Circle, Fill, Stroke, Style, Text } from "ol/style";
import type { FeatureLike } from "ol/Feature";
import "ol/ol.css";
import { frpStyles } from "./frp";
import type { Bounds, Hotspot } from "./hotspots";
import { incidentMarkerRadius, incidentStatuses } from "./incidents";
import type { IncidentSummary } from "./incidents";
import { weatherColor, weatherValue } from "./weather";
import type { WeatherMetric, WeatherPoint } from "./weather";

interface MapViewProps {
  hotspots: Hotspot[];
  weatherPoints: WeatherPoint[];
  weatherMetric: WeatherMetric;
  bounds: Bounds;
  areaBounds: Bounds | null;
  selectedId: string | null;
  onSelect: (eventId: string) => void;
  incidents?: IncidentSummary[];
  selectedIncidentId?: string | null;
  onSelectIncident?: (incidentId: string) => void;
  /** Zooms to these bounds whenever the value changes (for example a selected incident). */
  focusBounds?: Bounds | null;
}

/** Incident outlines appear from about zoom 8, where they are larger than the marker. */
const OUTLINE_MAX_RESOLUTION = 700;
function withAlpha(hex: string, alpha: number): string {
  const channels = [1, 3, 5].map((start) => Number.parseInt(hex.slice(start, start + 2), 16));
  return `rgba(${channels.join(",")},${alpha})`;
}

function incidentStyle(feature: FeatureLike, resolution: number): Style | Style[] | undefined {
  const incident = feature.get("incident") as IncidentSummary;
  const selected = feature.get("selected") === true;
  const color = incidentStatuses[incident.status].color;
  if (feature.get("kind") === "outline") {
    if (resolution > OUTLINE_MAX_RESOLUTION) return undefined;
    return new Style({
      stroke: new Stroke({ color, width: selected ? 2.5 : 1.5, lineDash: [4, 3] }),
      fill: new Fill({ color: withAlpha(color, 0.06) }),
    });
  }
  const radius = incidentMarkerRadius(incident.footprint_km2);
  const marker = new Style({
    image: new Circle({
      radius,
      fill: new Fill({ color: withAlpha(color, 0.22) }),
      stroke: new Stroke({ color, width: selected ? 3.5 : 2.5 }),
    }),
    zIndex: selected ? 2 : 1,
  });
  if (!selected) return marker;
  return [
    new Style({ image: new Circle({ radius: radius + 3, stroke: new Stroke({ color: "#fff", width: 4 }) }), zIndex: 2 }),
    marker,
  ];
}

export default function MapView({
  hotspots,
  weatherPoints,
  weatherMetric,
  bounds,
  areaBounds,
  selectedId,
  onSelect,
  incidents = [],
  selectedIncidentId = null,
  onSelectIncident,
  focusBounds = null,
}: MapViewProps) {
  const target = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const sourceRef = useRef(new VectorSource());
  const weatherSourceRef = useRef(new VectorSource());
  const areaSourceRef = useRef(new VectorSource());
  const incidentSourceRef = useRef(new VectorSource());
  const selectRef = useRef(onSelect);
  selectRef.current = onSelect;
  const selectIncidentRef = useRef(onSelectIncident);
  selectIncidentRef.current = onSelectIncident;

  useEffect(() => {
    if (!target.current) return;
    const hotspotLayer = new VectorLayer({ source: sourceRef.current });
    const weatherLayer = new VectorLayer({ source: weatherSourceRef.current });
    const incidentLayer = new VectorLayer({ source: incidentSourceRef.current, style: incidentStyle });
    const map = new Map({
      target: target.current,
      layers: [
        new TileLayer({ source: new OSM() }),
        weatherLayer,
        new VectorLayer({
          source: areaSourceRef.current,
          style: [
            new Style({ stroke: new Stroke({ color: "#fff", width: 4, lineDash: [8, 5] }) }),
            new Style({ stroke: new Stroke({ color: "#315f7b", width: 2, lineDash: [8, 5] }) }),
          ],
        }),
        incidentLayer,
        hotspotLayer,
      ],
      view: new View({ center: fromLonLat([114, 1.5]), zoom: 5 }),
    });
    map.on("singleclick", (event) => {
      const feature = map.forEachFeatureAtPixel(event.pixel, (item) => item, {
        hitTolerance: 6,
        layerFilter: (layer) => layer === hotspotLayer,
      });
      const id = feature?.getId();
      if (typeof id === "string") {
        selectRef.current(id);
        return;
      }
      // Detections take precedence: an incident marker sits on top of its own detections.
      const incident = map.forEachFeatureAtPixel(event.pixel, (item) => item, {
        hitTolerance: 2,
        layerFilter: (layer) => layer === incidentLayer,
      });
      const incidentId = incident?.get("incidentId") as unknown;
      if (typeof incidentId === "string") selectIncidentRef.current?.(incidentId);
    });
    mapRef.current = map;
    return () => {
      map.setTarget(undefined);
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const source = weatherSourceRef.current;
    source.clear();
    if (!weatherMetric) return;

    for (const point of weatherPoints) {
      const value = weatherValue(point, weatherMetric);
      if (value === null) continue;
      const feature = new Feature({
        geometry: new Polygon([point.boundary.map((coordinate) => fromLonLat(coordinate))]),
      });
      feature.setStyle(
        new Style({
          fill: new Fill({ color: weatherColor(value, weatherMetric) }),
          stroke: new Stroke({ color: "rgba(255,255,255,0.12)", width: 0.5 }),
        }),
      );
      source.addFeature(feature);
      if (weatherMetric === "wind" && point.wind_direction_degrees !== null) {
        const direction = new Feature({
          geometry: new Point(fromLonLat([point.longitude, point.latitude])),
        });
        direction.setStyle(
          new Style({
            text: new Text({
              text: "↑",
              font: "bold 14px sans-serif",
              fill: new Fill({ color: "#17352d" }),
              stroke: new Stroke({ color: "#fff", width: 3 }),
              rotation: ((point.wind_direction_degrees + 180) * Math.PI) / 180,
              rotateWithView: true,
            }),
          }),
        );
        source.addFeature(direction);
      }
    }
  }, [weatherPoints, weatherMetric]);

  useEffect(() => {
    const source = sourceRef.current;
    source.clear();
    source.addFeatures(
      hotspots.map((hotspot) => {
        const feature = new Feature({
          geometry: new Point(fromLonLat([hotspot.longitude, hotspot.latitude])),
        });
        feature.setId(hotspot.event_id);
        feature.setStyle(frpStyles(hotspot.attributes.frp, hotspot.event_id === selectedId));
        return feature;
      }),
    );
  }, [hotspots, selectedId]);

  useEffect(() => {
    const source = incidentSourceRef.current;
    source.clear();
    for (const incident of incidents) {
      const selected = incident.incident_id === selectedIncidentId;
      const marker = new Feature({
        geometry: new Point(fromLonLat([incident.longitude, incident.latitude])),
        kind: "marker",
        incident,
        incidentId: incident.incident_id,
        selected,
      });
      const outline = new Feature({
        geometry: fromExtent(transformExtent(incident.bbox, "EPSG:4326", "EPSG:3857")),
        kind: "outline",
        incident,
        incidentId: incident.incident_id,
        selected,
      });
      source.addFeatures([outline, marker]);
    }
  }, [incidents, selectedIncidentId]);

  useEffect(() => {
    const source = areaSourceRef.current;
    source.clear();
    if (areaBounds) {
      source.addFeature(new Feature(fromExtent(transformExtent(areaBounds, "EPSG:4326", "EPSG:3857"))));
    }
  }, [areaBounds]);

  useEffect(() => {
    mapRef.current?.getView().fit(transformExtent(bounds, "EPSG:4326", "EPSG:3857"), {
      padding: [40, 40, 40, 40],
      maxZoom: 7,
      duration: 300,
    });
  }, [bounds]);

  const focusKey = focusBounds?.join(",") ?? "";
  useEffect(() => {
    if (!focusKey) return;
    const extent = focusKey.split(",").map(Number) as Bounds;
    mapRef.current?.getView().fit(transformExtent(extent, "EPSG:4326", "EPSG:3857"), {
      padding: [60, 60, 60, 60],
      maxZoom: 11,
      duration: 300,
    });
  }, [focusKey]);

  return <div ref={target} className="map" role="region" aria-label="Hotspot map" tabIndex={0} />;
}
