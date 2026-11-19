import { useEffect, useRef } from "react";
import Feature from "ol/Feature";
import Map from "ol/Map";
import View from "ol/View";
import Point from "ol/geom/Point";
import { fromExtent } from "ol/geom/Polygon";
import TileLayer from "ol/layer/Tile";
import VectorLayer from "ol/layer/Vector";
import { fromLonLat, transformExtent } from "ol/proj";
import OSM from "ol/source/OSM";
import VectorSource from "ol/source/Vector";
import { Fill, Stroke, Style, Text } from "ol/style";
import "ol/ol.css";
import { frpStyles } from "./frp";
import type { Bounds, Hotspot } from "./hotspots";
import { weatherColor, weatherValue } from "./weather";
import type { WeatherMetric, WeatherPoint } from "./weather";

interface MapViewProps {
  hotspots: Hotspot[];
  weatherPoints: WeatherPoint[];
  weatherMetric: WeatherMetric;
  bounds: Bounds;
  areaBounds: Bounds | null;
  selectedId: number | null;
  onSelect: (id: number) => void;
}

export default function MapView({
  hotspots,
  weatherPoints,
  weatherMetric,
  bounds,
  areaBounds,
  selectedId,
  onSelect,
}: MapViewProps) {
  const target = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const sourceRef = useRef(new VectorSource());
  const weatherSourceRef = useRef(new VectorSource());
  const areaSourceRef = useRef(new VectorSource());
  const selectRef = useRef(onSelect);
  selectRef.current = onSelect;

  useEffect(() => {
    if (!target.current) return;
    const hotspotLayer = new VectorLayer({ source: sourceRef.current });
    const weatherLayer = new VectorLayer({ source: weatherSourceRef.current });
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
      if (typeof id === "number") selectRef.current(id);
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
        geometry: fromExtent(
          transformExtent(
            [point.west, point.south, point.east, point.north],
            "EPSG:4326",
            "EPSG:3857",
          ),
        ),
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
        feature.setId(hotspot.id);
        feature.setStyle(frpStyles(hotspot.attributes.frp, hotspot.id === selectedId));
        return feature;
      }),
    );
  }, [hotspots, selectedId]);

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

  return <div ref={target} className="map" role="region" aria-label="Hotspot map" tabIndex={0} />;
}
