import { useEffect, useRef } from "react";
import Feature from "ol/Feature";
import Map from "ol/Map";
import View from "ol/View";
import Point from "ol/geom/Point";
import TileLayer from "ol/layer/Tile";
import VectorLayer from "ol/layer/Vector";
import { fromLonLat, transformExtent } from "ol/proj";
import OSM from "ol/source/OSM";
import VectorSource from "ol/source/Vector";
import { Circle, Fill, Stroke, Style } from "ol/style";
import "ol/ol.css";
import type { Bounds, Hotspot } from "./hotspots";

interface MapViewProps {
  hotspots: Hotspot[];
  bounds: Bounds;
  selectedId: number | null;
  onSelect: (id: number) => void;
}

const markerStyle = new Style({
  image: new Circle({
    radius: 5,
    fill: new Fill({ color: "#e76c38" }),
    stroke: new Stroke({ color: "#fff8ef", width: 1.5 }),
  }),
});

const selectedStyle = new Style({
  image: new Circle({
    radius: 8,
    fill: new Fill({ color: "#ad3d1c" }),
    stroke: new Stroke({ color: "#fff8ef", width: 3 }),
  }),
});

export default function MapView({ hotspots, bounds, selectedId, onSelect }: MapViewProps) {
  const target = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const sourceRef = useRef(new VectorSource());
  const selectRef = useRef(onSelect);
  selectRef.current = onSelect;

  useEffect(() => {
    if (!target.current) return;
    const map = new Map({
      target: target.current,
      layers: [
        new TileLayer({ source: new OSM() }),
        new VectorLayer({ source: sourceRef.current, style: markerStyle }),
      ],
      view: new View({ center: fromLonLat([114, 1.5]), zoom: 5 }),
    });
    map.on("singleclick", (event) => {
      const feature = map.forEachFeatureAtPixel(event.pixel, (item) => item, {
        hitTolerance: 6,
      });
      const id = feature?.getId();
      if (typeof id === "number") selectRef.current(id);
    });
    map.on("pointermove", (event) => {
      const element = map.getTargetElement();
      if (!event.dragging && element) {
        element.style.cursor = map.hasFeatureAtPixel(event.pixel, {
          hitTolerance: 6,
        })
          ? "pointer"
          : "";
      }
    });
    mapRef.current = map;
    return () => {
      map.setTarget(undefined);
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const source = sourceRef.current;
    source.clear();
    source.addFeatures(
      hotspots.map((hotspot) => {
        const feature = new Feature({
          geometry: new Point(fromLonLat([hotspot.longitude, hotspot.latitude])),
        });
        feature.setId(hotspot.id);
        feature.setStyle(hotspot.id === selectedId ? selectedStyle : markerStyle);
        return feature;
      }),
    );
  }, [hotspots, selectedId]);

  useEffect(() => {
    mapRef.current?.getView().fit(transformExtent(bounds, "EPSG:4326", "EPSG:3857"), {
      padding: [40, 40, 40, 40],
      maxZoom: 7,
      duration: 300,
    });
  }, [bounds]);

  return <div ref={target} className="map" role="region" aria-label="Hotspot map" tabIndex={0} />;
}
