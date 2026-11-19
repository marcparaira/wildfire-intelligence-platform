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
import { Stroke, Style } from "ol/style";
import "ol/ol.css";
import { frpStyles } from "./frp";
import type { Bounds, Hotspot } from "./hotspots";

interface MapViewProps {
  hotspots: Hotspot[];
  bounds: Bounds;
  areaBounds: Bounds | null;
  selectedId: number | null;
  onSelect: (id: number) => void;
}

export default function MapView({ hotspots, bounds, areaBounds, selectedId, onSelect }: MapViewProps) {
  const target = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const sourceRef = useRef(new VectorSource());
  const areaSourceRef = useRef(new VectorSource());
  const selectRef = useRef(onSelect);
  selectRef.current = onSelect;

  useEffect(() => {
    if (!target.current) return;
    const hotspotLayer = new VectorLayer({ source: sourceRef.current });
    const map = new Map({
      target: target.current,
      layers: [
        new TileLayer({ source: new OSM() }),
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
