import { useEffect, useRef } from "react";
import Feature from "ol/Feature";
import Map from "ol/Map";
import View from "ol/View";
import Point from "ol/geom/Point";
import { fromExtent } from "ol/geom/Polygon";
import LayerGroup from "ol/layer/Group";
import ImageLayer from "ol/layer/Image";
import TileLayer from "ol/layer/Tile";
import VectorLayer from "ol/layer/Vector";
import { fromLonLat, transformExtent } from "ol/proj";
import ImageStatic from "ol/source/ImageStatic";
import OSM from "ol/source/OSM";
import VectorSource from "ol/source/Vector";
import { Stroke, Style } from "ol/style";
import "ol/ol.css";
import { frpStyles } from "./frp";
import type { Bounds, Hotspot } from "./hotspots";
import { sceneImageUrl } from "./imagery";
import type { SatelliteScene } from "./imagery";

interface ImageryMapProps {
  /** Scenes shown together, e.g. every tile of one layer on one day. */
  scenes: SatelliteScene[];
  incidentBounds: Bounds;
  detections: Hotspot[];
  /** Opacity of the imagery over the base map, 0–1. */
  opacity?: number;
}

const ATTRIBUTIONS: Record<string, string> = {
  "nasa-gibs":
    'Imagery: <a href="https://earthdata.nasa.gov/gibs" target="_blank" rel="noreferrer">NASA EOSDIS GIBS</a>',
  "planetary-computer":
    'Contains modified Copernicus Sentinel data, via <a href="https://planetarycomputer.microsoft.com" target="_blank" rel="noreferrer">Microsoft Planetary Computer</a>',
};
// Show some of the surroundings, but stay close enough to see the incident; small
// incidents get close enough for Sentinel-2's 20 m pixels.
const FIT_PADDING = [48, 48, 48, 48];
const FIT_MAX_ZOOM = 13;
const OUTLINE_STYLE = [
  new Style({ stroke: new Stroke({ color: "#fff", width: 4 }) }),
  new Style({ stroke: new Stroke({ color: "#b91c1c", width: 2, lineDash: [6, 4] }) }),
];

/** A map of satellite scenes (reprojected from geographic coordinates) with the incident on top. */
export default function ImageryMap({ scenes, incidentBounds, detections, opacity = 1 }: ImageryMapProps) {
  const target = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const imageryRef = useRef(new LayerGroup());
  const outlineSourceRef = useRef(new VectorSource());
  const detectionSourceRef = useRef(new VectorSource());

  useEffect(() => {
    if (!target.current) return;
    const map = new Map({
      target: target.current,
      layers: [
        new TileLayer({ source: new OSM() }),
        imageryRef.current,
        new VectorLayer({ source: outlineSourceRef.current, style: OUTLINE_STYLE }),
        new VectorLayer({ source: detectionSourceRef.current }),
      ],
      view: new View({ center: fromLonLat([114, 1.5]), zoom: 8 }),
    });
    mapRef.current = map;
    return () => {
      map.setTarget(undefined);
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const layers = scenes.map(
      (scene) =>
        new ImageLayer({
          source: new ImageStatic({
            url: sceneImageUrl(scene),
            imageExtent: scene.bbox,
            projection: "EPSG:4326",
            attributions: ATTRIBUTIONS[scene.provider] ?? ATTRIBUTIONS["nasa-gibs"],
          }),
        }),
    );
    const group = imageryRef.current.getLayers();
    group.clear();
    group.extend(layers);
  }, [scenes]);

  useEffect(() => {
    imageryRef.current.setOpacity(opacity);
  }, [opacity]);

  const boundsKey = incidentBounds.join(",");
  useEffect(() => {
    const bounds = boundsKey.split(",").map(Number) as Bounds;
    const extent = transformExtent(bounds, "EPSG:4326", "EPSG:3857");
    outlineSourceRef.current.clear();
    outlineSourceRef.current.addFeature(new Feature(fromExtent(extent)));
    mapRef.current?.getView().fit(extent, { padding: FIT_PADDING, maxZoom: FIT_MAX_ZOOM });
  }, [boundsKey]);

  useEffect(() => {
    const source = detectionSourceRef.current;
    source.clear();
    source.addFeatures(
      detections.map((hotspot) => {
        const feature = new Feature({ geometry: new Point(fromLonLat([hotspot.longitude, hotspot.latitude])) });
        feature.setId(hotspot.event_id);
        feature.setStyle(frpStyles(hotspot.attributes.frp, false));
        return feature;
      }),
    );
  }, [detections]);

  return <div ref={target} className="imagery-map" role="region" aria-label="Satellite imagery map" tabIndex={0} />;
}
