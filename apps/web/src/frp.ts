import { Circle, Fill, Stroke, Style } from "ol/style";

export const FRP_MAX_MW = 100;
const LOW_COLOR = [255, 239, 166];
const HIGH_COLOR = [167, 44, 28];
export const FRP_LOW_COLOR = `rgb(${LOW_COLOR.join(", ")})`;
export const FRP_HIGH_COLOR = `rgb(${HIGH_COLOR.join(", ")})`;
export const FRP_UNAVAILABLE_COLOR = "#66747b";

export function parseFrp(value: string | undefined): number | null {
  const text = value?.trim();
  if (!text || !/^\+?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/i.test(text)) return null;
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
}

export function frpPosition(value: number): number {
  return Math.log1p(Math.min(value, FRP_MAX_MW)) / Math.log1p(FRP_MAX_MW);
}

export function frpMarker(value: string | undefined): { radius: number; color: string } {
  const frp = parseFrp(value);
  if (frp === null) return { radius: 4, color: FRP_UNAVAILABLE_COLOR };
  const position = frpPosition(frp);
  const color = LOW_COLOR.map((channel, index) => Math.round(channel + (HIGH_COLOR[index] - channel) * position));
  return { radius: 4 + 9 * position, color: `rgb(${color.join(", ")})` };
}

export function frpStyles(value: string | undefined, selected: boolean): Style | Style[] {
  const { radius, color } = frpMarker(value);
  const marker = new Style({
    image: new Circle({
      radius,
      fill: new Fill({ color }),
      stroke: new Stroke({ color: "#514226", width: 1.5 }),
    }),
    zIndex: selected ? 2 : 1,
  });
  if (!selected) return marker;
  return [
    new Style({
      image: new Circle({ radius: radius + 3, stroke: new Stroke({ color: "#fff", width: 4 }) }),
      zIndex: 2,
    }),
    new Style({
      image: new Circle({ radius: radius + 3, stroke: new Stroke({ color: "#202b26", width: 1.5 }) }),
      zIndex: 2,
    }),
    marker,
  ];
}
