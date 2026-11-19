import { Circle } from "ol/style";
import { describe, expect, it } from "vitest";
import { FRP_HIGH_COLOR, FRP_LOW_COLOR, FRP_UNAVAILABLE_COLOR, frpMarker, frpStyles, parseFrp } from "./frp";

describe("FRP visualization", () => {
  it("uses a fixed logarithmic size and color scale with a visual cap", () => {
    const zero = frpMarker("0");
    const ten = frpMarker("10");
    const hundred = frpMarker("100");
    expect(zero).toEqual({ radius: 4, color: FRP_LOW_COLOR });
    expect(ten.radius).toBeCloseTo(8.6762, 3);
    expect(hundred).toEqual({ radius: 13, color: FRP_HIGH_COLOR });
    expect(ten.color).not.toBe(zero.color);
    expect(ten.color).not.toBe(hundred.color);
    expect(frpMarker("205.16")).toEqual(hundred);
  });

  it("distinguishes zero from missing, invalid, negative and non-finite values", () => {
    expect(parseFrp(" 0 ")).toBe(0);
    for (const value of [undefined, "", " ", "-1", "NaN", "Infinity", "1e309", "0x10", "10 MW"]) {
      expect(parseFrp(value)).toBeNull();
      expect(frpMarker(value)).toEqual({ radius: 4, color: FRP_UNAVAILABLE_COLOR });
    }
  });

  it("highlights a selection without changing its FRP color or size", () => {
    const normal = frpStyles("12.5", false);
    const selected = frpStyles("12.5", true);
    expect(Array.isArray(normal)).toBe(false);
    expect(Array.isArray(selected)).toBe(true);
    if (Array.isArray(normal) || !Array.isArray(selected)) throw new Error("Unexpected style shape");
    const normalCircle = normal.getImage();
    const selectedCircle = selected[selected.length - 1].getImage();
    expect(normalCircle).toBeInstanceOf(Circle);
    expect(selectedCircle).toBeInstanceOf(Circle);
    if (!(normalCircle instanceof Circle) || !(selectedCircle instanceof Circle)) throw new Error("Expected circle markers");
    expect(selectedCircle.getRadius()).toBe(normalCircle.getRadius());
    expect(selectedCircle.getFill()?.getColor()).toEqual(normalCircle.getFill()?.getColor());
    expect(selected.length).toBeGreaterThan(1);
    expect(selected[selected.length - 1].getZIndex()).toBeGreaterThan(normal.getZIndex() ?? 0);
  });
});
