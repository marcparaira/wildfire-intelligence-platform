import { describe, expect, it } from "vitest";
import { availableLayers, dayTiming, defaultImageryDay, imageryDays, parseIncidentImagery, sceneImageUrl } from "./imagery";
import { incidentReportHref, reportIncidentId } from "./incidents";
import { satelliteScene, SWIR } from "./test-fixtures";

describe("incident imagery", () => {
  it("groups scenes of one layer by acquisition date, oldest first", () => {
    const scenes = [
      satelliteScene({ scene_id: "c", acquisition_date: "2026-03-05" }),
      satelliteScene({ scene_id: "a", acquisition_date: "2026-03-04" }),
      satelliteScene({ scene_id: "d", acquisition_date: "2026-03-05", bbox: [114.5, 0.5, 115, 1] }),
    ];
    expect(imageryDays(scenes, SWIR).map((day) => [day.day, day.scenes.map((scene) => scene.scene_id)])).toEqual([
      ["2026-03-04", ["a"]],
      ["2026-03-05", ["c", "d"]],
    ]);
    expect(availableLayers(scenes)).toEqual([SWIR]);
    expect(availableLayers([])).toEqual([]);
  });

  it("validates the response and drops scenes of unknown layers", () => {
    const response = {
      incident_id: "i",
      first_day: "2026-03-03",
      last_day: "2026-03-05",
      scenes: [satelliteScene(), { ...satelliteScene(), layer: "VIIRS_NOAA20_CorrectedReflectance_TrueColor" }, { scene_id: 3 }],
    };
    expect(parseIncidentImagery(response)?.scenes).toEqual([satelliteScene()]);
    expect(parseIncidentImagery({ ...response, scenes: null })).toBeNull();
    // Older responses lack the pass fields; malformed ones are dropped.
    const older: Record<string, unknown> = { ...satelliteScene({ scene_id: "older" }) };
    delete older.acquired_at;
    delete older.cloud_cover;
    const malformed = { ...satelliteScene(), cloud_cover: "low" };
    expect(parseIncidentImagery({ ...response, scenes: [older, satelliteScene(), malformed] })?.scenes).toEqual([
      satelliteScene({ scene_id: "older", acquired_at: null, cloud_cover: null }),
      satelliteScene(),
    ]);
    expect(parseIncidentImagery("nope")).toBeNull();
  });

  it("places days relative to the detections and picks the most telling one first", () => {
    const active = ["2026-03-04", "2026-03-06"];
    expect(["2026-03-02", "2026-03-04", "2026-03-05", "2026-03-08"].map((day) => dayTiming(day, active))).toEqual([
      "before",
      "active",
      "between",
      "after",
    ]);
    const days = (...dates: string[]) => dates.map((day) => ({ day, scenes: [satelliteScene({ acquisition_date: day })] }));
    expect(defaultImageryDay(days("2026-03-02", "2026-03-04", "2026-03-09"), active)?.day).toBe("2026-03-04");
    expect(defaultImageryDay(days("2026-03-02", "2026-03-08", "2026-03-10"), active)?.day).toBe("2026-03-08");
    expect(defaultImageryDay(days("2026-03-01", "2026-03-02"), active)?.day).toBe("2026-03-02");
    expect(defaultImageryDay([], active)).toBeNull();
  });

  it("serves scene images through the API", () => {
    expect(sceneImageUrl({ scene_id: "1a2b" })).toBe("/api/imagery/1a2b");
  });

  it("round-trips report links through the hash router", () => {
    const href = incidentReportHref("0b6f3a52-1c1e");
    expect(href).toBe("#/incidents/0b6f3a52-1c1e");
    expect(reportIncidentId(href)).toBe("0b6f3a52-1c1e");
    expect(reportIncidentId("#/status")).toBeNull();
    expect(reportIncidentId("#/incidents/")).toBeNull();
    expect(reportIncidentId("#/incidents/a/b")).toBeNull();
    expect(reportIncidentId("#/incidents/%E0%A4%A")).toBeNull();
  });
});
