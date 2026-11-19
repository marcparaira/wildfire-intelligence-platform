import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import App from "./App";

describe("App", () => {
  it("introduces the platform foundation and its services", () => {
    render(<App />);

    expect(
      screen.getByRole("heading", { name: "Situational awareness starts here." }),
    ).toBeInTheDocument();
    expect(screen.getByText("PostGIS + Kafka")).toBeInTheDocument();
  });
});
