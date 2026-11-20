import { enrichmentLayerLabels, enrichmentLayerDescriptions } from "./enrichmentLayers";
import type { EnrichmentLayer } from "./enrichmentLayers";

export interface EnrichmentLayersProps {
  shown: Set<EnrichmentLayer>;
  onChange: (layers: Set<EnrichmentLayer>) => void;
}

export function EnrichmentLayers({ shown, onChange }: EnrichmentLayersProps) {
  const layers: EnrichmentLayer[] = ["risk", "weather"];

  function toggleLayer(layer: EnrichmentLayer) {
    const updated = new Set(shown);
    if (updated.has(layer)) {
      updated.delete(layer);
    } else {
      updated.add(layer);
    }
    onChange(updated);
  }

  return (
    <section className="enrichment-layers" aria-label="Enrichment data layers">
      <div className="panel-heading">
        <h2>Enrichment layers</h2>
      </div>
      <div className="layer-toggles">
        {layers.map((layer) => (
          <label key={layer} className="layer-toggle" title={enrichmentLayerDescriptions[layer]}>
            <input type="checkbox" checked={shown.has(layer)} onChange={() => toggleLayer(layer)} />
            {enrichmentLayerLabels[layer]}
          </label>
        ))}
      </div>
    </section>
  );
}
