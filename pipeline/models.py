"""
Normalised place record — the interface between the source adapter and all
downstream pipeline stages (SR-P-07, SR-P-08).

No source-specific fields belong here. Wikidata QIDs, SPARQL result keys, and
other source concepts are translated to this schema inside the adapter.
"""

from dataclasses import dataclass, field


@dataclass
class PlaceRecord:
    # Required fields (SR-P-08)
    id: str           # Unique identifier, e.g. "Q12345"
    name: str
    category: str     # Controlled vocabulary key matching config/categories.yml
    lon: float
    lat: float

    # Optional fields (SR-P-08)
    inception: int | None = None       # Integer year; negative = BC (SR-P-13)
    image_url: str | None = None       # Wikimedia Commons URL
    description: str | None = None     # Short English-language summary
    wikipedia_url: str | None = None   # Full en.wikipedia.org article URL

    # Populated during enrichment
    events: list[dict] = field(default_factory=list)
