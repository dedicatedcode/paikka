#!/usr/bin/env python3
"""Generate continent polygon and ISO->continent artifacts.

Continent outlines come from Natural Earth; the ISO 3166-1 mapping comes from the
complete ISO-3166-Countries-with-Regional-Codes dataset (plus a few overrides for
territories that are not in the official ISO list but appear in OSM).

Outputs (into the given output directory):
  continents/<CODE>.wkb   simplified continent outline as JTS-compatible WKB
  iso-continents.csv      ISO 3166-1 alpha-2 code -> continent code
  continents.geojson      human readable copy of the generated outlines

Requires: pip install shapely
"""

import argparse
import json
import pathlib
import sys
import urllib.request

SOURCE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
    "geojson/ne_110m_admin_0_countries.geojson"
)
ISO_SOURCE_URL = (
    "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/"
    "master/all/all.json"
)

# OSM uses some codes that are not in the official ISO list (or have no UN region).
ISO_OVERRIDES = {
    "AQ": "AN",
    "TW": "AS",
    "XK": "EU",
}

CONTINENT_CODES = {
    "Europe": "EU",
    "Asia": "AS",
    "Africa": "AF",
    "North America": "NA",
    "South America": "SA",
    "Oceania": "OC",
    "Antarctica": "AN",
}

# Clip boxes keep overseas territories from stretching a continent thumbnail across the globe.
CONTINENT_BOUNDS = {
    "EU": (-25, 34, 60, 72),
    "AS": (25, -12, 180, 56),
    "AF": (-20, -36, 55, 38),
    "NA": (-170, 5, -50, 84),
    "SA": (-82, -56, -34, 13),
    "OC": (110, -48, 180, 0),
    "AN": (-180, -90, 180, -60),
}


def load_source(source: str) -> dict:
    path = pathlib.Path(source)
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    print(f"Downloading {source} ...", file=sys.stderr)
    with urllib.request.urlopen(source) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="dist/continents", help="output directory")
    parser.add_argument("--source", default=SOURCE_URL, help="Natural Earth countries GeoJSON")
    parser.add_argument("--iso-source", default=ISO_SOURCE_URL, help="ISO 3166 regional codes JSON")
    parser.add_argument("--simplify", type=float, default=0.05, help="simplification tolerance in degrees")
    args = parser.parse_args()

    try:
        from shapely.geometry import box, mapping, shape
        from shapely.ops import unary_union
        from shapely import wkb as shapely_wkb
    except ImportError:
        print("shapely is required: pip install shapely", file=sys.stderr)
        return 1

    data = load_source(args.source)
    iso_data = load_source(args.iso_source)
    output = pathlib.Path(args.output)
    (output / "continents").mkdir(parents=True, exist_ok=True)

    geometries: dict[str, list] = {}
    for feature in data.get("features", []):
        properties = feature.get("properties", {})
        continent = properties.get("CONTINENT")
        code = CONTINENT_CODES.get(continent)
        if code is None:
            continue
        geometry = shape(feature["geometry"]).intersection(box(*CONTINENT_BOUNDS[code]))
        if not geometry.is_empty:
            geometries.setdefault(code, []).append(geometry)

    iso_mapping: dict[str, str] = {}
    for entry in iso_data:
        iso = (entry.get("alpha-2") or "").upper()
        if len(iso) != 2:
            continue
        if iso in ISO_OVERRIDES:
            iso_mapping[iso] = ISO_OVERRIDES[iso]
            continue
        region = entry.get("region")
        subregion = entry.get("sub-region") or ""
        intermediate = entry.get("intermediate-region") or ""
        if region == "Africa":
            iso_mapping[iso] = "AF"
        elif region == "Americas":
            iso_mapping[iso] = "SA" if (subregion == "South America" or intermediate == "South America") else "NA"
        elif region == "Asia":
            iso_mapping[iso] = "AS"
        elif region == "Europe":
            iso_mapping[iso] = "EU"
        elif region == "Oceania":
            iso_mapping[iso] = "OC"
        elif region == "Antarctica":
            iso_mapping[iso] = "AN"
    for iso, code in ISO_OVERRIDES.items():
        iso_mapping.setdefault(iso, code)

    geo_features = []
    for code in CONTINENT_CODES.values():
        parts = geometries.get(code)
        if not parts:
            continue
        merged = unary_union(parts).simplify(args.simplify, preserve_topology=True)
        if merged.is_empty:
            continue
        wkb_path = output / "continents" / f"{code}.wkb"
        wkb_path.write_bytes(shapely_wkb.dumps(merged))
        geo_features.append({
            "type": "Feature",
            "properties": {"continent": code},
            "geometry": mapping(merged),
        })
        print(f"{code}: {merged.geom_type}, {wkb_path.stat().st_size} bytes")

    (output / "continents.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": geo_features}, separators=(",", ":")),
        encoding="utf-8",
    )

    with (output / "iso-continents.csv").open("w", encoding="utf-8") as handle:
        for iso in sorted(iso_mapping):
            handle.write(f"{iso},{iso_mapping[iso]}\n")
    print(f"ISO mappings: {len(iso_mapping)}")
    print(f"Output: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
