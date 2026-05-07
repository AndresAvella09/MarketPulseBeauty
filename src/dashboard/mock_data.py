"""Mock data fixtures translated from the design's data.jsx.

Used as a fallback when real gold-layer columns are missing so every page in
the dashboard renders without errors. Anything sourced from this module should
be visibly labelled `mock` in the UI when surfaced.
"""
from __future__ import annotations

import math
import random
from typing import Sequence

from src.dashboard import theme as th


BRANDS: list[str] = [
    "Element Eight", "Lumia", "Verde Skin", "Atelier Roma", "Marisol",
    "North & Bare", "Hazelwood", "Ocho Botánica", "Pequeña Luna", "Field Notes",
]

FAMILIES: list[dict] = [
    {"k": "niacinamida", "label": "niacinamida", "color": th.C2_TEAL},
    {"k": "acido_hialuronico", "label": "ácido hialurónico", "color": th.C1_CORAL},
    {"k": "shampoo_sin_sulfatos", "label": "shampoo sin sulfatos", "color": th.C3_LAVENDER},
    {"k": "retinol", "label": "retinol", "color": th.C4_AMBER},
    {"k": "vitamina_c", "label": "vitamina c", "color": th.C5_SKY},
]

PRODUCTS: list[dict] = [
    {"brand": "Element Eight", "name": "Quiet Hours Niacinamide 10%", "cat": "Serum", "fam": "niacinamida", "score": 94, "rating": 4.7, "reviews": 3214},
    {"brand": "Lumia", "name": "Daydew Hyaluronic Mist", "cat": "Mist", "fam": "acido_hialuronico", "score": 91, "rating": 4.6, "reviews": 1820},
    {"brand": "Verde Skin", "name": "Soft Field Repair Cream", "cat": "Moisturizer", "fam": "niacinamida", "score": 89, "rating": 4.5, "reviews": 2710},
    {"brand": "Atelier Roma", "name": "Notte Retinol 0.3", "cat": "Serum", "fam": "retinol", "score": 87, "rating": 4.4, "reviews": 1455},
    {"brand": "Marisol", "name": "Salt-Free Wave Shampoo", "cat": "Shampoo", "fam": "shampoo_sin_sulfatos", "score": 85, "rating": 4.4, "reviews": 4012},
    {"brand": "North & Bare", "name": "Glassroom Vitamin C 15%", "cat": "Serum", "fam": "vitamina_c", "score": 83, "rating": 4.3, "reviews": 902},
    {"brand": "Hazelwood", "name": "Velvet Cloud Cleanser", "cat": "Cleanser", "fam": "acido_hialuronico", "score": 82, "rating": 4.3, "reviews": 1180},
    {"brand": "Ocho Botánica", "name": "Ocho Glow Serum", "cat": "Serum", "fam": "niacinamida", "score": 80, "rating": 4.2, "reviews": 638},
    {"brand": "Pequeña Luna", "name": "Lunita Sleep Mask", "cat": "Mask", "fam": "retinol", "score": 78, "rating": 4.2, "reviews": 421},
    {"brand": "Field Notes", "name": "No.7 Hydrating Toner", "cat": "Toner", "fam": "acido_hialuronico", "score": 77, "rating": 4.1, "reviews": 1564},
]

BOTTOM_PRODUCTS: list[dict] = [
    {"brand": "Lumia", "name": "Brightening Spot Patches", "cat": "Patches", "fam": "niacinamida", "score": 31, "rating": 2.6, "reviews": 218},
    {"brand": "Marisol", "name": "Hi-Volume Mousse", "cat": "Styling", "fam": "shampoo_sin_sulfatos", "score": 35, "rating": 2.7, "reviews": 322},
    {"brand": "Field Notes", "name": "Dawn Foam Cleanser", "cat": "Cleanser", "fam": "acido_hialuronico", "score": 38, "rating": 2.8, "reviews": 174},
    {"brand": "Pequeña Luna", "name": "Citron Eye Stick", "cat": "Eye", "fam": "vitamina_c", "score": 41, "rating": 2.9, "reviews": 89},
    {"brand": "Hazelwood", "name": "Gold Glaze Hair Oil", "cat": "Hair", "fam": "shampoo_sin_sulfatos", "score": 43, "rating": 3.0, "reviews": 246},
    {"brand": "Ocho Botánica", "name": "Deep Repair Conditioner", "cat": "Conditioner", "fam": "shampoo_sin_sulfatos", "score": 45, "rating": 3.0, "reviews": 412},
    {"brand": "North & Bare", "name": "Reset Toner Pads", "cat": "Toner", "fam": "niacinamida", "score": 47, "rating": 3.1, "reviews": 156},
    {"brand": "Verde Skin", "name": "Crystal Salt Scrub", "cat": "Scrub", "fam": "shampoo_sin_sulfatos", "score": 49, "rating": 3.2, "reviews": 318},
    {"brand": "Atelier Roma", "name": "Bella Sun Stick SPF30", "cat": "SPF", "fam": "vitamina_c", "score": 51, "rating": 3.3, "reviews": 203},
    {"brand": "Element Eight", "name": "Strand Strength Mask", "cat": "Hair", "fam": "shampoo_sin_sulfatos", "score": 53, "rating": 3.3, "reviews": 271},
]

HEALTH_BINS: list[int] = [4, 6, 11, 18, 23, 32, 41, 52, 64, 78, 92, 104, 118, 124, 113, 96, 71, 48, 27, 11]

VEL_DOTS: list[dict] = [
    {"x": 12, "y": 28, "s": 80, "name": "Quiet Hours"},
    {"x": 18, "y": 42, "s": 120, "name": "Daydew Mist"},
    {"x": 24, "y": 38, "s": 95, "name": "Soft Field"},
    {"x": 33, "y": 21, "s": 60, "name": "Notte Retinol"},
    {"x": 41, "y": 49, "s": 140, "name": "Salt-Free Wave"},
    {"x": 9, "y": 6, "s": 30, "name": "Brightening Patches"},
    {"x": 14, "y": 18, "s": 45, "name": "Hi-Volume Mousse"},
    {"x": 22, "y": 14, "s": 35, "name": "Dawn Foam"},
    {"x": 28, "y": 30, "s": 80, "name": "Citron Eye"},
    {"x": 36, "y": 28, "s": 60, "name": "Gold Glaze"},
    {"x": 44, "y": 36, "s": 70, "name": "Deep Repair"},
    {"x": 7, "y": 22, "s": 38, "name": "Reset Pads"},
    {"x": 11, "y": 33, "s": 88, "name": "Crystal Scrub"},
    {"x": 15, "y": 9, "s": 25, "name": "Bella Sun"},
    {"x": 30, "y": 47, "s": 95, "name": "Strand Mask"},
]

MONTHS_12: list[str] = ["May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
                        "Jan", "Feb", "Mar", "Apr"]
SEARCH_LINE: list[int] = [42, 48, 51, 49, 55, 63, 71, 78, 92, 88, 79, 84]
REVIEW_BARS: list[int] = [180, 210, 245, 268, 292, 318, 374, 422, 488, 466, 410, 398]


def _seeded(seed: int = 17):
    rng = random.Random(seed)
    return rng


def daily_reviews(seed: int = 17) -> tuple[list[float], list[float]]:
    rng = _seeded(seed)
    base = [16 + math.sin(i / 8) * 6 + math.sin(i / 17) * 4 for i in range(90)]
    daily = [max(2.0, round(b + (8 if i % 7 == 6 else 0) + rng.random() * 4))
             for i, b in enumerate(base)]
    ma = []
    for i in range(len(daily)):
        lo = max(0, i - 6)
        ma.append(sum(daily[lo:i + 1]) / (i - lo + 1))
    return daily, ma


POS_PCT = [62, 65, 64, 68, 71, 73, 70, 72, 75, 74, 71, 73]
NEU_PCT = [22, 21, 23, 19, 18, 17, 19, 18, 16, 17, 19, 17]
NEG_PCT = [16, 14, 13, 13, 11, 10, 11, 10, 9, 9, 10, 10]
RATING_LINE = [4.2, 4.3, 4.3, 4.4, 4.5, 4.6, 4.5, 4.6, 4.7, 4.7, 4.6, 4.7]


def umap_points(seed: int = 11) -> list[dict]:
    rng = _seeded(seed)
    clusters = [
        (0.22, 0.30, 0.10, 0, 38), (0.58, 0.22, 0.09, 1, 42),
        (0.78, 0.40, 0.08, 2, 30), (0.30, 0.65, 0.10, 3, 36),
        (0.62, 0.72, 0.09, 4, 34), (0.45, 0.45, 0.08, 5, 28),
        (0.83, 0.66, 0.07, 6, 22), (0.18, 0.82, 0.06, 7, 18),
        (0.72, 0.86, 0.06, 8, 18), (0.10, 0.50, 0.06, 9, 16),
    ]
    pts = []
    for cx, cy, r, t, n in clusters:
        for _ in range(n):
            a = rng.random() * math.pi * 2
            rr = rng.random() * r
            pts.append({"x": cx + math.cos(a) * rr, "y": cy + math.sin(a) * rr,
                        "t": t, "r": 2 + rng.random() * 2})
    return pts


TOPICS = [
    {"label": "hydration & glow", "count": 412, "t": 0},
    {"label": "texture & finish", "count": 386, "t": 1},
    {"label": "fragrance", "count": 268, "t": 2},
    {"label": "packaging", "count": 232, "t": 3},
    {"label": "price & value", "count": 219, "t": 4},
    {"label": "breakouts", "count": 184, "t": 5},
    {"label": "pump issues", "count": 142, "t": 6},
    {"label": "dryness / tight skin", "count": 128, "t": 7},
    {"label": "shipping & delivery", "count": 96, "t": 8},
    {"label": "staff recommendation", "count": 72, "t": 9},
]

REVIEWS = [
    {"title": "Skin felt like glass for 12 hours", "rating": 5, "sentiment": "pos",
     "age": "25-34", "skin": "combination", "helpful": 0.92, "date": "Apr 28",
     "text": '"I noticed the difference within a week — pores look smaller and that 4pm dullness is gone. The texture is silky, not sticky."'},
    {"title": "Not sure about the fragrance", "rating": 3, "sentiment": "neu",
     "age": "35-44", "skin": "sensitive", "helpful": 0.61, "date": "Apr 24",
     "text": '"Works fine but the scent is too floral for me. Wish there was an unscented version. Otherwise the pump and bottle are perfect."'},
    {"title": "Caused a breakout on day 4", "rating": 2, "sentiment": "neg",
     "age": "18-24", "skin": "oily", "helpful": 0.74, "date": "Apr 22",
     "text": '"Started getting small bumps along my jawline by day four. Stopped using and switched back to my old serum. Patch test next time."'},
    {"title": "My new staple", "rating": 5, "sentiment": "pos",
     "age": "45-54", "skin": "dry", "helpful": 0.88, "date": "Apr 19",
     "text": '"Layered under SPF and it never pills. Skin is plumper, fine lines around the eyes are softer."'},
    {"title": "Pump broke after two weeks", "rating": 2, "sentiment": "neg",
     "age": "25-34", "skin": "normal", "helpful": 0.69, "date": "Apr 16",
     "text": '"Loved the formula, hated the pump. Stopped dispensing properly and I had to unscrew the cap to use it."'},
]

BRAND_TABLE = [
    {"brand": "Element Eight", "products": 28, "reviews": 18420, "rating": 4.6, "recommended": 88, "sentiment": 0.42, "health": 81, "polarization": 0.18},
    {"brand": "Lumia", "products": 22, "reviews": 12180, "rating": 4.4, "recommended": 81, "sentiment": 0.31, "health": 73, "polarization": 0.24},
    {"brand": "Verde Skin", "products": 19, "reviews": 9840, "rating": 4.5, "recommended": 84, "sentiment": 0.38, "health": 78, "polarization": 0.21},
    {"brand": "Atelier Roma", "products": 15, "reviews": 7250, "rating": 4.3, "recommended": 78, "sentiment": 0.28, "health": 70, "polarization": 0.31},
    {"brand": "Marisol", "products": 24, "reviews": 14600, "rating": 4.2, "recommended": 76, "sentiment": 0.25, "health": 68, "polarization": 0.29},
]

DEMO_ROWS = ["Dry", "Normal", "Combination", "Oily", "Sensitive", "Acne-prone", "Mature"]
DEMO_COLS = ["avg_rating", "avg_sentiment", "pct_recommended", "pct_positive"]
DEMO_VALUES = [
    [0.91, 0.78, 0.88, 0.82],
    [0.86, 0.72, 0.84, 0.76],
    [0.89, 0.74, 0.86, 0.79],
    [0.72, 0.55, 0.68, 0.58],
    [0.42, 0.32, 0.48, 0.41],
    [0.38, 0.28, 0.44, 0.36],
    [0.81, 0.66, 0.78, 0.71],
]
DEMO_VOLUME = [842, 1220, 1685, 920, 540, 312, 456]


def search_trends(seed: int = 23) -> list[dict]:
    rng = _seeded(seed)
    niac = [50 + math.sin(i / 9) * 12 + (max(0, i - 60) * 0.6) + rng.random() * 6 for i in range(90)]
    ah = [38 + math.sin(i / 6 + 1) * 10 + rng.random() * 4 for i in range(90)]
    sh = [28 + math.sin(i / 11 + 2) * 8 + rng.random() * 4 + (22 if 70 < i < 78 else 0) for i in range(90)]
    return [
        {"color": th.C1_CORAL, "data": niac, "label": "niacinamida"},
        {"color": th.C2_TEAL, "data": ah, "label": "ácido hialurónico"},
        {"color": th.C3_LAVENDER, "data": sh, "label": "shampoo sin sulfatos"},
    ]


SPIKES = [
    {"keyword": "shampoo sin sulfatos", "geo": "MX-CDMX", "start": "Apr 14", "end": "Apr 22", "pct": 184},
    {"keyword": "niacinamida 10%", "geo": "US-CA", "start": "Mar 22", "end": "Apr 03", "pct": 142},
    {"keyword": "retinol nocturno", "geo": "AR-BA", "start": "Mar 11", "end": "Mar 19", "pct": 118},
    {"keyword": "ácido hialurónico mist", "geo": "US-NY", "start": "Feb 20", "end": "Feb 28", "pct": 96},
    {"keyword": "vitamina c serum", "geo": "CO-BOG", "start": "Feb 02", "end": "Feb 10", "pct": 78},
]


def pipeline_runs(seed: int = 5) -> list[dict]:
    rng = _seeded(seed)
    runs = []
    for i in range(30):
        status = "fail" if i in (18, 24) else "success"
        runs.append({"status": status, "duration": 14 + rng.random() * 8})
    return runs
