def generate_docs() -> dict[str, dict[str, object]]:
    docs = {}

    # Paris hotels (paris_001 .. paris_010)
    paris_rows = [
        ("paris_001", 150.0, 4.5, 0.8,  "FREE",           True,  ["wifi", "pool", "gym"],          "Luxury hotel excellent amenities"),
        ("paris_002", 75.0,  3.0, 3.2,  "NON_REFUNDABLE",  True,  ["wifi", "breakfast"],            "Budget hotel near station"),
        ("paris_003", 350.0, 5.0, 1.5,  "FREE",           False, ["wifi", "pool", "spa", "gym"],   "Luxury hotel premium spa pool"),
        ("paris_004", 120.0, 4.0, 2.1,  "PARTIAL",         True,  ["wifi", "gym"],                  "Modern hotel central location"),
        ("paris_005", 200.0, 4.8, 0.5,  "FREE",           True,  ["wifi", "pool", "river_view"],   "Luxury hotel riverside views"),
        ("paris_006", 30.0,  2.5, 5.0,  "NON_REFUNDABLE",  True,  ["wifi"],                         "Economy hotel basic amenities"),
        ("paris_007", 180.0, 4.2, 0.3,  "FREE",           True,  ["wifi", "gym"],                  "Boutique hotel near center"),
        ("paris_008", 280.0, 4.7, 0.9,  "PARTIAL",         True,  ["wifi", "pool", "gym", "spa"],   "Upscale hotel premium facilities"),
        ("paris_009", 25.0,  3.2, 4.5,  "NON_REFUNDABLE",  True,  ["wifi"],                         "Affordable hotel cozy rooms"),
        ("paris_010", 95.0,  3.8, 2.8,  "FREE",           False, ["wifi", "gym"],                  "Standard hotel good value"),
    ]
    for suffix, price, rating, dist, policy, avail, amenities, desc in paris_rows:
        docs[f"hotel::{suffix}"] = {
            "type": "hotel",
            "name": f"Paris Hotel {suffix.split('_')[1]}",
            "city": "Paris",
            "price_per_night": price,
            "rating": rating,
            "distanceFromCenterKm": dist,
            "cancellationPolicy": policy,
            "available": avail,
            "amenities": amenities,
            "description": desc,
            "indexed_only": "tag_paris",
        }

    # Edge-case hotels (missing fields)
    docs["hotel::edge_001"] = {
        "type": "hotel",
        "name": "Edge Hotel One",
        "city": "Paris",
        "price_per_night": 100.0,
        "distanceFromCenterKm": 1.0,
        "cancellationPolicy": "FREE",
        "available": True,
        "amenities": ["wifi"],
        "indexed_only": "tag_edge",
        # no rating
    }
    docs["hotel::edge_002"] = {
        "type": "hotel",
        "name": "Edge Hotel Two",
        "city": "Paris",
        "rating": 4.0,
        "distanceFromCenterKm": 1.5,
        "cancellationPolicy": "FREE",
        "available": True,
        "amenities": ["wifi", "gym"],
        "indexed_only": "tag_edge",
        # no price_per_night
    }
    docs["hotel::edge_003"] = {
        "type": "hotel",
        "name": "Edge Hotel Three",
        "city": "Paris",
        # intentionally empty — no price, rating, dist, policy, available, amenities
    }

    # London hotels
    london_rows = [
        ("london_001", 200.0, 4.5, 1.0, "FREE",           True,  ["wifi", "gym"],                   "Luxury hotel central London"),
        ("london_002", 60.0,  3.0, 4.0, "NON_REFUNDABLE",  True,  ["wifi", "breakfast"],             "Budget hotel near tube"),
        ("london_003", 400.0, 5.0, 0.5, "PARTIAL",         True,  ["wifi", "pool", "spa", "gym"],    "Luxury hotel top facilities"),
    ]
    for suffix, price, rating, dist, policy, avail, amenities, desc in london_rows:
        docs[f"hotel::{suffix}"] = {
            "type": "hotel",
            "name": f"London Hotel {suffix.split('_')[1]}",
            "city": "London",
            "price_per_night": price,
            "rating": rating,
            "distanceFromCenterKm": dist,
            "cancellationPolicy": policy,
            "available": avail,
            "amenities": amenities,
            "description": desc,
            "indexed_only": "tag_london",
        }

    # Breweries (5)
    brewery_rows = [
        ("001", "Brussels Brewing Co", "Brussels",  8.5, None),
        ("002", "Denver Brewing Co",   "Denver",    5.0, "United States"),
        ("003", "Munich Brewing Co",   "Munich",    4.8, None),
        ("004", "London Brewing Co",   "London",   11.0, None),
        ("005", "Portland Brewing Co", "Portland",  3.5, "United States"),
    ]
    for num, name, city, abv, country in brewery_rows:
        d = {
            "type": "brewery",
            "name": name,
            "city": city,
            "abv": abv,
            "description": "Traditional brewing company",
        }
        if country:
            d["country"] = country
        docs[f"brewery::{num}"] = d

    # Beers (5)
    beer_rows = [
        ("001", 9.0,  None),
        ("002", 6.5,  "United States"),
        ("003", 4.9,  None),
        ("004", 12.0, None),
        ("005", 3.8,  "United States"),
    ]
    for num, abv, country in beer_rows:
        d = {
            "type": "beer",
            "name": f"Beer {num}",
            "abv": abv,
        }
        if country:
            d["country"] = country
        docs[f"beer::{num}"] = d

    return docs


def compute_ground_truth() -> dict[str, int]:
    return {
        "total": 26,
        "cf_hotels": 16,
        "cf_breweries": 5,
        "cf_beers": 5,
        # CF expected counts
        "cf_1_always_true": 16,
        "cf_2_always_false": 0,
        "cf_3_free_policy": 8,
        "cf_4_price_gt_100": 8,
        "cf_5_budget_750_5nights": 8,
        "cf_6_available": 13,
        "cf_7_pool_amenity": 5,
        "cf_8_rating_4plus": 9,
        "cf_9_brewery_only": 5,
        "cf_10_avail_and_free": 6,
        "cf_11_avail_or_rating45": 14,
        "cf_12_brewery_id": 5,
        "cf_13_refundable": 11,
        # P expected counts
        "p_1_params_check": 16,
        "p_2_nested_params": 8,
        "p_3_array_params": 11,
        # QC expected counts
        "qc_2_paris_not_nonrefund": 10,
        "qc_5_paris_free": 7,
        "qc_6_avail_hotels": 13,
        "qc_7_avail_and_free": 6,
        "qc_8_price100_300_rating4": 6,
        # JS expected counts
        "js_1_id_check": 16,
        "js_3_score_positive": 16,
    }
