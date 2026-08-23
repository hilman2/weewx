# Horizon skin — screenshots

Images for the discussion of the Horizon skin and the JSON generator. This branch holds
nothing but pictures; the code is on `horizon-skin`.

They are taken from a demo station running on synthetic data from the project's own
`gen_fake_data.py` — 400 days, 57,601 records, rendered in German and English.

| | |
|---|---|
| `01-desktop-light.png` | The start page, light theme. The grey bands are night, with a real gradient across civil twilight (33 minutes here), computed from the station's location. |
| `02-desktop-dark.png` | The same page, dark theme. Charts read their colours from CSS at draw time. |
| `03-history.png` | January 2026 — a month the station recorded but never rendered to an image. Whole calendar units, over the entire record. |
| `04-mobile.png` | A phone. One layout, two columns above 60 rem; no second set of templates. |
| `05-png-seasons.png` | The classic 500×180 PNG, for comparison. |
| `06-png-horizon.png` | The same plot, same data, at 1000×360 with 2× supersampling — configuration only, no new code. |
| `07-summary-image.png` | `current.png`: the readings as a picture at a fixed URL, for forum signatures and chat. WeeWX could not draw this before. |
