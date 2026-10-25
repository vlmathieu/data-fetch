# resources

Reference tables, **versioned on purpose** rather than downloaded at runtime.

Three reasons they are committed:

- `resolve_params()` runs at DAG time. A network call there would make
  `snakemake -n` fail on a connection blip.
- `comtrade_common.py` must stay cheap to import: no pandas, no urllib.
- the fingerprint of a request must not depend on a remote file UNSD can
  rewrite without notice.

Both files are plain CSV, so GitHub renders them as sortable tables. The JSON
is what the code reads; the CSV is the same content for humans.

## `comtrade_areas.{json,csv}` — generated

Every Comtrade area: its numeric code, its ISO3 when it has one, its label, and
what kind of thing it is.

```zsh
python workflow/scripts/build_comtrade_areas.py   # refresh, public endpoints, no key
```

| `kind` | Meaning | `exclude_pseudo` |
|---|---|---|
| `country` | real country or territory with an ISO 3166-1 alpha-3 code | kept |
| `proxy` | a single reporting economy published under a residual label | **kept** |
| `aggregate` | a sum of several areas, or a catch-all | dropped |
| `territory` | a real place with no ISO3 | dropped |

`kind` is an **explicit list** in the build script, not a rule, because every
mechanical test fails in both directions:

- "no valid ISO3" misses the European Union (`97` / `EUR`), "Europe EU, nes"
  (`492` / `MCO`, which carries Monaco's ISO3) and the Neutral Zone
  (`536` / `NTZ`).
- "no valid ISO3" also catches real places that simply have none: Kosovo, the
  Channel Islands, Sarawak.
- UNSD's own `isGroup` flag marks only 4 of the 312 areas. Bunkers, Free Zones
  and every "..., nes" are not flagged.

The one `proxy` is **`490`, "Other Asia, nes", which is Taiwan**. UNSD cannot
name it, so publishes it under a residual label. It is a single reporting
economy, not a sum of countries, so `exclude_pseudo` keeps it: dropping it
would delete Taiwan's trade.

If UNSD adds an area, the build script **refuses to run** rather than letting an
unclassified entity through as a country. Classify it by hand in
`NOT_A_COUNTRY`.

## `eu_membership.csv` — written by hand

Read by `aggregate_eu.py`. Keyed on **ISO3**, not on Comtrade codes, so that
each member expands to every code it ever had: `BEL` covers code `56` (Belgium)
**and** code `58` (Belgium-Luxembourg, which carries 1996-1998). Nothing is
typed by hand.

`from` and `to` are **inclusive years**. A row is relabelled as EU only for the
periods it was actually a member, which keeps `GBR` out of the EU after Brexit
and `HRV` out of it before 2013. `to: 9999` means "still a member".

**`GBR` ends at 2020, and that is a decision, not a fact.** The United Kingdom
left the EU on 2020-01-31 but stayed inside the customs territory until
2020-12-31, so its 2020 trade is still reported under EU customs rules. Change
it to 2019 if you want the political date instead.

An ISO3 in this file that matches no area is an error raised by
`build_comtrade_areas.py`, so a typo here cannot silently drop a member from
the aggregation.
