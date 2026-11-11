# conprof jeheap Fetch

When TiKV uses jemalloc, heap data is exposed via jeprof-compatible endpoints (e.g. `/debug/pprof/heap`). conprof collects this via the **native Rust** jeprof path only.

## Behavior

1. GET the heap URL with `?debug=1` (text profile so PCs can be parsed).
2. Parse program counters from the profile.
3. POST to `/debug/pprof/symbol` for symbolization; GET `/debug/pprof/cmdline` for binary name.
4. Assemble output compatible with historical `jeprof --raw`: `--- symbol`, `binary=...`, symbol lines, `---`, `--- heap`, then raw body.

The result is self-contained for offline `jeprof --text` without the live process. No Perl or curl is required.

## Config note

`jeprof_fetch_mode` is deprecated and ignored. Existing configs may still set `rust` or `perl`; both load, and fetch always uses the native path. Prefer omitting the field in new configs.
