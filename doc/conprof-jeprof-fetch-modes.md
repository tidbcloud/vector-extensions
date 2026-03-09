# conprof jeprof/jeheap Fetch Mode Description

## Background

When TiKV uses jemalloc, heap data is exposed via jeprof-compatible endpoints (e.g. `/debug/pprof/heap`). conprof supports two fetch modes, selectable via `jeprof_fetch_mode`.

## What the jeprof Script Actually Does with `--raw` + Remote URL

The Perl script `jeprof --raw <url>` on a remote URL does **more than** a single HTTP GET:

1. **GET profile**  
   Uses `URL_FETCHER` (default `curl -s --fail`) to request the URL and writes the response to temp file `$collected_profile`.

2. **Parse profile to get PC list**  
   `ReadProfile` reads the file, parses heap format (e.g. `heap profile: ...` header, stack entries), and collects all program counter addresses into `$pcs`.

3. **Fetch symbols from server**  
   `FetchSymbols($pcs)`: POSTs the PC list to the same host's `/pprof/symbol`, gets address→symbol mapping; uses `c++filt` for demangling when needed.

4. **Optional: Fetch program name**  
   `FetchProgramName()`: GET `/pprof/cmdline` for the binary name.

5. **Output "symbolized raw" format**  
   `PrintSymbolizedProfile` outputs to stdout:
   - one line `--- symbol`
   - one line `binary=<program name>`
   - symbol table lines: `0x<addr> <symbol>`
   - one line `---`
   - one line `--- heap` (or growth/contention/cpu)
   - **then** the raw content of `$collected_profile` (the GET response body) verbatim

So **Perl mode stdout = symbol header + raw heap body**—a self-contained format usable offline with `jeprof --text` without the live process.

## Mode Comparison

| Item | `jeprof_fetch_mode = "perl"` (default) | `jeprof_fetch_mode = "rust"` |
|------|---------------------------------------|------------------------------|
| Implementation | Spawn Perl process to run jeprof script | In-process Rust: GET heap → parse PCs → POST symbol → compose output |
| Dependencies | Needs Perl, curl (your curl for TLS) | Rust/reqwest only, no Perl |
| Output | **Symbol header + raw heap** | **Symbol header + raw heap** (same as Perl) |
| jeprof compatible | Matches `jeprof --raw` output | Matches `jeprof --raw` output |
| Offline analysis | Saved blob works with `jeprof --text` | Same |

## When to Use Which Mode

- **Use `perl`** when:  
  You need full parity with existing jeprof workflows, or downstream stores data for offline `jeprof --text` analysis without the live process; or you need a quick fallback if the Rust implementation has bugs.

- **Use `rust`** when:  
  You prefer not to depend on Perl, only need collection and archival, and downstream does not rely on the "symbolized raw" format; or symbol resolution will be done elsewhere.

## Rust Mode Implementation

Rust mode (`jeprof_fetch_mode = "rust"`) implements the same flow as Perl:

1. GET `/debug/pprof/heap`, get body.
2. Parse heap text format, extract PCs; apply FixCallerAddresses (minus 1) to addresses except the first.
3. POST those PCs (`0xaddr1+0xaddr2+...`) to same base URL's `/debug/pprof/symbol`, parse response for symbol table.
4. GET `/debug/pprof/cmdline` for program name.
5. Assemble per jeprof: `--- symbol`, `binary=...`, symbol lines, `---`, `--- heap`, then raw body.

If heap is binary, no PCs can be parsed, or symbol request fails, it falls back to returning only the raw body (equivalent to plain GET).
