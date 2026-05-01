# IBM Hexadecimal Floating Point

Pure-Rust types for IBM 32-bit and 64-bit hexadecimal floating point (IBM HFP),
the numeric formats used by SAS XPORT (64-bit), SEG-Y seismic data (32-bit),
and other IBM-derived legacy data formats. The two formats share the same 7-bit
characteristic and therefore the same numeric range (~5.4e-79 to ~7.2e75); they
differ only in mantissa width (24 vs 56 bits).

## 32-bit support

`IbmFloat32`'s public conversion surface is split deliberately by
losslessness:

**Lossless (trait impls)**
- `From<IbmFloat32> for f64` — bit-exact (IBM32's 24-bit mantissa fits f64's
  53-bit significand with margin; `16^k` is exact in f64 across the range).
- `From<IbmFloat32> for IbmFloat64` — byte zero-pad of the mantissa, no
  arithmetic.
- `FromStr` — parses decimal strings through f64 (not f32, to preserve the
  full IBM32 range; `"1e50"` is representable in IBM32 but would saturate to
  infinity going through f32).

**Lossy (named methods, no trait)**
- `IbmFloat32::try_from_f64_lossy(value: f64) -> Result<Self, IbmFloatError>`
- `IbmFloat32::try_from_f32_lossy(value: f32) -> Result<Self, IbmFloatError>`
- `IbmFloat32::from_ibm_float_64_lossy(ibm64: IbmFloat64) -> Self`

These exist as inherent methods rather than `From`/`TryFrom` trait impls
because the precision-truncation loss they incur is silent — no error
variant could surface it. Trait conversions tend to suggest "free" or
"strictly captured" semantics; neither holds here. The `_lossy` suffix is
the warning label, in the spirit of `String::from_utf8_lossy` and
`Path::to_string_lossy`. Std follows the same instinct for floats:
`From<f32> for f64` exists (lossless widening), but no `From<f64> for f32`
or `TryFrom<f64> for f32` (silent precision loss in the narrow direction).

`FromStr`'s `ParseIbmFloatError` likewise can't surface precision
truncation — but that's expected for a string parser ("parse to nearest
representable value" is the contract), so no `_lossy` suffix is warranted
on `FromStr` itself.

## IBM → IEEE conversion: truncation, by design

IBM HFP 64-bit has a 56-bit mantissa; IEEE-754 `f64` has a 53-bit mantissa. The
IBM-to-IEEE conversion must drop 3 low mantissa bits. This crate **truncates**
those bits (`ieee_fraction = ibm_fraction >> 3`). The choice was made
deliberately:

- **It matches the SAS ecosystem.** Every other open-source SAS XPORT reader we
  surveyed (ReadStat, pandas `pandas.io.sas.sas_xport`, Michael Selik's `xport`)
  truncates. All three trace back to SAS's TS-140 reference algorithm, which
  truncates and explicitly notes the lost bits as expected behavior.
- **It preserves round-tripping.** Any in-range IBM value, converted to `f64`
  and back via `IbmFloat64::try_from(f64)`, reproduces the original IBM bytes.
- **An RTE alternative exists, but it breaks ecosystem interop.** Round-ties-to-even
  (used by Enthought's `ibm2ieee` and willglynn's `ibmfloat`) produces an
  output that is on average ½ ULP closer to the mathematical IBM value, but
  disagrees with the truncating XPORT readers above on roughly 34% of inputs by
  exactly 1 ULP. For SAS XPORT use that's a regression, not an improvement.

If your use case is purely numerical (no XPORT round-trip, minimum mean error
matters more than ecosystem agreement), willglynn's `ibmfloat` may suit you
better.

## IEEE → IBM conversion: strict, with typed errors

`<IbmFloat64 as TryFrom<f64>>::try_from` is **strict**. Anything that cannot be
faithfully represented returns `IbmFloatError` with a specific variant
(`NotANumber`, `PositiveInfinity` / `NegativeInfinity`, `PositiveOverflow` /
`NegativeOverflow`, `PositiveUnderflow` / `NegativeUnderflow`). The crate
deliberately does not implement saturating semantics on the trait — callers
that want clamping at the IBM range boundary should match on the error variant
and substitute `MAX_VALUE`, `MIN_VALUE`, or signed zero as appropriate. This
keeps the lossy-conversion decision visible at the call site.

`FromStr` returns `ParseIbmFloatError`, which composes a `ParseFloatError`
(parse failure) and `IbmFloatError` (out-of-range f64) so both failure modes
are surfaced through a single `?` chain.

## Equality, ordering, and hashing

`PartialEq`, `Eq`, and `Hash` are bit-exact over the underlying `[u8; 8]`. IBM
HFP allows multiple byte representations of zero (any byte with a zero mantissa
is numerically zero, regardless of exponent); these compare unequal here. Convert
to `f64` for numeric equality.

`PartialOrd` and `Ord` are derived as **lexicographic byte order**, which is
not numeric order — negative values sort *after* positive values because the
sign bit is set. The type is usable as a `BTreeMap` key with deterministic
ordering, but `<` and `>` should not be relied on for arithmetic comparison.
Convert to `f64` for that.

## Non-goals

- **No arithmetic operations.** `Add`/`Sub`/`Mul`/`Div`/`Neg` are deliberately
  not implemented. Native IBM HFP arithmetic is wobbling-precision and would
  need full software emulation; the `vax-floating` crate's port from SimH is
  the obvious reference for anyone wanting to take this on. PRs welcome.
- **No `num_traits` integration**, for the same reason.

## References

- IBM System/360 Floating Point format
- SAS Technical Support TS-140 (the XPORT reference)
- Enthought `ibm2ieee` — BSD-3-Clause C reference for IBM → IEEE
- willglynn `ibmfloat` — BSD-3-Clause Rust port of `ibm2ieee`
