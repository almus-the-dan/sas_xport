# IBM Hexadecimal Floating Point

Pure-Rust types for IBM 64-bit hexadecimal floating point (IBM HFP), the numeric
format used by SAS XPORT and other IBM-derived legacy data formats. A 32-bit
`IbmFloat32` companion is planned.

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
faithfully represented returns `IbmFloat64Error` with a specific variant
(`NotANumber`, `PositiveInfinity` / `NegativeInfinity`, `PositiveOverflow` /
`NegativeOverflow`, `PositiveUnderflow` / `NegativeUnderflow`). The crate
deliberately does not implement saturating semantics on the trait — callers
that want clamping at the IBM range boundary should match on the error variant
and substitute `MAX_VALUE`, `MIN_VALUE`, or signed zero as appropriate. This
keeps the lossy-conversion decision visible at the call site.

`FromStr` returns `ParseIbmFloat64Error`, which composes a `ParseFloatError`
(parse failure) and `IbmFloat64Error` (out-of-range f64) so both failure modes
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
