# Assumptions and Rulebook

## Candidate population assumptions
1. **V1 transaction definition**: a property is in candidate set if its latest observed sale price >= £2,000,000.
2. **Threshold proximity band**: properties with latest sale price in [£1,500,000, £1,999,999] are tracked as near-threshold risk.
3. **V2 current-value proxy**: latest observed sale is uplifted using UK HPI growth from sale month to analysis month at best available geography.
4. Where geography-level HPI is unavailable, fallback to England/Wales national index and mark lower confidence.

## Ownership assumptions
1. Ownership type `UK_company` or `overseas_company` is a strong non-owner-occupation indicator.
2. `individual` ownership does **not** prove owner-occupation.
3. `trust_or_other` and unresolved legal entities are treated as uncertain unless additional evidence exists.

## EPC assumptions
1. EPC tenure/occupancy fields (when present) are key signals:
   - owner_occupied
   - rented_private
   - rented_social
   - unknown
2. EPC signals can be stale or missing; no EPC match does not imply owner-occupied.

## Classification rules
### High confidence not owner-occupied
- ownership_type in {UK_company, overseas_company}
- OR EPC category in {rented_private, rented_social}
- OR multiple rental-oriented signals.

### High confidence owner-occupied
- EPC category == owner_occupied
- AND ownership_type not company-linked
- AND no rental signal
- AND no conflicting evidence.

### Medium confidence not owner-occupied
- Company-linked/indirect ownership with incomplete occupancy evidence,
- OR multiple non-owner-occupation proxies.

### Medium confidence owner-occupied
- individual ownership,
- no rental or company signal,
- no contradictory EPC,
- but lacks direct high-confidence occupancy evidence.

### Low confidence / uncertain
- conflicting signals,
- sparse evidence,
- failed joins,
- unresolved ownership and occupancy.

## Range construction assumptions
- **Conservative owner-occupation estimate**: ambiguous cases pushed away from owner-occupation where justified.
- **Central estimate**: weighted by confidence tiers (high=1.0, medium=0.7, low=0.4 for owner-likely class; mirrored for non-owner-likely).
- **Upper estimate**: ambiguous cases pushed toward owner-occupation where plausible.

## Sensitivity assumptions (tested)
- V1 vs V2 candidate population.
- Uplift multiplier perturbation (+/- 10%).
- Unmatched records treatment (exclude, neutral, pessimistic).
- Signal-poor individual-owned treatment.
- EPC coverage optimism/pessimism.
- Ownership resolution strictness.

## Known caveats
- Open-data coverage and linkage quality limit confidence.
- Aggregate contextual datasets are not treated as property-level truth.
- Beneficial ownership may diverge from legal owner type.
