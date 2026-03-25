# Method: Estimating the Defensible Range of Owner-Occupation for £2m+ Residential Properties (England & Wales)

## Core research question
What is the **defensible range** of owner-occupation among residential properties valued at **£2,000,000+** in England and Wales, using open/accessible administrative data and transparent assumptions?

## Why this is a classification problem (not a simple lookup)
There is no complete public register that directly labels every high-value home as owner-occupied or not owner-occupied at a point in time. Instead, we observe fragments:
- transaction history,
- ownership clues,
- EPC tenure/occupancy-related signals,
- and contextual aggregates.

The task is therefore to classify each candidate property using a hierarchy of evidence and then aggregate classifications with uncertainty bounds.

## What is directly observed
Directly observed elements include (subject to source access):
- observed transaction prices and dates (Price Paid Data),
- reported EPC attributes (where available),
- legal ownership indicators where explicitly available from property/land data routes,
- geography/address text as recorded in source systems.

## What is inferred
Inferred elements include:
- current-value eligibility for £2m+ when based on historic sale plus UK HPI uplift,
- occupancy status where no direct occupancy truth exists,
- likely owner-occupation status using combinations of ownership type + EPC/rental signals,
- confidence tier assignments based on evidence quality and agreement.

## What cannot be known from open data alone
Open data is unlikely to fully reveal:
- definitive current occupancy for every dwelling,
- complete beneficial ownership structures (e.g., layered corporate/trust arrangements),
- all rental activity at property level,
- perfect linkage across all datasets without authoritative universal property identifiers.

## Why outputs must be ranges, not one false-precision percentage
Because evidence coverage and signal quality vary materially across properties, a single percentage would overstate certainty. The pipeline therefore reports:
- **Conservative estimate**,
- **Central estimate**,
- **Upper estimate**,
plus confidence composition and sensitivity to major assumptions.

## Why EPC is treated as a key occupancy signal (when present), but not complete truth
EPC records can contain tenure/occupancy-related signals that are highly informative when present and recent. However:
- EPC coverage is incomplete,
- fields can be missing or inconsistently coded,
- and EPC status may not represent current tenure at analysis date.

So EPC is a strong but non-exhaustive signal.

## Why company ownership is a strong non-owner-occupation signal, but not perfect
Corporate ownership (UK or overseas company) is often associated with non-owner occupation, investment, or indirect beneficial use. But exceptions exist (e.g., occupancy by beneficial owner). Therefore company ownership is treated as a strong signal, not absolute proof.

## Why “no evidence” is not evidence of owner-occupation
Absence of rental/corporate flags may simply reflect data gaps, failed joins, or unobserved channels. The model avoids defaulting silent records to owner-occupied and instead uses uncertainty tiers.

## How uncertainty is represented
Uncertainty is explicitly represented through:
1. **Status class**: owner_occupied_likely / not_owner_occupied_likely / uncertain
2. **Confidence tier**: high / medium / low
3. **Evidence basis**: direct, proxy, mixed, or sparse
4. **Conflicting signals flag**
5. **Scenario analysis**: varying key assumptions and handling of unmatched/signal-poor records
6. **Range outputs**: conservative, central, upper estimates rather than a single point claim
