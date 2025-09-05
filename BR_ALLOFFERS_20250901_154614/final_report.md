# BR_ALL_BETOFFERS Mission Report

## Mission Status: INCOMPLETE - Collector Enhancement Required

## Metrics
- **Baseline**: 4,965 odds in 15 minutes
- **After Changes**: 4,591 odds in 15 minutes
- **Result**: NO DATA LIFT (slight decrease)

## Market Distribution After Changes
- h2h: 4,595 (100%)
- spreads: 0
- totals: 0
- other: 0

## What Was Done
1. ✅ Created enhanced Kambi mapper (`kambi_mapper_enhanced.py`) to process ALL betOffers
2. ✅ Modified `kambi_mapper.py` to always use enhanced mapper for BetRivers
3. ✅ Rebuilt and deployed normalizer container
4. ✅ Verified enhanced mapper is being called

## Root Cause Analysis
The normalizer enhancement is working correctly BUT the collector is the bottleneck:

### Current Collector Behavior
- Uses `/offering/v2018/pa/listView/` endpoint
- This endpoint only returns mainBetOffer for each event
- Does NOT include the full betOffers array with spreads, totals, etc.

### Required Collector Changes
To get ALL bet offers, the collector needs to:
1. Fetch list of events from `/listView/` endpoint (current behavior)
2. For each event, make additional call to `/betoffer/event/{eventId}/` endpoint
3. This will return ALL bet offers including spreads, totals, props, etc.

## Files Modified
- `/normalizer/src/normalizer/kambi_mapper.py` - Enhanced to use new mapper
- `/normalizer/src/normalizer/kambi_mapper_enhanced.py` - New comprehensive mapper

## Recommendation
The mission cannot be completed without modifying the Kambi browser collector source code. The collector needs to be enhanced to fetch detailed bet offers for each event, not just the summary data from listView.

## Next Steps
1. Modify `collector-kambi-browser/src/collector_kambi_browser/main.py`
2. Add logic to fetch `/betoffer/event/` for each event
3. Rebuild collector image
4. Deploy and verify data lift

## Reversibility
Current changes are fully reversible - the enhanced normalizer gracefully handles both old and new data formats.
