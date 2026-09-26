# Data notes

Observations from exploring the raw API responses.

## Fields and types
result list, fields Lines, Lon, VehicleNumber, Time, Lat, Brigade
## Vehicles per call

## Timezone of Time field
Warsaw local time, no offset in the string, needs conversion to UTC
## Update frequency
around 1 minute behind real time

Group	               Trams	    Share
Frozen (1)	            118	        38%
Fully active (30)	    180	        58%
In between (2 to 29)    13	        4%
## Stale records
Active vehicles: ~18 s median delay
~38% of returned trams are not live (Saturday evening), some stale for up to 700 days
Rule: live if staleness < 2 min (configurable)

## Duplicates / missing / out-of-bounds
Top-level result list, 6 fields, IDs as strings
Time is Warsaw local, no offset, convert to UTC, watch DST on Oct 25
Active vehicles update at least every 10 s, delay ~18 s → poll every 10 s
~38% of trams returned are not live (Saturday evening), up to 700 days stale, mostly parked at depots
Live rule: staleness < 2 min (configurable), applied in silver, bronze keeps everything
No missing or out-of-bounds coordinates seen, keep a validation rule anyway
Brigade has leading zeros (016), keep as string
## API error behavior
No API errors in 30 polls.
## Unique vehicle key
VehicleNumber is the unique key. Line + brigade only unique among live vehicles