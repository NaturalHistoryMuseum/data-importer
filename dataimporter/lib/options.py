from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.diffing import SG_DATE_FORMATS

builder = ParsingOptionsBuilder()

# some basics
builder.with_keyword_length(8191)
builder.with_float_format("{0:.15g}")

# true values
builder.with_true_value("true").with_true_value("yes").with_true_value("y")

# false values
builder.with_false_value("false").with_false_value("no").with_false_value("n")

# date formats
# add the formats used by Splitgill itself allowing us to pickup datetime objects
for fmt in SG_DATE_FORMATS:
    builder.with_date_format(fmt)
# then add the formats we want to support (some of which will already be in the SG list)
builder.with_date_format("%Y-%m")
builder.with_date_format("%Y-%m-%d")
builder.with_date_format("%Y-%m-%dT%H:%M:%S")
builder.with_date_format("%Y-%m-%dT%H:%M:%S.%f")
builder.with_date_format("%Y-%m-%dT%H:%M:%S%z")
builder.with_date_format("%Y-%m-%dT%H:%M:%S.%f%z")

# geo hints
builder.with_geo_hint(
    "decimalLatitude",
    "decimalLongitude",
    "coordinateUncertaintyInMeters",
    16,
)

PARSING_OPTIONS = builder.build()
