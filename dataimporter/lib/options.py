from splitgill.indexing.options import ParsingOptionsBuilder

builder = ParsingOptionsBuilder()

# some basics
builder.with_keyword_length(8191)
builder.with_float_format("{0:.15g}")

# true values
builder.with_true_value("true").with_true_value("yes").with_true_value("y")

# false values
builder.with_false_value("false").with_false_value("no").with_false_value("n")

# date formats
# some common basic formats
builder.with_date_format("%Y-%m-%d")
# rfc 3339ish
builder.with_date_format("%Y-%m-%dT%H:%M:%S")
builder.with_date_format("%Y-%m-%dT%H:%M:%S.%f")
builder.with_date_format("%Y-%m-%dT%H:%M:%S%z")
builder.with_date_format("%Y-%m-%dT%H:%M:%S.%f%z")
builder.with_date_format("%Y%m%dT%H%m%s")
# same as the above, just with a space instead of the T separator
builder.with_date_format("%Y-%m-%d %H:%M:%S")
builder.with_date_format("%Y-%m-%d %H:%M:%S.%f")
builder.with_date_format("%Y-%m-%d %H:%M:%S%z")
builder.with_date_format("%Y-%m-%d %H:%M:%S.%f%z")
builder.with_date_format("%Y%m%d %H%m%s")

# geo hints
builder.with_geo_hint(
    "decimalLatitude",
    "decimalLongitude",
    "coordinateUncertaintyInMeters",
    16,
)

DEFAULT_OPTIONS = builder.build()
