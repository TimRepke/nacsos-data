# source: https://raw.githubusercontent.com/rafguns/wosfile/master/wosfile/tags.py
# http://images.webofknowledge.com/WOKRS534DR1/help/WOS/hs_wos_fieldtags.html
# Format: (Abbreviation, Full label, Iterable?, One item per line?)
# - Abbreviation: WoS field tag
# - Full label: full label as provided by Thomson Reuters (or abbreviation if
#   not available)
# - Splittable: whether or not the field should be split into multiple items
# - One item per line: whether or not each item in an iterable field appears on
#   a new line in WoS plain text format
tags = (
    ("AB", "Abstract", False, False),
    ("AF", "Author Full Name", True, True),
    ("AR", "Article Number", False, False),
    ("AU", "Authors", True, True),
    ("BA", "Book Authors", True, True),  # Correct?
    ("BE", "Editors", True, True),
    ("BF", "Book Authors Full Name", True, True),  # Correct?
    ("BN", "International Standard Book Number (ISBN)", False, False),
    ("BP", "Beginning Page", False, False),
    ("BS", "Book Series Subtitle", False, False),
    ("C1", "Author Address", False, True),  # Splitting of this field is handled separately
    ("C3", "C3", True, False),  # this is presumably the author institution without address
    ("CA", "Group Authors", False, False),
    ("CL", "Conference Location", False, False),
    ("CR", "Cited References", True, True),
    ("CT", "Conference Title", False, False),
    ("CY", "Conference Date", False, False),
    ("CL", "Conference Location", False, False),
    ("DA", "Date this report was generated", False, False),
    ("DE", "Author Keywords", True, False),
    ("DI", "Digital Object Identifier (DOI)", False, False),
    ("DT", "Document Type", False, False),
    ("D2", "Book Digital Object Identifier (DOI)", False, False),
    ("EA", "Early access date", False, False),
    ("ED", "Editors", False, False),
    ("EM", "E-mail Address", True, False),
    ("EI", "Electronic International Standard Serial Number (eISSN)", False, False),
    ("EP", "Ending Page", False, False),
    ("EY", "Early access year", False, False),
    ("FU", "Funding Agency and Grant Number", False, False),
    ("FX", "Funding Text", False, False),
    ("GA", "Document Delivery Number", False, False),
    ("GP", "Book Group Authors", False, False),
    ("HC", "ESI Highly Cited Paper", False, False),
    ("HO", "Conference Host", False, False),
    ("HP", "ESI Hot Paper", False, False),
    ("ID", "Keywords Plus", True, False),
    ("IS", "Issue", False, False),
    ("J9", "29-Character Source Abbreviation", False, False),
    ("JI", "ISO Source Abbreviation", False, False),
    ("LA", "Language", False, False),
    ("MA", "Meeting Abstract", False, False),
    ("NR", "Cited Reference Count", False, False),
    ("OA", "Open Access Indicator", False, False),
    ("OI", "ORCID Identifier (Open Researcher and Contributor ID)", True, False),
    ("P2", "Chapter count (Book Citation Index)", False, False),
    ("PA", "Publisher Address", False, False),
    ("PD", "Publication Date", False, False),
    ("PG", "Page Count", False, False),
    ("PI", "Publisher City", False, False),
    ("PM", "PubMed ID", False, False),
    ("PN", "Part Number", False, False),
    ("PT", "Publication Type (J=Journal; B=Book; S=Series; P=Patent)", False, False),
    ("PU", "Publisher", False, False),
    ("PY", "Year Published", False, False),
    ("RI", "ResearcherID Number", True, False),
    ("RP", "Reprint Address", False, False),
    ("SC", "Research Areas", True, False),
    ("SE", "Book Series Title", False, False),
    ("SI", "Special Issue", False, False),
    ("SN", "International Standard Serial Number (ISSN)", False, False),
    ("SO", "Publication Name", False, False),
    ("SP", "Conference Sponsors", False, False),
    ("SU", "Supplement", False, False),
    ("TC", "Web of Science Core Collection Times Cited Count", False, False),
    ("TI", "Document Title", False, False),
    ("U1", "Usage Count (Last 180 Days)", False, False),
    ("U2", "Usage Count (Since 2013)", False, False),
    ("UT", "Unique Article Identifier", False, False),
    ("VL", "Volume", False, False),
    ("WC", "Web of Science Categories", True, False),
    ("WE", "Web of Science indexes", True, False),
    ("Z9", "Total Times Cited Count (WoS Core, BCI, and CSCD)", False, False),
)
is_splittable = {abbr: iterable for abbr, _, iterable, _ in tags}
has_item_per_line = {abbr: item_per_line for abbr, _, _, item_per_line in tags}