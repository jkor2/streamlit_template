import streamlit as st
import pandas as pd
import numpy as np
from datetime import date

# ----------------------------
# Config
# ----------------------------
st.set_page_config(page_title="Caller → Assigned Orgs + Season Invites", layout="wide")

MASTER_FILE = "MASTER Orgs and Callers Asana.xlsx"
TRACKING_FILE = "Organizational Tracking 1-7-26.xlsx"

MASTER_SHEET = "Sheet1"
TRACKING_SHEET = "Export"

# Season windows (inclusive bounds)
SEASON_2025_START = pd.Timestamp("2024-10-15")
SEASON_2025_END   = pd.Timestamp("2025-10-01")  # inclusive
SEASON_2026_START = pd.Timestamp("2025-10-15")
SEASON_2026_END   = pd.Timestamp(date.today())  # inclusive

# ----------------------------
# Helpers
# ----------------------------
def _clean_str(s):
    if pd.isna(s):
        return ""
    return str(s).strip()

def normalize_blank_series(s: pd.Series) -> pd.Series:
    return s.fillna("").map(_clean_str).replace({"": "(Blank)"})

def apply_blank_filter(df, col, selected):
    if selected is None:
        return df
    ser = normalize_blank_series(df[col])
    all_vals = sorted(ser.unique().tolist())
    if set(selected) == set(all_vals):
        return df
    return df[ser.isin(selected)]

def assign_season(dt: pd.Timestamp):
    if pd.isna(dt):
        return pd.NA
    if SEASON_2025_START <= dt <= SEASON_2025_END:
        return 2025
    if SEASON_2026_START <= dt <= SEASON_2026_END:
        return 2026
    return pd.NA

def season_pivot_counts(df: pd.DataFrame) -> pd.DataFrame:
    out_cols = ["OrganizationID", "Organization Name", "2025", "2026", "Total", "YoY Δ", "YoY %"]
    if df.empty:
        return pd.DataFrame(columns=out_cols)

    g = (
        df.groupby(["OrganizationID", "Organization Name", "Season"], dropna=False)
          .size()
          .reset_index(name="Invites")
    )

    pivot = g.pivot_table(
        index=["OrganizationID", "Organization Name"],
        columns="Season",
        values="Invites",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    for season in [2025, 2026]:
        if season not in pivot.columns:
            pivot[season] = 0

    pivot = pivot.rename(columns={2025: "2025", 2026: "2026"})
    pivot["Total"] = pivot["2025"] + pivot["2026"]
    pivot["YoY Δ"] = pivot["2026"] - pivot["2025"]
    pivot["YoY %"] = np.where(pivot["2025"] == 0, np.nan, (pivot["YoY Δ"] / pivot["2025"]) * 100.0)

    pivot = pivot.sort_values(by=["2026", "Total"], ascending=False)
    return pivot[out_cols]

def org_detail_metrics(trk_df: pd.DataFrame, org_id: int):
    d = trk_df[trk_df["OrganizationID"] == org_id]
    inv_2025 = int((d["Season"] == 2025).sum())
    inv_2026 = int((d["Season"] == 2026).sum())
    total = inv_2025 + inv_2026
    yoy_delta = inv_2026 - inv_2025
    yoy_pct = None if inv_2025 == 0 else (yoy_delta / inv_2025) * 100.0
    return inv_2025, inv_2026, total, yoy_delta, yoy_pct

# ----------------------------
# Robust header normalization
# ----------------------------
def _norm_header(h: str) -> str:
    h = str(h).strip().lower()
    out = []
    prev_us = False
    for ch in h:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    return "".join(out).strip("_")

def _find_first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _find_cols_by_keywords(df: pd.DataFrame, keywords: list[str]) -> list[str]:
    # returns columns where ANY keyword appears in the normalized header
    hits = []
    for c in df.columns:
        for k in keywords:
            if k in c:
                hits.append(c)
                break
    return hits

def _pretty_label(norm_col: str) -> str:
    # Convert normalized header to a readable label
    return norm_col.replace("_", " ").title()

# ----------------------------
# Loaders
# ----------------------------
@st.cache_data(show_spinner=False)
def load_master(path=MASTER_FILE) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    sheet = MASTER_SHEET if MASTER_SHEET in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)
    original_headers = list(df.columns)

    # normalize headers
    df.columns = [_norm_header(c) for c in df.columns]

    # required columns (robust)
    org_id_col = _find_first_col(df, ["org_id", "orgid", "organizationid", "organization_id"])
    name_col   = _find_first_col(df, ["name", "org_name", "organization", "organization_name", "orgname", "account"])
    caller_col = _find_first_col(df, ["section_column", "sectioncolumn", "section", "caller", "owner", "rep", "assigned_to", "assignee"])

    missing = []
    if org_id_col is None: missing.append("Org ID")
    if name_col is None: missing.append("Name")
    if caller_col is None: missing.append("Section/Column")

    if missing:
        raise ValueError(
            f"MASTER missing required columns {missing}. "
            f"Original headers: {original_headers} | Normalized headers: {list(df.columns)}"
        )

    # contact-ish columns: be generous
    email_cols = _find_cols_by_keywords(df, ["email", "e_mail", "mail"])
    phone_cols = _find_cols_by_keywords(df, ["phone", "telephone", "mobile", "cell"])
    contact_name_cols = _find_cols_by_keywords(df, ["contact", "coach", "director", "admin", "name"])

    # but don't duplicate the main org name col in contact fields
    contact_name_cols = [c for c in contact_name_cols if c not in {name_col}]

    # Build output with canonical cols + extra contact cols
    out = pd.DataFrame()
    out["Org ID"] = pd.to_numeric(df[org_id_col], errors="coerce").astype("Int64")
    out["Name"] = df[name_col].map(_clean_str)
    out["Section/Column"] = df[caller_col].map(_clean_str)

    # Always include these canonical fields (populate from best match if available)
    best_email = email_cols[0] if email_cols else None
    best_phone = phone_cols[0] if phone_cols else None
    best_contact_name = None
    # pick a contact-ish name col that is not literally "name" and not org name
    for c in contact_name_cols:
        if "organization" not in c and c != name_col:
            best_contact_name = c
            break

    out["Email"] = df[best_email].map(_clean_str) if best_email else ""
    out["Phone Number"] = df[best_phone].map(_clean_str) if best_phone else ""
    out["Contact Name"] = df[best_contact_name].map(_clean_str) if best_contact_name else ""

    # Also pass through ALL additional contact columns (so you never “lose” info)
    # We'll prefix them to avoid collisions.
    extra_cols = []
    for c in sorted(set(email_cols + phone_cols + contact_name_cols)):
        if c in {org_id_col, name_col, caller_col, best_email, best_phone, best_contact_name}:
            continue
        extra_cols.append(c)

    for c in extra_cols:
        out[f"Extra: {_pretty_label(c)}"] = df[c].map(_clean_str)

    out = out.dropna(subset=["Org ID"])
    out = out[out["Section/Column"] != ""].copy()
    return out

@st.cache_data(show_spinner=False)
def load_tracking(path=TRACKING_FILE) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    sheet = TRACKING_SHEET if TRACKING_SHEET in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)
    original_headers = list(df.columns)

    df.columns = [_norm_header(c) for c in df.columns]

    org_id_col = _find_first_col(df, ["organizationid", "organization_id", "org_id", "orgid"])
    org_name_col = _find_first_col(df, ["organization_name", "org_name", "name", "organization"])
    date_col = _find_first_col(df, ["date_requested", "requested_date", "invite_date", "daterequest"])

    event_year_col = _find_first_col(df, ["start_date_calendar_year", "event_year", "start_year", "calendar_year"])
    event_name_col = _find_first_col(df, ["event_name", "event", "eventtitle"])
    reg_col = _find_first_col(df, ["registration_status", "reg_status", "status"])
    tag_col = _find_first_col(df, ["tag_level", "level", "tag", "tier"])

    missing = []
    if org_id_col is None: missing.append("OrganizationID")
    if org_name_col is None: missing.append("Organization Name")
    if date_col is None: missing.append("Date Requested")

    if missing:
        raise ValueError(
            f"TRACKING missing required columns {missing}. "
            f"Original headers: {original_headers} | Normalized headers: {list(df.columns)}"
        )

    out = pd.DataFrame()
    out["OrganizationID"] = pd.to_numeric(df[org_id_col], errors="coerce").astype("Int64")
    out["Organization Name"] = df[org_name_col].map(_clean_str)
    out["Date Requested"] = pd.to_datetime(df[date_col], errors="coerce")

    out["Start Date Calendar Year"] = (
        pd.to_numeric(df[event_year_col], errors="coerce").astype("Int64")
        if event_year_col else pd.Series([pd.NA] * len(df), dtype="Int64")
    )
    out["Event name"] = df[event_name_col].map(_clean_str) if event_name_col else ""
    out["Registration Status"] = df[reg_col].map(_clean_str) if reg_col else ""
    out["Tag Level"] = df[tag_col].map(_clean_str) if tag_col else ""

    out = out.dropna(subset=["OrganizationID"]).copy()
    return out

# ----------------------------
# UI
# ----------------------------
st.title("Caller → Assigned Orgs + Invite Comparison (2025 vs 2026)")

with st.spinner("Loading data..."):
    master = load_master()
    tracking_raw = load_tracking()

# Season filter
tracking_raw["Season"] = tracking_raw["Date Requested"].apply(assign_season).astype("Int64")
tracking_raw = tracking_raw[tracking_raw["Season"].isin([2025, 2026])].copy()

# Global filter options from full season-filtered tracking file
ALL_TAG_OPTIONS = sorted(normalize_blank_series(tracking_raw["Tag Level"]).unique().tolist())
ALL_REG_OPTIONS = sorted(normalize_blank_series(tracking_raw["Registration Status"]).unique().tolist())

# Caller selector
callers = sorted(master["Section/Column"].dropna().unique().tolist())
st.sidebar.header("Caller")
caller = st.sidebar.selectbox("Caller (Section/Column)", callers, index=0 if callers else None)

# Assigned orgs for caller
assigned = master[master["Section/Column"] == caller].copy()
assigned_org_ids = assigned["Org ID"].dropna().unique().tolist()

# Caller/org-filtered tracking
trk = tracking_raw[tracking_raw["OrganizationID"].isin(assigned_org_ids)].copy()

# Filters
st.sidebar.header("Filters")
tag_sel = st.sidebar.multiselect("Tag Level", ALL_TAG_OPTIONS, default=ALL_TAG_OPTIONS)
reg_sel = st.sidebar.multiselect("Registration Status", ALL_REG_OPTIONS, default=ALL_REG_OPTIONS)

trk = apply_blank_filter(trk, "Tag Level", tag_sel)
trk = apply_blank_filter(trk, "Registration Status", reg_sel)

# KPIs
inv_2025_all = int((trk["Season"] == 2025).sum())
inv_2026_all = int((trk["Season"] == 2026).sum())
delta_all = inv_2026_all - inv_2025_all

c1, c2, c3, c4 = st.columns(4)
c1.metric("Caller", caller)
c2.metric("Assigned Orgs", f"{len(assigned_org_ids):,}")
c3.metric("Invites (rows in season window)", f"{len(trk):,}")
c4.metric("YoY Invites (2025→2026)", f"{inv_2026_all:,}", f"{delta_all:+,}")

st.caption(
    f"Season invite windows: 2025 = {SEASON_2025_START.date()} to {SEASON_2025_END.date()} | "
    f"2026 = {SEASON_2026_START.date()} to {SEASON_2026_END.date()}"
)

st.divider()

# ----------------------------
# STACKED LAYOUT
# ----------------------------
st.subheader("Assigned Orgs")

# show canonical contact fields + any extra contact fields automatically
base_cols = ["Org ID", "Name", "Contact Name", "Email", "Phone Number"]
extra_cols = [c for c in assigned.columns if c.startswith("Extra: ")]
cols = [c for c in base_cols if c in assigned.columns] + extra_cols

assigned_show = (
    assigned[cols]
    .drop_duplicates()
    .rename(columns={"Org ID": "OrganizationID", "Name": "Organization"})
    .sort_values("Organization")
    .reset_index(drop=True)
)

st.dataframe(assigned_show, use_container_width=True, height=420)

st.divider()

st.subheader("Invite Comparison by Org (2025 vs 2026)")

piv = season_pivot_counts(trk)
piv_show = piv.copy()
if "YoY %" in piv_show.columns:
    piv_show["YoY %"] = piv_show["YoY %"].round(1)

st.dataframe(
    piv_show,
    use_container_width=True,
    height=320,
    column_config={"YoY %": st.column_config.NumberColumn(format="%.1f%%")},
)

st.divider()

st.markdown("### 🔎 Select an org to view details")

org_labels = [
    f"{int(row.OrganizationID)} — {row.Organization}"
    for row in assigned_show.itertuples(index=False)
    if pd.notna(row.OrganizationID)
]
label_to_id = {lbl: int(lbl.split(" — ", 1)[0]) for lbl in org_labels}

selected_label = st.selectbox(
    "Organization",
    options=["(Pick an org)"] + org_labels,
    index=0,
)

selected_org_id = None
if selected_label != "(Pick an org)":
    selected_org_id = label_to_id[selected_label]

st.divider()

st.subheader("📌 Org Details")

if selected_org_id is None:
    st.info("Select an org above to view details.")
else:
    mrow = assigned_show[assigned_show["OrganizationID"] == selected_org_id]
    org_name_master = mrow["Organization"].iloc[0] if not mrow.empty else ""

    inv25, inv26, total, yoy_delta, yoy_pct = org_detail_metrics(trk, selected_org_id)

    tnames = trk.loc[trk["OrganizationID"] == selected_org_id, "Organization Name"].dropna().unique().tolist()
    org_name_tracking = tnames[0] if tnames else ""
    header_name = org_name_master or org_name_tracking or f"Org {selected_org_id}"

    st.markdown(f"### {header_name}")
    st.write(f"**OrganizationID:** {selected_org_id}")

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Invites — 2025 Season", f"{inv25:,}")
    d2.metric("Invites — 2026 Season", f"{inv26:,}", f"{yoy_delta:+,}")
    d3.metric("Total (2025+2026)", f"{total:,}")
    d4.metric("YoY %", "—" if yoy_pct is None else f"{yoy_pct:.1f}%")

    st.markdown("**Contact**")

    # Show ALL contact-ish fields available in the assigned_show row
    contact_fields = [c for c in assigned_show.columns if c not in ("OrganizationID", "Organization")]
    if not mrow.empty:
        for c in contact_fields:
            val = mrow[c].iloc[0]
            if pd.isna(val) or str(val).strip() == "":
                val = "(Blank)"
            st.write(f"- **{c}**: {val}")
    else:
        st.write("(No contact info found in MASTER for this org.)")

    with st.expander("Show this org’s invite rows (filtered)"):
        org_rows = trk[trk["OrganizationID"] == selected_org_id].copy()
        show_cols = [
            "Date Requested",
            "Season",
            "Organization Name",
            "Event name",
            "Start Date Calendar Year",
            "Registration Status",
            "Tag Level",
        ]
        show_cols = [c for c in show_cols if c in org_rows.columns]
        org_rows = org_rows.sort_values("Date Requested", ascending=False)
        st.dataframe(org_rows[show_cols], use_container_width=True, height=320)