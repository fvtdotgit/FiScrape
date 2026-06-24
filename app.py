# app.py
"""FiScrape — Streamlit UI

UI boundaries:
- No direct network calls here. All data access goes through app_core.* facades.
- Keep formatting in app_format.py; keep compute pure in fiscrape_core.*.
- Surface warnings non-blocking; show friendly empty states.
- Avoid mutating st.session_state[widget_key] after a widget is instantiated.
"""

import re
import time

import streamlit as st
from streamlit_autorefresh import st_autorefresh

import app_core as ac
import app_format as af
from fiscrape_core import (
    get_financial_statements,
    hasRetrievablePrice,
    list_move_inplace,
    resolve_timeframe,
)

# Wide page config
st.set_page_config(layout="wide")

# Hide the arrow in st.metric
st.write(
    """
    <style>
    [data-testid="stMetricDelta"] svg {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Session State Initialization (must run before any ac.* usage) ---
# Contract (created once on app load; do not overwrite widget-bound keys later):
# - portfolios: dict[str, list[str]]          # user portfolios → tickers
# - portfolio_objs: dict[str, Portfolio]       # cached Portfolio objects
# - portfolio_order: list[str]                 # UI display order for portfolios
# - selected_portfolio: Optional[str]          # current working portfolio (sidebar)
# - last_runs: dict                            # scrape/fundamentals cache keys
# - pf_settings: dict                          # per-portfolio refresh/workers config
# - profiles: dict                             # cached profile blobs (used by ac stash)
# - refresh_nonce: int                         # manual cache-busting bump from UI
_defaults = {
    "portfolios": {},
    "portfolio_objs": {},
    "portfolio_order": [],
    "selected_portfolio": None,
    "last_runs": {},
    "pf_settings": {},
    "profiles": {},
    "refresh_nonce": 0,
}

for k, v in _defaults.items():
    st.session_state.setdefault(k, v)

# --- Add/Edit Portfolio ---
st.sidebar.title("Portfolio Manager")

st.sidebar.subheader("Add/Edit Portfolio")
new_portfolio = st.sidebar.text_input("Enter portfolio name", key="new_portfolio").strip()
if st.sidebar.button("Confirm", key="new_portfolio_confirm"):
    if new_portfolio:
        if new_portfolio not in st.session_state.portfolios:
            st.session_state.portfolios[new_portfolio] = []
            st.session_state.portfolio_order.append(new_portfolio)
            st.session_state.selected_portfolio = new_portfolio  # <— select it
            st.sidebar.success(f"Added portfolio: {new_portfolio}")
            st.rerun()
        else:
            st.sidebar.warning("Portfolio already exists.")
    else:
        st.sidebar.warning("Please enter a portfolio name.")

# --- Edit Portfolios (collapsible) ---
with st.sidebar.expander("Edit/Arrange Portfolios", expanded=False):
    po = st.session_state.portfolio_order

    if not po:
        st.info("No portfolios yet — add one above.")
    else:
        # Quick sort controls (like in Edit/Arrange Tickers)
        cA, cB = st.columns(2)
        if cA.button("Sort A→Z", key="po_sort_az"):
            po.sort()
            st.rerun()
        if cB.button("Reverse", key="po_sort_rev"):
            po.reverse()
            st.rerun()

        # Inline arrows for quick nudges (up/down)
        # NOTE: We render from a .copy() so index computations remain stable
        # while clicking buttons that modify 'po' in-place via list_move_inplace.
        for i, name in enumerate(po.copy()):
            c1, c2, c3 = st.columns([5, 1, 1])
            c1.write(f"{i + 1}. {name}")
            if c2.button("⬆", key=f"po_up_{name}") and i > 0:
                list_move_inplace(po, i, i - 1)
                st.rerun()
            if c3.button("⬇", key=f"po_down_{name}") and i < len(po) - 1:
                list_move_inplace(po, i, i + 1)
                st.rerun()

        st.markdown("---")

        label_map = af.numbered_label_map(po)

        edit_sel = st.selectbox(
            "Choose portfolio to edit",
            options=po,
            format_func=lambda n: label_map.get(n, n),  # show "1. Core", "2. Growth", ...
            key="po_edit_sel",
            help="Independent of the 'Select Working Portfolio' above.",
        )
        idx = po.index(edit_sel)

        new_pos = st.number_input(
            "Move to position",
            min_value=1,
            max_value=len(po),
            value=idx + 1,
            step=1,
            key="po_pos_picker",
        )
        if st.button("Move", key="po_move_to_pos_btn"):
            list_move_inplace(po, idx, int(new_pos) - 1)
            st.rerun()

        new_name = st.text_input(
            "Rename chosen portfolio", value=edit_sel, key="po_rename_name"
        ).strip()

        # RENAME invariants:
        # - Preserve tickers list by moving the dict value.
        # - Keep display order stable by replacing the old name at its index.
        # - If current working portfolio equals old name, switch it to the new name.
        if st.button("Rename", key="po_rename_btn"):
            if new_name and new_name != edit_sel:
                if new_name in st.session_state.portfolios:
                    st.warning("That portfolio name already exists. Pick a different name.")
                else:
                    # Move tickers list
                    st.session_state.portfolios[new_name] = st.session_state.portfolios.pop(
                        edit_sel
                    )

                    # Update display order
                    po[po.index(edit_sel)] = new_name

                    # If the currently selected working portfolio was the old name, update it
                    if st.session_state.selected_portfolio == edit_sel:
                        st.session_state.selected_portfolio = new_name

                    # Migrate backend objects
                    po_map = st.session_state.portfolio_objs
                    if edit_sel in po_map:
                        po_map[new_name] = po_map.pop(edit_sel)

                    # Migrate per-portfolio settings
                    pf_cfg = st.session_state.pf_settings
                    if edit_sel in pf_cfg:
                        pf_cfg[new_name] = pf_cfg.pop(edit_sel)

                    # Clear last_run stamps for both old and new names (safety)
                    lr = st.session_state.last_runs
                    lr.pop((edit_sel, "scrape"), None)
                    lr.pop((edit_sel, "analyze"), None)
                    lr.pop((new_name, "scrape"), None)
                    lr.pop((new_name, "analyze"), None)

                    # Purge any lingering per-portfolio widget keys
                    ac._purge_portfolio_widget_keys(edit_sel)
                    ac._purge_portfolio_widget_keys(new_name)

                    # Keep the editor pointing at the newly renamed portfolio
                    st.session_state["po_edit_sel"] = new_name

                    st.success(f"Portfolio renamed to {new_name}")
                    st.rerun()
            else:
                st.info("No changes made.")

# --- Multi-select Delete Portfolios ---  ✅ also clear backend + memo
st.sidebar.subheader("Delete Portfolios")

with st.sidebar.expander("Delete portfolios", expanded=False):
    all_ports = sorted(st.session_state.portfolios.keys())

    sel_ports, confirmed, clicked = af.render_bulk_delete_portfolios_controls(
        all_ports, key_prefix="bulk_del_ports", title="Delete portfolios"
    )

    if clicked:
        # 1) Remove selected from state
        for p in sel_ports:
            st.session_state.portfolios.pop(p, None)

        # 2) Keep order list in sync (if you track one)
        if "portfolio_order" in st.session_state:
            st.session_state.portfolio_order = [
                p for p in st.session_state.portfolio_order if p not in sel_ports
            ]

        # 3) If active portfolio was deleted (or none remains), clear it
        cur = st.session_state.get("active_portfolio")
        if cur in sel_ports or not st.session_state.portfolios:
            st.session_state["active_portfolio"] = None  # allow zero portfolios

        st.success("Deleted: " + (", ".join(sel_ports) if sel_ports else "(none)"))
        st.rerun()

st.sidebar.divider()

# --- Portfolio Selection via Selectbox ---
st.sidebar.title("Portfolio Selector")

ac.ensure_selection()
if st.session_state.portfolio_order:
    names = st.session_state.portfolio_order
    default_index = names.index(st.session_state.selected_portfolio)
    name_labels = {n: f"{i + 1}. {n}" for i, n in enumerate(names)}

    choice = st.sidebar.selectbox(
        "Select Working Portfolio",
        options=names,
        index=default_index,
        format_func=lambda n: name_labels.get(n, n),
        key="portfolio_select",
    )

    st.session_state.selected_portfolio = choice
else:
    st.sidebar.info("No portfolios available.")

st.sidebar.divider()

# --- Ticker Manager ---
st.sidebar.title("Ticker Manager")

if st.session_state.selected_portfolio:
    portfolio = st.session_state.selected_portfolio
    tickers = st.session_state.portfolios.get(portfolio, [])

    # NOTE:
    # - We keep the canonical ticker list simple: uppercase strings, no duplicates.
    # - Dedupe is order-preserving; first occurrence wins.
    # - Availability checks (hasRetrievablePrice) are advisory; warnings are non-blocking.

    st.sidebar.markdown(f"## Current Portfolio: {portfolio}")

    # Auto-dedupe existing list (keeps first occurrence)
    removed_dupes = ac.dedupe_portfolio(portfolio)
    if removed_dupes:
        st.sidebar.info(f"Removed {removed_dupes} duplicate ticker(s).")

    # -------------------- Ticker Add (single or batch) — one-shot form with nonce --------------------
    # A) Build a stable nonce per-portfolio to force a fresh form after submit
    _nonce_key = f"add_form_nonce_{portfolio}"
    if _nonce_key not in st.session_state:
        st.session_state[_nonce_key] = 0
    _form_nonce = st.session_state[_nonce_key]

    # B) Form — single submit path; no on_change; no direct widget state mutation
    with st.sidebar.form(key=f"add_form_{portfolio}_{_form_nonce}"):
        add_input = st.text_input(
            "Enter ticker(s)",
            placeholder="AAPL, GOOGL, AMZN …",
            # no key on purpose; we reset via the form nonce
        )
        add_submit = st.form_submit_button("Add")

    # C) Submit handler — parse, validate, mutate once
    # SUBMIT RULES:
    # 1) Parse on commas/semicolons/pipes/whitespace.
    # 2) Uppercase + order-preserving dedupe.
    # 3) Filter out already-present tickers; append only new ones.
    # 4) Show service-edge warnings but do not block insertion.
    if add_submit:
        raw = (add_input or "").strip()

        if not raw:
            st.sidebar.warning("Enter at least one ticker symbol.")
        else:
            # Parse on commas/semicolons/pipes/whitespace, strip, uppercase, order-preserving dedupe
            parts = [p.strip().upper() for p in re.split(r"[,\s;|]+", raw) if p.strip()]
            seen = set()
            parts = [p for p in parts if not (p in seen or seen.add(p))]

            added, dupes, no_price = [], [], []

            # `tickers` should be your canonical list for this portfolio (existing in your scope)
            for sym in parts:
                if sym in tickers:
                    dupes.append(sym)
                    continue
                try:
                    ok = hasRetrievablePrice(sym)  # uses Services boundary
                except Exception:
                    # Be permissive if the check raises; or set ok=False if you prefer strict
                    ok = True
                if not ok:
                    no_price.append(sym)
                    continue
                tickers.append(sym)
                added.append(sym)

            # User feedback
            if added:
                st.sidebar.success(f"Added {len(added)}: {', '.join(added)}")
            if dupes:
                st.sidebar.info(f"Already in {portfolio}: {', '.join(dupes)}")
            if no_price:
                st.sidebar.error(f"No retrievable price: {', '.join(no_price)}")

            # Optional: belt-and-suspenders dedupe once per submit (not on every render)
            removed_dupes = ac.dedupe_portfolio(portfolio)
            if removed_dupes and not added:
                st.sidebar.info(f"Removed {removed_dupes} duplicate ticker(s).")

        # D) Rotate nonce to reset the form (clears the input without touching widget state)
        st.session_state[_nonce_key] += 1
        st.rerun()

    # Edit / Arrange Tickers (clutter-free UI)
    with st.sidebar.expander("Edit/Arrange Tickers", expanded=False):
        if not tickers:
            st.info("No tickers yet — add some below.")
        else:
            # Quick sort controls
            cA, cB = st.columns(2)
            if cA.button("Sort A→Z", key=f"tick_sort_az_{portfolio}"):
                tickers.sort()
                st.rerun()
            if cB.button("Reverse", key=f"tick_sort_rev_{portfolio}"):
                tickers.reverse()
                st.rerun()

            # Numbered labels for display only
            label_map = af.numbered_label_map(tickers)

            # A) Inline arrows for quick nudges (up/down) + delete
            for i, t in enumerate(tickers.copy()):
                c1, c2, c3, c4 = st.columns([4, 1, 1, 1])
                c1.write(label_map.get(t, t))  # shows "1. AAPL", "2. MSFT", ...
                # NEW: show per-ticker warnings (if any)
                warns = ac.get_ticker_warnings(portfolio, t)
                if warns:
                    with c1.expander(f"⚠️ {len(warns)} issue(s)", expanded=False):
                        af.render_warnings(warns, title="", compact=True)
                        inst = ac._pf_inst(portfolio, t)
                        if (
                            inst
                            and hasattr(inst, "clear_warnings")
                            and st.button(
                                f"Clear {t} warnings", key=f"clr_warn_{portfolio}_{t}_{i}"
                            )
                        ):
                            inst.clear_warnings()  # type: ignore[attr-defined]
                            st.rerun()

                if c2.button("⬆", key=f"tup_{portfolio}_{i}") and i > 0:
                    list_move_inplace(tickers, i, i - 1)
                    st.rerun()
                if c3.button("⬇", key=f"tdown_{portfolio}_{i}") and i < len(tickers) - 1:
                    list_move_inplace(tickers, i, i + 1)
                    st.rerun()
                if c4.button("✖", key=f"tdel_{portfolio}_{i}"):
                    tickers.pop(i)
                    st.rerun()

            st.markdown("---")

            # B) Absolute reposition (pick a ticker → target position)
            sel_ticker = st.selectbox(
                "Reposition ticker",
                options=tickers,
                format_func=lambda t: label_map.get(t, t),  # numbered in the UI
                key=f"tick_sel_{portfolio}",
            )
            new_pos = st.number_input(
                "Move to position",
                min_value=1,
                max_value=len(tickers),
                value=tickers.index(sel_ticker) + 1,
                step=1,
                key=f"tick_pos_{portfolio}",
            )
            if st.button("Move", key=f"tick_move_btn_{portfolio}"):
                src = tickers.index(sel_ticker)
                list_move_inplace(tickers, src, int(new_pos) - 1)
                st.rerun()

    # Collapsed bulk Move tools (kept out of the way)
    with st.sidebar.expander("Move tickers", expanded=False):
        all_ports = sorted(st.session_state.portfolios.keys())
        others = [p for p in all_ports if p != portfolio]

        selected_pf, dest, clicked = af.render_bulk_move_controls(
            portfolio, tickers, others, key_prefix=f"bm_sb_{portfolio}", title="Move tickers"
        )

        if clicked:
            if not selected_pf:
                st.warning("Select at least one ticker to move.")
            elif not dest:
                st.info("No other portfolios available. Create one first.")
            else:
                # RULES:
                # - Do not duplicate in the destination; only move items not already present.
                # - Removal from source happens only for items we actually append to dest.
                # - 'skipped' shows the user which selections were already in the destination.

                dest_list = st.session_state.portfolios.setdefault(dest, [])
                to_move = [x for x in selected_pf if x not in dest_list]
                for x in to_move:
                    if x in tickers:
                        tickers.remove(x)
                    dest_list.append(x)
                skipped = set(selected_pf) - set(to_move)
                if to_move:
                    st.success(f"Moved {len(to_move)} ticker(s) → {dest}")
                if skipped:
                    st.info(f"Skipped (already in {dest}): {', '.join(sorted(skipped))}")
                st.rerun()

else:
    st.sidebar.info("Please create a portfolio before adding tickers.")

# --- Always-on refresh at bottom of sidebar ---
if st.sidebar.button("🔄 Refresh data", use_container_width=True):
    # 1) Bust snapshot caches immediately (changes the cache key)
    st.session_state["refresh_nonce"] += 1

    # 2) (Optional) also clear pipeline timestamps so profile/fundamentals can re-run
    sel_now = st.session_state.selected_portfolio
    if sel_now:
        st.session_state.last_runs.pop((sel_now, "scrape"), None)
        st.session_state.last_runs.pop((sel_now, "analyze"), None)

    # 3) Rerun the app to reflect fresh data
    st.rerun()

# --- Main Page: FiScrape + Tabs ---
st.title("FiScrape")
st.caption("_A simple dashboard to visualize and analyze public market data._")

selected_pf = st.session_state.selected_portfolio
if not selected_pf:
    st.info("Create or select a portfolio in the sidebar to get started.")
    st.stop()

# --- Global, page-level auto-refresh ---
rcfg = ac.get_pf_settings(selected_pf)["refresh"]
live_key = f"live_{selected_pf}"
with st.sidebar:
    _refresh_holder = st.empty()
    if st.session_state.get(live_key, bool(rcfg.get("live", False))):
        with _refresh_holder:
            st_autorefresh(
                interval=int(ac.compute_ui_refresh_secs(selected_pf)) * 1000,
                key=f"ui_live_{selected_pf}",
            )
    # if live is off, leave the slot empty

# --- Backend objects + tickers ---
pf = ac.get_portfolio(selected_pf)
tickers = st.session_state.portfolios.get(selected_pf, [])

# Pre-UI prune invalids
invalid = [t for t in list(tickers) if not hasRetrievablePrice(t)]
if invalid:
    tickers[:] = [t for t in tickers if t not in invalid]
    st.session_state.portfolios[selected_pf] = tickers
    choose_key = f"ov_choose_{selected_pf}"
    if st.session_state.get(choose_key) in invalid:
        st.session_state[choose_key] = tickers[0] if tickers else None
    st.warning(f"Removed tickers with no retrievable price: {', '.join(invalid)}.")

# Run pipeline
with st.spinner("Fetching data…"):
    rcfg_for_pipeline = ac.get_pf_settings(selected_pf)["refresh"]
    pipeline_bucket = int(time.time() // int(rcfg_for_pipeline.get("ttl_pipeline", 120)))
    ac.run_pipeline_for_portfolio(selected_pf, tickers, _pipeline_bucket=pipeline_bucket)

# The pipeline may have further filtered tickers; refresh our local view
tickers = st.session_state.portfolios.get(selected_pf, [])

# Header
hdr1, hdr2 = st.columns([2, 1])
hdr1.markdown(f"### Portfolio: **{selected_pf}**")
hdr2.markdown(f"### Tickers: **{len(tickers)}**")

# Keep selection valid, then render the selectbox once (now safe to create the widget)
chosen = ac.ensure_ticker_choice(selected_pf, tickers)
if tickers:
    st.selectbox(
        "Choose a ticker",
        options=tickers,
        index=(tickers.index(chosen) if chosen in tickers else 0),
        key=f"ov_choose_{selected_pf}",
        help="This selection is shared across tabs.",
    )
else:
    pass

# Resolve normalized symbol once for all tabs (shared by Settings/Overview/etc.)
inst_pf = None
ticker_sym = None
if tickers:
    inst_pf = ac._pf_inst(selected_pf, chosen)
    ticker_sym = (inst_pf.get_attr("ticker") if inst_pf else chosen) or chosen
else:
    ticker_sym = ""  # Settings remains accessible with empty timings


# Always render tabs so Settings remains reachable even when no tickers are selected
tabs = st.tabs(["Overview", "Technicals", "Fundamentals", "Settings"])

# ========================= OVERVIEW =========================
with tabs[0]:
    if not tickers:
        st.info(
            "No tickers in this portfolio yet. Add some in the sidebar. "
            "(Settings tab is available to preconfigure refresh/scrape.)"
        )
    else:
        # ------- Resolve instance & display name -------
        warns = ac.get_ticker_warnings(selected_pf, ticker_sym)
        if warns:
            with st.expander(f"⚠️ {len(warns)} issue(s) for {ticker_sym}", expanded=False):
                af.render_warnings(warns, title="", compact=True)
                if (
                    inst_pf
                    and hasattr(inst_pf, "clear_warnings")
                    and st.button("Clear warnings", key=f"clear_warn_{selected_pf}_{ticker_sym}")
                ):
                    inst_pf.clear_warnings()  # type: ignore[attr-defined]
                    st.rerun()

        # --- Read current chart params from state (pre-widgets) ---
        period_choice, interval_choice, plot_style = ac.get_chart_selections(selected_pf)

        # Normalize + map once (invalid -> 1D / 1m)
        tf = resolve_timeframe(period_choice, interval_choice)
        api_period = tf["apiPeriod"]  # "1d" | "1mo" | ...
        api_interval = tf["apiInterval"]  # "1m" | "1wk" | ...

        # --- cache-busting nonce for manual Refresh (elsewhere, bump it on button click) ---
        if "refresh_nonce" not in st.session_state:
            st.session_state.refresh_nonce = 0

        # --- Normalize active portfolio (must run after sidebar actions) ---
        ports = st.session_state.setdefault("portfolios", {})  # dict[str, list[str]]
        active = st.session_state.get("active_portfolio")

        # Empty state: allow zero portfolios
        if not ports:
            st.info("No portfolios yet. Create one in the sidebar to get started.")
            st.stop()

        # Ensure active points to a real portfolio
        if not active or active not in ports:
            st.session_state["active_portfolio"] = sorted(ports.keys())[0]
            active = st.session_state["active_portfolio"]

        # ------- Data fetch (UI → Core boundary) -------
        # - profile: pipeline-cached snapshot; no direct network in app.py
        # - metrics/hist/info: live snapshot composed behind app_core (services handle yfinance)
        profile = ac.get_profile_snapshot(
            selected_pf, ticker_sym
        )  # one-and-done profile from pipeline

        info, metrics, hist = ac.get_metrics_and_history(
            selected_pf,
            ticker_sym,
            api_period,  # <-- pass canonical API period
            api_interval,  # <-- pass canonical API interval
            include_history=True,
            refresh_nonce=st.session_state["refresh_nonce"],  # manual cache-bust knob
        )

        # Preferred display name precedence: live longName → raw symbol (fallback)
        disp_name = info.get("longName") or ticker_sym
        st.subheader(f"**{disp_name} ({ticker_sym})**")

        # ------- Top metrics -------
        cols = st.columns(4)

        # 1) Current price, after-hour/pre-market price, and changes ---
        with cols[0]:
            ui = af.render_price_section(info)
            st.metric(
                label=ui.get("label"),
                value=ui.get("value") or "—",
                delta=ui.get("delta") or None,
                delta_color=ui.get("deltaColor"),
                help=ui.get("help") or None,
            )

        # 2) Market Cap (Equity) / Net Assets (ETF)
        ui = af.render_cap_section(info, metrics)

        cols[1].metric(
            label=ui["label"],
            value=af.format_abbrev(ui["value_raw"]) if ui["value_raw"] is not None else "—",
            delta=af.format_abbrev(ui["delta"]) if ui["delta"] is not None else "—",
            delta_color="off",
            help=ui["help"],
        )

        # 3) Volume metric (metrics-first; ETF-safe fallback)
        vol = info.get("regularMarketVolume") or info.get("volume")
        val_txt = af.format_abbrev(vol) if isinstance(vol, (int, float)) else "—"
        ui = af.render_volume_section(info, metrics)

        cols[2].metric("Volume", val_txt, delta=ui["delta"], delta_color="off", help=ui["help"])

        # 4) 52-week range (absolute endpoints + relative context)
        price_val = metrics.get("price")
        ui = af.render_52w_section(info, price_val)
        cols[3].metric(
            label=ui["label"],
            value=ui["value"],
            delta=ui["delta"],
            delta_color=ui["deltaColor"],
            help=ui["help"],
        )

        st.write("")

        # --- Chart card ---------------------------------------------------
        with st.container(border=True):
            # Controls (Overview; unchanged UX)
            p_label, i_label, style_label = af.ov_chart_controls(selected_pf)

            # Body: chart (left) + compact stats (right)
            left, right = st.columns([5, 1], vertical_alignment="top")

            with left:
                # app.py (Overview section)
                chart, msg = af.ov_render_history_chart(
                    hist=hist,
                    style_label=style_label,
                    show_bottom_x_axis=True,
                    overlays=None,
                )
                # ... render `chart` as before; no other changes required
                if chart is not None:
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info(msg)

            with right:
                for item in af.ov_chart_metric_items(
                    hist,
                    fmt_price=getattr(af, "fmt_price", None),
                    fmt_percent_signed=getattr(af, "fmt_percent_signed", None),
                    fmt_percent_plain=getattr(af, "fmt_percent_plain", None),
                ):
                    st.metric(
                        label=item["label"],
                        value=item["value"],
                        delta=item["delta"],
                        help=item["help"],
                        **({"delta_color": item["delta_color"]} if "delta_color" in item else {}),
                    )

        # Statistics

        c1, c2, c3 = st.columns([1, 1, 1])
        if info.get("quoteType") == "ETF":
            with c1:
                st.caption("Nani")
                st.write("kore")

        else:
            with c1:
                st.caption("Close / Open")
                st.write(
                    f"{af.format_number(info.get('previousClose'), 2)} - {af.format_number(info.get('open'), 2)}"
                )
            with c2:
                st.caption("Bid / Ask")
                st.write(
                    f"{af.format_number(info.get('bid'), 2)} - {af.format_number(info.get('ask'), 2)}"
                )
            with c3:
                st.caption("Beta (5Y Monthly)")
                st.write(af.format_number(info.get("beta"), 2))

        c4, c5, c6 = st.columns([1, 1, 1])
        if info.get("quoteType") == "ETF":
            pass

        else:
            with c4:
                st.caption("Price/Earnings")
                st.write(af.format_number(info.get("trailingPE"), 2))
            with c5:
                st.caption("Price/Sales")
                st.write(af.format_number(info.get("priceToSalesTrailing12Months"), 2))
            with c6:  # Turn into EPS instead
                st.caption("Earnings Date")
                st.write(af.format_date(info.get("exDividendDate")))

        # ------- Company / Fund Description -------
        af.ov_render_entity_description(info or {})


# ========================= TECHNICALS =========================
with tabs[1]:
    if not tickers:
        st.info("Add tickers to see technical charts. (You can still configure Settings.)")
    else:
        st.subheader("Technical Chart")

# ========================= FUNDAMENTALS =========================
with tabs[2]:
    st.subheader("Financial Statements")

    if not tickers:
        st.info("Add tickers to see financial statements.")
    else:
        # Persist/reflect portfolio settings
        s = ac.get_pf_settings(selected_pf)

        # Toggle: Yearly vs Quarterly (sets s["period"])
        show_quarterly = st.toggle(
            "Show quarterly (TTM/quarterly view)",
            value=(s.get("period") != "yearly"),
            key=f"funds_period_toggle_{selected_pf}",
            help="Switch between latest annual statements and last 4 quarters (TTM).",
        )
        s["period"] = "quarterly" if show_quarterly else "yearly"

        st.caption(
            "Period affects growth and some ratios; overview metrics are typically unaffected."
        )

        # Use the single, currently-selected ticker from the dropdown above
        sym = chosen
        inst = ac._pf_inst(selected_pf, sym)
        info = (inst.get_attr("ticker_info") or {}) if inst else {}
        is_etf = info.get("quoteType") == "ETF"

        # ETF path: no statements
        if is_etf:
            st.warning("Fundamentals are typically unavailable for ETFs (statements not provided).")
        else:
            # Per-document selector for this ticker
            DOC_KEY_MAP = {
                "Income Statement": ("income", "q_income"),
                "Balance Sheet": ("balance", "q_balance"),
                "Cash Flow": ("cashflow", "q_cashflow"),
            }
            doc_choice = st.selectbox(
                f"Document — {sym}",
                options=list(DOC_KEY_MAP.keys()),
                index=0,
                key=f"doc_choice_{selected_pf}_{sym}",
            )
            annual_key, quarterly_key = DOC_KEY_MAP[doc_choice]
            key = quarterly_key if show_quarterly else annual_key

            # Fetch statements via Services (single boundary to yfinance)
            try:
                stmnts = get_financial_statements(sym)
            except Exception as e:
                st.error(f"Failed to load statements for {sym}: {e}")
            else:
                df = stmnts.get(key)
                if df is None or df.empty:
                    st.info(
                        f"No {doc_choice.lower()} data for this ticker "
                        f"({'quarterly' if show_quarterly else 'yearly'}). "
                        "Statements may be missing or delayed."
                    )
                else:
                    # Columns are newest→oldest (Services normalizes)
                    st.dataframe(df)

# ========================= SETTINGS =========================
with tabs[3]:
    st.subheader("Refresh Configuration")
    s = ac.get_pf_settings(selected_pf)
    r = s["refresh"]

    live_key = f"live_{selected_pf}"
    st.session_state.setdefault(live_key, bool(r.get("live", False)))
    st.session_state.pop(f"set_live_{selected_pf}", None)

    st.checkbox(
        "Live mode (auto-refresh)",
        key=live_key,
        help=(
            "When on, the app auto-refreshes at the fastest of the intervals below. "
            "• Price uses Live quote • Intraday charts use Intraday history • "
            "Daily/weekly/monthly charts use End-of-day history • Fundamentals/profile "
            "refresh on Pipeline."
        ),
    )
    r["live"] = bool(st.session_state[live_key])

    colA, colB = st.columns(2)

    with colA:
        title_quote = "Live quote — refresh every (sec)"
        rec_quote = ac.get_lane_timing(selected_pf, ticker_sym, "quote")
        ttl_quote = int(r.get("ttl_quote", 10))

        label_quote = af.compose_refresh_slider_label(title_quote, rec_quote, ttl_quote)
        r["ttl_quote"] = st.slider(
            label_quote,  # inline title + badge
            min_value=2,
            max_value=15,
            value=int(max(2, r.get("ttl_quote", 10))),
            step=1,
            help=af.build_lane_help(
                rec_quote, ttl_quote, extras="inline: Updates last trade and derived metrics."
            ),
            key=f"set_ttl_quote_{selected_pf}",
            label_visibility="visible",  # show label so (i) appears; no extra header row needed
        )

    with colB:
        title_intraday = "Intraday history — refresh every (sec)"
        rec_intraday = ac.get_lane_timing(selected_pf, ticker_sym, "intraday")
        ttl_intraday = int(max(60, r.get("ttl_intraday", 90)))

        label_intraday = af.compose_refresh_slider_label(title_intraday, rec_intraday, ttl_intraday)
        r["ttl_intraday"] = st.slider(
            label_intraday,
            min_value=60,
            max_value=180,
            value=ttl_intraday,
            step=10,
            help=af.build_lane_help(
                rec_intraday, ttl_intraday, extras="inline: Minute bars for Overview/Technicals."
            ),
            key=f"set_ttl_intraday_{selected_pf}",
            label_visibility="visible",
        )

    title_eod = "End-of-day/weekly/monthly history — refresh every (sec)"
    rec_eod = ac.get_lane_timing(selected_pf, ticker_sym, "eod")
    ttl_eod = int(max(300, r.get("ttl_eod", 1800)))

    label_eod = af.compose_refresh_slider_label(title_eod, rec_eod, ttl_eod)
    r["ttl_eod"] = st.slider(
        label_eod,
        min_value=300,
        max_value=3600,
        value=ttl_eod,
        step=30,
        help=af.build_lane_help(rec_eod, ttl_eod, extras="inline: Daily/weekly/monthly bars."),
        key=f"set_ttl_eod_{selected_pf}",
        label_visibility="visible",
    )

    # --- Pipeline / Profile (one-and-done) — same inline format as others ---
    title_profile = "Profile snapshot — refresh every (sec)"
    rec_profile = ac.get_lane_timing(selected_pf, ticker_sym, "profile")
    ttl_profile = int(r.get("ttl_pipeline", 3600))

    label_profile = af.compose_refresh_slider_label(title_profile, rec_profile, ttl_profile)
    r["ttl_pipeline"] = st.slider(
        label_profile,
        min_value=600,
        max_value=86400,  # ← keep your existing bounds if different
        value=ttl_profile,
        step=300,
        help=af.build_lane_help(
            rec_profile,
            ttl_profile,
            extras="inline: One-and-done profile cache written during pipeline runs.",
        ),
        key=f"set_ttl_pipeline_{selected_pf}",
        label_visibility="visible",  # label row visible → help (i) shows; no extra header rows
    )

    # Optional gentle guards
    TTL_QUOTE_MIN = 2
    TTL_INTRADAY_MIN = 60
    if r["ttl_quote"] < TTL_QUOTE_MIN:
        st.info("Sub-2s quote refresh can be aggressive and may hit rate limits.")
    if r["ttl_intraday"] < TTL_INTRADAY_MIN:
        st.info("Intraday history under 60s is aggressive; 60s+ is recommended.")

    # Show derived interval AND which knob is limiting right now
    derived = ac.compute_ui_refresh_secs(selected_pf)
    itv = st.session_state.get(f"ov_interval_{selected_pf}")  # e.g. '1m' or '1D'
    intraday = ac.is_intraday_interval(itv or "1m")

    active_hist_ttl = r["ttl_intraday"] if intraday else r["ttl_eod"]
    limiting_val = min(r["ttl_quote"], active_hist_ttl, r["ttl_pipeline"])
    if limiting_val == r["ttl_quote"]:
        limiting_name = "Live quote"
    # 'derived' comes from app_core.compute_ui_refresh_secs():
    #   min(ttl_quote, active_history_ttl (intraday/EOD), ttl_pipeline)
    # We label the limiting component for transparency.
    elif limiting_val == active_hist_ttl and intraday:
        limiting_name = "Intraday history"
    elif limiting_val == active_hist_ttl and not intraday:
        limiting_name = "End-of-day history"
    else:
        limiting_name = "Pipeline"

    st.caption(f"Auto-refresh every **{derived}s** (currently limited by **{limiting_name}**).")

    st.subheader("Scrape Configuration")
    s["parallel"] = st.checkbox(
        "Parallel scraping", value=s["parallel"], key=f"set_parallel_{selected_pf}"
    )
    s["workers"] = st.slider(
        "Max workers",
        min_value=2,
        max_value=16,
        value=int(s["workers"]),
        key=f"set_workers_{selected_pf}",
    )
    st.caption("These settings apply to scraping for this portfolio.")
    # NOTE: These knobs affect scrape orchestration in app_core/portfolio,
    # not UI refresh directly. UI cadence still respects the limiting TTL above.
