"""Generate Data in Brief overview figures from processed RNPDNO data.

Usage
-----
    python scripts/processing/generate_figures.py

Outputs
-------
    figures/fig1_monthly_trend.pdf
    figures/fig2_state_distribution.pdf
"""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIGURES_DIR = PROJECT_ROOT / "figures"


def fig1_monthly_trend():
    """Monthly national count of persons disappeared and not located (status 7),
    2015-2025, disaggregated by sex. Excludes unknown-state records."""
    df = pd.read_csv(PROCESSED_DIR / "rnpdno_disappeared_not_located.csv")

    # Exclude sentinel/unresolved geo-codes
    cvs = df["cvegeo"].astype(str)
    keep = ~cvs.str.match(r"^(99998|99999)$")
    df = df[keep]

    monthly = df.groupby(["year", "month"])[["male", "female", "undefined"]].sum().reset_index()
    monthly["date"] = pd.to_datetime(
        monthly["year"].astype(str) + "-" + monthly["month"].astype(str) + "-15"
    )
    monthly = monthly.sort_values("date")

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(monthly["date"], monthly["male"], label="Male", color="#2166ac", linewidth=1.2)
    ax.plot(monthly["date"], monthly["female"], label="Female", color="#b2182b", linewidth=1.2)
    ax.plot(monthly["date"], monthly["undefined"], label="Undefined", color="#999999",
            linewidth=0.8, linestyle="--")

    ax.set_xlabel("Date")
    ax.set_ylabel("Persons per month")
    ax.legend(frameon=False)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()

    out = FIGURES_DIR / "fig1_monthly_trend.pdf"
    fig.savefig(out, dpi=300, bbox_inches="tight")
    fig.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {out}")


def fig2_state_distribution():
    """Cumulative total by state (status 0), 2015-2025, disaggregated by sex.
    Horizontal bar chart ranked ascending. Excludes unknown-state records."""
    df = pd.read_csv(PROCESSED_DIR / "rnpdno_total.csv")

    # Exclude sentinel/unresolved and sentinel municipalities
    cvs = df["cvegeo"].astype(str)
    keep = ~(cvs.str.match(r"^(99998|99999)$") | cvs.str.match(r"^\d{2}(998|999)$"))
    df = df[keep].copy()

    # Use cve_estado to group (avoids "coahuila" vs "coahuila de zaragoza" split)
    state_names = df.groupby("cve_estado")["state"].first()
    by_state = df.groupby("cve_estado")[["male", "female", "undefined"]].sum()
    by_state["total"] = by_state.sum(axis=1)
    by_state = by_state.sort_values("total", ascending=True)
    by_state.index = [state_names[s].title() for s in by_state.index]

    fig, ax = plt.subplots(figsize=(8, 10))
    y = range(len(by_state))
    ax.barh(y, by_state["male"], label="Male", color="#2166ac", height=0.7)
    ax.barh(y, by_state["female"], left=by_state["male"], label="Female",
            color="#b2182b", height=0.7)
    ax.barh(y, by_state["undefined"],
            left=by_state["male"] + by_state["female"],
            label="Undefined", color="#999999", height=0.7)

    ax.set_yticks(y)
    ax.set_yticklabels(by_state.index, fontsize=8)
    ax.set_xlabel("Total persons (2015\u20132025)")
    ax.legend(frameon=False, loc="lower right")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()

    out = FIGURES_DIR / "fig2_state_distribution.pdf"
    fig.savefig(out, dpi=300, bbox_inches="tight")
    fig.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {out}")


if __name__ == "__main__":
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    print("Generating figures...")
    fig1_monthly_trend()
    fig2_state_distribution()
    print("Done.")
