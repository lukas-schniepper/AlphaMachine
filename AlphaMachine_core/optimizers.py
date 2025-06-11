import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.covariance import LedoitWolf
from scipy.optimize import minimize
from scipy.cluster.hierarchy import linkage
from scipy.spatial.distance import pdist
from typing import Literal


def get_cov_matrix(
    returns: pd.DataFrame,
    method: Literal["ledoit-wolf", "constant-corr", "factor-model"] = "ledoit-wolf",
) -> pd.DataFrame:
    if method == "ledoit-wolf":
        lw = LedoitWolf()
        lw.fit(returns.values)
        return pd.DataFrame(
            lw.covariance_, index=returns.columns, columns=returns.columns
        )
    elif method == "constant-corr":
        std = returns.std()
        corr_matrix = returns.corr().fillna(0.0) 
        avg_corr = (
            corr_matrix.values[np.triu_indices_from(corr_matrix.values, 1)]
        ).mean()
        const_corr = np.full_like(corr_matrix, avg_corr)
        np.fill_diagonal(const_corr, 1.0)
        const_cov = np.outer(std, std) * const_corr
        return pd.DataFrame(const_cov, index=returns.columns, columns=returns.columns)
    elif method == "factor-model":
        n_factors = min(5, returns.shape[1] - 1)
        pca = PCA(n_components=n_factors)
        factors = pca.fit_transform(returns)
        factor_cov = np.cov(factors, rowvar=False)
        loadings = pca.components_.T
        specific_var = returns.var().values - np.sum(
            loadings @ factor_cov * loadings, axis=1
        )
        factor_cov_matrix = loadings @ factor_cov @ loadings.T + np.diag(specific_var)
        return pd.DataFrame(
            factor_cov_matrix, index=returns.columns, columns=returns.columns
        )
    else:
        raise ValueError(f"Unbekannte Kovarianzschätzmethode: {method}")


def get_quasi_diag(link):
    link = link.astype(int)
    sort_ix = pd.Series([link[-1, 0], link[-1, 1]])
    num_items = link[-1, 3]
    while sort_ix.max() >= num_items:
        sort_ix.index = range(0, sort_ix.shape[0] * 2, 2)
        df0 = sort_ix[sort_ix >= num_items]
        i = df0.index
        j = df0.values - num_items
        sort_ix[i] = link[j, 0]
        df1 = pd.Series(link[j, 1], index=i + 1)
        sort_ix = pd.concat([sort_ix, df1]).sort_index()
    return sort_ix.tolist()


def get_recursive_bisection(cov):
    w = pd.Series(1.0, index=cov.index)
    clusters = [cov.index]
    while clusters:
        clusters = [
            i[j:k]
            for i in clusters
            for j, k in ((0, len(i) // 2), (len(i) // 2, len(i)))
            if len(i) > 1
        ]
        for sub_cluster in clusters:
            if len(sub_cluster) <= 1:
                continue
            cov_sub = cov.loc[sub_cluster, sub_cluster]
            inv_diag = 1 / np.diag(cov_sub)
            parity_w = inv_diag * (1 / np.sum(inv_diag))
            w[sub_cluster] *= parity_w
    return w / w.sum()

def _safe_linkage_from_returns(returns: pd.DataFrame):
    """
    Erstellt (corr, link) für HRP:
      • füllt fehlende Korrelationen mit 0
      • ersetzt nicht-finite Distanzen durch den größten finiten Wert + ε
    """
    corr = returns.corr().fillna(0.0)                 # NaN-Korrelation = 0
    dist = np.sqrt(0.5 * (1 - corr))
    dist_cond = pdist(dist.values)                    # -> condensed vector

    if not np.isfinite(dist_cond).all():              # Notfall-Ersatz
        max_finite = np.nanmax(dist_cond[np.isfinite(dist_cond)]) or 1.0
        dist_cond = np.where(
            np.isfinite(dist_cond), dist_cond, max_finite + 1e-6
        )
    link = linkage(dist_cond, method="single")
    return corr, link

def optimize_portfolio(
    returns: pd.DataFrame,
    method: Literal["equal", "minvar", "ledoit-wolf", "hrp"] = "ledoit-wolf",
    cov_estimator: Literal[
        "ledoit-wolf", "constant-corr", "factor-model"
    ] = "ledoit-wolf",
    min_weight: float = 0.01,
    max_weight: float = 0.20,
    force_equal_weight: bool = False,
    debug_label: str = "",
    num_stocks: int = None,
) -> pd.Series:

    #print("⚙️ Optimizer Call")
    #print(f"   → Variante: {debug_label}")
    #print(f"   → Methode: {method}")
    #print(f"   → Kovarianzschätzer: {cov_estimator}")
    #print(f"   → Force Equal Weight: {force_equal_weight}")
    #print(f"   → Tickers: {len(returns.columns)} → {list(returns.columns[:5])}...")

    # --------------------------------------------------------
    # Cleanup: entferne Spalten ohne Varianz oder nur NaN
    # --------------------------------------------------------
    returns = (
        returns.loc[:, returns.std(ddof=0).replace(0, np.nan).notna()]  # 0-Varianz raus
                .dropna(axis=1, how="all")                              # nur-NaN raus
    )
    if returns.shape[1] == 0:
        raise ValueError("Keine gültigen Return-Daten nach Cleaning.")

    tickers = returns.columns

    # Wichtige Änderung: Wähle Top N Aktien bei optimize-subset
    # Prüfe, ob wir im optimize-subset Modus sind (anhand des debug_label)
    is_optimize_subset = "B - Optimizer selects & weights" in debug_label

    # Wenn num_stocks gesetzt ist und wir im optimize-subset Modus sind
    if is_optimize_subset and num_stocks is not None and num_stocks < len(tickers):
        cov = get_cov_matrix(returns, method=cov_estimator)
        mean_returns = returns.mean().values

        # Methode-spezifische Vorauswahl
        if method == "hrp":
            corr, link = _safe_linkage_from_returns(returns)
            sort_ix = get_quasi_diag(link)
            sorted_tickers = corr.index[sort_ix]
            cov_ = cov.loc[sorted_tickers, sorted_tickers]
            prelim_weights = get_recursive_bisection(cov_).reindex(tickers).fillna(0)
        elif method == "minvar":
            # Für MinVar nehmen wir einfach die niedrigste Volatilität
            prelim_weights = pd.Series(1 / np.diag(cov), index=tickers)
            prelim_weights = prelim_weights / prelim_weights.sum()
        elif method == "ledoit-wolf":
            # Für Ledoit-Wolf nehmen wir Sortino oder Sharpe
            std = np.sqrt(np.diag(cov))
            prelim_weights = pd.Series(mean_returns / std, index=tickers)
            prelim_weights = prelim_weights.clip(0)  # Nur positive Werte
            if prelim_weights.sum() > 0:
                prelim_weights = prelim_weights / prelim_weights.sum()
            else:
                prelim_weights = pd.Series(1 / len(tickers), index=tickers)
        else:
            # Fallback: Equal Weight
            prelim_weights = pd.Series(1 / len(tickers), index=tickers)

        # Wähle die Top N Aktien basierend auf den vorläufigen Gewichten
        selected_tickers = (
            prelim_weights.sort_values(ascending=False).head(num_stocks).index
        )

        # Reduziere die Returns auf die ausgewählten Aktien
        returns = returns[selected_tickers]
        tickers = selected_tickers

        print(f"   → Selected {num_stocks} stocks for {method}: {list(tickers)}")

    # Case: Equal weight
    if force_equal_weight:
        return pd.Series(np.ones(len(tickers)) / len(tickers), index=tickers)

    # Case: Full optimizer weighting
    cov = get_cov_matrix(returns, method=cov_estimator)
    mean_returns = returns.mean().values

    def normalize(w):
        w = np.clip(w, min_weight, max_weight)
        return w / np.sum(w)

    if method in ["ledoit-wolf", "minvar"]:

        def objective(w):
            if method == "minvar":
                return w.T @ cov @ w
            elif method == "ledoit-wolf":
                port_return = w @ mean_returns
                port_vol = np.sqrt(w.T @ cov @ w)
                return -port_return / port_vol if port_vol > 0 else np.inf

        cons = {"type": "eq", "fun": lambda w: np.sum(w) - 1}
        bounds = [(min_weight, max_weight)] * len(tickers)
        x0 = np.ones(len(tickers)) / len(tickers)
        x0 = np.clip(x0, min_weight, max_weight)   # innerhalb der [min_weight, max_weight] bringen
        x0 = x0 / x0.sum()        
        
        result = minimize(
            objective, x0, method="SLSQP", bounds=bounds, constraints=[cons]
        )
        weights = result.x if result.success else x0

    elif method == "hrp":
        corr, link = _safe_linkage_from_returns(returns)
        sort_ix = get_quasi_diag(link)
        sorted_tickers = corr.index[sort_ix]
        cov_ = cov.loc[sorted_tickers, sorted_tickers]
        raw_weights = get_recursive_bisection(cov_).reindex(tickers).fillna(0).values
        weights = normalize(raw_weights)

    else:
        raise ValueError(f"Unbekannte Optimierungsmethode: {method}")

    weights = pd.Series(weights, index=tickers)

    # === Constraint-Check ===
    out_of_bounds = ((weights < min_weight) | (weights > max_weight)).sum()
    if out_of_bounds > 0:
        print(
            f"⚠️ {out_of_bounds} Gewichte liegen außerhalb der erlaubten Bandbreite ({min_weight:.2f}–{max_weight:.2f})"
        )

    return weights
