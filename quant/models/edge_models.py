"""Model-based edge detection: sklearn + optional LightGBM."""

from __future__ import annotations

import json
import warnings
from typing import Any

import numpy as np
import pandas as pd

from quant.core.constants import MIN_TRADES_RELIABLE


def _safe_impute(X: pd.DataFrame) -> pd.DataFrame:
    return X.fillna(X.median(numeric_only=True)).fillna(0.0)


def fit_models(
    df: pd.DataFrame,
    feature_cols: list[str],
    *,
    random_state: int = 42,
) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "reason": "", "metrics": {}, "feature_importance": {}}
    sub = df.dropna(subset=["pnl"])
    for c in feature_cols:
        if c not in sub.columns:
            out["reason"] = f"missing column {c}"
            return out
    if len(sub) < 15:
        out["reason"] = "insufficient rows for CV"
        return out

    X = _safe_impute(sub[feature_cols].astype(float))
    y_cls = sub["win"].astype(int).values
    y_reg = sub["return_risk_adj"].replace([np.inf, -np.inf], np.nan).fillna(0.0).values

    if len(np.unique(y_cls)) < 2:
        out["reason"] = "single class for win/loss"
        return out

    try:
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier, RandomForestRegressor
        from sklearn.linear_model import LogisticRegression, Ridge
        from sklearn.model_selection import TimeSeriesSplit, cross_val_score
    except ImportError:
        out["reason"] = "sklearn not installed"
        return out

    n_splits = min(5, max(2, len(sub) // 6))
    tscv = TimeSeriesSplit(n_splits=n_splits)

    lr = LogisticRegression(max_iter=200, random_state=random_state)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            aucs = cross_val_score(lr, X, y_cls, cv=tscv, scoring="roc_auc")
        out["metrics"]["logistic_roc_auc_mean"] = float(np.nanmean(aucs))
        out["metrics"]["logistic_roc_auc_std"] = float(np.nanstd(aucs))
    except Exception as e:
        out["metrics"]["logistic_error"] = str(e)

    ridge = Ridge(alpha=1.0)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r2s = cross_val_score(ridge, X, y_reg, cv=tscv, scoring="r2")
        out["metrics"]["ridge_r2_mean"] = float(np.nanmean(r2s))
        out["metrics"]["ridge_r2_std"] = float(np.nanstd(r2s))
    except Exception as e:
        out["metrics"]["ridge_error"] = str(e)

    rf_c = RandomForestClassifier(
        n_estimators=80,
        max_depth=4,
        min_samples_leaf=max(2, len(sub) // 20),
        random_state=random_state,
    )
    rf_r = RandomForestRegressor(
        n_estimators=80,
        max_depth=4,
        min_samples_leaf=max(2, len(sub) // 20),
        random_state=random_state,
    )
    try:
        rf_c.fit(X, y_cls)
        out["feature_importance"]["random_forest_classifier"] = {
            c: float(v) for c, v in zip(feature_cols, rf_c.feature_importances_, strict=False)
        }
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pr = cross_val_score(rf_c, X, y_cls, cv=tscv, scoring="roc_auc")
        out["metrics"]["rf_clf_roc_auc_mean"] = float(np.nanmean(pr))
    except Exception as e:
        out["metrics"]["rf_clf_error"] = str(e)

    try:
        rf_r.fit(X, y_reg)
        out["feature_importance"]["random_forest_regressor"] = {
            c: float(v) for c, v in zip(feature_cols, rf_r.feature_importances_, strict=False)
        }
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pr2 = cross_val_score(rf_r, X, y_reg, cv=tscv, scoring="r2")
        out["metrics"]["rf_reg_r2_mean"] = float(np.nanmean(pr2))
    except Exception as e:
        out["metrics"]["rf_reg_error"] = str(e)

    gbc = GradientBoostingClassifier(
        n_estimators=60,
        learning_rate=0.08,
        min_samples_leaf=max(2, len(sub) // 25),
        random_state=random_state,
    )
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            gbc.fit(X, y_cls)
        out["feature_importance"]["gradient_boosting_classifier"] = {
            c: float(v) for c, v in zip(feature_cols, gbc.feature_importances_, strict=False)
        }
        auc_g = cross_val_score(gbc, X, y_cls, cv=tscv, scoring="roc_auc")
        out["metrics"]["gb_clf_roc_auc_mean"] = float(np.nanmean(auc_g))
    except Exception as e:
        out["metrics"]["gb_clf_error"] = str(e)

    try:
        import lightgbm as lgb  # type: ignore

        lgb_c = lgb.LGBMClassifier(
            n_estimators=80,
            max_depth=4,
            min_child_samples=max(5, len(sub) // 15),
            random_state=random_state,
            verbose=-1,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            lgb_c.fit(X, y_cls)
        out["feature_importance"]["lightgbm_classifier"] = {
            c: float(v) for c, v in zip(feature_cols, lgb_c.feature_importances_, strict=False)
        }
    except Exception:
        pass

    try:
        p_win = rf_c.predict_proba(X)[:, 1]
        out["predicted_score_stats"] = {
            "mean": float(np.mean(p_win)),
            "std": float(np.std(p_win)),
        }
    except Exception:
        pass

    out["ok"] = True
    out["n_samples"] = len(sub)
    out["unreliable_n_lt_30"] = len(sub) < MIN_TRADES_RELIABLE
    return out


def model_metrics_to_jsonable(d: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(d, default=str))
