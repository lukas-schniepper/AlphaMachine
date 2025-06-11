# AlphaMachine_core/risk_overlay/overlay.py
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Tuple # Tuple hinzugefügt
import importlib # Für das dynamische Laden in der Factory (wahrscheinlich)

# Deine Factory zum Laden von Indikatoren
from .indicator_factory import load_indicator # Stelle sicher, dass diese Datei existiert und funktioniert

# Normalisierungsfunktionen (angenommen in utils.normalization)
from .utils.normalization import min_max_scaler, z_score_scaler, binary_scaler

class RiskOverlay:
    def __init__(self, overlay_config_path: str):
        try:
            with open(overlay_config_path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Overlay configuration file not found: {overlay_config_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Error decoding JSON from configuration file: {overlay_config_path}")

        self.raw_indicators_config: List[Dict[str, Any]] = config.get("indicators", [])
        self.mapping_config: Dict[str, Any] = config.get("mapping", {})
        self.hysteresis_config: Dict[str, Any] = config.get("hysteresis", {})
        self.enabled: bool = config.get("enabled", True)
        self.safe_asset_config: Dict[str, Any] = config.get("safe_asset_config", {"default": "CASH"})


        # Mapping-Parameter für map_to_equity_weight extrahieren
        # Deine Config hat "mapping": {"type": "three_band", "params": {"low": -0.3, "high": 0.1}}
        # map_to_equity_weight erwartet threshold_on (positive obere Schwelle)
        # und threshold_off_negative (negative untere Schwelle)
        mapping_params = self.mapping_config.get("params", {})
        self.config_threshold_on = mapping_params.get("high", 0.1) # Deine 'high'
        self.config_threshold_off_negative = mapping_params.get("low", -0.3) # Deine 'low'
        self.mapping_type = self.mapping_config.get("type", "linear") # "three_band" oder "linear"

        # Hysteresis-Parameter (für spätere Verwendung in apply_hysteresis_and_time_in_state)
        self.min_days_in_state = self.hysteresis_config.get("min_days", 3)
        # Deine Hysteresis-Config hat 'low' und 'high'. Diese sind eher für Score-Schwellen zum Zustandswechsel.
        # Wir benötigen sie für eine State-Machine.
        self.hysteresis_score_low_entry_risk_off = self.hysteresis_config.get("low", -0.05)
        self.hysteresis_score_high_exit_risk_off = self.hysteresis_config.get("high", 0.15)
        
        self.score_log: List[Dict[str, Any]] = []
        self.indicators: List[Dict[str, Any]] = [] # Wird jetzt Indikator-Daten und Instanzen halten
        self._initialize_indicators()

        # Zustandsvariablen für Hysterese und Time-in-State
        self.current_overlay_state = "NEUTRAL" # z.B. "RISK_ON", "RISK_OFF", "NEUTRAL"
        self.days_in_current_overlay_state = 0
        self.last_applied_equity_quote = 0.5 # Startwert oder letzter bekannter Wert


    def _initialize_indicators(self):
        """Lädt und instanziiert Indikatoren und speichert ihre Konfiguration."""
        for conf in self.raw_indicators_config:
            try:
                # Verwende deine load_indicator Factory
                # Annahme: load_indicator(conf) gibt die Indikator-Instanz zurück
                # conf enthält 'path', 'class', 'params'
                instance = load_indicator(conf) 
                
                self.indicators.append({
                    "name": conf.get("name", f"{conf.get('path', '')}.{conf.get('class', '')}"),
                    "instance": instance,
                    "asset": conf.get("asset", "SPY"), # WICHTIG: 'asset' muss in deiner config sein
                    "mode": conf.get("mode", "both"),
                    "weight": conf.get("weight", 1.0),
                    "transform": conf.get("transform", "none"),
                    "transform_params": conf.get("transform_params", {}),
                    "direction": conf.get("direction", 1),
                    "confidence_static": conf.get("confidence_static", 1.0)
                })
            except Exception as e:
                print(f"FEHLER beim Initialisieren des Indikators (Config: {conf}): {e}")
                import traceback
                traceback.print_exc()


    def get_processed_indicator_signals(self, eod_data_map: Dict[str, pd.DataFrame]) -> List[Tuple[pd.Series, str, float]]:
        """
        Berechnet Rohsignale, transformiert/normalisiert sie und wendet die Richtung an.
        Gibt eine Liste von (final_signal_series, mode, weight) für jeden Indikator zurück.
        """
        processed_signals_with_meta = []

        for ind_data in self.indicators:
            instance = ind_data['instance']
            name = ind_data['name']
            asset_ticker = ind_data['asset']
            
            mode = ind_data['mode']
            weight = ind_data['weight']
            transform_method = ind_data.get('transform', 'none')
            transform_params = ind_data.get('transform_params', {})
            direction = ind_data.get('direction', 1)
            # confidence = ind_data.get('confidence_static', 1.0) # Für spätere Verwendung

            if asset_ticker not in eod_data_map:
                print(f"WARNUNG: Daten für Asset {asset_ticker} (Indikator: {name}) nicht gefunden. Erzeuge leere Serie.")
                # Erzeuge eine leere Serie mit dem Index des ersten verfügbaren Assets, falls vorhanden
                empty_signal = pd.Series([], dtype=float)
                if eod_data_map:
                    example_index = next(iter(eod_data_map.values())).index
                    empty_signal = pd.Series(0.0, index=example_index)
                processed_signals_with_meta.append((empty_signal, mode, weight))
                continue
            
            asset_eod_data = eod_data_map[asset_ticker]

            # 1. Roh-Signal
            try:
                raw_signal = instance.calculate(asset_eod_data)
            except Exception as e:
                print(f"FEHLER bei Roh-Signalberechnung für {name} auf {asset_ticker}: {e}")
                raw_signal = pd.Series(0.0, index=asset_eod_data.index) # Fallback

            # 2. Transformieren/Normalisieren
            normalized_signal = raw_signal.copy()
            if transform_method == "minmax":
                lookback = transform_params.get('lookback', 0)
                feature_range_str = transform_params.get('feature_range', "(-1, 1)")
                try: f_range = tuple(map(float, feature_range_str.strip("()[]").split(',')))
                except: f_range = (-1.0, 1.0)
                normalized_signal = min_max_scaler(raw_signal, feature_range=f_range, lookback_period=lookback)
            elif transform_method == "zscore":
                lookback = transform_params.get('lookback', 0)
                normalized_signal = z_score_scaler(raw_signal, lookback_period=lookback)
            elif transform_method == "binary":
                threshold = transform_params.get('threshold', 0.0)
                condition_above = transform_params.get('condition_above', True)
                normalized_signal = binary_scaler(raw_signal, threshold=threshold, condition_above=condition_above)
            elif transform_method != "none" and transform_method:
                 print(f"WARNUNG: Unbekannte Transformation '{transform_method}' für {name}. Rohsignal verwendet.")

            # 3. Richtung anwenden
            final_signal = normalized_signal * direction
            final_signal_filled = final_signal.fillna(0.0) # Wichtig: NaNs füllen

            processed_signals_with_meta.append((final_signal_filled, mode, weight))
        
        return processed_signals_with_meta


    def aggregate_scores(self, processed_signals: List[Tuple[pd.Series, str, float]]) -> Dict[str, float]:
        """
        Aggregiert die bereits prozessierten Signale (letzter Wert der Serie) getrennt nach Mode.
        """
        # Wir benötigen den letzten Wert jeder Signal-Serie für die Aggregation
        score_on_values = []
        score_off_values = []
        score_both_values = []

        for signal_series, mode, weight in processed_signals:
            if signal_series.empty:
                score_last = 0.0
            else:
                score_last = signal_series.iloc[-1] # Letzter Wert der Serie

            # Hier könnte auch `confidence` multipliziert werden
            # score_last *= confidence 

            if mode == "risk_on":
                score_on_values.append(weight * score_last)
            elif mode == "risk_off":
                score_off_values.append(weight * score_last)
            else:  # both
                score_both_values.append(weight * score_last)
        
        # Aggregation (gewichteter Durchschnitt oder Summe)
        # Hier einfacher Durchschnitt der gewichteten Scores pro Kategorie
        # Wenn du gewichtete Summe willst: np.sum(...)
        # Wenn du gewichteten Durchschnitt über alle Beiträge: np.sum(...) / sum_of_abs_weights
        agg = {
            "risk_on": np.mean(score_on_values) if score_on_values else 0.0,
            "risk_off": np.mean(score_off_values) if score_off_values else 0.0,
            "both": np.mean(score_both_values) if score_both_values else 0.0
        }
        return agg
  
    def map_to_equity_weight(self, agg_scores: Dict[str, float]) -> float:
        """
        Kombiniert die aggregierten Scores und mappt sie auf eine Aktienquote.
        Verwendet Schwellenwerte aus der Konfiguration.
        """
        # Gesamt-Score (Risk-Off wird subtrahiert, um die Risikobereitschaft zu reduzieren)
        # Dieser Ansatz für den Gesamtscore ist eine Möglichkeit.
        # Alternative: State-Machine basierend auf Schwellen für risk_on und risk_off Scores.
        overlay_score = (
            agg_scores["risk_on"]
            - agg_scores["risk_off"] # Risk-Off Score reduziert den Gesamtscore
            + agg_scores["both"]
        )
        
        # Deine Mapping-Logik:
        if self.mapping_type == "three_band" or self.mapping_type == "linear": # Behandle linear ähnlich
            # threshold_on ist die obere Grenze für 100% Aktien
            # threshold_off_negative ist die untere Grenze für 0% Aktien
            if overlay_score >= self.config_threshold_on:
                return 1.0
            elif overlay_score <= self.config_threshold_off_negative:
                return 0.0
            else:
                # Lineare Interpolation im Band [config_threshold_off_negative, config_threshold_on]
                if self.config_threshold_on <= self.config_threshold_off_negative: # Ungültig
                    print("WARNUNG: config_threshold_on <= config_threshold_off_negative in Mapping. Fallback.")
                    return 0.0 if overlay_score < self.config_threshold_on else 1.0

                relative_position = (overlay_score - self.config_threshold_off_negative) / \
                                    (self.config_threshold_on - self.config_threshold_off_negative)
                return np.clip(relative_position, 0.0, 1.0)
        else:
            print(f"WARNUNG: Unbekannter Mapping-Typ '{self.mapping_type}'. Verwende 50% Allokation.")
            return 0.5


    def apply_hysteresis_and_time_in_state(self, target_equity_quote: float, current_date: pd.Timestamp) -> float:
        """
        Wendet Hysterese und Time-in-State Regeln auf die Ziel-Aktienquote an.
        Gibt die tatsächlich anzuwendende Aktienquote zurück.
        (Diese Funktion ist eine vereinfachte Darstellung und muss verfeinert werden)
        """
        # Time-in-State Logik
        # Bestimme den neuen gewünschten Zustand basierend auf target_equity_quote
        new_desired_state = "NEUTRAL"
        if target_equity_quote >= 0.8: new_desired_state = "RISK_ON" # Beispielschwellen
        elif target_equity_quote <= 0.2: new_desired_state = "RISK_OFF"

        if new_desired_state == self.current_overlay_state:
            self.days_in_current_overlay_state += 1
        else: # Zustandswechsel gewünscht
            # Hier könnten komplexere Hysterese-Schwellen für den Score-Wechsel rein
            # z.B. self.hysteresis_score_low_entry_risk_off etc.
            # Fürs Erste: einfacher Wechsel, wenn min_days erreicht ist
            if self.days_in_current_overlay_state >= self.min_days_in_state:
                self.current_overlay_state = new_desired_state
                self.days_in_current_overlay_state = 1 # Reset
            else:
                # Noch nicht lange genug im aktuellen Zustand, bleibe dabei
                # und verwende die Allokation, die diesem Zustand entspricht,
                # oder die `last_applied_equity_quote`.
                # Fürs Erste: Behalte die letzte Quote bei, wenn TimeInState nicht erfüllt.
                return self.last_applied_equity_quote


        # Hysterese für Allokationsänderung
        # Nur ändern, wenn die neue Quote signifikant von der letzten abweicht
        min_alloc_change_from_config = self.hysteresis_config.get("min_allocation_change_pct", 5) / 100.0
        
        if self.last_applied_equity_quote is None or \
           abs(target_equity_quote - self.last_applied_equity_quote) >= min_alloc_change_from_config:
            final_equity_quote = target_equity_quote
        else:
            final_equity_quote = self.last_applied_equity_quote
        
        self.last_applied_equity_quote = final_equity_quote
        return final_equity_quote

        
    def run_daily_overlay(self, current_date: pd.Timestamp, eod_data_map: Dict[str, pd.DataFrame]) -> float:
        """
        Führt den gesamten Overlay-Prozess für einen Tag aus.
        Gibt die finale Aktienquote nach Hysterese etc. zurück.
        """
        if not self.enabled:
            return 1.0 # Wenn disabled, volle Aktienquote

        # 1. Berechne prozessierte Signale (inkl. Transformation, Direction)
        processed_signals = self.get_processed_indicator_signals(eod_data_map)
        
        # 2. Aggregiere Scores
        agg_scores = self.aggregate_scores(processed_signals)
        
        # 3. Mappe Scores zu einer Ziel-Aktienquote
        target_equity_quote = self.map_to_equity_weight(agg_scores)
        
        # 4. Wende Hysterese und Time-in-State an
        final_equity_quote = self.apply_hysteresis_and_time_in_state(target_equity_quote, current_date)

        # Logging (ähnlich zu deiner apply Methode)
        self.score_log.append({
            "date": current_date,
            "Agg_RiskOn_Score": agg_scores["risk_on"],
            "Agg_RiskOff_Score": agg_scores["risk_off"],
            "Agg_Both_Score": agg_scores["both"],
            "Combined_Overlay_Score": agg_scores["risk_on"] - agg_scores["risk_off"] + agg_scores["both"], # Beispiel
            "Target_Equity_Quote": target_equity_quote,
            "Final_Equity_Quote": final_equity_quote,
            "Overlay_State": self.current_overlay_state,
            "Days_In_State": self.days_in_current_overlay_state
        })
        
        return final_equity_quote

    # Deine `apply` Methode war für die Integration in einen Backtester gedacht,
    # um base_orders zu skalieren. Die `run_daily_overlay` gibt jetzt die Quote zurück.
    # Der Backtester müsste diese Quote dann verwenden, um Orders zu generieren.
    # Wir können die `apply` Methode später wieder anpassen, wenn wir die Order-Generierung machen.