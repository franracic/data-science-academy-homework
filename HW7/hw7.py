import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from statsmodels.tsa.seasonal import STL, seasonal_decompose
from statsmodels.tsa.stattools import adfuller, kpss
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from itertools import product

# 1.b
df = pd.read_csv("hw7_data.csv")
df["ev_date"] = pd.to_datetime(df["ev_date"], format="%Y%m%d")
df = df.set_index("ev_date").sort_index()

s = df["avg_events_per_user"].asfreq("D")

ma7 = s.rolling(7, center=True).mean()

plt.figure()
plt.plot(s, label="Original")
plt.plot(ma7, label="MA(7)")
plt.legend()
plt.show()

# Red 7 jer su podaci dnevni s tjednom sezonalnoscu - prozor pokriva tocno jedan
# ciklus pa vikendi/radni dani izgladuju u cisti trend (uglavnom ravan oko ~4).


# 1.c
stl = STL(s, period=7).fit()
classic = seasonal_decompose(s, model="additive", period=7)

stl.plot()
plt.suptitle("STL dekompozicija")
plt.show()
classic.plot()
plt.suptitle("Klasicna dekompozicija")
plt.show()

plt.figure()
plt.plot(stl.trend, label="STL trend")
plt.plot(classic.trend, label="Klasicni trend")
plt.title("Usporedba")
plt.legend()
plt.show()

# Tjedna sezonalnost je stabilna, trend ima rast u veljaci i pad krajem svibnja, reziduali su sum.
# STL pokriva rubove i prati promjene fleksibilnije nego klasicni MA7.


# 1.d
# ADF: H0 = serija nije stacionarna
# KPSS: H0 = serija je stacionarna
# ACF: kod nestacionarne pada sporo

adf_p = adfuller(s.dropna())[1]
kpss_p = kpss(s.dropna(), regression="c")[1]
print("ADF p-value:", round(adf_p, 4))
print("KPSS p-value:", round(kpss_p, 4))

plot_acf(s.dropna(), lags=30)
plt.title("ACF")
plt.show()

s_diff = s.diff().dropna()
print("ADF (1. diff):", round(adfuller(s_diff)[1], 4))
plot_acf(s_diff, lags=30)
plt.title("Nakon 1. diferenciranja")
plt.show()

# ADF p=0.04 odbacuje H0 nestacionarnosti, dok KPSS p=0.1 ne odbacuje
# Oba testa kazu da je serija vec stacionarna (d=0)
# ACF pokazuje jake spike-ove na tjednu sezonalnost


# 1.e
train = s.loc[:"2024-05-15"]
test = s.loc["2024-05-16":"2024-05-30"]
print("train:", len(train), " test:", len(test))


# ARIMA
best_aic, best_order = float("inf"), None
for d in [0, 1]:
    for p in range(3):
        for q in range(3):
            try:
                m = ARIMA(train, order=(p, d, q)).fit()
                if m.aic < best_aic:
                    best_aic, best_order = m.aic, (p, d, q)
            except Exception:
                pass
print("ARIMA:", best_order)

arima = ARIMA(train, order=best_order).fit()
arima_in = arima.predict(start=train.index[0], end=train.index[-1])
arima_fc = arima.forecast(15)


# Exponential Smoothing
es = ExponentialSmoothing(train, trend="add", seasonal="add", seasonal_periods=7).fit()
es_in = es.fittedvalues
es_fc = es.forecast(15)

plt.figure()
plt.plot(train, label="train", alpha=0.5)
plt.plot(arima_in, label="ARIMA in-sample")
plt.plot(es_in, label="ES in-sample")
plt.title("In-sample")
plt.legend()
plt.show()

plt.figure()
plt.plot(train.iloc[-40:], label="Train (zadnjih 40)")
plt.plot(test, label="Test", color="black")
plt.plot(arima_fc, label="ARIMA forecast")
plt.plot(es_fc, label="ES forecast")
plt.title("15-dnevni forecast")
plt.legend()
plt.show()


# Druge metode:
naive = pd.Series(train.iloc[-1], index=test.index)
mean_fc = pd.Series(train.mean(), index=test.index)

prev_start = test.index[0] - pd.DateOffset(months=1)
prev_end = test.index[-1] - pd.DateOffset(months=1)
prev = s.loc[prev_start:prev_end].values[: len(test)]
seasonal_naive = pd.Series(prev, index=test.index)

plt.figure()
plt.plot(test, label="Test", color="black", linewidth=2)
plt.plot(arima_fc, label="ARIMA")
plt.plot(es_fc, label="ES")
plt.plot(naive, "--", label="Naive")
plt.plot(seasonal_naive, "--", label="Seasonal naive")
plt.plot(mean_fc, "--", label="Mean")
plt.title("Sve metode")
plt.legend()
plt.show()


# MAE i RMSE
def mae(a, f):
    return float(np.mean(np.abs(a.values - f.values)))


def rmse(a, f):
    return float(np.sqrt(np.mean((a.values - f.values) ** 2)))


methods = {
    "arima": arima_fc,
    "es": es_fc,
    "naive": naive,
    "seasonal_naive": seasonal_naive,
    "mean": mean_fc,
}

scores = pd.DataFrame(
    {
        "MAE": [mae(test, f) for f in methods.values()],
        "RMSE": [rmse(test, f) for f in methods.values()],
    },
    index=methods.keys(),
).sort_values("RMSE")
print(scores)

# ES najbolji (hvata razinu + tjedni ciklus)
# ARIMA bez sezonalnog clana daje ravan forecast
# seasonal naive najgori jer mjesecni pomak razbija dan u tjednu.


# 2.a
# datum, metoda

first_week = test.index[:7]
rows = []
for name, fc in methods.items():
    for d in s.index:
        actual = float(s.loc[d])
        fcast = float(fc.loc[d]) if d in fc.index else None
        wow = None
        if d in first_week:
            wow = float(fc.loc[d] - s.loc[d - pd.Timedelta(days=7)])
        rows.append((d.date(), name, actual, fcast, wow))

out = pd.DataFrame(
    rows, columns=["ev_date", "method", "actual", "forecast", "wow_change"]
)

out.to_csv("hw7_forecast.csv", index=False)
print("Spremljeno redaka:", len(out))


# bonus
s_sdiff = s.diff(7).dropna()
plot_acf(s_sdiff, lags=30)
plt.title("ACF")
plt.show()
plot_pacf(s_sdiff, lags=30)
plt.title("PACF")
plt.show()

manual_order = (0, 0, 1)
manual_seasonal = (0, 1, 1, 7)
manual = SARIMAX(train, order=manual_order, seasonal_order=manual_seasonal).fit(
    disp=False
)
print("SARIMA AIC:", round(manual.aic, 1), "BIC:", round(manual.bic, 1))

records = []
for p, d, q, P, D, Q in product(range(3), repeat=6):
    try:
        m = SARIMAX(train, order=(p, d, q), seasonal_order=(P, D, Q, 7)).fit(disp=False)
        records.append(
            {"order": (p, d, q), "seasonal": (P, D, Q, 7), "aic": m.aic, "bic": m.bic}
        )
    except Exception:
        pass

grid = pd.DataFrame(records)
aic_best = grid.loc[grid["aic"].idxmin()]
bic_best = grid.loc[grid["bic"].idxmin()]
print("AIC:", aic_best["order"], aic_best["seasonal"], round(aic_best["aic"], 1))
print("BIC:", bic_best["order"], bic_best["seasonal"], round(bic_best["bic"], 1))

candidates = {
    "manual": (manual_order, manual_seasonal),
    "aic": (aic_best["order"], aic_best["seasonal"]),
    "bic": (bic_best["order"], bic_best["seasonal"]),
}
seen, distinct = set(), {}
for name, (o, so) in candidates.items():
    key = (tuple(o), tuple(so))
    if key not in seen:
        seen.add(key)
        distinct[name] = SARIMAX(train, order=o, seasonal_order=so).fit(disp=False)

for name, model in distinct.items():
    fit_in = model.predict(start=train.index[0], end=train.index[-1])
    plt.figure()
    plt.plot(train.iloc[-60:], label="Train", alpha=0.5)
    plt.plot(fit_in.iloc[-60:], label=f"SARIMA {name}")
    plt.title(f"SARIMA ({name})")
    plt.legend()
    plt.show()

scores = {"arima": (mae(test, arima_fc), rmse(test, arima_fc))}
for name, model in distinct.items():
    fc = model.forecast(15)
    scores[f"sarima_{name}"] = (mae(test, fc), rmse(test, fc))

bonus = pd.DataFrame(scores, index=["MAE", "RMSE"]).T.sort_values("RMSE")
print(bonus)

# ACF ima spike na lag 1 i lag 7 -> (0,0,1)(0,1,1)7 = BIC pobjednik
# AIC bira (0,0,1)(0,1,2)7
# SARIMA prati vikend ciklus koji ARIMA nije hvatala
# RMSE 0.94 -> 0.75, sezonalni član pomaže.
