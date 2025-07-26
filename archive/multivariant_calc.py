import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# --- 1. Connect to SQLite ---
conn = sqlite3.connect("mlb.db")
cursor = conn.cursor()

# --- 2. Define SQL Query ---
query = """
SELECT
  g.away_score,
  COUNT(*) FILTER (WHERE a.about_halfinning = 'top' AND a.result_event = 'Walk')      AS walk,
  COUNT(*) FILTER (WHERE a.about_halfinning = 'top' AND a.result_event = 'Single')    AS single,
  COUNT(*) FILTER (WHERE a.about_halfinning = 'top' AND a.result_event = 'Double')    AS double,
  COUNT(*) FILTER (WHERE a.about_halfinning = 'top' AND a.result_event = 'Home Run')  AS homerun,
  COUNT(*) FILTER (WHERE a.about_halfinning = 'top' AND a.result_event = 'Triple')    AS triple
FROM game g
LEFT JOIN atbat a ON g.game_pk = a.game_pk
GROUP BY g.game_pk, g.away_score
"""

# --- 3. Load results into a DataFrame ---
df = pd.read_sql_query(query, conn)

from sklearn.linear_model import LinearRegression

# Select features (X) and target (y)
X = df[['walk', 'single', 'double', 'triple', 'homerun']]
y = df['away_score']

# Fit the model
reg = LinearRegression()
reg.fit(X, y)

# Print coefficients and intercept
print("Coefficients:", reg.coef_)
print("Intercept:", reg.intercept_)
#
df['predicted_away_score'] = reg.predict(X)
result = df[['away_score', 'predicted_away_score']]
result.to_sql('away_score_vs_predicted', conn, if_exists='replace', index=False)
# conn.close()


query = """
SELECT
  away_score,
  predicted_away_score
FROM away_score_vs_predicted
"""

df = pd.read_sql_query(query, conn)
# Calculate residuals (errors)
df['error'] = df['away_score'] - df['predicted_away_score']

# Calculate mean and std of the residuals
error_mean = df['error'].mean()
error_std = df['error'].std()

print(f"Mean error: {error_mean:.4f}")
print(f"Standard deviation of error: {error_std:.4f}")