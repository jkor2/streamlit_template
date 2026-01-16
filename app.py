import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="Demo Dashboard", layout="wide")
st.title("Demo Dashboard")

# Sidebar controls
st.sidebar.header("Controls")
n = st.sidebar.slider("Rows", 50, 1000, 200, 50)
noise = st.sidebar.slider("Noise", 0.0, 2.0, 0.5, 0.1)

# Generate example data
rng = np.random.default_rng(42)
x = np.arange(n)
y = np.sin(x / 12) + rng.normal(0, noise, size=n)

df = pd.DataFrame({"x": x, "y": y})

# Layout
c1, c2 = st.columns([1, 2])

with c1:
    st.subheader("Summary")
    st.metric("Rows", f"{len(df):,}")
    st.metric("Mean(y)", f"{df['y'].mean():.3f}")
    st.metric("Std(y)", f"{df['y'].std():.3f}")
    st.divider()
    st.subheader("Preview")
    st.dataframe(df.head(20), use_container_width=True)

with c2:
    st.subheader("Chart")
    st.line_chart(df, x="x", y="y", use_container_width=True)

st.caption("Deployed on Streamlit Community Cloud • Demo app")