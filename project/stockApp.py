import streamlit as st
import mysql.connector
import bcrypt
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.linear_model import LinearRegression
import random

# ✅ SESSION STATE INITIALIZATION (must be at the top)
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "username" not in st.session_state:
    st.session_state.username = ""

# ✅ MySQL connection
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="System@123#456",
        database="stockanalysis"
    )

# ✅ Create users table if it doesn't exist
def create_users_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

create_users_table()

# ✅ Signup user
def signup_user(username, email, password):
    conn = get_connection()
    cursor = conn.cursor()
    hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
    try:
        cursor.execute("INSERT INTO users (username, email, password) VALUES (%s, %s, %s)", (username, email, hashed_pw))
        conn.commit()
        return True
    except mysql.connector.Error:
        return False
    finally:
        conn.close()

# ✅ Login user
def login_user(username, password):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT password FROM users WHERE username = %s", (username,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return bcrypt.checkpw(password.encode(), result[0].encode())
    return False

# ✅ Main app
def main():
    st.set_page_config(page_title="📊 Stock Analysis with Login", layout="wide")

    menu = ["Login", "Sign Up"]
    choice = st.sidebar.selectbox("Menu", menu)

    if not st.session_state.logged_in:
        st.title("🔐 Stock Dashboard Login")

        if choice == "Login":
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")

            if st.button("Login"):
                if login_user(username, password):
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.success(f"Welcome {username}!")
                    st.experimental_rerun()  # Trigger a rerun to reload the page with the menu
                else:
                    st.error("Incorrect credentials.")

        elif choice == "Sign Up":
            st.title("📝 Create an Account")
            new_user = st.text_input("New Username")
            new_email = st.text_input("Email")
            new_pass = st.text_input("New Password", type="password")
            if st.button("Create Account"):
                if signup_user(new_user, new_email, new_pass):
                    st.success("Account created successfully. Please login.")
                else:
                    st.error("Username or Email already exists.")
    else:
        # ✅ Logged-in Dashboard
        st.sidebar.success(f"Logged in as {st.session_state.username}")
        if st.sidebar.button("Logout"):
            st.session_state.logged_in = False
            st.session_state.username = ""
            st.experimental_rerun()  # Trigger a rerun to reload the page without the menu

        # Stock Analysis Section
        if st.session_state.logged_in:
            menu = st.sidebar.radio("Navigation", [
            "Stock Details",
            "Daily Returns",
            "Stock Averages",
            "Stock Correlation",
            "Stock Prediction",
            "Investor Sentiment",
            "Stock Price"
            ])

            stock_symbol = st.sidebar.text_input("Enter Stock Symbol", value="AAPL").upper()

            if stock_symbol:
                stock_data = yf.download(stock_symbol, start="2020-01-01", end="2025-12-31")

            if stock_data.empty:
                st.warning("No data found.")
            else:
                if menu == "Stock Details":
                    # st.title("📘 Stock Details")
                    # st.write(stock_data.tail(180))
                    st.title("📘 Stock Details")
                    # Filter last 180 rows
                    filtered_data = stock_data.tail(180).reset_index()

                    # Pagination settings
                    rows_per_page = 5
                    total_rows = len(filtered_data)
                    total_pages = (total_rows - 1) // rows_per_page + 1

                    # Page selector in sidebar or main
                    page = st.number_input("Select Page", min_value=1, max_value=total_pages, step=1, key="stock_details_page")

                    # Calculate start and end row
                    start_idx = (page - 1) * rows_per_page
                    end_idx = start_idx + rows_per_page

                    # Display paginated data
                    st.write(f"Showing rows {start_idx + 1} to {min(end_idx, total_rows)} of {total_rows}")
                    st.dataframe(filtered_data.iloc[start_idx+1:end_idx+1])
 


                elif menu == "Daily Returns":
                    st.title("📈 Daily Return Distribution")
                    stock_data['Daily Return'] = stock_data['Close'].pct_change()
                    sns.histplot(stock_data['Daily Return'].dropna(), bins=100, kde=True)
                    st.pyplot(plt)

                elif menu == "Stock Averages":
                    st.title("📊 50 & 200-Day Moving Averages")
                    stock_data['MA50'] = stock_data['Close'].rolling(50).mean()
                    stock_data['MA200'] = stock_data['Close'].rolling(200).mean()
                    fig, ax = plt.subplots(figsize=(12, 6))
                    ax.plot(stock_data['Close'], label='Close')
                    ax.plot(stock_data['MA50'], label='50-Day MA')
                    ax.plot(stock_data['MA200'], label='200-Day MA')
                    ax.legend()
                    st.pyplot(fig)

                elif menu == "Stock Correlation":
                    st.title("📌 Stock Correlation Heatmap")
                    tickers = ['AAPL', stock_symbol]
                    data = yf.download(tickers, start="2020-01-01", end="2025-12-31")['Close']
                    returns = data.pct_change()
                    fig_corr, ax_corr = plt.subplots()
                    sns.heatmap(returns.corr(), annot=True, cmap='coolwarm', ax=ax_corr)
                    st.pyplot(fig_corr)

                elif menu == "Stock Prediction":
                    st.title("📉 Stock Price Prediction")
                    stock_data['Target'] = stock_data['Close'].shift(-1)
                    stock_data.dropna(inplace=True)
                    X = np.array(stock_data[['Close']])
                    y = np.array(stock_data['Target'])
                    model = LinearRegression()
                    model.fit(X, y)
                    predicted = model.predict(X)
                    fig_pred, ax_pred = plt.subplots()
                    ax_pred.plot(stock_data.index, y, label='Actual')
                    ax_pred.plot(stock_data.index, predicted, label='Predicted')
                    ax_pred.set_title('Stock Price Prediction')
                    ax_pred.legend()
                    st.pyplot(fig_pred)

                elif menu == "Investor Sentiment":
                    st.title("🧠 Investor Sentiment Analysis")

                    # Example simulated data (you can replace with real API later)
                    buyers_pct = random.randint(35, 55)
                    sellers_pct = random.randint(15, 40)
                    holding_pct = 100 - buyers_pct - sellers_pct

                    # Ensure values make sense (optional safeguard)
                    if holding_pct < 0:
                        holding_pct = 0
                        buyers_pct = 50
                        sellers_pct = 30

                    sentiment_df = pd.DataFrame({
                        'Category': ['Buyers', 'Sellers', 'Holding'],
                        'Percentage': [buyers_pct, sellers_pct, holding_pct]
                    })

                    # Show values
                    st.write(f"**{stock_symbol}** Sentiment Breakdown:")
                    st.write(sentiment_df)

                    # Pie chart
                    fig, ax = plt.subplots()
                    colors = ['green', 'red', 'orange']
                    ax.pie(sentiment_df['Percentage'], labels=sentiment_df['Category'], autopct='%1.1f%%', startangle=140, colors=colors)
                    ax.axis('equal')
                    st.pyplot(fig)


                elif menu == "Stock Price":
                    st.title("💲 Stock Price Details")
                    highest_price = float(stock_data['High'].max())
                    lowest_price = float(stock_data['Low'].min())
                    st.markdown(f"- **Highest Price:** ${highest_price:.2f}")
                    st.markdown(f"- **Lowest Price:** ${lowest_price:.2f}")
                    fig_price, ax_price = plt.subplots(figsize=(10, 5))
                    ax_price.plot(stock_data['Close'], label='Close Price')
                    ax_price.axhline(highest_price, color='green', linestyle='--', label='Highest Price')
                    ax_price.axhline(lowest_price, color='red', linestyle='--', label='Lowest Price')
                    ax_price.legend()
                    st.pyplot(fig_price)

if __name__ == "__main__":
    main()
