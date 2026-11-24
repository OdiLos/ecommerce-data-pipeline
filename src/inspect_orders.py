import pandas as pd

def clean_orders(df):

    original_count = len(df)

    #Remove exact duplicate records
    df = df.drop_duplicates()

    duplicates_removed = original_count - len(df)

    #Identify invalid records
    missing_prices = df['price'].isnull()
    invalid_quantities = df['quantity'] <= 0

    #Seperate valid and invalid records
    invalid_orders = df[missing_prices | invalid_quantities]
    valid_orders = df[~(missing_prices | invalid_quantities)]

    return valid_orders, invalid_orders, duplicates_removed

#Load the CSV file into a DataFrame
df = pd.read_csv("data/raw/orders.csv")

#Clean the DataFrame and separate valid and invalid records
valid_orders, invalid_orders, duplicates_removed = clean_orders(df)

#Display pipeline report
print("\n---PIPELINE REPORT---")

print(f"Original records: {len(df)}")
print(f"Duplicates removed: {duplicates_removed}")
print(f"Valid orders: {len(valid_orders)}")
print(f"Invalid orders: {len(invalid_orders)}")

print(
    f"Data quality: {len(valid_orders) / (len(df) - duplicates_removed) * 100:.2f}%"
)


#Identify records with successful payments
#paid_orders = valid_orders[valid_orders['payment_status'] == 'paid']

#Calculate the total revenue from paid order
#paid_revenue = (paid_orders['price'] * paid_orders['quantity']).sum()

#print("\nPaid orders:")
#print(paid_orders)

#print("\nTotal paid revenue:", paid_revenue)
