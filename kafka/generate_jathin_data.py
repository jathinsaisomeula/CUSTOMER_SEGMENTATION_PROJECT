import os 
import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
import numpy as np

# Initialize Faker with specific locales for general and German data
fake = Faker('en_US') # For general data like UUIDs
fake_de = Faker('de_DE') # For German names, cities, etc.

# --- Configuration ---
NUM_ROWS = 1500
START_DATE = datetime(2024, 1, 1, 8, 0, 0) # Start of our one-year data (Jathin Markt operates 8 AM - 10 PM)
END_DATE = datetime(2024, 12, 31, 22, 0, 0)

# --- Master Lists (based on our discussions) ---

# Product & Sales Data
PRODUCT_TYPES = [
    "Furniture", "Home Appliances", "Home Decor", "Textiles",
    "Kitchenware", "Electronics", "Fashion"
]

# Price Ranges per Product Type (Approximate, for base price before discount/markup)
# These ranges are based on our earlier Google searches for Germany.
PRODUCT_PRICE_RANGES = {
    "Furniture": (20, 2500),
    "Home Appliances": (30, 2500),
    "Home Decor": (5, 500),
    "Textiles": (5, 300),
    "Kitchenware": (3, 700),
    "Electronics": (10, 2500),
    "Fashion": (5, 500)
}

BRANDS = [
    "GlobalHome", "EcoLiving", "TechNova", "StyleFlex", "PureComfort",
    "BrightSpark", "Kompakt Solutions", "UrbanEdge", "Zenith Goods",
    "DeinMöbel", "HausFreude", "TechBlitz", "ModeGefühl"
]

# Customer Information
CITIES = [
    "Berlin", "Frankfurt", "Munich", "Hamburg", "Cologne",
    "Stuttgart", "Düsseldorf", "Leipzig", "Dresden"
]

CITY_STATE_MAP = {
    "Berlin": "Berlin",
    "Frankfurt": "Hesse (Hessen)",
    "Munich": "Bavaria (Bayern)",
    "Hamburg": "Hamburg",
    "Cologne": "North Rhine-Westphalia (Nordrhein-Westfalen)",
    "Stuttgart": "Baden-Württemberg",
    "Düsseldorf": "North Rhine-Westphalia (Nordrhein-Westfalen)",
    "Leipzig": "Saxony (Sachsen)",
    "Dresden": "Saxony (Sachsen)"
}

PROFESSIONS = [
    "Student", "Engineer", "Employee", "Businessman/Entrepreneur",
    "Doctor", "Professor"
]

# Income Level Categories and their approximate ranges (Annual Gross in Euros)
INCOME_LEVEL_RANGES = {
    "Entry-Level/Student Income": (0, 25000),
    "Mid-Range/Early-Career Professional": (25001, 60000),
    "High Income/Experienced Professional": (60001, 100000),
    "Very High Income": (100001, 300000) # Capped for generation simplicity
}

# Transaction & Financials
PAYMENT_TYPES = [
    "Credit Card", "Debit Card", "Cash", "Online Payment (PayPal/Klarna)"
]

# Logistics & Delivery
SUPPLIERS = [
    "GlobalCraft GmbH", "ElektronikWelt AG", "TextilHarmonie UG",
    "Haus & Herd Handel", "Regio Möbel Manufaktur", "FashionFlow Inc."
]

TRANSPORT_MODES = { # (min_days, max_days) from Supplier to Jathin Markt DC
    "Road Freight (Domestic Germany)": (1, 3),
    "Road Freight (European)": (3, 7),
    "Rail Freight": (2, 5),
    "Air Freight": (1, 5),
    "Sea Freight": (20, 45)
}

DELIVERY_MODES = { # (min_days, max_days) from Jathin Markt DC to Customer
    "Bicycle/Scooter Delivery": (0, 1),
    "Express Delivery (Van/Lorry)": (1, 2),
    "Customer Pick-up (Click & Collect)": (1, 2),
    "Standard Home Delivery (Van/Lorry)": (2, 5),
    "Courier Service": (2, 5)
}

GERMAN_FIRST_NAMES = [
    "Hannah", "Mia", "Emilia", "Sophia", "Emma", "Noah", "Leon", "Paul", "Ben", "Luca",
    "Marie", "Julian", "Lena", "Felix", "Anna", "Lisa", "Moritz", "Laura", "Tim", "David"
]
GERMAN_LAST_NAMES = [
    "Müller", "Schmidt", "Schneider", "Fischer", "Meyer", "Weber", "Schulz", "Wagner",
    "Becker", "Hoffmann", "Schäfer", "Koch", "Richter", "Klein", "Wolf", "Neumann"
]

# Marketing Channels
MARKETING_CHANNELS = [
    "Referral", "Google Search", "Friends/Family", "Colleagues",
    "Social Media", "Online Ads", "Billboards"
]

# --- Helper Functions ---

def get_random_datetime(start_dt, end_dt):
    """Generates a random datetime between two datetimes."""
    delta = end_dt - start_dt
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start_dt + timedelta(seconds=random_second)

def generate_product_name(product_type):
    """Generates a more specific product name based on type."""
    if product_type == "Furniture":
        return random.choice(["Sofa", "Dining Table", "Wardrobe", "Bed Frame", "Bookshelf", "Armchair"]) + " " + fake.word().capitalize()
    elif product_type == "Home Appliances":
        return random.choice(["Washing Machine", "Refrigerator", "Microwave", "Vacuum Cleaner", "Coffee Maker", "Blender"]) + " " + fake.word().capitalize()
    elif product_type == "Home Decor":
        return random.choice(["Vase", "Wall Art", "Lamp", "Cushion", "Mirror", "Candle Holder"]) + " " + fake.word().capitalize()
    elif product_type == "Textiles":
        return random.choice(["Bedding Set", "Towel Set", "Curtains", "Rug", "Blanket", "Tablecloth"]) + " " + fake.word().capitalize()
    elif product_type == "Kitchenware":
        return random.choice(["Cookware Set", "Dinnerware Set", "Cutlery Set", "Coffee Mug", "Baking Tray", "Chopping Board"]) + " " + fake.word().capitalize()
    elif product_type == "Electronics":
        return random.choice(["Smart TV", "Laptop", "Smartphone", "Headphones", "Smart Speaker", "Gaming Console"]) + " " + fake.word().capitalize()
    elif product_type == "Fashion":
        return random.choice(["Jacket", "Jeans", "Dress", "T-shirt", "Sneakers", "Watch"]) + " " + fake.word().capitalize()
    return fake.word().capitalize() + " " + product_type # Fallback

def generate_review_text(rating):
    """Generates a short review text based on a 0-10 star rating."""
    if rating >= 8:
        return random.choice(["Good product", "Satisfied", "Excellent", "Great value", "As expected", "Happy customer"])
    elif rating >= 4:
        return random.choice(["Could be better", "Okay product", "Average", "Decent", "Functional"])
    else:
        return random.choice(["Not worthy", "Damaged product", "Poor packaging", "Difficult assembly", "Disappointing", "Problematic"])

def generate_overall_experience_feedback(customer_rating, delivery_rating):
    """Generates overall shopping experience text based on product and delivery ratings."""
    avg_rating = (customer_rating + delivery_rating) / 2 # Simple average for overall feel

    if avg_rating >= 7:
        return random.choice([
            "Smooth shopping experience", "Easy to find what I needed",
            "Great customer service", "Website was user-friendly",
            "Will definitely shop again", "Very satisfied with my overall experience",
            "Highly recommend Jathin Markt!"
        ])
    elif avg_rating >= 4:
        return random.choice([
            "Okay experience, nothing special", "Website a bit slow",
            "Standard shopping experience", "Could be improved",
            "Decent, but some hiccups."
        ])
    else:
        return random.choice([
            "Difficult checkout process", "Poor customer service interaction",
            "Website glitches", "Wouldn't recommend based on overall hassle",
            "Frustrating experience", "Many issues encountered."
        ])

# --- Generate a Pool of Unique Customer IDs and associated Customer Info ---
# Generate a pool of customer IDs (e.g., 30-50% unique customers for 1500 orders)
NUM_UNIQUE_CUSTOMERS = random.randint(NUM_ROWS // 3, NUM_ROWS // 2)
customer_pool_data = []
used_customer_ids = set()

for i in range(NUM_UNIQUE_CUSTOMERS):
    first_name = random.choice(GERMAN_FIRST_NAMES)
    last_name = random.choice(GERMAN_LAST_NAMES)
    
    # Generate customer ID as [FirstInitial][LastInitial][SequentialNumber]
    customer_id_base = f"{first_name[0].upper()}{last_name[0].upper()}{i+1:03d}"
    customer_id = customer_id_base
    counter = 0
    while customer_id in used_customer_ids: # Ensure uniqueness
        counter += 1
        customer_id = f"{customer_id_base}_{counter}"
    used_customer_ids.add(customer_id)

    customer_age = random.randint(18, 45)
    profession = random.choice(PROFESSIONS)
    
    # Determine income level category based on profession
    if profession == "Student":
        income_level_category = "Entry-Level/Student Income"
    elif profession in ["Engineer", "Employee"]:
        income_level_category = random.choices(
            ["Mid-Range/Early-Career Professional", "High Income/Experienced Professional"],
            weights=[0.7, 0.3], k=1
        )[0]
    elif profession in ["Businessman/Entrepreneur", "Doctor", "Professor"]:
        income_level_category = random.choices(
            ["High Income/Experienced Professional", "Very High Income"],
            weights=[0.6, 0.4], k=1
        )[0]
    else: # Fallback or other general employees
        income_level_category = random.choices(
            ["Entry-Level/Student Income", "Mid-Range/Early-Career Professional"],
            weights=[0.2, 0.8], k=1
        )[0]

    marital_status = random.choice(["Single", "Married"])
    city = random.choice(CITIES)
    state = CITY_STATE_MAP[city]
    gender = random.choice(["Male", "Female"])
    marketing_channel = random.choice(MARKETING_CHANNELS)

    customer_pool_data.append({
        "CUSTOMER ID": customer_id,
        "CUSTOMER AGE": customer_age,
        "PROFESSION": profession,
        "INCOME LEVEL": income_level_category,
        "MARITAL STATUS": marital_status,
        "CITY": city,
        "STATE": state,
        "GENDER": gender,
        "MARKETING CHANNEL": marketing_channel,
        "COUNTRY": "Germany"
    })

# --- Main Data Generation Loop ---
data = []
order_id_counter = {} # For ORD-DDMM-XXX format
product_id_counter = 0 # Simple counter for PRODUCT_ID
delivery_person_names = [f"{random.choice(GERMAN_FIRST_NAMES)} {random.choice(GERMAN_LAST_NAMES)}" for _ in range(15)] # Pool of 15 delivery persons

for i in range(NUM_ROWS):
    row = {}

    # 1. ORDER ID & DATE OF SALE
    row['DATE OF SALE'] = get_random_datetime(START_DATE, END_DATE)
    date_part = row['DATE OF SALE'].strftime("%d%m")
    order_id_counter[date_part] = order_id_counter.get(date_part, 0) + 1
    row['ORDER ID'] = f"ORD-{date_part}-{order_id_counter[date_part]:03d}"

    # 2. CUSTOMER INFORMATION (selected from pool)
    chosen_customer = random.choice(customer_pool_data)
    row.update(chosen_customer) # Adds all customer attributes to the row

    # 3. PRODUCT & SALES
    row['PRODUCT TYPE'] = random.choice(PRODUCT_TYPES)
    
    # Generate Product Name & Description based on type
    product_name = generate_product_name(row['PRODUCT TYPE'])
    row['PRODUCT NAME'] = product_name
    row['PRODUCT DESCRIPTION'] = f"High-quality {row['PRODUCT TYPE'].lower()} product - {product_name}." # Simple description
    row['PAYMENT TYPE'] = random.choice(PAYMENT_TYPES)
    product_id_counter += 1
    row['PRODUCT_ID'] = f"PRD{product_id_counter:05d}" # PRD00001, PRD00002...

    # Base Price for Product
    min_price, max_price = PRODUCT_PRICE_RANGES[row['PRODUCT TYPE']]
    row['PRICE'] = round(random.uniform(min_price, max_price), 2)

    # DISCOUNT APPLIED (Percentage: 0.00 to 0.50)
    # Most items have no discount, some small, few large
    discount_prob = random.random()
    if discount_prob < 0.7: # 70% no discount
        row['DISCOUNT APPLIED'] = 0.00
    elif discount_prob < 0.9: # 20% small discount (5-20%)
        row['DISCOUNT APPLIED'] = round(random.uniform(0.05, 0.20), 2)
    else: # 10% larger discount (20-50%)
        row['DISCOUNT APPLIED'] = round(random.uniform(0.20, 0.50), 2)

    # QUANTITY_PURCHASED (Based on product type: 1-5-7 logic)
    if row['PRODUCT TYPE'] in ["Furniture", "Home Appliances", "Electronics"]:
        row['QUANTITY_PURCHASED'] = 1 # Usually single large items
    elif row['PRODUCT TYPE'] in ["Fashion", "Textiles"]:
        row['QUANTITY_PURCHASED'] = random.choice([1, 1, 1, 2, 3]) # Mostly 1, sometimes 2 or 3
    else: # Home Decor, Kitchenware (smaller items often bought in multiples)
        row['QUANTITY_PURCHASED'] = random.choice([1, 1, 2, 3, 4, 5, 7]) # Up to 7 for very small items

    # REVENUE GENERATED (Calculated)
    # The "20 euros greater than original price" is a potential markup that would be applied
    # *before* this calculation to the base price. Assuming PRICE here is the listed price.
    revenue_before_discount = row['PRICE'] * row['QUANTITY_PURCHASED']
    row['REVENUE GENERATED'] = round(revenue_before_discount * (1 - row['DISCOUNT APPLIED']), 2)

    # AVALIABILITY (Stock Quantity - Numeric)
    # Skewed towards positive availability, some low, few out of stock
    stock_prob = random.random()
    if stock_prob < 0.05: # 5% out of stock
        row['AVALIABILITY'] = 0
    elif stock_prob < 0.2: # 15% low stock (1-10 units)
        row['AVALIABILITY'] = random.randint(1, 10)
    elif stock_prob < 0.6: # 40% medium stock (11-50 units)
        row['AVALIABILITY'] = random.randint(11, 50)
    else: # 40% high stock (51-300 units)
        row['AVALIABILITY'] = random.randint(51, 300)
    
    # Randomly assign a BRAND
    row['BRAND'] = random.choice(BRANDS)

    # 4. LOGISTICS & DELIVERY
    row['SUPPLIERS'] = random.choice(SUPPLIERS)
    
    # TRANSPORT MODE and TRANSPORT TIME (Supplier to Jathin Markt DC)
    chosen_transport_mode = random.choice(list(TRANSPORT_MODES.keys()))
    row['TRANSPORT MODE'] = chosen_transport_mode
    min_tt, max_tt = TRANSPORT_MODES[chosen_transport_mode]
    row['TRANSPORT TIME'] = random.randint(min_tt, max_tt) # Days

    # DELIVERY MODE and DELIVERY_DATE_TIME (Jathin Markt DC to Customer)
    chosen_delivery_mode = random.choice(list(DELIVERY_MODES.keys()))
    row['DELIVERY MODE'] = chosen_delivery_mode
    min_dt, max_dt = DELIVERY_MODES[chosen_delivery_mode]

    # Calculate delivery date based on DATE OF SALE and delivery lead time
    # This simulates the time from order placement to customer receipt
    delivery_lead_time_days = random.randint(min_dt, max_dt)
    
    # If the item was Out of Stock (0 availability), it implies it had to be ordered first.
    # Add a buffer for items that might have been ordered from supplier (simple simulation)
    # This is a simplification; in reality, this logic would be more complex (e.g., if AVALIABILITY=0, add TRANSPORT TIME)
    # For now, let's keep it based on delivery mode from DATE OF SALE for flat file simplicity.
    # The actual transport time from supplier is captured in 'TRANSPORT TIME' column.
    
    row['DELIVERY_DATE_TIME'] = row['DATE OF SALE'] + timedelta(days=delivery_lead_time_days, hours=random.randint(0,23), minutes=random.randint(0,59))

    # Ensure Delivery Date Time is always after Date of Sale, even for 0-day deliveries
    # Add a few hours if it's "same day" to make it realistic transaction-wise
    if row['DELIVERY_DATE_TIME'] <= row['DATE OF SALE']:
        row['DELIVERY_DATE_TIME'] = row['DATE OF SALE'] + timedelta(hours=random.randint(1, 24))


    row['DELIVERY_PERSON_NAME'] = random.choice(delivery_person_names)

    # 5. CUSTOMER FEEDBACK & EXPERIENCE
    # DELIVERY_RATING (1-5 Stars) - Skewed positive
    delivery_rating_prob = random.random()
    if delivery_rating_prob < 0.75: # 75% are 4-5 stars
        row['DELIVERY_RATING'] = random.randint(4, 5)
    elif delivery_rating_prob < 0.90: # 15% are 3 stars
        row['DELIVERY_RATING'] = 3
    else: # 10% are 1-2 stars
        row['DELIVERY_RATING'] = random.randint(1, 2)

    # CUSTOMER RATING (for product) (0-10 Stars) - Skewed positive
    customer_rating_prob = random.random()
    if customer_rating_prob < 0.65: # 65% are 9-10 stars
        row['CUSTOMER RATING'] = random.randint(9, 10)
    elif customer_rating_prob < 0.85: # 20% are 7-8 stars
        row['CUSTOMER RATING'] = random.randint(7, 8)
    elif customer_rating_prob < 0.95: # 10% are 4-6 stars
        row['CUSTOMER RATING'] = random.randint(4, 6)
    else: # 5% are 0-3 stars
        row['CUSTOMER RATING'] = random.randint(0, 3)

    # REVIEW TEXT (Short phrase, sentiment aligned)
    row['REVIEW TEXT'] = generate_review_text(row['CUSTOMER RATING'])

    # OVERALL_SHOPPING_EXPERIENCE_RATING (Text feedback)
    row['OVERALL_SHOPPING_EXPERIENCE_RATING'] = generate_overall_experience_feedback(row['CUSTOMER RATING'], row['DELIVERY_RATING'])
    
    data.append(row)

# Convert to DataFrame
df = pd.DataFrame(data)

# Ensure correct column order as agreed (32 columns)
FINAL_COLUMNS_ORDER = [
    'PRODUCT TYPE', 'PRODUCT_ID', 'PRODUCT NAME', 'PRODUCT DESCRIPTION', 'PRICE',
    'AVALIABILITY', 'QUANTITY_PURCHASED', 'BRAND', 'CUSTOMER ID', 'ORDER ID',
    'DATE OF SALE', 'PAYMENT TYPE', 'SUPPLIERS', 'REVENUE GENERATED',
    'TRANSPORT TIME', 'TRANSPORT MODE', 'DELIVERY MODE', 'DELIVERY_DATE_TIME',
    'DELIVERY_PERSON_NAME', 'CUSTOMER RATING', 'DELIVERY_RATING', 'REVIEW TEXT',
    'OVERALL_SHOPPING_EXPERIENCE_RATING', 'CUSTOMER AGE', 'PROFESSION',
    'INCOME LEVEL', 'CITY', 'STATE', 'GENDER', 'MARKETING CHANNEL',
    'DISCOUNT APPLIED', 'COUNTRY'
]

df = df[FINAL_COLUMNS_ORDER]

# --- Export to CSV ---
output_filename = "jathin_markt_sales_data.csv"
current_directory = os.getcwd()
print(f"The script is running in this directory: {current_directory}")
df.to_csv(output_filename, index=False)

print(f"--- Data Generation Complete ---")
print(f"Generated {NUM_ROWS} rows of data.")
print(f"Data saved to {output_filename}")
print("\nFirst 5 rows of the generated DataFrame:")
print(df.head())
print("\nDataFrame Info:")
df.info()
print("\nColumn Counts (should be 32):")
print(len(df.columns))