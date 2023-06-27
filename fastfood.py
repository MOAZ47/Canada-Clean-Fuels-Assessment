import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_database(csv_file, db_file):
    """
    Create a SQLite database from a CSV file.

    Args:
        csv_file (str): Path to the CSV file.
        db_file (str): Path to the SQLite database file.

    Returns:
        None
    """
    try:
        data = pd.read_csv(csv_file)
        conn = sqlite3.connect(db_file)
        data.to_sql('fastfood', conn, if_exists='replace', index=False)
        conn.close()
        logging.info('Database created successfully.')
    except Exception as e:
        logging.error('Error creating database: {}'.format(str(e)))

def read_data_from_database(db_file):
    """
    Read data from a SQLite database.

    Args:
        db_file (str): Path to the SQLite database file.

    Returns:
        pandas.DataFrame: Data read from the database.
    """
    try:
        conn = sqlite3.connect(db_file)
        data = pd.read_sql_query('SELECT * FROM fastfood', conn)
        conn.close()
        logging.info('Data read from database successfully.')
        return data
    except Exception as e:
        logging.error('Error reading data from database: {}'.format(str(e)))

def calculate_calorie_stats(data):
    """
    Calculate calorie statistics for each restaurant.

    Args:
        data (pandas.DataFrame): Dataframe containing fast food data.

    Returns:
        pandas.DataFrame: Calorie statistics for each restaurant.
    """
    try:
        calorie_stats = data.groupby('restaurant')['calories'].agg(['mean', 'min', 'max']).reset_index()
        logging.info('Calorie statistics calculated successfully.')
        return calorie_stats
    except Exception as e:
        logging.error('Error calculating calorie statistics: {}'.format(str(e)))

def rank_restaurants_by_carbs(data):
    """
    Rank restaurants based on average carbs.

    Args:
        data (pandas.DataFrame): Dataframe containing fast food data.

    Returns:
        pandas.DataFrame: Top ranked restaurants based on average carbs.
    """
    try:
        carb_stats = data.groupby('restaurant')['total_carb'].mean().sort_values().reset_index()
        ranked_restaurants = carb_stats.head(5)
        logging.info('Restaurants ranked by carbs on average successfully.')
        return ranked_restaurants
    except Exception as e:
        logging.error('Error ranking restaurants by carbs: {}'.format(str(e)))

def plot_top_restaurants_chart(ranked_restaurants):
    """
    Plot a bar chart of the top ranked restaurants.

    Args:
        ranked_restaurants (pandas.DataFrame): Top ranked restaurants data.

    Returns:
        None
    """
    try:
        plt.bar(ranked_restaurants['restaurant'], ranked_restaurants['total_carb'])
        plt.xlabel('Restaurant')
        plt.ylabel('Average Carbs')
        plt.title('Top 5 Restaurants by Average Carbs')
        plt.xticks(rotation=45)
        plt.show()
        logging.info('Chart plotted successfully.')
    except Exception as e:
        logging.error('Error plotting chart: {}'.format(str(e)))

def categorize_food_items(data):
    """
    Categorize food items based on their names.

    Args:
        data (pandas.DataFrame): Dataframe containing fast food data.

    Returns:
        pandas.DataFrame: Categorized food items dataframe.
    """
    try:
        main_keywords = ['burger', 'sandwich', 'pizza']
        side_keywords = ['fries', 'salad']
        dessert_keywords = ['ice cream', 'cake']

        def categorize_item(row):
            item = row['item']

            if any(keyword in item.lower() for keyword in main_keywords):
                return 'Main'
            elif any(keyword in item.lower() for keyword in side_keywords):
                return 'Side'
            elif any(keyword in item.lower() for keyword in dessert_keywords):
                return 'Dessert'
            else:
                return 'Other'

        data['category'] = data.apply(categorize_item, axis=1)
        logging.info('Food items categorized successfully.')
        return data
    except Exception as e:
        logging.error('Error categorizing food items: {}'.format(str(e)))

def add_subcategory(data):
    """
    Add subcategories to main food items.

    Args:
        data (pandas.DataFrame): Dataframe containing categorized food items.

    Returns:
        pandas.DataFrame: Dataframe with added subcategories.
    """
    try:
        def get_subcategory(row):
            category = row['category']
            item = row['item']
            subcategories = []

            if category == 'Main':
                if 'chicken' in item.lower():
                    subcategories.append('Chicken')
                if 'beef' in item.lower():
                    subcategories.append('Beef')
                if 'seafood' in item.lower():
                    subcategories.append('Seafood')
                if 'pork' in item.lower():
                    subcategories.append('Pork')
                if all(subcategory not in item.lower() for subcategory in subcategories):
                    subcategories.append('Other')

            return ','.join(subcategories)

        data['sub_category'] = data.apply(get_subcategory, axis=1)
        logging.info('Subcategories added for main food items.')
        return data
    except Exception as e:
        logging.error('Error adding subcategories: {}'.format(str(e)))

def export_categorized_data_to_csv(categorized_data, csv_file):
    """
    Export categorized data to a CSV file.

    Args:
        categorized_data (pandas.DataFrame): Categorized food items dataframe.
        csv_file (str): Path to the output CSV file.

    Returns:
        None
    """
    try:
        categorized_data.to_csv(csv_file, index=False)
        logging.info('Categorized data exported to CSV successfully.')
    except Exception as e:
        logging.error('Error exporting categorized data to CSV: {}'.format(str(e)))

def main():
    csv_file = 'fastfood.csv'
    db_file = 'fastfood.db'
    output_csv_file = 'food_cats.csv'

    create_database(csv_file, db_file)
    data = read_data_from_database(db_file)
    calorie_stats = calculate_calorie_stats(data)
    ranked_restaurants = rank_restaurants_by_carbs(data)
    plot_top_restaurants_chart(ranked_restaurants)
    categorized_data = categorize_food_items(data)
    categorized_data = add_subcategory(categorized_data)
    export_categorized_data_to_csv(categorized_data, output_csv_file)

if __name__ == '__main__':
    main()
