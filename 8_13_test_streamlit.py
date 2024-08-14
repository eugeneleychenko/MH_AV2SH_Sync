import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Custom StreamHandler to capture log messages
class StreamlitHandler(logging.Handler):
    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder

    def emit(self, record):
        log_entry = self.format(record)
        self.placeholder.empty()
        self.placeholder.text(log_entry)

class AtVenuDataFetcher:
    def __init__(self, api_key):
        self.endpoint = "https://api.atvenu.com/"
        self.headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }

    def execute_query(self, query, variables=None):
        logger.info(f"Executing query with variables: {variables}")
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers
        )
        if response.status_code == 200:
            result = response.json()
            if 'errors' in result:
                logger.error(f"GraphQL query returned errors: {result['errors']}")
                raise Exception(f"GraphQL query failed: {result['errors']}")
            return result
        else:
            logger.error(f"Query failed with status code: {response.status_code}. Response: {response.text}")
            raise Exception(f"Query failed with status code: {response.status_code}. Response: {response.text}")

    def fetch_accounts(self):
        query = """
        query accounts($cursor: String) {
          organization {
            accounts(first: 20, after: $cursor) {
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                uuid
                artistName: name
              }
            }
          }
        }
        """
        accounts = []
        cursor = None
        while True:
            result = self.execute_query(query, {"cursor": cursor})
            accounts_data = result['data']['organization']['accounts']
            accounts.extend(accounts_data['nodes'])
            if not accounts_data['pageInfo']['hasNextPage']:
                break
            cursor = accounts_data['pageInfo']['endCursor']
        return accounts

    def fetch_tours(self, account_uuid):
        query = """
        query tours($accountUuid: UUID!, $cursor: String) {
          account: node(uuid: $accountUuid) {
            ... on Account {
              tours(first: 200, after: $cursor) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                nodes {
                  uuid
                  tourName: name
                }
              }
            }
          }
        }
        """
        tours = []
        cursor = None
        while True:
            result = self.execute_query(query, {"accountUuid": account_uuid, "cursor": cursor})
            tours_data = result['data']['account']['tours']
            tours.extend(tours_data['nodes'])
            if not tours_data['pageInfo']['hasNextPage']:
                break
            cursor = tours_data['pageInfo']['endCursor']
        return tours

    def fetch_shows(self, tour_uuid, start_date, end_date):
        query = """
        query shows($tourUuid: UUID!, $startDate: Date!, $endDate: Date!, $cursor: String) {
          tour: node(uuid: $tourUuid) {
            ... on Tour {
              shows(first: 200, after: $cursor, showsOverlap: {start: $startDate, end: $endDate}) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                nodes {
                  uuid
                  showDate
                  showEndDate
                  state
                  attendance
                  capacity
                  currencyFormat {
                    code
                  }
                  location {
                    capacity
                    city
                    stateProvince
                    country
                  }
                }
              }
            }
          }
        }
        """
        shows = []
        cursor = None
        while True:
            result = self.execute_query(query, {
                "tourUuid": tour_uuid,
                "startDate": start_date,
                "endDate": end_date,
                "cursor": cursor
            })
            shows_data = result['data']['tour']['shows']
            shows.extend(shows_data['nodes'])
            if not shows_data['pageInfo']['hasNextPage']:
                break
            cursor = shows_data['pageInfo']['endCursor']
        return shows

    def fetch_shows_in_date_range(self, start_date, end_date):
        logger.info(f"Fetching shows between {start_date} and {end_date}")
        all_shows = []
        
        accounts = self.fetch_accounts()
        for account in accounts:
            logger.info(f"Fetching tours for account: {account['artistName']}")
            tours = self.fetch_tours(account['uuid'])
            for tour in tours:
                logger.info(f"Fetching shows for tour: {tour['tourName']}")
                shows = self.fetch_shows(tour['uuid'], start_date, end_date)
                for show in shows:
                    show['account'] = account
                    show['tour'] = tour
                    all_shows.append(show)

        logger.info(f"Fetched {len(all_shows)} shows in total")
        return all_shows

    def fetch_merchandise(self, account_uuid):
        logger.info(f"Fetching merchandise for account {account_uuid}")
        query = """
        query merchandise($uuid: UUID!, $cursor: String) {
          account: node(uuid: $uuid) {
            ... on Account {
              uuid
              merchItems(first: 200, after: $cursor) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                nodes {
                  name
                  category
                  uuid
                  productType {
                    name
                  }
                  merchVariants {
                    sku
                    size
                    uuid
                    price
                  }
                }
              }
            }
          }
        }
        """
        merch_items = []
        cursor = None
        while True:
            result = self.execute_query(query, {"uuid": account_uuid, "cursor": cursor})
            merch_items.extend(result['data']['account']['merchItems']['nodes'])
            if not result['data']['account']['merchItems']['pageInfo']['hasNextPage']:
                break
            cursor = result['data']['account']['merchItems']['pageInfo']['endCursor']
        logger.info(f"Fetched {len(merch_items)} merchandise items for account {account_uuid}")
        return merch_items

    def fetch_counts(self, show_uuid):
        logger.info(f"Fetching counts for show {show_uuid}")
        query = """
        query counts($uuid: UUID!, $cursor: String) {
          show: node(uuid: $uuid) {
            ... on Show {
              settlements {
                path
                mainCounts(first: 100, after: $cursor) {
                  pageInfo {
                    hasNextPage
                    endCursor
                  }
                  nodes {
                    merchVariantUuid
                    priceOverride
                    countIn
                    countOut
                    comps
                    merchAdds {
                      quantity
                    }
                  }
                }
              }
            }
          }
        }
        """
        counts = []
        cursor = None
        while True:
            result = self.execute_query(query, {"uuid": show_uuid, "cursor": cursor})
            counts.extend(result['data']['show']['settlements'][0]['mainCounts']['nodes'])
            if not result['data']['show']['settlements'][0]['mainCounts']['pageInfo']['hasNextPage']:
                break
            cursor = result['data']['show']['settlements'][0]['mainCounts']['pageInfo']['endCursor']
        logger.info(f"Fetched {len(counts)} counts for show {show_uuid}")
        return counts

    def calculate_sold(self, count):
        logger.debug(f"Calculating sold for count: {count}")
        count_in = count.get('countIn', 0) or 0
        count_out = count.get('countOut', 0) or 0
        comps = count.get('comps', 0) or 0
        adds = sum((add.get('quantity', 0) or 0) for add in count.get('merchAdds', []))
        sold = count_in + adds - count_out - comps
        logger.debug(f"Calculated sold: {sold}")
        return sold

    def fetch_all_data(self, start_date, end_date):
        logger.info(f"Fetching all data between {start_date} and {end_date}")
        all_data = []
        shows = self.fetch_shows_in_date_range(start_date, end_date)
        
        # Create a dictionary to cache merchandise data for each account
        merchandise_cache = {}
        
        for show in shows:
            logger.info(f"Processing show: {show['uuid']} on {show['showDate']}")
            
            account_uuid = show['account']['uuid']
            if account_uuid not in merchandise_cache:
                merchandise_cache[account_uuid] = self.fetch_merchandise(account_uuid)
            
            merchandise = merchandise_cache[account_uuid]
            counts = self.fetch_counts(show['uuid'])
            
            for count in counts:
                merch_item = next((item for item in merchandise for variant in item['merchVariants'] if variant['uuid'] == count['merchVariantUuid']), None)
                if merch_item:
                    variant = next((v for v in merch_item['merchVariants'] if v['uuid'] == count['merchVariantUuid']), None)
                    
                    sold = self.calculate_sold(count)
                    
                    all_data.append({
                        'Band': show['account']['artistName'],
                        'Tour Name': show['tour']['tourName'],
                        'Venue': f"{show['location']['city']}, {show['location']['stateProvince']}, {show['location']['country']}",
                        'Product Name': merch_item['name'],
                        'Size': variant['size'] if variant else 'N/A',
                        'SKU': variant['sku'] if variant else 'N/A',
                        'In': count.get('countIn', 0) or 0,
                        'Out': count.get('countOut', 0) or 0,
                        'Sold': sold,
                        'Show Date': show['showDate'],
                        'Price': count['priceOverride'] or (variant['price'] if variant else 'N/A')
                    })
        
        logger.info(f"Fetched {len(all_data)} total data points")
        return all_data

def fetch_data(start_date, end_date):
    api_key = os.getenv("API_TOKEN")
    fetcher = AtVenuDataFetcher(api_key)
    return fetcher.fetch_all_data(start_date, end_date)

def main():
    st.set_page_config(page_title="AtVenu to ShipHero Nightly Settlement Sync")
    st.title("AtVenu to ShipHero Nightly Settlement Sync")

    # Initialize session state
    if 'data' not in st.session_state:
        st.session_state.data = None

    # Sidebar date range selector
    st.sidebar.header("Date Range")
    start_date = st.sidebar.date_input("Start Date", datetime.now().date())
    end_date = st.sidebar.date_input("End Date", datetime.now().date())

    if start_date > end_date:
        st.sidebar.error("Error: End date must be after start date.")
        return

    # Move the Fetch Data button to the sidebar
    fetch_button = st.sidebar.button("Fetch Data")

    # Create a placeholder for log messages
    log_placeholder = st.empty()

    # Set up the StreamlitHandler
    handler = StreamlitHandler(log_placeholder)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if fetch_button:
        start_time = time.time()
        with st.spinner("Fetching data..."):
            # Use st.cache to memoize the fetch_data function
            @st.cache_data
            def cached_fetch_data(start, end):
                return fetch_data(start, end)
            
            st.session_state.data = cached_fetch_data(start_date.isoformat(), end_date.isoformat())
        end_time = time.time()
        duration = end_time - start_time
        st.success(f"Data fetched successfully in {duration:.2f} seconds!")

    if st.session_state.data is not None:
        # Process the data
        df = pd.DataFrame(st.session_state.data)
        df['Location'] = df['Band'] + " - " + df['Tour Name']
        df['Quantity'] = -df['Sold']
        df['Reason'] = 'Nightly Sales'
        df = df[['SKU', 'Quantity', 'Location', 'Reason']]
        df.insert(1, 'Action', 'Replace')

        # Get unique bands for checkboxes
        bands = df['Location'].str.split(' - ').str[0].unique()

        # Create checkboxes for each band
        st.sidebar.header("Filter Bands")
        selected_bands = [band for band in bands if st.sidebar.checkbox(band, value=True, key=f"checkbox_{band}")]

        # Filter the dataframe based on selected bands
        filtered_df = df[df['Location'].str.split(' - ').str[0].isin(selected_bands)]

        # Display the filtered dataframe
        st.dataframe(filtered_df)

        # Option to download the filtered data as CSV
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name="atvenu_data.csv",
            mime="text/csv",
        )

if __name__ == "__main__":
    main()